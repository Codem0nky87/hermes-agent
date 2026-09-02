"""
Transport-agnostic WhatsApp behavior shared by the Baileys bridge adapter
and the official WhatsApp Cloud API adapter.

The mixin provides:
- Allow-list / DM / group gating
- Mention detection (explicit @-mentions + configurable regex patterns)
- Quoted-reply-to-bot detection
- Broadcast / Channel / Newsletter filtering
- WhatsApp-flavored markdown conversion
- Outgoing chunk length budgeting

It is the *behavior layer*. Transport-specific concerns (subprocess management,
HTTP webhooks, Graph API calls, media upload protocols) live in each adapter.

Mixin contract — the adapter must set these on ``self`` before any of the
mixin's methods are called (typically in ``__init__``):

    self.config        # gateway.config.PlatformConfig
    self.name          # str — adapter name (used in log lines)
    self._dm_policy             # str: "open" | "allowlist" | "disabled"
    self._allow_from            # set[str]
    self._group_policy          # str: "open" | "allowlist" | "disabled"
    self._group_allow_from      # set[str]
    self._mention_patterns      # list[re.Pattern]
    self._reply_prefix          # Optional[str]

Class attributes ``MAX_MESSAGE_LENGTH`` and ``DEFAULT_REPLY_PREFIX`` are
defined on the mixin and may be overridden per-adapter if needed.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import secrets
import stat
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from agent.secret_scope import UnscopedSecretError as _UnscopedSecretError
from agent.secret_scope import get_secret as _scoped_get_secret


def _get_wsecret(name, default=None):
    """Scope-aware WHATSAPP_* read with the default-profile startup fallback.

    Secondary profiles run under ``_profile_runtime_scope`` -- the scope is
    authoritative and a scoped miss returns ``default`` (no cross-profile
    borrow). The DEFAULT profile's adapter constructs and sends *unscoped*
    under multiplexing, where a bare ``get_secret`` would raise
    ``UnscopedSecretError`` and crash its WhatsApp path; there ``os.environ``
    is that profile's own value, so fall back to it. Same pattern as the
    Slack ``SLACK_APP_TOKEN`` read (#59739).
    """
    try:
        val = _scoped_get_secret(name, default)
    except _UnscopedSecretError:
        val = os.getenv(name)
    return val if val is not None else default

logger = logging.getLogger(__name__)


class WhatsAppBehaviorMixin:
    """Shared behavior for all WhatsApp adapters (Baileys + Cloud API).

    See module docstring for the attribute contract the host adapter must
    satisfy. This mixin owns no state of its own — every value it touches
    is either a class attribute or set by the adapter's ``__init__``.
    """

    # WhatsApp message limits — practical UX limit, not protocol max.
    # WhatsApp allows ~65K but long messages are unreadable on mobile.
    MAX_MESSAGE_LENGTH: int = 4096
    supports_code_blocks = True  # WhatsApp renders fenced code blocks (monospace)

    DEFAULT_REPLY_PREFIX: str = "⚕ *Hermes Agent*\n────────────\n"

    _OUTBOUND_INVISIBLE_CHARS_RE = re.compile(r"[\u200b\u2060\u2063\ufeff]")
    _OUTBOUND_ODD_SPACE_RE = re.compile(r"[\u00a0\u1680\u180e\u2000-\u200a\u202f\u205f\u3000]")

    @classmethod
    def _sanitize_outbound_text(cls, content: str) -> str:
        """Remove invisible formatting chars that leak badly in WhatsApp.

        Some provider/gateway formatting paths can emit unicode like WORD
        JOINER (U+2060) plus NARROW NO-BREAK SPACE (U+202F). WhatsApp may
        render those as mojibake-looking prefixes (``⁠ text``) instead of
        invisible spacing. Keep normal text and emoji joiners intact, but
        strip known zero-width format chars and normalize odd unicode spaces.
        """
        if not content:
            return content
        content = cls._OUTBOUND_INVISIBLE_CHARS_RE.sub("", content)
        return cls._OUTBOUND_ODD_SPACE_RE.sub(" ", content)

    @property
    def enforces_own_access_policy(self) -> bool:
        """WhatsApp gates DM/group access at intake via dm_policy/group_policy."""
        return True

    # ------------------------------------------------------------------ config
    def _effective_reply_prefix(self) -> str:
        """Return the prefix to add to outgoing replies in self-chat mode.

        Subclasses that don't have a self-chat concept (the Cloud API
        adapter) can override this to always return ``""`` or apply a
        different policy.
        """
        whatsapp_mode = _get_wsecret("WHATSAPP_MODE", default="self-chat") or "self-chat"
        if whatsapp_mode != "self-chat":
            return ""
        if self._reply_prefix is not None:
            return self._reply_prefix.replace("\\n", "\n")
        env_prefix = _get_wsecret("WHATSAPP_REPLY_PREFIX")
        if env_prefix is not None:
            return env_prefix.replace("\\n", "\n")
        return self.DEFAULT_REPLY_PREFIX

    def _outgoing_chunk_limit(self) -> int:
        """Reserve room for the reply prefix so the final message fits."""
        prefix_len = len(self._effective_reply_prefix())
        # Keep enough space for truncate_message's pagination indicator and
        # code-fence repair even if a user configures a very long prefix.
        return max(1024, self.MAX_MESSAGE_LENGTH - prefix_len)

    def _whatsapp_require_mention(self) -> bool:
        configured = self.config.extra.get("require_mention")
        if configured is not None:
            if isinstance(configured, str):
                return configured.lower() in {"true", "1", "yes", "on"}
            return bool(configured)
        return (_get_wsecret("WHATSAPP_REQUIRE_MENTION", default="false") or "false").lower() in {
            "true",
            "1",
            "yes",
            "on",
        }

    def _whatsapp_free_response_chats(self) -> set[str]:
        raw = self.config.extra.get("free_response_chats")
        if raw is None:
            raw = _get_wsecret("WHATSAPP_FREE_RESPONSE_CHATS", default="") or ""
        if isinstance(raw, list):
            return {str(part).strip() for part in raw if str(part).strip()}
        return {part.strip() for part in str(raw).split(",") if part.strip()}

    @staticmethod
    def _coerce_allow_list(raw) -> set[str]:
        """Parse allow_from / group_allow_from from config or env var."""
        if raw is None:
            return set()
        if isinstance(raw, list):
            return {str(part).strip() for part in raw if str(part).strip()}
        return {part.strip() for part in str(raw).split(",") if part.strip()}

    def _live_dm_allow_from(self) -> set[str]:
        """Allowlist currently enforced for DM intake / strict DM auth.

        Source precedence matches construction: explicit config wins over any
        env carrier. When the adapter was seeded from an env var, re-read that
        same key so pairing approve/revoke takes effect without restart
        (including an empty value while the key is still present). When the key
        is absent — sole-entry revoke calls ``remove_env_value`` — treat the
        allowlist as empty instead of falling back to the construction-time
        snapshot. Config-seeded adapters keep the in-memory snapshot, which
        pairing revoke purges in place — a lower-precedence or stale env value
        must not broaden access.
        """
        source = getattr(self, "_dm_allowlist_source", None)
        if isinstance(source, str) and source != "config":
            if source in os.environ:
                return self._coerce_allow_list(os.environ.get(source, ""))
            # Key removed (e.g. sole-entry pairing revoke) — do not revive the
            # stale construction snapshot.
            return set()
        return set(self._allow_from or ())

    # ------------------------------------------------------------------ JID helpers
    @staticmethod
    def _normalize_whatsapp_id(value: Optional[str]) -> str:
        if not value:
            return ""
        normalized = str(value).strip()
        if ":" in normalized and "@" in normalized:
            normalized = normalized.replace(":", "@", 1)
        return normalized

    @staticmethod
    def _is_broadcast_chat(chat_id: str) -> bool:
        """True for WhatsApp pseudo-chats that aren't real conversations.

        Covers Status updates (Stories) and Channel/Newsletter broadcasts.
        These show up as inbound messages on Baileys but the agent should
        never reply — answering a Story update spams the contact's status
        feed, and Channel posts aren't addressable in the first place.
        """
        if not chat_id:
            return False
        cid = chat_id.strip().lower()
        if cid == "status@broadcast":
            return True
        # @broadcast suffix covers status@broadcast plus any future
        # broadcast-list variants. @newsletter is the Channel JID suffix.
        if cid.endswith("@broadcast") or cid.endswith("@newsletter"):
            return True
        return False

    # ------------------------------------------------------------------ gating
    def _open_dm_opted_in(self) -> bool:
        if os.getenv("GATEWAY_ALLOW_ALL_USERS", "").lower() in {"true", "1", "yes"}:
            return True
        return (_get_wsecret("WHATSAPP_ALLOW_ALL_USERS", default="") or "").lower() in {"true", "1", "yes"}

    @staticmethod
    def _matches_whatsapp_allowlist(candidate: str, allow_from) -> bool:
        """Match a WhatsApp identifier against an allowlist across phone/LID forms.

        WhatsApp delivers inbound senders in LID form (``<id>@lid``) while
        operators usually configure allowlists with phone numbers, and vice
        versa. A raw set-membership check therefore never matches a known
        contact. Resolve both the candidate and each allowlist entry through
        the bridge's ``lid-mapping-*.json`` files (the shared
        ``gateway.whatsapp_identity`` helper that the gateway authz and
        session-key paths already use) so either configured form resolves to
        the inbound form.
        """
        if not allow_from:
            return False
        # Fast path: exact match against the raw configured value (e.g. a full
        # ``@g.us`` group JID or an entry that already matches verbatim).
        if candidate in allow_from:
            return True

        from gateway.whatsapp_identity import (
            expand_whatsapp_aliases,
            normalize_whatsapp_identifier,
        )

        candidate_aliases = expand_whatsapp_aliases(candidate)
        if not candidate_aliases:
            return False
        for entry in allow_from:
            if entry == "*":
                return True
            if normalize_whatsapp_identifier(entry) in candidate_aliases:
                return True
            # Entry may itself be an unmapped form; expand it too so a phone
            # allowlist entry resolves when the inbound sender arrived as a LID.
            if expand_whatsapp_aliases(entry) & candidate_aliases:
                return True
        return False

    def _is_dm_allowed(self, sender_id: str) -> bool:
        """Strict DM authorization — pairing does not imply access."""
        if self._dm_policy == "disabled":
            return False
        if self._dm_policy == "allowlist":
            return self._matches_whatsapp_allowlist(sender_id, self._live_dm_allow_from())
        if self._dm_policy == "open":
            return self._open_dm_opted_in()
        return False

    def _is_dm_intake_allowed(self, sender_id: str) -> bool:
        """Whether a DM may reach the gateway intake (pairing handshake path)."""
        principal = str(sender_id or "").strip()
        if not principal:
            return False
        if self._dm_policy == "disabled":
            return False
        if self._dm_policy == "allowlist":
            return self._matches_whatsapp_allowlist(principal, self._live_dm_allow_from())
        if self._dm_policy == "pairing":
            return True
        if self._dm_policy == "open":
            return self._open_dm_opted_in()
        return False

    def _is_group_allowed(self, chat_id: str) -> bool:
        """Check whether a group chat should be processed."""
        if self._group_policy == "disabled":
            return False
        if self._group_policy == "allowlist":
            return self._matches_whatsapp_allowlist(chat_id, self._group_allow_from)
        if self._group_policy == "pairing":
            return False
        if self._group_policy == "open":
            return True
        return False

    def _compile_mention_patterns(self):
        patterns = self.config.extra.get("mention_patterns")
        if patterns is None:
            raw = (_get_wsecret("WHATSAPP_MENTION_PATTERNS", default="") or "").strip()
            if raw:
                try:
                    patterns = json.loads(raw)
                except Exception:
                    patterns = [
                        part.strip() for part in raw.splitlines() if part.strip()
                    ]
                    if not patterns:
                        patterns = [
                            part.strip() for part in raw.split(",") if part.strip()
                        ]
        if patterns is None:
            return []
        if isinstance(patterns, str):
            patterns = [patterns]
        if not isinstance(patterns, list):
            logger.warning(
                "[%s] whatsapp mention_patterns must be a list or string; got %s",
                self.name,
                type(patterns).__name__,
            )
            return []

        compiled = []
        for pattern in patterns:
            if not isinstance(pattern, str) or not pattern.strip():
                continue
            try:
                compiled.append(re.compile(pattern, re.IGNORECASE))
            except re.error as exc:
                logger.warning(
                    "[%s] Invalid WhatsApp mention pattern %r: %s",
                    self.name,
                    pattern,
                    exc,
                )
        if compiled:
            logger.info(
                "[%s] Loaded %d WhatsApp mention pattern(s)", self.name, len(compiled)
            )
        return compiled

    def _bot_ids_from_message(self, data: Dict[str, Any]) -> set[str]:
        bot_ids = set()
        for candidate in data.get("botIds") or []:
            normalized = self._normalize_whatsapp_id(candidate)
            if normalized:
                bot_ids.add(normalized)
        return bot_ids

    def _message_is_reply_to_bot(self, data: Dict[str, Any]) -> bool:
        quoted_participant = self._normalize_whatsapp_id(data.get("quotedParticipant"))
        if not quoted_participant:
            return False
        return quoted_participant in self._bot_ids_from_message(data)

    def _message_mentions_bot(self, data: Dict[str, Any]) -> bool:
        bot_ids = self._bot_ids_from_message(data)
        if not bot_ids:
            return False
        mentioned_ids = {
            nid
            for candidate in (data.get("mentionedIds") or [])
            if (nid := self._normalize_whatsapp_id(candidate))
        }
        if mentioned_ids & bot_ids:
            return True

        body = str(data.get("body") or "")
        lower_body = body.lower()
        for bot_id in bot_ids:
            bare_id = bot_id.split("@", 1)[0].lower()
            if bare_id and (f"@{bare_id}" in lower_body or bare_id in lower_body):
                return True
        return False

    def _message_matches_mention_patterns(self, data: Dict[str, Any]) -> bool:
        if not self._mention_patterns:
            return False
        body = str(data.get("body") or "")
        return any(pattern.search(body) for pattern in self._mention_patterns)

    def _clean_bot_mention_text(self, text: str, data: Dict[str, Any]) -> str:
        if not text:
            return text
        bot_ids = self._bot_ids_from_message(data)
        cleaned = text
        for bot_id in bot_ids:
            bare_id = bot_id.split("@", 1)[0]
            if bare_id:
                cleaned = re.sub(
                    rf"@{re.escape(bare_id)}\b[,:\-]*\s*", "", cleaned
                )
        return cleaned.strip() or text

    def _should_process_message(self, data: Dict[str, Any]) -> bool:
        chat_id_raw = str(data.get("chatId") or "")
        # WhatsApp uses pseudo-chats for Status updates (Stories) and
        # Channel/Newsletter broadcasts. These are not real conversations
        # and the agent should never reply to them — even in self-chat mode
        # where the bridge may surface them as "fromMe" events.
        if self._is_broadcast_chat(chat_id_raw):
            return False
        is_group = data.get("isGroup", False)
        if is_group:
            chat_id = chat_id_raw
            if not self._is_group_allowed(chat_id):
                return False
        else:
            sender_id = str(data.get("senderId") or data.get("from") or "")
            if not self._is_dm_intake_allowed(sender_id):
                return False
            # DMs that pass the policy gate are always processed
            return True
        # Group messages: check mention / free-response settings
        chat_id = str(data.get("chatId") or "")
        if chat_id in self._whatsapp_free_response_chats():
            return True
        if not self._whatsapp_require_mention():
            return True
        body = str(data.get("body") or "").strip()
        if body.startswith("/"):
            return True
        if self._message_is_reply_to_bot(data):
            return True
        if self._message_mentions_bot(data):
            return True
        return self._message_matches_mention_patterns(data)

    # ------------------------------------------------------------------ formatting
    def format_message(self, content: str) -> str:
        """Convert standard markdown to WhatsApp-compatible formatting.

        WhatsApp supports: *bold*, _italic_, ~strikethrough~, ```code```,
        and monospaced `inline`. Standard markdown uses different syntax
        for bold/italic/strikethrough, so we convert here.

        Code blocks (``` fenced) and inline code (`) are protected from
        conversion via placeholder substitution.
        """
        if not content:
            return content

        content = self._sanitize_outbound_text(content)

        # --- 1. Protect fenced code blocks from formatting changes ---
        _FENCE_PH = "\x00FENCE"
        fences: list[str] = []

        def _save_fence(m: re.Match) -> str:
            fences.append(m.group(0))
            return f"{_FENCE_PH}{len(fences) - 1}\x00"

        result = re.sub(r"```[\s\S]*?```", _save_fence, content)

        # --- 2. Protect inline code ---
        _CODE_PH = "\x00CODE"
        codes: list[str] = []

        def _save_code(m: re.Match) -> str:
            codes.append(m.group(0))
            return f"{_CODE_PH}{len(codes) - 1}\x00"

        result = re.sub(r"`[^`\n]+`", _save_code, result)

        # --- 3. Convert markdown formatting to WhatsApp syntax ---
        # Italic: standard Markdown *text* → WhatsApp _text_.  Do this before
        # bold conversion so **bold** does not become italic by accident.  The
        # lookarounds avoid list bullets and bold delimiters.
        result = re.sub(
            r"(?<!\*)\*(?!\s|\*)([^*\n]*?\S[^*\n]*?)\*(?!\*)",
            r"_\1_",
            result,
        )
        # Bold: **text** or __text__ → *text*
        result = re.sub(r"\*\*(.+?)\*\*", r"*\1*", result)
        result = re.sub(r"__(.+?)__", r"*\1*", result)
        # Strikethrough: ~~text~~ → ~text~
        result = re.sub(r"~~(.+?)~~", r"~\1~", result)
        # _text_ is already WhatsApp italic — leave as-is

        # --- 4. Convert markdown headers to bold text ---
        # # Header → *Header*. Strip any *...* wrapping already produced
        # by step 3 (e.g. "# **Title**" → "*Title*", not "**Title**",
        # which WhatsApp renders with literal asterisks).
        def _header_to_bold(m: re.Match) -> str:
            inner = m.group(1).strip()
            while len(inner) > 1 and inner.startswith("*") and inner.endswith("*"):
                inner = inner[1:-1].strip()
            return f"*{inner}*"

        result = re.sub(
            r"^#{1,6}\s+(.+)$", _header_to_bold, result, flags=re.MULTILINE
        )

        # --- 5. Convert markdown links: [text](url) → text (url) ---
        result = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", result)

        # --- 6. Restore protected sections ---
        for i, fence in enumerate(fences):
            result = result.replace(f"{_FENCE_PH}{i}\x00", fence)
        for i, code in enumerate(codes):
            result = result.replace(f"{_CODE_PH}{i}\x00", code)

        return result


# ---------------------------------------------------------------------------
# Revoked-session marker — shared by the adapter, the CLI and the dashboard
# ---------------------------------------------------------------------------

#: Filename the Node bridge writes inside the session directory when WhatsApp
#: durably revokes the device.
#:
#: DOCUMENTED CONTRACT with scripts/whatsapp-bridge/bridge_helpers.js
#: (``sessionRevokedMarkerPath``).  The bridge writes it atomically, mode
#: 0600, immediately *before* exiting ``BRIDGE_EXIT_LOGGED_OUT``.  It carries
#: no auth material — only a bounded status code, two sanitised reason tags
#: and a timestamp.
#:
#: It lives *inside* the session directory so that re-pairing — which removes
#: that directory wholesale — clears it without a separate cleanup step the
#: control paths could forget.
WHATSAPP_REVOKED_MARKER_NAME = "revoked.json"


def is_whatsapp_session_revoked(session_dir: Path) -> bool:
    """Return whether the revocation marker directory entry is present.

    Marker contents are deliberately irrelevant: the Node bridge writes the
    entry only for a terminal revocation, and reset removes the whole session.
    ``lstat`` observes a symlink, FIFO, device, directory, or invalid-byte file
    without opening it, so hostile local state cannot block this preflight.
    Only a genuine ``FileNotFoundError`` means there is no marker; every other
    result or inspection failure remains terminal and fails closed.
    """
    marker = Path(session_dir) / WHATSAPP_REVOKED_MARKER_NAME
    try:
        marker.lstat()
    except FileNotFoundError:
        return False
    except OSError:
        return True
    return True


class WhatsAppSessionStateError(RuntimeError):
    """Local WhatsApp session state is missing, hostile, or unsafely owned.

    Carries a fixed message only: the underlying ``OSError`` stringifies to an
    absolute path that discloses the operator's home directory and username.
    """


def ensure_whatsapp_session_dir(session_dir: Path) -> Path:
    """Create/verify the paired-session directory as owner-only, exactly 0700.

    The directory holds Baileys credentials, so group/other access is never
    acceptable. ``mkdir(parents=True)`` alone creates at ``0777 & ~umask``
    (normally ``0755``), and ``exist_ok=True`` silently accepts a directory an
    earlier run — or another user — left readable, so both the creation mode
    and the resulting mode are enforced here.

    Errors are fixed strings: an ``OSError`` stringifies to a path that
    discloses the operator's home directory and username.
    """
    session_dir = Path(session_dir)
    try:
        session_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    except OSError as exc:
        raise WhatsAppSessionStateError(
            "The WhatsApp session directory could not be created."
        ) from exc

    try:
        entry_stat = os.lstat(session_dir)
    except OSError as exc:
        raise WhatsAppSessionStateError(
            "The WhatsApp session directory could not be inspected."
        ) from exc

    # Refuse a symlinked or non-directory session path outright.
    if not stat.S_ISDIR(entry_stat.st_mode):
        raise WhatsAppSessionStateError(
            "The WhatsApp session directory is not a directory."
        )

    # Everything from here runs against a no-follow descriptor rather than the
    # path. A path-based os.chmod resolves the name again at call time, so an
    # entry swapped for a symlink after the lstat above would have had its
    # *target* re-permissioned before any check could reject the swap — the
    # rejection would arrive too late to matter.
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    try:
        dir_fd = os.open(session_dir, directory_flags)
    except OSError as exc:
        raise WhatsAppSessionStateError(
            "The WhatsApp session directory could not be opened safely."
        ) from exc

    try:
        opened_stat = os.fstat(dir_fd)
        if not stat.S_ISDIR(opened_stat.st_mode):
            raise WhatsAppSessionStateError(
                "The WhatsApp session directory is not a directory."
            )
        # The entry inspected must be the entry opened.
        if (opened_stat.st_dev, opened_stat.st_ino) != (
            entry_stat.st_dev,
            entry_stat.st_ino,
        ):
            raise WhatsAppSessionStateError(
                "The WhatsApp session directory changed while it was opened."
            )
        if hasattr(os, "getuid") and opened_stat.st_uid != os.getuid():
            raise WhatsAppSessionStateError(
                "The WhatsApp session directory has the wrong owner."
            )

        # `mode=` is masked by umask on creation and ignored entirely when the
        # directory already existed, so tighten through the descriptor and
        # re-verify through the descriptor.
        if stat.S_IMODE(opened_stat.st_mode) != 0o700:
            try:
                os.fchmod(dir_fd, 0o700)
            except OSError as exc:
                raise WhatsAppSessionStateError(
                    "The WhatsApp session directory permissions could not be set."
                ) from exc
            if stat.S_IMODE(os.fstat(dir_fd).st_mode) != 0o700:
                raise WhatsAppSessionStateError(
                    "The WhatsApp session directory has unsafe permissions."
                )
    finally:
        try:
            os.close(dir_fd)
        except OSError:
            pass
    return session_dir


def reset_whatsapp_session_dir(session_dir: Path) -> None:
    """Fail closed: pathname-only destructive reset is no longer authorised.

    This legacy symbol remains importable so an older caller gets a fixed,
    non-destructive failure rather than an ``ImportError`` or the former
    symlink-following ``shutil.rmtree`` behavior. Destructive recovery must be
    performed by the active lease that captured and locked the canonical
    directory identity::

        lease.reset_session(session_dir)

    See :meth:`gateway.platforms.whatsapp_recovery.WhatsAppRecoveryLease.reset_session`.
    """
    del session_dir
    raise RuntimeError(
        "WhatsApp session reset requires an active WhatsApp recovery lease."
    )


# ---------------------------------------------------------------------------
# Shared bridge directory resolution for CLI and adapter
# ---------------------------------------------------------------------------

_WHATSAPP_BRIDGE_DEPENDENCY_STAMP = ".hermes-pkg-hash"
_WHATSAPP_BRIDGE_DEPENDENCY_INPUTS = ("package.json", "package-lock.json")
WHATSAPP_BRIDGE_RUNTIME_INPUTS = (
    "allowlist.js",
    "baileys_logger.js",
    "bridge.js",
    "bridge_helpers.js",
    "connection_close.js",
    "outbound_ids.js",
    "owner_message_gate.js",
    "package.json",
    "package-lock.json",
)
_WHATSAPP_BRIDGE_MISSING_SENTINEL = b"HERMES-BRIDGE-MISSING-v1"
_MAX_WHATSAPP_BRIDGE_INPUT_BYTES = 4 * 1024 * 1024
_MAX_WHATSAPP_BRIDGE_STAMP_BYTES = 128


class _WhatsAppBridgeInputMissing(Exception):
    """A required bridge input was absent at its initial inspection."""


class _WhatsAppBridgeInputInvalid(Exception):
    """A present bridge input was not one stable, bounded regular file."""


def _bridge_input_stat_fingerprint(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_whatsapp_bridge_input(
    path: Path, *, limit: int = _MAX_WHATSAPP_BRIDGE_INPUT_BYTES
) -> bytes:
    """Read one bounded bridge input without following or blocking on it."""
    path = Path(path)
    try:
        before_open = path.lstat()
    except FileNotFoundError as exc:
        raise _WhatsAppBridgeInputMissing from exc
    except OSError as exc:
        raise _WhatsAppBridgeInputInvalid from exc
    if (
        not stat.S_ISREG(before_open.st_mode)
        or before_open.st_size < 0
        or before_open.st_size > limit
    ):
        raise _WhatsAppBridgeInputInvalid

    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        # ENOENT here is a substitution race, not initial absence.
        raise _WhatsAppBridgeInputInvalid from exc

    payload = bytearray()
    read_error: Optional[BaseException] = None
    try:
        opened = os.fstat(fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or _bridge_input_stat_fingerprint(opened)
            != _bridge_input_stat_fingerprint(before_open)
        ):
            raise _WhatsAppBridgeInputInvalid
        while len(payload) <= limit:
            chunk = os.read(fd, min(64 * 1024, limit + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        after_read = os.fstat(fd)
        if (
            len(payload) > limit
            or _bridge_input_stat_fingerprint(after_read)
            != _bridge_input_stat_fingerprint(opened)
            or len(payload) != after_read.st_size
        ):
            raise _WhatsAppBridgeInputInvalid
    except (_WhatsAppBridgeInputInvalid, OSError) as exc:
        read_error = exc
    try:
        os.close(fd)
    except OSError as exc:
        read_error = read_error or exc
    if read_error is not None:
        raise _WhatsAppBridgeInputInvalid from read_error
    return bytes(payload)


def _whatsapp_bridge_input_fingerprint(
    bridge_dir: Path, filenames: Iterable[str]
) -> tuple[str, bool]:
    """Hash sorted ``filename\0bytes\0`` records and report required presence."""
    bridge_dir = Path(bridge_dir)
    try:
        before_root = bridge_dir.lstat()
    except OSError:
        return "", False
    if not stat.S_ISDIR(before_root.st_mode):
        return "", False

    digest = hashlib.sha256()
    all_present = True
    for filename in sorted(set(filenames)):
        digest.update(filename.encode("utf-8"))
        digest.update(b"\0")
        try:
            payload = _read_whatsapp_bridge_input(bridge_dir / filename)
        except _WhatsAppBridgeInputMissing:
            payload = _WHATSAPP_BRIDGE_MISSING_SENTINEL
            all_present = False
        except _WhatsAppBridgeInputInvalid:
            return "", False
        digest.update(payload)
        digest.update(b"\0")
    try:
        after_root = bridge_dir.lstat()
    except OSError:
        return "", False
    if (
        not stat.S_ISDIR(after_root.st_mode)
        or _bridge_input_stat_fingerprint(after_root)
        != _bridge_input_stat_fingerprint(before_root)
    ):
        return "", False
    return digest.hexdigest(), all_present


def whatsapp_bridge_runtime_hash(bridge_dir: Path) -> str:
    """Return the required composite runtime fingerprint, or ``""``."""
    fingerprint, all_present = _whatsapp_bridge_input_fingerprint(
        bridge_dir, WHATSAPP_BRIDGE_RUNTIME_INPUTS
    )
    return fingerprint if all_present else ""


def whatsapp_bridge_manifest_hash(bridge_dir: Path) -> str:
    """Return the composite package manifest/lock fingerprint, or ``""``."""
    fingerprint, all_present = _whatsapp_bridge_input_fingerprint(
        bridge_dir, _WHATSAPP_BRIDGE_DEPENDENCY_INPUTS
    )
    return fingerprint if all_present else ""


def whatsapp_bridge_dependencies_fresh(bridge_dir: Path) -> bool:
    """Return whether installed dependencies match package.json and its lock."""
    bridge_dir = Path(bridge_dir)
    manifest_hash = whatsapp_bridge_manifest_hash(bridge_dir)
    node_modules = bridge_dir / "node_modules"
    if not manifest_hash:
        return False
    try:
        node_modules_stat = node_modules.lstat()
        if not stat.S_ISDIR(node_modules_stat.st_mode):
            return False
        installed_hash = _read_whatsapp_bridge_input(
            node_modules / _WHATSAPP_BRIDGE_DEPENDENCY_STAMP,
            limit=_MAX_WHATSAPP_BRIDGE_STAMP_BYTES,
        ).decode("ascii").strip()
        node_modules_after = node_modules.lstat()
    except (
        OSError,
        UnicodeError,
        _WhatsAppBridgeInputMissing,
        _WhatsAppBridgeInputInvalid,
    ):
        return False
    if (
        not stat.S_ISDIR(node_modules_after.st_mode)
        or _bridge_input_stat_fingerprint(node_modules_after)
        != _bridge_input_stat_fingerprint(node_modules_stat)
    ):
        return False
    return installed_hash == manifest_hash


def _open_whatsapp_bridge_directory(path: Path) -> tuple[int, os.stat_result]:
    """Open one real directory without following its final path component."""
    try:
        before_open = Path(path).lstat()
        if not stat.S_ISDIR(before_open.st_mode):
            raise OSError
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        fd = os.open(path, flags)
        opened = os.fstat(fd)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_dev != before_open.st_dev
            or opened.st_ino != before_open.st_ino
            or opened.st_mode != before_open.st_mode
        ):
            os.close(fd)
            raise OSError
        return fd, opened
    except OSError as exc:
        raise OSError("WhatsApp bridge directory is unavailable.") from exc


def _whatsapp_bridge_directory_is_writable(path: Path) -> bool:
    """Probe directory writes without following or replacing an existing entry."""
    directory_fd: Optional[int] = None
    probe_fd: Optional[int] = None
    probe_name = f".write-test-{secrets.token_hex(12)}"
    try:
        directory_fd, _ = _open_whatsapp_bridge_directory(path)
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        probe_fd = os.open(probe_name, flags, 0o600, dir_fd=directory_fd)
        os.close(probe_fd)
        probe_fd = None
        os.unlink(probe_name, dir_fd=directory_fd)
        probe_name = ""
        return True
    except OSError:
        return False
    finally:
        if probe_fd is not None:
            try:
                os.close(probe_fd)
            except OSError:
                pass
        if directory_fd is not None:
            if probe_name:
                try:
                    os.unlink(probe_name, dir_fd=directory_fd)
                except OSError:
                    pass
            try:
                os.close(directory_fd)
            except OSError:
                pass


def _atomic_write_whatsapp_bridge_file(
    directory_fd: int, filename: str, payload: bytes
) -> None:
    """Atomically replace one flat allowlisted file through a locked root fd."""
    temporary = f".{filename}.{secrets.token_hex(12)}.tmp"
    output_fd: Optional[int] = None
    try:
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        output_fd = os.open(temporary, flags, 0o600, dir_fd=directory_fd)
        view = memoryview(payload)
        written = 0
        while written < len(view):
            count = os.write(output_fd, view[written:])
            if count <= 0:
                raise OSError
            written += count
        os.fsync(output_fd)
        os.close(output_fd)
        output_fd = None
        os.replace(
            temporary,
            filename,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        temporary = ""
    finally:
        if output_fd is not None:
            try:
                os.close(output_fd)
            except OSError:
                pass
        if temporary:
            try:
                os.unlink(temporary, dir_fd=directory_fd)
            except OSError:
                pass


def write_whatsapp_bridge_dependency_stamp(bridge_dir: Path) -> None:
    """Atomically record the manifest used by the last successful install."""
    bridge_dir = Path(bridge_dir)
    manifest_hash = whatsapp_bridge_manifest_hash(bridge_dir)
    if not manifest_hash:
        raise OSError("WhatsApp bridge manifests are unavailable.")

    node_modules = bridge_dir / "node_modules"
    stamp = node_modules / _WHATSAPP_BRIDGE_DEPENDENCY_STAMP
    try:
        try:
            stamp_state = stamp.lstat()
        except FileNotFoundError:
            stamp_state = None
        if stamp_state is not None and (
            not stat.S_ISREG(stamp_state.st_mode)
            or stamp_state.st_size > _MAX_WHATSAPP_BRIDGE_STAMP_BYTES
        ):
            raise OSError
        directory_fd, opened = _open_whatsapp_bridge_directory(node_modules)
        try:
            _atomic_write_whatsapp_bridge_file(
                directory_fd,
                _WHATSAPP_BRIDGE_DEPENDENCY_STAMP,
                manifest_hash.encode("ascii"),
            )
            os.fsync(directory_fd)
            after_root = node_modules.lstat()
            if (
                not stat.S_ISDIR(after_root.st_mode)
                or after_root.st_dev != opened.st_dev
                or after_root.st_ino != opened.st_ino
            ):
                raise OSError
        finally:
            try:
                os.close(directory_fd)
            except OSError:
                pass
    except OSError as exc:
        raise OSError("WhatsApp bridge dependency stamp is unavailable.") from exc


def _sync_whatsapp_bridge_source(source: Path, destination: Path) -> None:
    """Safely refresh only production inputs, preserving all runtime state."""
    source = Path(source)
    destination = Path(destination)
    sync_error = "WhatsApp bridge source synchronization failed."

    # Capture every source byte before touching the destination. Missing names
    # are represented separately so a stale allowlisted mirror entry can be
    # removed, but the mirror can never be accepted as complete afterward.
    try:
        source_before = source.lstat()
        if not stat.S_ISDIR(source_before.st_mode):
            raise OSError
        payloads: dict[str, Optional[bytes]] = {}
        missing = False
        for filename in WHATSAPP_BRIDGE_RUNTIME_INPUTS:
            try:
                payloads[filename] = _read_whatsapp_bridge_input(source / filename)
            except _WhatsAppBridgeInputMissing:
                payloads[filename] = None
                missing = True
            except _WhatsAppBridgeInputInvalid as exc:
                raise OSError from exc
        source_after = source.lstat()
        if (
            not stat.S_ISDIR(source_after.st_mode)
            or _bridge_input_stat_fingerprint(source_after)
            != _bridge_input_stat_fingerprint(source_before)
        ):
            raise OSError

        try:
            destination_state = destination.lstat()
        except FileNotFoundError:
            destination.parent.mkdir(parents=True, exist_ok=True)
            parent_state = destination.parent.lstat()
            if not stat.S_ISDIR(parent_state.st_mode):
                raise OSError
            destination.mkdir(mode=0o700)
        else:
            if not stat.S_ISDIR(destination_state.st_mode):
                raise OSError

        destination_fd, opened_root = _open_whatsapp_bridge_directory(destination)
        try:
            # Validate every destination production entry before the first
            # replacement/removal. Symlinks, FIFOs, devices, directories,
            # unreadable files, and oversized files all fail closed.
            destination_states: dict[str, Optional[tuple[int, ...]]] = {}
            for filename in WHATSAPP_BRIDGE_RUNTIME_INPUTS:
                destination_path = destination / filename
                try:
                    state = destination_path.lstat()
                except FileNotFoundError:
                    destination_states[filename] = None
                    continue
                if (
                    not stat.S_ISREG(state.st_mode)
                    or state.st_size < 0
                    or state.st_size > _MAX_WHATSAPP_BRIDGE_INPUT_BYTES
                ):
                    raise OSError
                _read_whatsapp_bridge_input(destination_path)
                stable_state = destination_path.lstat()
                if (
                    _bridge_input_stat_fingerprint(stable_state)
                    != _bridge_input_stat_fingerprint(state)
                ):
                    raise OSError
                destination_states[filename] = _bridge_input_stat_fingerprint(
                    stable_state
                )

            root_now = destination.lstat()
            if (
                not stat.S_ISDIR(root_now.st_mode)
                or root_now.st_dev != opened_root.st_dev
                or root_now.st_ino != opened_root.st_ino
            ):
                raise OSError

            for filename in WHATSAPP_BRIDGE_RUNTIME_INPUTS:
                expected = destination_states[filename]
                try:
                    current = os.stat(
                        filename, dir_fd=destination_fd, follow_symlinks=False
                    )
                    current_fingerprint: Optional[tuple[int, ...]] = (
                        _bridge_input_stat_fingerprint(current)
                    )
                except FileNotFoundError:
                    current_fingerprint = None
                if current_fingerprint != expected:
                    raise OSError

                payload = payloads[filename]
                if payload is None:
                    if expected is not None:
                        os.unlink(filename, dir_fd=destination_fd)
                    continue
                _atomic_write_whatsapp_bridge_file(
                    destination_fd, filename, payload
                )

            os.fsync(destination_fd)
            final_root = destination.lstat()
            if (
                not stat.S_ISDIR(final_root.st_mode)
                or final_root.st_dev != opened_root.st_dev
                or final_root.st_ino != opened_root.st_ino
            ):
                raise OSError
        finally:
            os.close(destination_fd)

        if missing:
            raise OSError
    except OSError as exc:
        raise OSError(sync_error) from exc


def resolve_whatsapp_bridge_dir() -> Path:
    """Resolve the WhatsApp bridge directory, mirroring to HERMES_HOME if needed.

    When the install tree is read-only (e.g., Docker /opt/hermes), this function
    mirrors the bridge source to a writable HERMES_HOME location and returns that
    path. This ensures npm install works in Docker environments.

    Returns the resolved bridge directory path.
    """
    from pathlib import Path as _Path

    # Default location in install tree (may be read-only)
    from hermes_constants import get_hermes_home
    install_bridge = _Path(__file__).resolve().parents[2] / "scripts" / "whatsapp-bridge"

    # Try HERMES_HOME location first
    hermes_home = get_hermes_home()
    hermes_home_bridge = hermes_home / "scripts" / "whatsapp-bridge"

    # Check if the exact install directory is writable without following a
    # predictable probe path that may already be a symlink.
    if _whatsapp_bridge_directory_is_writable(install_bridge):
        return install_bridge

    # Install dir is read-only. Refresh every packaged source file in the
    # writable mirror so an existing mirror cannot strand an older release.
    # Runtime-only files and node_modules are deliberately left in place.
    try:
        hermes_home_bridge.parent.mkdir(parents=True, exist_ok=True)
        _sync_whatsapp_bridge_source(install_bridge, hermes_home_bridge)
        return hermes_home_bridge
    except Exception:
        return install_bridge
