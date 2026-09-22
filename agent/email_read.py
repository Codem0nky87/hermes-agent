"""Bounded, allowlisted email-read tool for the §16.4 OTP relay (Wave-4 Task 6).

Fetches verification codes / links from a provider's very recent mail so the
orchestrator can relay them into an interactive auth flow. This tool is
deliberately narrow:

  * It reads ONLY messages newer than a short ``since`` window.
  * It reads ONLY messages whose sender suffix-matches, and whose subject
    regex-matches, the provider's ``code_patterns`` in
    ``config/orchestration/providers.yaml`` (the woodhouse repo).
  * It extracts ONLY 4-8 digit codes and ``https://`` links.
  * It returns AT MOST 3 items and NEVER returns subjects or bodies.

**Review Focus #1 — codes/links never leak into logs.** Module logging records
counts only ("fetched 1 code for codex"); it never logs a code, a link, a
subject, or a body. See ``test_fetch_codes_never_logs_values``.

Transports are injectable so unit tests run against fakes and never touch live
mail. The default transport reads Microsoft Graph exactly the way
``~/.hermes/scripts/scs_graph_mail_monitor.py`` does after the Wave-4 R2
migration: the client secret comes from ``SCS_GRAPH_CLIENT_SECRET`` in the
process environment, with a ``~/.hermes/.env`` self-load fallback (the macOS
Keychain cannot be read non-interactively from a background/cron/SSH session).
"""
from __future__ import annotations

import json
import logging
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# 4-8 digit runs (word-bounded, so "123" and a 12-digit run are ignored) and
# https links. http:// is intentionally excluded — verification links are https.
_CODE_RE = re.compile(r"\b\d{4,8}\b")
_LINK_RE = re.compile(r"https://[^\s\"'<>)\]]+")

# Woodhouse repo path — this tool runs in the hermes tree, so the registry is
# loaded by absolute path (overridable for tests / relocation).
DEFAULT_PROVIDERS_PATH = "/Users/rufus/Projects/woodhouse/config/orchestration/providers.yaml"

_HERMES_HOME = Path.home() / ".hermes"
_GRAPH = "https://graph.microsoft.com/v1.0"

MAX_ITEMS = 3


def extract_codes(text: str) -> List[str]:
    """Extract 4-8 digit codes and https links from ``text``.

    Returns a de-duplicated list preserving first-seen order. Digit runs
    shorter than 4 or longer than 8 are ignored (a phone-number fragment or a
    long id is not a one-time code). No subject/body is returned by callers —
    only the extracted tokens.
    """
    if not text:
        return []
    found: List[str] = []
    seen: set[str] = set()
    for match in _LINK_RE.findall(text):
        token = match.rstrip(".,;")
        if token not in seen:
            seen.add(token)
            found.append(token)
    for match in _CODE_RE.findall(text):
        if match not in seen:
            seen.add(match)
            found.append(match)
    return found


def _sender_matches(from_addr: str, senders: List[str]) -> bool:
    """True if ``from_addr`` suffix-matches any allowlisted sender domain."""
    addr = (from_addr or "").strip().lower()
    if not addr:
        return False
    domain = addr.rsplit("@", 1)[-1]
    for raw in senders or []:
        s = str(raw).strip().lower()
        if not s:
            continue
        if addr == s or addr.endswith("@" + s) or domain == s or domain.endswith("." + s):
            return True
    return False


def _subject_match(subject: str, subject_regexes: List[str]) -> Optional[str]:
    """Return the first subject regex that matches ``subject``, else None."""
    for rx in subject_regexes or []:
        try:
            if re.search(str(rx), subject or "", re.IGNORECASE):
                return str(rx)
        except re.error:
            continue
    return None


# ── Providers registry ──────────────────────────────────────────────────


def _load_providers(path: Optional[str] = None) -> Dict[str, Any]:
    """Load the provider registry from the woodhouse repo path (YAML)."""
    import yaml  # lazy: unit tests inject ``providers=`` and never hit this

    resolved = Path(path or os.environ.get("WOODHOUSE_PROVIDERS_PATH", DEFAULT_PROVIDERS_PATH))
    data = yaml.safe_load(resolved.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"providers registry at {resolved} is not a mapping")
    return data


# ── Default Graph transport (mirrors scs_graph_mail_monitor.py) ───────────


def _load_dotenv(path: Path) -> None:
    """Minimal .env loader — never overrides a var already in the environment.

    Copied from ``scs_graph_mail_monitor.load_dotenv`` (Wave-4 R2): split on
    the first '=', strip whitespace and matching quotes, skip blanks/comments.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


def _post_form(url: str, data: dict) -> dict:
    body = urllib.parse.urlencode(data).encode()
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return json.loads(e.read().decode())


def _graph_get(path: str, token: str, query: Optional[dict] = None) -> Any:
    url = path if path.startswith("http") else _GRAPH + path
    if query:
        url += ("&" if "?" in url else "?") + urllib.parse.urlencode(query)
    req = urllib.request.Request(
        url, method="GET",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        raise RuntimeError(f"Graph GET {url} failed {e.code}: {raw}")


def _graph_access_token() -> tuple[str, str]:
    """Acquire an app-only Graph token, secret from env / ~/.hermes/.env.

    Returns ``(access_token, account)``. Never reads the macOS Keychain —
    background/cron/SSH sessions cannot, hence the Wave-4 R2 env migration.
    """
    config_path = _HERMES_HOME / "mail_monitor_scs_graph" / "config.json"
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    client_id = cfg["client_id"]
    tenant = cfg.get("tenant", "organizations")
    account = cfg.get("account", "rufus@scs-systems.io")
    secret = os.environ.get("SCS_GRAPH_CLIENT_SECRET", "").strip()
    if not secret:
        _load_dotenv(_HERMES_HOME / ".env")
        secret = os.environ.get("SCS_GRAPH_CLIENT_SECRET", "").strip()
    if not secret:
        raise RuntimeError(
            "SCS_GRAPH_CLIENT_SECRET not set (checked process env and ~/.hermes/.env)"
        )
    token = _post_form(
        f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
        {
            "client_id": client_id,
            "client_secret": secret,
            "grant_type": "client_credentials",
            "scope": "https://graph.microsoft.com/.default",
        },
    )
    if token.get("error"):
        # Surface the error class only; never echo the secret or full payload.
        raise RuntimeError(f"Graph client-credentials token failed: {token.get('error')}")
    return token["access_token"], account


def _graph_transport(since: datetime) -> List[Dict[str, Any]]:
    """Default transport: read inbox messages newer than ``since`` via Graph."""
    token, account = _graph_access_token()
    since_iso = since.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    prefix = "/users/" + urllib.parse.quote(account, safe="")
    data = _graph_get(
        f"{prefix}/mailFolders/inbox/messages",
        token,
        query={
            "$filter": f"receivedDateTime ge {since_iso}",
            "$orderby": "receivedDateTime desc",
            "$top": "25",
            "$select": "id,subject,from,receivedDateTime,bodyPreview,body",
        },
    )
    messages: List[Dict[str, Any]] = []
    for m in data.get("value", []):
        ea = ((m.get("from") or {}).get("emailAddress") or {})
        body = ((m.get("body") or {}).get("content")) or m.get("bodyPreview") or ""
        messages.append({
            "received_at": m.get("receivedDateTime", ""),
            "from": ea.get("address", ""),
            "subject": m.get("subject") or "",
            "body": body,
        })
    return messages


def _default_transport(mailbox: str) -> Callable[[datetime], List[Dict[str, Any]]]:
    """Select the built-in transport for ``mailbox`` (Graph is the default)."""
    mb = (mailbox or "auto").strip().lower()
    if mb in {"auto", "graph", "m365", "scs"}:
        return _graph_transport
    raise ValueError(
        f"no built-in transport for mailbox {mailbox!r}; pass transport= explicitly"
    )


# ── Public API ────────────────────────────────────────────────────────────


def fetch_codes(
    provider: str,
    since_minutes: int = 10,
    mailbox: str = "auto",
    *,
    transport: Optional[Callable[[datetime], List[Dict[str, Any]]]] = None,
    providers: Optional[Dict[str, Any]] = None,
    providers_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Fetch recent verification codes/links for ``provider``.

    Returns ``{"ok": bool, "codes": [...], "error": str|None}`` where each item
    is ``{"code_or_link": str, "received_at": iso, "matched": "<pattern>"}``.
    At most 3 items. Subjects and bodies are never returned, and never logged.

    ``transport`` (injected in tests) is a callable ``transport(since) -> list``
    of message dicts ``{"received_at", "from", "subject", "body"}``. The default
    reads Microsoft Graph. ``providers`` overrides the on-disk registry.
    """
    try:
        registry = providers if providers is not None else _load_providers(providers_path)
        cfg = registry.get(provider)
        if not isinstance(cfg, dict):
            raise ValueError(f"unknown provider {provider!r}")
        patterns = cfg.get("code_patterns") or {}
        senders = list(patterns.get("senders") or [])
        subject_regexes = list(patterns.get("subject_regexes") or [])

        fetch = transport if transport is not None else _default_transport(mailbox)
        since = datetime.now(timezone.utc) - timedelta(minutes=max(0, int(since_minutes)))
        messages = fetch(since)

        codes: List[Dict[str, Any]] = []
        for msg in messages or []:
            if len(codes) >= MAX_ITEMS:
                break
            if not _sender_matches(msg.get("from", ""), senders):
                continue
            matched = _subject_match(msg.get("subject", ""), subject_regexes)
            if matched is None:
                continue
            # Extract from subject + body: some providers put the code in the
            # subject line. We return only the extracted tokens, never the text.
            haystack = f"{msg.get('subject', '')}\n{msg.get('body', '')}"
            for token in extract_codes(haystack):
                codes.append({
                    "code_or_link": token,
                    "received_at": msg.get("received_at", ""),
                    "matched": matched,
                })
                if len(codes) >= MAX_ITEMS:
                    break
    except Exception as exc:  # transport / auth / registry failure
        # Log the error CLASS/message only — it never contains a code or body.
        logger.warning("fetch_codes failed for %s: %s", provider, exc.__class__.__name__)
        return {"ok": False, "codes": [], "error": str(exc)}

    # Count-only logging (Review Focus #1): never the values.
    logger.info("fetched %d code(s) for %s", len(codes), provider)
    return {"ok": True, "codes": codes, "error": None}
