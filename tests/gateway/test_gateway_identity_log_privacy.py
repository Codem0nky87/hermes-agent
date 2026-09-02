"""Gateway log-privacy boundary for WhatsApp-reachable records.

Pinned Hermes ``0.20.1`` interpolates raw source identifiers into gateway log
records on three verified WhatsApp-reachable paths (hook skip, unauthorised
sender, empty handler).  ``privacy.redact_pii`` does **not** close this: it
governs LLM session-context rendering, and ``RedactingFormatter`` is a secret
formatter that does not remove bare WhatsApp JIDs.  The boundary therefore has
to be the log call itself — omit the identity rather than hash it.

Every identifier below is synthetic.  The phone-shaped values sit in the
``555-0000`` fictional range, the names/reasons/payloads carry explicit
``SYNTHETIC``/``CANARY``/``FAKE`` markers, and none of them is a real WhatsApp
JID, group, allowlist entry, or pairing payload.
"""

from __future__ import annotations

import json
import logging
import os
import stat
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.base import MessageEvent
from gateway.session import SessionSource

# --------------------------------------------------------------------------
# Synthetic canaries — no real identifiers appear in this file.
# --------------------------------------------------------------------------
SYNTHETIC_USER_JID = "15550000001@s.whatsapp.net"
SYNTHETIC_LINKED_DEVICE_JID = "15550000001:37@s.whatsapp.net"
SYNTHETIC_LID_JID = "99990000000000@lid"
SYNTHETIC_GROUP_JID = "15550000001-1600000000@g.us"
SYNTHETIC_USER_NAME = "SyntheticCanaryUser-DO-NOT-LOG"
SYNTHETIC_CHAT_ID = SYNTHETIC_USER_JID
SYNTHETIC_ALLOWLIST = "15550000001,15550000002"
SYNTHETIC_HOOK_REASON = "attacker-supplied-reason-CANARY-7f3a2b"
SYNTHETIC_MESSAGE = "canary-message-body-CANARY-9b21ce"
SYNTHETIC_SESSION_KEY = f"whatsapp:{SYNTHETIC_CHAT_ID}"

ALL_CANARIES = (
    SYNTHETIC_USER_JID,
    SYNTHETIC_LINKED_DEVICE_JID,
    SYNTHETIC_LID_JID,
    SYNTHETIC_GROUP_JID,
    SYNTHETIC_USER_NAME,
    SYNTHETIC_CHAT_ID,
    SYNTHETIC_ALLOWLIST,
    SYNTHETIC_HOOK_REASON,
    SYNTHETIC_MESSAGE,
)

LOG_LEVELS = (logging.INFO, logging.DEBUG)


def _clear_whatsapp_env(monkeypatch) -> None:
    """Unset WHATSAPP_DEBUG and every auth toggle the records depend on."""
    for key in (
        "WHATSAPP_DEBUG",
        "TELEGRAM_ALLOWED_USERS",
        "WHATSAPP_ALLOWED_USERS",
        "GATEWAY_ALLOWED_USERS",
        "TELEGRAM_ALLOW_ALL_USERS",
        "WHATSAPP_ALLOW_ALL_USERS",
        "GATEWAY_ALLOW_ALL_USERS",
    ):
        monkeypatch.delenv(key, raising=False)


def _rendered(caplog) -> str:
    """Every captured record as the handler would render it, args included.

    ``logger.warning("user=%s", jid)`` keeps the JID in ``record.args`` until a
    handler formats it, so asserting on ``record.message`` alone would pass
    while the identifier still reaches any real sink.
    """
    parts: list[str] = []
    for record in caplog.records:
        parts.append(record.getMessage())
        parts.append(repr(record.args))
    return "\n".join(parts)


def _assert_no_canaries(text: str) -> None:
    for canary in ALL_CANARIES:
        assert canary not in text, f"raw canary reached a log sink: {canary!r}"


def _make_event(text: str = SYNTHETIC_MESSAGE, chat_type: str = "dm") -> MessageEvent:
    return MessageEvent(
        text=text,
        message_id="synthetic-message-id-CANARY",
        source=SessionSource(
            platform=Platform.WHATSAPP,
            user_id=SYNTHETIC_USER_JID,
            chat_id=SYNTHETIC_CHAT_ID,
            user_name=SYNTHETIC_USER_NAME,
            chat_type=chat_type,
        ),
    )


def _make_runner():
    from gateway.run import GatewayRunner

    config = GatewayConfig(
        platforms={Platform.WHATSAPP: PlatformConfig(enabled=True)},
    )
    runner = object.__new__(GatewayRunner)
    runner.config = config
    adapter = SimpleNamespace(send=AsyncMock())
    runner.adapters = {Platform.WHATSAPP: adapter}
    runner.pairing_store = MagicMock()
    runner.pairing_store.is_approved.return_value = False
    runner.pairing_store._is_rate_limited.return_value = False
    runner.session_store = MagicMock()
    runner._running_agents = {}
    runner._update_prompt_pending = {}
    return runner, adapter


# ---------------------------------------------------------------------------
# gateway/run.py — pre_gateway_dispatch skip record
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("level", LOG_LEVELS)
@pytest.mark.asyncio
async def test_hook_skip_record_omits_chat_id_and_plugin_reason(
    monkeypatch, caplog, level
):
    """The skip record must not carry the chat JID or plugin free-text reason.

    ``reason`` is attacker-influenced: any plugin (or a compromised one) can put
    arbitrary text there, so interpolating it makes the gateway log a value it
    does not control.  ``chat_id`` is a raw WhatsApp JID.  Only the generic
    platform and a fixed allowlisted reason code may survive.
    """
    _clear_whatsapp_env(monkeypatch)

    def _fake_hook(name, **kwargs):
        if name == "pre_gateway_dispatch":
            return [{"action": "skip", "reason": SYNTHETIC_HOOK_REASON}]
        return []

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", _fake_hook)

    runner, adapter = _make_runner()

    with caplog.at_level(level, logger="gateway.run"):
        result = await runner._handle_message(_make_event())

    assert result is None
    adapter.send.assert_not_awaited()
    _assert_no_canaries(_rendered(caplog))
    # The generic platform is the one identifier that may remain.
    assert "whatsapp" in _rendered(caplog).lower()


@pytest.mark.parametrize("level", LOG_LEVELS)
@pytest.mark.asyncio
async def test_hook_skip_record_omits_group_jid(monkeypatch, caplog, level):
    """Group chats route the same record; the group JID is equally private."""
    _clear_whatsapp_env(monkeypatch)

    def _fake_hook(name, **kwargs):
        if name == "pre_gateway_dispatch":
            return [{"action": "skip", "reason": SYNTHETIC_HOOK_REASON}]
        return []

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", _fake_hook)

    runner, _adapter = _make_runner()
    event = _make_event(chat_type="group")
    event.source.chat_id = SYNTHETIC_GROUP_JID

    with caplog.at_level(level, logger="gateway.run"):
        await runner._handle_message(event)

    _assert_no_canaries(_rendered(caplog))


# ---------------------------------------------------------------------------
# gateway/run.py — unauthorised sender record
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("level", LOG_LEVELS)
@pytest.mark.asyncio
async def test_unauthorised_sender_record_omits_identity(monkeypatch, caplog, level):
    """The unauthorised-user warning must not carry user_id or user_name."""
    _clear_whatsapp_env(monkeypatch)
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda name, **kw: [])

    runner, _adapter = _make_runner()
    runner._is_user_authorized = lambda source: False
    runner._get_unauthorized_dm_behavior = lambda *a, **kw: "ignore"

    with caplog.at_level(level, logger="gateway.run"):
        await runner._handle_message(_make_event())

    _assert_no_canaries(_rendered(caplog))


@pytest.mark.parametrize("level", LOG_LEVELS)
@pytest.mark.asyncio
async def test_unauthorised_linked_device_jid_is_not_logged(
    monkeypatch, caplog, level
):
    """A linked-device JID (``:37@s.whatsapp.net``) is identity too."""
    _clear_whatsapp_env(monkeypatch)
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda name, **kw: [])

    runner, _adapter = _make_runner()
    runner._is_user_authorized = lambda source: False
    runner._get_unauthorized_dm_behavior = lambda *a, **kw: "ignore"

    event = _make_event()
    event.source.user_id = SYNTHETIC_LINKED_DEVICE_JID
    event.source.chat_id = SYNTHETIC_LID_JID

    with caplog.at_level(level, logger="gateway.run"):
        await runner._handle_message(event)

    _assert_no_canaries(_rendered(caplog))


@pytest.mark.parametrize("level", LOG_LEVELS)
@pytest.mark.asyncio
async def test_busy_session_unauthorised_record_omits_identity(
    monkeypatch, caplog, level
):
    """The busy-path drop record shares the same identity exposure.

    ``_handle_active_session_busy_message`` re-checks authorisation so an
    unauthorised sender cannot inject into a live session; its warning logs the
    same raw user_id/user_name pair as the cold path.
    """
    _clear_whatsapp_env(monkeypatch)

    runner, _adapter = _make_runner()
    runner._is_user_authorized = lambda source: False

    with caplog.at_level(level, logger="gateway.run"):
        handled = await runner._handle_active_session_busy_message(
            _make_event(), SYNTHETIC_SESSION_KEY
        )

    assert handled is True
    _assert_no_canaries(_rendered(caplog))


# ---------------------------------------------------------------------------
# plugins/platforms/whatsapp/adapter.py — the managed bridge.log sink
#
# The managed bridge is spawned with stdout/stderr pointed at bridge.log, and
# an unpaired/revoked child prints a live pairing QR on stdout.  That makes
# bridge.log an authentication-material sink unless the descriptor is opened
# defensively and the child is stopped before it can emit a QR.
# ---------------------------------------------------------------------------


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode semantics")
def test_bridge_log_is_created_as_a_0600_regular_file(tmp_path):
    """The managed bridge log must be a private regular file, mode 0600."""
    from plugins.platforms.whatsapp.adapter import open_bridge_log

    log_path = tmp_path / "bridge.log"
    handle = open_bridge_log(log_path)
    try:
        assert stat.S_ISREG(log_path.lstat().st_mode)
        mode = stat.S_IMODE(log_path.lstat().st_mode)
        assert mode == 0o600, f"bridge.log mode is {mode:04o}, expected 0600"
    finally:
        handle.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX local-state semantics")
@pytest.mark.parametrize("hostile", ["symlink", "dangling_symlink", "directory", "fifo"])
def test_bridge_log_refuses_hostile_local_state(tmp_path, hostile):
    """A symlinked/non-regular bridge.log is refused, never followed."""
    from plugins.platforms.whatsapp.adapter import (
        WhatsAppBridgeLogError,
        open_bridge_log,
    )

    log_path = tmp_path / "bridge.log"
    if hostile == "symlink":
        victim = tmp_path / "victim.txt"
        victim.write_text("pre-existing")
        log_path.symlink_to(victim)
    elif hostile == "dangling_symlink":
        log_path.symlink_to(tmp_path / "absent.txt")
    elif hostile == "directory":
        log_path.mkdir()
    elif hostile == "fifo":
        os.mkfifo(log_path)

    with pytest.raises(WhatsAppBridgeLogError) as excinfo:
        open_bridge_log(log_path)

    # Value-free diagnostics: no path, no errno text.
    message = str(excinfo.value)
    assert str(log_path) not in message
    assert "Errno" not in message

    if hostile == "symlink":
        # The symlink target must not have been truncated or appended to.
        assert (tmp_path / "victim.txt").read_text() == "pre-existing"


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode semantics")
def test_bridge_log_tightens_a_pre_existing_world_readable_file(tmp_path):
    """An existing 0644 bridge.log is refused or tightened, never appended to.

    A log left world-readable by an earlier run must not silently keep
    receiving bridge output.
    """
    from plugins.platforms.whatsapp.adapter import (
        WhatsAppBridgeLogError,
        open_bridge_log,
    )

    log_path = tmp_path / "bridge.log"
    log_path.write_text("")
    os.chmod(log_path, 0o644)

    try:
        handle = open_bridge_log(log_path)
    except WhatsAppBridgeLogError:
        return  # refusing outright is an acceptable boundary
    try:
        mode = stat.S_IMODE(log_path.lstat().st_mode)
        assert mode == 0o600, f"bridge.log left at {mode:04o}"
    finally:
        handle.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX local-state semantics")
def test_bridge_log_rejects_replacement_between_lstat_and_open(tmp_path, monkeypatch):
    """A bridge.log swapped between inspection and open fails the fstat check."""
    from plugins.platforms.whatsapp import adapter as wa_adapter

    log_path = tmp_path / "bridge.log"
    log_path.write_text("")
    os.chmod(log_path, 0o600)
    decoy = tmp_path / "decoy.log"
    decoy.write_text("")
    os.chmod(decoy, 0o600)

    real_open = os.open

    def _swapping_open(path, flags, *args, **kwargs):
        if str(path) == str(log_path):
            os.unlink(log_path)
            os.link(decoy, log_path)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(wa_adapter.os, "open", _swapping_open)

    with pytest.raises(wa_adapter.WhatsAppBridgeLogError):
        wa_adapter.open_bridge_log(log_path)


# ---------------------------------------------------------------------------
# gateway/platforms/base.py — the empty/None handler record
#
# The third pinned 0.20.1 exposure. This drives the real
# BasePlatformAdapter._process_message_background so the record is produced by
# production code, not asserted about by inspection.
# ---------------------------------------------------------------------------


def _make_base_adapter():
    """Build a minimal real adapter that can run the background processor."""
    from gateway.platforms.base import BasePlatformAdapter

    class _MinimalAdapter(BasePlatformAdapter):
        """Concrete stand-in: the abstract transport methods are unused here."""

        name = "whatsapp"

        async def connect(self):  # pragma: no cover - not exercised
            return True

        async def disconnect(self):  # pragma: no cover - not exercised
            return None

        async def get_chat_info(self, chat_id):  # pragma: no cover
            return {}

        async def send(self, *args, **kwargs):  # pragma: no cover
            return None

    adapter = object.__new__(_MinimalAdapter)
    adapter.config = SimpleNamespace(typing_indicator=False)
    adapter._active_sessions = {}
    adapter._pending_messages = {}
    adapter._background_tasks = set()
    adapter._session_tasks = {}
    adapter._streaming_tts_completed_turns = set()
    adapter._post_delivery_callbacks = {}
    adapter._auto_tts_disabled_chats = set()
    adapter._message_handler = AsyncMock(return_value=None)
    adapter._run_processing_hook = AsyncMock(return_value=None)
    adapter._stop_typing_refresh = AsyncMock(return_value=None)
    adapter.send_message = AsyncMock(return_value=None)
    return adapter


@pytest.mark.parametrize("level", LOG_LEVELS)
@pytest.mark.asyncio
async def test_empty_handler_record_omits_chat_identity(monkeypatch, caplog, level):
    """A None/empty handler response must not log the chat identity.

    Logged at DEBUG, so it is invisible at INFO and easy to miss — which is
    precisely why it is asserted at both levels here.
    """
    _clear_whatsapp_env(monkeypatch)

    adapter = _make_base_adapter()
    event = _make_event()

    with caplog.at_level(level, logger="gateway.platforms.base"):
        await adapter._process_message_background(event, SYNTHETIC_SESSION_KEY)

    adapter._message_handler.assert_awaited()
    rendered = _rendered(caplog)
    _assert_no_canaries(rendered)
    if level <= logging.DEBUG:
        assert "empty/None response" in rendered, (
            "the empty-handler path did not run — the test proves nothing"
        )


def _write_creds(session_dir: Path, payload: dict, mode: int = 0o600) -> Path:
    """Write a synthetic creds.json at an explicit mode."""
    creds = session_dir / "creds.json"
    creds.write_text(json.dumps(payload))
    os.chmod(creds, mode)
    return creds


#: A complete, well-formed synthetic identity. Not a real account.
_COMPLETE_CREDS = {
    "me": {"id": SYNTHETIC_USER_JID, "name": SYNTHETIC_USER_NAME},
    "noiseKey": {"private": "FAKENOISECANARY="},
}


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode/owner semantics")
@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"registered": True},
        {"me": {}},
        {"me": {"id": ""}},
        {"me": {"id": "   "}},
        {"me": "not-a-dict"},
        {"me": {"id": 12345}},
    ],
    ids=[
        "empty-object",
        "registered-only",
        "empty-me",
        "blank-id",
        "whitespace-id",
        "me-not-a-dict",
        "non-string-id",
    ],
)
def test_managed_start_rejects_credentials_without_a_complete_identity(
    tmp_path, payload
):
    """A parseable JSON object is not a paired session.

    ``registered`` is deliberately not the verdict: Baileys writes it as soon
    as an auth state is initialised, before any QR is scanned, and a freshly
    paired session can stay connected while still reporting ``false``. The
    verdict is a non-empty ``me.id``.
    """
    from plugins.platforms.whatsapp.adapter import managed_start_is_permitted

    session_dir = tmp_path / "session"
    session_dir.mkdir(mode=0o700)
    _write_creds(session_dir, payload)

    assert managed_start_is_permitted(session_dir) is False


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode semantics")
@pytest.mark.parametrize("mode", [0o644, 0o640, 0o604, 0o660, 0o666, 0o700])
def test_managed_start_rejects_credentials_that_are_not_exactly_0600(
    tmp_path, mode
):
    """Credential files must be owner read/write only — exactly 0600."""
    from plugins.platforms.whatsapp.adapter import managed_start_is_permitted

    session_dir = tmp_path / "session"
    session_dir.mkdir(mode=0o700)
    _write_creds(session_dir, _COMPLETE_CREDS, mode=mode)

    assert managed_start_is_permitted(session_dir) is False


@pytest.mark.skipif(os.name != "posix", reason="POSIX owner semantics")
def test_managed_start_rejects_credentials_owned_by_another_user(
    tmp_path, monkeypatch
):
    """Non-owner credential state is refused.

    Uses a synthetic ``getuid`` seam rather than a real ``chown``, which would
    need privilege this test must never require.
    """
    from plugins.platforms.whatsapp import adapter as wa_adapter

    session_dir = tmp_path / "session"
    session_dir.mkdir(mode=0o700)
    _write_creds(session_dir, _COMPLETE_CREDS)

    real_uid = os.getuid()
    monkeypatch.setattr(wa_adapter.os, "getuid", lambda: real_uid + 4242)

    assert wa_adapter.managed_start_is_permitted(session_dir) is False


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode/owner semantics")
def test_managed_start_permits_a_correct_owner_only_paired_session(tmp_path):
    """Positive control: the gate must still allow a genuinely paired session."""
    from plugins.platforms.whatsapp.adapter import managed_start_is_permitted

    session_dir = tmp_path / "session"
    session_dir.mkdir(mode=0o700)
    _write_creds(session_dir, _COMPLETE_CREDS, mode=0o600)

    assert managed_start_is_permitted(session_dir) is True


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode semantics")
@pytest.mark.parametrize("mode", [0o644, 0o640, 0o660])
def test_credential_reader_rejects_files_that_are_not_exactly_0600(
    tmp_path, mode
):
    """``_read_saved_credentials`` enforces owner-only mode itself."""
    from plugins.platforms.whatsapp.adapter import (
        _SavedCredentialsInvalid,
        _read_saved_credentials,
    )

    session_dir = tmp_path / "session"
    session_dir.mkdir(mode=0o700)
    creds = _write_creds(session_dir, _COMPLETE_CREDS, mode=mode)

    with pytest.raises(_SavedCredentialsInvalid) as excinfo:
        _read_saved_credentials(creds)
    assert str(creds) not in str(excinfo.value)


@pytest.mark.skipif(os.name != "posix", reason="POSIX owner semantics")
def test_credential_reader_rejects_a_file_owned_by_another_user(
    tmp_path, monkeypatch
):
    """``_read_saved_credentials`` refuses non-owner state, value-free."""
    from plugins.platforms.whatsapp import adapter as wa_adapter

    session_dir = tmp_path / "session"
    session_dir.mkdir(mode=0o700)
    creds = _write_creds(session_dir, _COMPLETE_CREDS)

    real_uid = os.getuid()
    monkeypatch.setattr(wa_adapter.os, "getuid", lambda: real_uid + 4242)

    with pytest.raises(wa_adapter._SavedCredentialsInvalid) as excinfo:
        wa_adapter._read_saved_credentials(creds)
    assert str(creds) not in str(excinfo.value)
    assert "Errno" not in str(excinfo.value)


@pytest.mark.skipif(os.name != "posix", reason="POSIX owner semantics")
def test_bridge_log_rejects_a_file_owned_by_another_user(tmp_path, monkeypatch):
    """The bridge log must refuse a pre-existing file owned by someone else."""
    from plugins.platforms.whatsapp import adapter as wa_adapter

    log_path = tmp_path / "bridge.log"
    log_path.write_text("")
    os.chmod(log_path, 0o600)

    real_uid = os.getuid()
    monkeypatch.setattr(wa_adapter.os, "getuid", lambda: real_uid + 4242)

    with pytest.raises(wa_adapter.WhatsAppBridgeLogError) as excinfo:
        wa_adapter.open_bridge_log(log_path)
    assert str(log_path) not in str(excinfo.value)


@pytest.mark.skipif(os.name != "posix", reason="POSIX local-state semantics")
def test_bridge_log_absent_entry_creation_is_exclusive(tmp_path, monkeypatch):
    """A file planted after the absence check must not be adopted.

    ``lstat`` reporting ENOENT followed by a plain ``O_CREAT`` open leaves a
    window in which an attacker can insert their own file (or a hardlink to
    one) and have the bridge append to it. First creation must be exclusive.
    """
    from plugins.platforms.whatsapp import adapter as wa_adapter

    log_path = tmp_path / "bridge.log"
    planted = tmp_path / "planted.log"
    planted.write_text("attacker-owned")
    os.chmod(planted, 0o600)

    real_open = os.open

    def _planting_open(path, flags, *args, **kwargs):
        # Between the absence check and the open, plant a hardlink.
        if str(path) == str(log_path) and not log_path.exists():
            os.link(planted, log_path)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(wa_adapter.os, "open", _planting_open)

    with pytest.raises(wa_adapter.WhatsAppBridgeLogError):
        wa_adapter.open_bridge_log(log_path)

    # The planted file must not have been appended to.
    assert planted.read_text() == "attacker-owned"


@pytest.mark.skipif(os.name != "posix", reason="POSIX local-state semantics")
def test_session_dir_replacement_never_mutates_the_swap_target(
    tmp_path, monkeypatch
):
    """A directory swapped for a symlink must not have its target chmod'ed.

    Validating with ``lstat`` and then tightening with a path-based
    ``os.chmod`` follows whatever the path resolves to at that later moment,
    so the rejection arrives only after the attacker's target was already
    modified. Enforcement has to run through a no-follow descriptor.
    """
    from gateway.platforms import whatsapp_common as wa_common

    session_dir = tmp_path / "session"
    session_dir.mkdir(mode=0o755)  # needs tightening to 0700

    victim = tmp_path / "victim"
    victim.mkdir(mode=0o755)
    victim_mode_before = stat.S_IMODE(victim.lstat().st_mode)

    real_lstat = os.lstat

    def _swapping_lstat(path, *args, **kwargs):
        result = real_lstat(path, *args, **kwargs)
        # After inspection, replace the directory with a symlink to the victim.
        if str(path) == str(session_dir) and session_dir.is_dir() and not session_dir.is_symlink():
            session_dir.rmdir()
            session_dir.symlink_to(victim)
        return result

    monkeypatch.setattr(wa_common.os, "lstat", _swapping_lstat)

    with pytest.raises(wa_common.WhatsAppSessionStateError):
        wa_common.ensure_whatsapp_session_dir(session_dir)

    assert stat.S_IMODE(victim.lstat().st_mode) == victim_mode_before, (
        "the swap target was mutated before the replacement was rejected"
    )


def test_managed_bridge_start_requires_a_valid_paired_session(tmp_path):
    """Managed startup must refuse to spawn when the session is not paired.

    An unpaired managed child is precisely the child that prints a QR into
    bridge.log, so the refusal has to happen before the spawn.
    """
    from plugins.platforms.whatsapp.adapter import managed_start_is_permitted

    session_dir = tmp_path / "session"
    session_dir.mkdir(mode=0o700)
    # No creds.json at all: not a paired session.
    assert managed_start_is_permitted(session_dir) is False


def test_revoked_session_produces_only_a_generic_repair_status(tmp_path):
    """A revoked session yields a fixed re-pair status, free of identity."""
    from plugins.platforms.whatsapp.adapter import managed_start_refusal_status

    session_dir = tmp_path / "session"
    session_dir.mkdir(mode=0o700)

    code, message = managed_start_refusal_status(session_dir)

    assert code == "whatsapp_repair_required"
    _assert_no_canaries(f"{code} {message}")
    assert str(session_dir) not in message
