"""Tests for WhatsApp connect() error handling.

Regression tests for two bugs in WhatsAppAdapter.connect():

1. Uninitialized ``data`` variable: when ``resp.json()`` raised after the
   health endpoint returned HTTP 200, ``http_ready`` was set to True but
   ``data`` was never assigned.  The subsequent ``data.get("status")``
   check raised ``NameError``.

2. Bridge log file handle leaked on error paths: the file was opened before
   the health-check loop but never closed when ``connect()`` returned False.
   Repeated connection failures accumulated open file descriptors.
"""

import asyncio
from contextlib import ExitStack, contextmanager
import os
import signal
import subprocess
import sys
import textwrap
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway.config import Platform


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _AsyncCM:
    """Minimal async context manager returning a fixed value."""

    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *exc):
        return False


_PRIVATE_VALUE = "privacy-probe-runtime-detail"
_PRIVATE_PATH = "/diagnostic-private-root/bridge/session"
_PRIVATE_JID = "privacy-probe-user@s.whatsapp.net"
_INVALID_CREDENTIAL_CODE = "whatsapp_credentials_invalid"
_INVALID_CREDENTIAL_MESSAGE = (
    "WhatsApp saved credentials are unavailable or invalid. "
    "Re-pair with `hermes whatsapp` (or from the dashboard), or remove "
    "WHATSAPP_ENABLED from your .env to disable WhatsApp."
)


class DiagnosticProbeError(RuntimeError):
    """Exception whose value is safe to use in diagnostic privacy tests."""


def _printed_output(mock_print) -> str:
    return "\n".join(
        " ".join(str(arg) for arg in call.args)
        for call in mock_print.call_args_list
    )


def _assert_private_values_absent(text: str) -> None:
    for value in (_PRIVATE_VALUE, _PRIVATE_PATH, _PRIVATE_JID):
        assert value not in text


def _make_adapter(session_path=None):
    """Create a WhatsAppAdapter with test attributes (bypass __init__).

    ``session_path`` defaults to a path that does not exist; pass a
    ``tmp_path`` whenever the test cares about what is *in* the session
    directory (notably the revoked-session marker).
    """
    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter

    adapter = WhatsAppAdapter.__new__(WhatsAppAdapter)
    adapter.platform = Platform.WHATSAPP
    adapter.config = MagicMock()
    adapter._bridge_port = 19876
    adapter._bridge_script = "/tmp/test-bridge.js"
    adapter._session_path = Path(session_path or "/tmp/test-wa-session")
    adapter._bridge_log_fh = None
    adapter._bridge_log = None
    adapter._bridge_process = None
    adapter._reply_prefix = None
    adapter._send_read_receipts = False
    adapter._running = False
    adapter._message_handler = None
    adapter._fatal_error_code = None
    adapter._fatal_error_message = None
    adapter._fatal_error_retryable = True
    adapter._fatal_error_handler = None
    adapter._active_sessions = {}
    adapter._pending_messages = {}
    adapter._background_tasks = set()
    adapter._auto_tts_disabled_chats = set()
    adapter._message_queue = asyncio.Queue()
    adapter._http_session = None
    return adapter


def _mock_aiohttp(status=200, json_data=None, json_side_effect=None):
    """Build a mock ``aiohttp.ClientSession`` returning a fixed response."""
    mock_resp = MagicMock()
    mock_resp.status = status
    if json_side_effect:
        mock_resp.json = AsyncMock(side_effect=json_side_effect)
    else:
        mock_resp.json = AsyncMock(return_value=json_data or {})

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=_AsyncCM(mock_resp))

    return MagicMock(return_value=_AsyncCM(mock_session))


def _discard_create_task(coro, *args, **kwargs):
    """Stand in for ``asyncio.create_task`` without leaking the coroutine.

    ``connect()`` ends with ``asyncio.create_task(self._poll_messages())``.
    These tests stub ``create_task`` because they do not want the poll loop
    running — but ``_poll_messages`` is an ``async def``, so ``patch.object``
    replaces it with an ``AsyncMock``, and *calling* that already built a
    coroutine.  A plain ``MagicMock`` create_task then dropped it on the
    floor, and Python reported ``coroutine ... was never awaited`` at an
    unrelated point later in the session.

    A real ``create_task`` takes ownership of the coroutine, so the stub must
    too: closing it is the disposal a never-scheduled coroutine needs.
    """
    if asyncio.iscoroutine(coro):
        coro.close()
    return MagicMock()


@contextmanager
def _connect_patches(mock_proc, mock_fh, mock_client_cls=None):
    """Apply the common patches needed to reach the health-check loop."""
    with ExitStack() as stack:
        stack.enter_context(
            patch(
                "plugins.platforms.whatsapp.adapter.check_whatsapp_requirements",
                return_value=True,
            )
        )
        stack.enter_context(
            patch(
                "plugins.platforms.whatsapp.adapter.whatsapp_bridge_runtime_hash",
                return_value="a" * 64,
            )
        )
        stack.enter_context(patch.object(Path, "exists", return_value=True))
        stack.enter_context(patch.object(Path, "mkdir", return_value=None))
        stack.enter_context(patch("subprocess.run", return_value=MagicMock(returncode=0)))
        stack.enter_context(patch("subprocess.Popen", return_value=mock_proc))
        stack.enter_context(patch("builtins.open", return_value=mock_fh))
        # These tests drive the bridge lifecycle against a session path that
        # does not exist on disk (Path.mkdir is a no-op above), so the local
        # state hardening would fail closed before the code under test runs.
        # Both boundaries have their own dedicated coverage in
        # tests/gateway/test_gateway_identity_log_privacy.py and
        # tests/hermes_cli/test_whatsapp_cli_privacy.py.
        stack.enter_context(
            patch(
                "plugins.platforms.whatsapp.adapter.ensure_whatsapp_session_dir",
                side_effect=lambda path: path,
            )
        )
        stack.enter_context(
            patch(
                "plugins.platforms.whatsapp.adapter.open_bridge_log",
                return_value=mock_fh,
            )
        )
        stack.enter_context(
            patch("plugins.platforms.whatsapp.adapter.asyncio.sleep", new_callable=AsyncMock)
        )
        stack.enter_context(
            patch(
                "plugins.platforms.whatsapp.adapter.asyncio.create_task",
                side_effect=_discard_create_task,
            )
        )
        if mock_client_cls is not None:
            stack.enter_context(patch("aiohttp.ClientSession", mock_client_cls))
        # These tests exercise paths *after* the pairing preflight; a blanket
        # ``Path.exists`` patch no longer satisfies it, since the preflight now
        # reads and parses creds.json rather than just testing for the file.
        stack.enter_context(
            patch(
                "plugins.platforms.whatsapp.adapter.classify_saved_credentials",
                return_value=None,
            )
        )
        # The managed-start gate additionally requires a complete, owner-only
        # credential identity on disk. These tests run against a session path
        # that does not exist, so the gate is stubbed permissive here; it has
        # dedicated coverage (including its rejections and a positive control)
        # in tests/gateway/test_gateway_identity_log_privacy.py.
        stack.enter_context(
            patch(
                "plugins.platforms.whatsapp.adapter.managed_start_is_permitted",
                return_value=True,
            )
        )
        yield


# ---------------------------------------------------------------------------
# _close_bridge_log() unit tests
# ---------------------------------------------------------------------------

class TestCloseBridgeLog:
    """Direct tests for the _close_bridge_log() helper method."""

    @staticmethod
    def _bare_adapter():
        from plugins.platforms.whatsapp.adapter import WhatsAppAdapter
        a = WhatsAppAdapter.__new__(WhatsAppAdapter)
        a._bridge_log_fh = None
        return a

    def test_closes_open_handle(self):
        adapter = self._bare_adapter()
        mock_fh = MagicMock()
        adapter._bridge_log_fh = mock_fh

        adapter._close_bridge_log()

        mock_fh.close.assert_called_once()
        assert adapter._bridge_log_fh is None


# ---------------------------------------------------------------------------
# data variable initialization
# ---------------------------------------------------------------------------

class TestDataInitialized:
    """Verify ``data = {}`` prevents NameError when resp.json() fails."""

    @pytest.mark.asyncio
    async def test_no_name_error_when_json_always_fails(self):
        """HTTP 200 sets http_ready but json() always raises.

        Without the fix, ``data`` was never assigned and the Phase 2 check
        ``data.get("status")`` raised NameError.  With ``data = {}``, the
        check evaluates to ``None != "connected"`` and Phase 2 runs normally.
        """
        adapter = _make_adapter()

        mock_proc = MagicMock()
        mock_proc.poll.return_value = None  # bridge stays alive

        mock_client_cls = _mock_aiohttp(
            status=200, json_side_effect=ValueError("bad json"),
        )
        mock_fh = MagicMock()

        with _connect_patches(mock_proc, mock_fh, mock_client_cls), \
             patch.object(type(adapter), "_poll_messages", return_value=MagicMock()):
            # Must NOT raise NameError
            result = await adapter.connect()

        # connect() returns True (warn-and-proceed path)
        assert result is True
        assert adapter._running is True


# ---------------------------------------------------------------------------
# File handle cleanup on error paths
# ---------------------------------------------------------------------------

class TestFileHandleClosedOnError:
    """Verify the bridge log file handle is closed on every failure path."""

    @pytest.mark.asyncio
    async def test_closed_when_bridge_dies_phase1(self):
        """Bridge process exits during Phase 1 health-check loop."""
        adapter = _make_adapter()

        mock_proc = MagicMock()
        mock_proc.poll.return_value = 1  # dead immediately
        mock_proc.returncode = 1

        mock_fh = MagicMock()
        with _connect_patches(mock_proc, mock_fh):
            result = await adapter.connect()

        assert result is False
        mock_fh.close.assert_called_once()
        assert adapter._bridge_log_fh is None
        # An ordinary crash during startup stays retryable.
        assert adapter._fatal_error_code != "whatsapp_logged_out"

    @pytest.mark.asyncio
    async def test_logged_out_during_startup_is_non_retryable(self):
        """The bridge can be revoked mid-handshake, before it ever reports
        healthy.  That death is terminal too — without this, connect() just
        returned False and the watcher re-spawned the bridge forever."""
        from plugins.platforms.whatsapp.adapter import BRIDGE_EXIT_LOGGED_OUT

        adapter = _make_adapter()

        mock_proc = MagicMock()
        mock_proc.poll.return_value = BRIDGE_EXIT_LOGGED_OUT
        mock_proc.returncode = BRIDGE_EXIT_LOGGED_OUT

        mock_fh = MagicMock()
        with _connect_patches(mock_proc, mock_fh):
            result = await adapter.connect()

        assert result is False
        assert adapter._fatal_error_code == "whatsapp_logged_out"
        assert adapter._fatal_error_retryable is False
        assert "hermes whatsapp" in adapter._fatal_error_message
        mock_fh.close.assert_called_once()


class TestConnectCleanup:
    """Verify failure paths release the scoped session lock."""

    @pytest.mark.asyncio
    async def test_parks_before_reading_credentials_while_recovery_is_active(
        self, tmp_path
    ):
        adapter = _make_adapter(session_path=tmp_path / "session")
        bridge = tmp_path / "bridge.js"
        bridge.write_text("// bridge", encoding="utf-8")
        adapter._bridge_script = str(bridge)

        with patch(
            "plugins.platforms.whatsapp.adapter.check_whatsapp_requirements",
            return_value=True,
        ), patch(
            "plugins.platforms.whatsapp.adapter.whatsapp_bridge_runtime_hash",
            return_value="a" * 64,
        ), patch(
            "plugins.platforms.whatsapp.adapter.acquire_whatsapp_connect_gate",
            return_value=None,
        ), patch(
            "plugins.platforms.whatsapp.adapter.classify_saved_credentials"
        ) as classify:
            result = await adapter.connect()

        assert result is False
        classify.assert_not_called()
        assert adapter.fatal_error_retryable is True

    @pytest.mark.asyncio
    async def test_releases_lock_when_npm_install_fails(self):
        adapter = _make_adapter()

        def _path_exists(path_obj):
            return not str(path_obj).endswith("node_modules")

        install_result = MagicMock(returncode=1, stderr="install failed")

        with patch("plugins.platforms.whatsapp.adapter.check_whatsapp_requirements", return_value=True), \
             patch("plugins.platforms.whatsapp.adapter.whatsapp_bridge_runtime_hash", return_value="a" * 64), \
             patch.object(Path, "exists", autospec=True, side_effect=_path_exists), \
             patch("plugins.platforms.whatsapp.adapter.classify_saved_credentials", return_value=None), \
             patch("subprocess.run", return_value=install_result), \
             patch("gateway.status.acquire_scoped_lock", return_value=(True, None)), \
             patch("gateway.status.release_scoped_lock") as mock_release:
            result = await adapter.connect()

        assert result is False
        assert adapter.fatal_error_code == "whatsapp_npm_install_failed"
        assert adapter.fatal_error_retryable is False
        assert "npm install failed" in (adapter.fatal_error_message or "")
        mock_release.assert_called_once_with("whatsapp-session", str(adapter._session_path.resolve()))
        assert adapter._platform_lock_identity is None


class TestBridgeRuntimeFailure:
    """Verify runtime bridge death is surfaced as a fatal adapter error."""

    @pytest.mark.asyncio
    async def test_send_marks_retryable_fatal_when_managed_bridge_exits(self):
        adapter = _make_adapter()
        fatal_handler = AsyncMock()
        adapter.set_fatal_error_handler(fatal_handler)
        adapter._running = True
        adapter._http_session = MagicMock()  # Persistent session active
        mock_fh = MagicMock()
        adapter._bridge_log_fh = mock_fh

        mock_proc = MagicMock()
        mock_proc.poll.return_value = 7
        adapter._bridge_process = mock_proc

        result = await adapter.send("chat-123", "hello")

        assert result.success is False
        assert "exited unexpectedly" in result.error
        assert adapter.fatal_error_code == "whatsapp_bridge_exited"
        assert adapter.fatal_error_retryable is True
        fatal_handler.assert_awaited_once()
        mock_fh.close.assert_called_once()
        assert adapter._bridge_log_fh is None

    @pytest.mark.asyncio
    async def test_logged_out_exit_code_is_non_retryable(self):
        """The bridge's dedicated logged-out exit must end the retry loop.

        WhatsApp revoked this device (401 / device_removed); restarting the
        bridge just gets it revoked again.  Retrying is the bug.
        """
        from plugins.platforms.whatsapp.adapter import BRIDGE_EXIT_LOGGED_OUT

        adapter = _make_adapter()
        fatal_handler = AsyncMock()
        adapter.set_fatal_error_handler(fatal_handler)
        adapter._running = True
        adapter._http_session = MagicMock()
        adapter._bridge_log_fh = MagicMock()

        mock_proc = MagicMock()
        mock_proc.poll.return_value = BRIDGE_EXIT_LOGGED_OUT
        adapter._bridge_process = mock_proc

        result = await adapter.send("chat-123", "hello")

        assert result.success is False
        assert adapter.fatal_error_code == "whatsapp_logged_out"
        assert adapter.fatal_error_retryable is False
        assert "hermes whatsapp" in adapter.fatal_error_message
        fatal_handler.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("returncode", [1, 2, 7, 137])
    async def test_other_exit_codes_stay_retryable(self, returncode):
        """Ordinary crashes keep their retryable classification."""
        adapter = _make_adapter()
        adapter.set_fatal_error_handler(AsyncMock())
        adapter._running = True
        adapter._http_session = MagicMock()
        adapter._bridge_log_fh = MagicMock()

        mock_proc = MagicMock()
        mock_proc.poll.return_value = returncode
        adapter._bridge_process = mock_proc

        await adapter.send("chat-123", "hello")

        assert adapter.fatal_error_code == "whatsapp_bridge_exited"
        assert adapter.fatal_error_retryable is True

    def test_logged_out_exit_code_is_the_agreed_number(self):
        """Pin the exit code both sides of the process boundary agree on.

        CROSS-LANGUAGE CONTRACT: the bridge exits with this number
        (``BRIDGE_EXIT_LOGGED_OUT`` in
        scripts/whatsapp-bridge/bridge_helpers.js) and this adapter reads it
        back off ``Popen.returncode``.  It is a bare integer with nothing to
        type-check it, so a one-sided edit would silently turn "session
        revoked" back into "ordinary crash, retry forever".

        Both sides pin the same number, but matching literals do not prove
        the two implementations agree — see
        ``TestCrossLanguageMarkerContract``, which runs the bridge's real
        marker writer against the real Python parser.
        Reading the other language's source text here is banned
        (AGENTS.md:1490+) and would not be a behavioural check anyway.
        """
        from plugins.platforms.whatsapp.adapter import BRIDGE_EXIT_LOGGED_OUT

        assert BRIDGE_EXIT_LOGGED_OUT == 78

    @pytest.mark.asyncio
    async def test_send_normalizes_bare_phone_numbers_to_jid(self):
        """A bare phone target (with or without +) becomes a full JID.

        Baileys' jidDecode crashes on a bare number (#8637); the adapter
        must rewrite it to ``<digits>@s.whatsapp.net`` before the bridge
        call. Regression guard for that crash.
        """
        adapter = _make_adapter()
        adapter._running = True
        adapter._bridge_process = None  # unmanaged bridge — skip exit check

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"messageId": "msg-1"})
        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=_AsyncCM(mock_resp))
        adapter._http_session = mock_session

        result = await adapter.send("+50766715226", "hello")

        assert result.success is True
        payload = mock_session.post.call_args.kwargs["json"]
        assert payload["chatId"] == "50766715226@s.whatsapp.net"


    @pytest.mark.asyncio
    async def test_closed_when_bridge_dies_phase2(self):
        """Bridge alive during Phase 1 but dies during Phase 2."""
        adapter = _make_adapter()

        # Phase 1 (15 iterations): alive.  Phase 2 (iteration 16): dead.
        call_count = [0]

        def poll_side_effect():
            call_count[0] += 1
            return None if call_count[0] <= 15 else 1

        mock_proc = MagicMock()
        mock_proc.poll.side_effect = poll_side_effect
        mock_proc.returncode = 1

        # Health returns 200 with status != "connected" -> triggers Phase 2
        mock_client_cls = _mock_aiohttp(
            status=200, json_data={"status": "disconnected"},
        )
        mock_fh = MagicMock()
        with _connect_patches(mock_proc, mock_fh, mock_client_cls):
            result = await adapter.connect()

        assert result is False
        mock_fh.close.assert_called_once()
        assert adapter._bridge_log_fh is None


# ---------------------------------------------------------------------------
# _kill_port_process() cross-platform tests
# ---------------------------------------------------------------------------

class TestKillPortProcess:
    """Verify _kill_port_process uses platform-appropriate commands."""

    @pytest.mark.windows_only
    def test_uses_netstat_and_taskkill_on_windows(self):
        """``windows_only``: netstat/taskkill are Windows binaries. The old
        ``_IS_WINDOWS`` patch selected this branch on Linux, where neither
        exists, so the mocked argv was the only thing under test."""
        from plugins.platforms.whatsapp.adapter import _kill_port_process

        netstat_output = (
            "  Proto  Local Address          Foreign Address        State           PID\n"
            "  TCP    0.0.0.0:3000           0.0.0.0:0              LISTENING       12345\n"
            "  TCP    0.0.0.0:3001           0.0.0.0:0              LISTENING       99999\n"
        )
        mock_netstat = MagicMock(stdout=netstat_output)
        mock_taskkill = MagicMock()

        def run_side_effect(cmd, **kwargs):
            if cmd[0] == "netstat":
                return mock_netstat
            if cmd[0] == "taskkill":
                return mock_taskkill
            return MagicMock()

        with patch("plugins.platforms.whatsapp.adapter.subprocess.run", side_effect=run_side_effect) as mock_run:
            _kill_port_process(3000)

        # netstat called
        assert any(
            call.args[0][0] == "netstat" for call in mock_run.call_args_list
        )
        # taskkill called with correct PID
        assert any(
            call.args[0] == ["taskkill", "/PID", "12345", "/F"]
            for call in mock_run.call_args_list
        )


    @pytest.mark.linux_only
    def test_kills_only_listeners_on_linux(self):
        """POSIX path SIGTERMs only LISTENer PIDs (never clients) — the #43846 fix.

        Replaces the old fuser-based test: ``fuser``/bare ``lsof -i`` also
        matched client sockets sharing the port number, which closed unrelated
        processes (a browser tab on the same port). The implementation now
        resolves listeners via ``_listener_pids_on_port`` and signals only those.

        ``linux_only``: asserts the POSIX ``os.kill``/SIGTERM path, which is
        genuinely selected here without patching ``_IS_WINDOWS``.
        """
        from plugins.platforms.whatsapp import adapter as wa

        kills = []
        with patch("plugins.platforms.whatsapp.adapter._listener_pids_on_port",
                   return_value=[55555]) as mock_listeners, \
             patch("plugins.platforms.whatsapp.adapter.os.kill",
                   side_effect=lambda pid, sig: kills.append((pid, sig))):
            wa._kill_port_process(3000)

        mock_listeners.assert_called_once_with(3000)
        assert kills == [(55555, signal.SIGTERM)]


# ---------------------------------------------------------------------------
# Persistent HTTP session lifecycle
# ---------------------------------------------------------------------------

class TestHttpSessionLifecycle:
    """Verify persistent aiohttp.ClientSession is created and cleaned up."""

    @pytest.mark.asyncio
    @pytest.mark.windows_only
    async def test_disconnect_uses_taskkill_tree_on_windows(self):
        """Windows disconnect should target the bridge process tree, not just the parent PID.

        ``windows_only``: ``taskkill /T`` is the Windows tree-kill primitive;
        on Linux the branch was reachable only by faking ``_IS_WINDOWS``.
        """
        adapter = _make_adapter()
        mock_proc = MagicMock()
        mock_proc.pid = 12345
        mock_proc.poll.side_effect = [0]
        adapter._bridge_process = mock_proc
        adapter._poll_task = None
        adapter._http_session = None
        adapter._running = True
        adapter._session_lock_identity = None

        with patch("plugins.platforms.whatsapp.adapter.subprocess.run", return_value=MagicMock(returncode=0)) as mock_run, \
             patch("plugins.platforms.whatsapp.adapter.asyncio.sleep", new_callable=AsyncMock):
            await adapter.disconnect()

        mock_run.assert_called_once_with(
            ["taskkill", "/PID", "12345", "/T"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
        )
        mock_proc.terminate.assert_not_called()
        mock_proc.kill.assert_not_called()

    @pytest.mark.asyncio
    async def test_session_closed_on_disconnect(self):
        """disconnect() should close self._http_session."""
        adapter = _make_adapter()
        mock_session = AsyncMock()
        mock_session.closed = False
        adapter._http_session = mock_session
        adapter._poll_task = None
        adapter._bridge_process = None
        adapter._running = True
        adapter._session_lock_identity = None

        await adapter.disconnect()

        mock_session.close.assert_called_once()
        assert adapter._http_session is None


# ---------------------------------------------------------------------------
# Pre-flight: refuse to start the bridge when creds.json is missing
# ---------------------------------------------------------------------------


class TestNoCredsPreflight:
    """Verify ``connect()`` fast-fails as non-retryable when WhatsApp is
    enabled but the user never finished pairing (no ``creds.json``).

    Without this guard, every gateway boot:
      • spawned the bridge subprocess (npm install if needed)
      • waited 30s for status:connected (never happens without creds)
      • queued WhatsApp for indefinite retries that would just repeat
    With the guard, ``connect()`` returns False immediately with a
    non-retryable fatal error so the reconnect watcher drops the platform
    and the gateway gets a single clear log line telling the user to run
    ``hermes whatsapp``.
    """


    @pytest.mark.asyncio
    async def test_connect_proceeds_when_creds_present(self, tmp_path):
        """When creds.json exists, the preflight check is bypassed and
        connect() proceeds to the bridge bootstrap path. We don't fully
        simulate the bridge here — we just verify no fast-fail occurs.
        """
        from plugins.platforms.whatsapp.adapter import WhatsAppAdapter

        adapter = WhatsAppAdapter.__new__(WhatsAppAdapter)
        adapter.platform = Platform.WHATSAPP
        adapter.config = MagicMock()
        adapter._bridge_port = 19877
        bridge = tmp_path / "bridge.js"
        bridge.write_text("// stub")
        adapter._bridge_script = str(bridge)
        session_dir = tmp_path / "session"
        session_dir.mkdir()
        (session_dir / "creds.json").write_text('{"registered": true}')
        adapter._session_path = session_dir
        adapter._bridge_log_fh = None
        adapter._fatal_error_code = None
        adapter._fatal_error_message = None
        adapter._fatal_error_retryable = True
        # Stub _acquire_platform_lock to return False so connect() exits
        # cleanly *after* the preflight, without spawning subprocesses.
        adapter._acquire_platform_lock = MagicMock(return_value=False)

        with patch(
            "plugins.platforms.whatsapp.adapter.check_whatsapp_requirements",
            return_value=True,
        ):
            result = await adapter.connect()

        # Preflight passed — exits because we faked lock acquisition,
        # but the fatal-error code is NOT the "not paired" one.
        assert result is False
        assert adapter._fatal_error_code != "whatsapp_not_paired"


class TestCredentialPairingPreflight:
    """``creds.json`` existing is not the same as being paired — but the
    ``registered`` field is not reliable evidence either way.

    The file is written by Baileys as soon as an auth state is initialised —
    before any QR is scanned — with ``registered: false``. It was assumed
    that WhatsApp also leaves it in exactly that state when it revokes a
    device (``stream:error 401 conflict type=device_removed``), so a static
    preflight rejected any ``registered: false`` creds file outright.

    That assumption does not hold: a freshly paired session reaches
    ``connection: open`` and stays connected while its saved ``creds.json``
    still reports ``registered: false``. A static preflight that rejects on
    that field therefore blocks sessions that can actually connect. Only unreadable,
    corrupt, or non-object credential JSON is rejected statically; whether a
    parseable session can connect is decided live by the bridge, and a
    genuine revocation is still caught at runtime via exit code 78
    (``BRIDGE_EXIT_LOGGED_OUT`` / ``_mark_fatal_if_logged_out``).
    """

    def _classify(self, tmp_path, contents):
        from plugins.platforms.whatsapp.adapter import classify_saved_credentials

        creds = tmp_path / "creds.json"
        if contents is not None:
            creds.write_text(contents)
        return classify_saved_credentials(creds)

    def test_registered_true_is_paired(self, tmp_path):
        assert self._classify(tmp_path, '{"registered": true, "me": {"id": "1@s.whatsapp.net"}}') is None

    def test_missing_file_is_not_paired(self, tmp_path):
        code, message = self._classify(tmp_path, None)
        assert code == "whatsapp_not_paired"
        assert "hermes whatsapp" in message

    @pytest.mark.parametrize(
        "contents",
        ["", "   \n", "not json at all", "{", "[]", '"registered"', "null"],
    )
    def test_empty_or_malformed_is_fixed_invalid_failure(self, tmp_path, contents):
        code, message = self._classify(tmp_path, contents)
        assert (code, message) == (
            _INVALID_CREDENTIAL_CODE,
            _INVALID_CREDENTIAL_MESSAGE,
        )

    def test_missing_registered_key_is_allowed_through(self, tmp_path):
        """A well-formed object missing ``registered`` is still parseable —
        let the live bridge decide, not a static field check."""
        assert self._classify(tmp_path, '{"me": {"id": "1@s.whatsapp.net"}}') is None

    def test_registered_false_is_allowed_through(self, tmp_path):
        """Not reliable evidence of revocation — the bridge, not this
        static check, decides connectivity."""
        assert self._classify(tmp_path, '{"registered": false, "me": {"id": "1@s.whatsapp.net"}}') is None

    @pytest.mark.parametrize("contents", ['{"registered": "true"}', '{"registered": 1}'])
    def test_non_boolean_registered_is_allowed_through(self, tmp_path, contents):
        """The ``registered`` value is no longer inspected at all — a
        well-formed object always proceeds to the live bridge."""
        assert self._classify(tmp_path, contents) is None


class TestCredentialFailuresDoNotDisclosePaths:
    """A preflight verdict is user-facing output, and must stay generic.

    ``classify_saved_credentials`` returns a message that is logged at WARNING
    *and* stored as ``fatal_error_message``, which the dashboard renders and
    users paste into issues.  Two things were leaking into it:

    * the absolute ``creds_path`` — which spells out the OS user's home
      directory, and therefore their username, on every unpaired install;
    * ``str(OSError)``, which appends that same path to an errno string.

    Neither is actionable.  The reader cannot act on a path they did not
    choose, and the repair for all of these cases is identical: re-pair, or
    disable WhatsApp.  So the guidance stays and the specifics go.
    """

    #: A home directory shaped like a real one, carrying a distinctive name.
    SECRET_HOME = "sentinel-operator"

    def _session(self, tmp_path):
        session = tmp_path / "Users" / self.SECRET_HOME / ".hermes" / "whatsapp" / "session"
        session.mkdir(parents=True)
        return session

    def _assert_generic(self, message, session):
        from plugins.platforms.whatsapp.adapter import _REPAIR_HINT

        assert _REPAIR_HINT in message, "the actionable repair guidance must survive"
        assert "WhatsApp" in message, "the message must still say what failed"

        assert self.SECRET_HOME not in message, "the OS user must not be disclosed"
        assert str(session) not in message
        assert str(session / "creds.json") not in message
        assert ".hermes" not in message, "no absolute path may be reflected"
        assert "Errno" not in message, "raw exception text must not be reflected"

    def test_missing_credentials_message_has_no_path(self, tmp_path):
        from plugins.platforms.whatsapp.adapter import classify_saved_credentials

        session = self._session(tmp_path)
        result = classify_saved_credentials(session / "creds.json")
        assert result is not None
        code, message = result

        assert code == "whatsapp_not_paired"
        self._assert_generic(message, session)

    def test_unreadable_credentials_message_has_no_path_or_errno(self, tmp_path):
        """The OSError branch: its ``str()`` is the errno *and* the path."""
        from plugins.platforms.whatsapp.adapter import classify_saved_credentials

        session = self._session(tmp_path)
        creds = session / "creds.json"
        creds.write_text('{"registered": true}')

        secret_error = PermissionError(13, "Permission denied", str(creds))

        with patch(
            "plugins.platforms.whatsapp.adapter.os.open",
            side_effect=secret_error,
        ):
            result = classify_saved_credentials(creds)

        assert result is not None
        code, message = result
        assert code == _INVALID_CREDENTIAL_CODE
        assert message == _INVALID_CREDENTIAL_MESSAGE
        self._assert_generic(message, session)
        assert "Permission denied" not in message

    @pytest.mark.parametrize("contents", ["", "not json at all", "[]"])
    def test_corrupt_credentials_message_has_no_path(self, tmp_path, contents):
        from plugins.platforms.whatsapp.adapter import classify_saved_credentials

        session = self._session(tmp_path)
        (session / "creds.json").write_text(contents)

        result = classify_saved_credentials(session / "creds.json")
        assert result is not None
        code, message = result

        assert code == _INVALID_CREDENTIAL_CODE
        assert message == _INVALID_CREDENTIAL_MESSAGE
        self._assert_generic(message, session)

    @pytest.mark.asyncio
    async def test_connect_proceeds_when_registered_false(self, tmp_path):
        """A parseable ``registered: false`` creds file must not fast-fail
        the preflight — connect() proceeds past it to the bridge bootstrap
        (session-lock acquisition), same as any other parseable creds file.
        """
        from plugins.platforms.whatsapp.adapter import WhatsAppAdapter

        adapter = WhatsAppAdapter.__new__(WhatsAppAdapter)
        adapter.platform = Platform.WHATSAPP
        adapter.config = MagicMock()
        adapter._bridge_port = 19878
        bridge = tmp_path / "bridge.js"
        bridge.write_text("// stub")
        adapter._bridge_script = str(bridge)
        session_dir = tmp_path / "session"
        session_dir.mkdir()
        (session_dir / "creds.json").write_text('{"registered": false}')
        adapter._session_path = session_dir
        adapter._bridge_log_fh = None
        adapter._fatal_error_code = None
        adapter._fatal_error_message = None
        adapter._fatal_error_retryable = True
        # Stub _acquire_platform_lock to return False so connect() exits
        # cleanly *after* the preflight, without spawning subprocesses.
        adapter._acquire_platform_lock = MagicMock(return_value=False)

        with patch(
            "plugins.platforms.whatsapp.adapter.check_whatsapp_requirements",
            return_value=True,
        ), patch(
            "plugins.platforms.whatsapp.adapter.whatsapp_bridge_runtime_hash",
            return_value="a" * 64,
        ), patch("subprocess.Popen") as mock_popen:
            result = await adapter.connect()

        # Preflight passed — exits because we faked lock acquisition, but
        # the fatal-error code is neither preflight-rejection code.
        assert result is False
        assert adapter._fatal_error_code not in ("whatsapp_not_paired", "whatsapp_logged_out")
        mock_popen.assert_not_called()
        # The preflight must have let it through to the lock-acquisition step.
        adapter._acquire_platform_lock.assert_called_once()


class TestCredentialFileBoundary:
    """Present credential state must be bounded, regular, stable, and no-follow."""

    @staticmethod
    def _assert_fixed_failure(result, *private_values):
        assert result == (
            _INVALID_CREDENTIAL_CODE,
            _INVALID_CREDENTIAL_MESSAGE,
        )
        message = result[1]
        for value in private_values:
            assert str(value) not in message

    def test_directory_credentials_are_fixed_failure(self, tmp_path):
        from plugins.platforms.whatsapp.adapter import classify_saved_credentials

        creds = tmp_path / "creds.json"
        creds.mkdir()

        self._assert_fixed_failure(classify_saved_credentials(creds), creds)

    @pytest.mark.require_symlinks
    def test_symlinked_credentials_are_not_followed(self, tmp_path):
        from plugins.platforms.whatsapp.adapter import classify_saved_credentials

        sentinel = "credential-target-secret"
        target = tmp_path / sentinel
        target.write_text('{"registered": true}', encoding="utf-8")
        creds = tmp_path / "creds.json"
        creds.symlink_to(target)

        self._assert_fixed_failure(
            classify_saved_credentials(creds), creds, target, sentinel
        )

    @pytest.mark.skipif(os.name == "nt", reason="POSIX FIFO semantics")
    def test_fifo_credentials_fail_without_blocking(self, tmp_path):
        creds = tmp_path / "creds.json"
        os.mkfifo(creds)
        probe = textwrap.dedent(
            """
            import sys
            from pathlib import Path
            from plugins.platforms.whatsapp.adapter import classify_saved_credentials

            result = classify_saved_credentials(Path(sys.argv[1]))
            raise SystemExit(
                0 if result and result[0] == "whatsapp_credentials_invalid" else 2
            )
            """
        )
        try:
            result = subprocess.run(
                [sys.executable, "-c", probe, str(creds)],
                env=os.environ.copy(),
                timeout=1,
            )
        except subprocess.TimeoutExpired:
            pytest.fail("credential FIFO inspection blocked")
        assert result.returncode == 0

    def test_oversize_credentials_are_rejected_before_json_parse(self, tmp_path):
        from plugins.platforms.whatsapp.adapter import classify_saved_credentials

        creds = tmp_path / "creds.json"
        creds.write_bytes(b'{"registered":true}' + b" " * (1024 * 1024 + 1))

        self._assert_fixed_failure(classify_saved_credentials(creds), creds)

    def test_invalid_utf8_credentials_are_fixed_failure(self, tmp_path):
        from plugins.platforms.whatsapp.adapter import classify_saved_credentials

        creds = tmp_path / "creds.json"
        creds.write_bytes(b"{\xff}")

        self._assert_fixed_failure(classify_saved_credentials(creds), creds)

    def test_replacement_between_lstat_and_open_is_fixed_failure(
        self, tmp_path, monkeypatch
    ):
        from plugins.platforms.whatsapp import adapter as adapter_module

        creds = tmp_path / "creds.json"
        creds.write_text('{"registered": true}', encoding="utf-8")
        original = tmp_path / "original-creds"
        real_open = os.open
        swapped = False

        def swap_then_open(path, flags, *args, **kwargs):
            nonlocal swapped
            if Path(path) == creds and not swapped:
                swapped = True
                creds.replace(original)
                creds.write_text('{"registered": true}', encoding="utf-8")
            return real_open(path, flags, *args, **kwargs)

        monkeypatch.setattr(adapter_module.os, "open", swap_then_open)

        self._assert_fixed_failure(
            adapter_module.classify_saved_credentials(creds), creds, original
        )


# ---------------------------------------------------------------------------
# Durable revoked-session marker
# ---------------------------------------------------------------------------

def _write_marker(session_dir: Path, payload: str) -> Path:
    """Write a raw revoked-session marker exactly as the bridge would name it."""
    session_dir.mkdir(parents=True, exist_ok=True)
    marker = session_dir / "revoked.json"
    marker.write_text(payload, encoding="utf-8")
    return marker


#: A marker in the shape the Node bridge actually writes (bridge_helpers.js
#: ``writeSessionRevokedMarker``), reproduced here rather than read from JS.
_VALID_MARKER = (
    '{"revoked":true,"statusCode":401,"reason":"conflict",'
    '"detail":"device_removed","at":"2026-08-25T06:00:00.000Z"}'
)


class TestRevokedSessionMarker:
    """A revoked session must stay dead across process boundaries.

    Exit code 78 only reaches us while we are the parent of a live bridge.
    It is invisible on the supported *reuse* path, where Hermes attaches to a
    bridge somebody else started and never observes an exit code at all — and
    it is gone entirely once the gateway itself restarts.

    So the bridge also records the verdict durably, as
    ``<session>/revoked.json``, immediately before exiting 78.  Re-pairing
    removes the whole session directory (``hermes whatsapp`` rmtree()s it),
    so a fresh session is naturally unmarked.

    The marker is written by Node and read by Python.  The two sides agreeing
    is checked by running the real writer against the real parser — see
    ``TestCrossLanguageMarkerContract`` below.  The cases here fix the Python
    side's reading of markers it did not write.
    """

    # -- preflight: refuse before any bridge or socket is attempted --------

    def test_valid_marker_is_non_retryable_before_any_bridge_starts(self, tmp_path):
        from plugins.platforms.whatsapp.adapter import classify_saved_credentials

        (tmp_path / "creds.json").write_text('{"registered": true}')
        _write_marker(tmp_path, _VALID_MARKER)

        result = classify_saved_credentials(tmp_path / "creds.json")

        assert result is not None, "a revoked session must not reach the bridge"
        code, message = result
        assert code == "whatsapp_logged_out"
        assert "hermes whatsapp" in message

    def test_marker_wins_even_when_creds_look_perfectly_usable(self, tmp_path):
        """The creds file is intact — WhatsApp revoked the device anyway.

        That is the failure this guards: valid-looking credentials the
        server will never accept again.
        """
        from plugins.platforms.whatsapp.adapter import classify_saved_credentials

        (tmp_path / "creds.json").write_text(
            '{"registered": true, "me": {"id": "1@s.whatsapp.net"}}'
        )
        _write_marker(tmp_path, _VALID_MARKER)

        result = classify_saved_credentials(tmp_path / "creds.json")
        assert result is not None
        code, _ = result
        assert code == "whatsapp_logged_out"

    def test_no_marker_leaves_a_parseable_session_alone(self, tmp_path):
        from plugins.platforms.whatsapp.adapter import classify_saved_credentials

        (tmp_path / "creds.json").write_text('{"registered": false}')
        assert classify_saved_credentials(tmp_path / "creds.json") is None

    @pytest.mark.parametrize(
        "payload",
        [
            "",
            "   \n",
            "not json at all",
            "{",
            "[]",
            "null",
            '"revoked"',
            "{}",
            '{"revoked": false}',
            '{"revoked": "true"}',
            '{"revoked": 1}',
            '{"statusCode": 401}',
        ],
        ids=[
            "empty", "whitespace", "garbage", "truncated", "array", "null",
            "string", "empty-object", "explicitly-false", "string-true",
            "int-one", "no-verdict",
        ],
    )
    def test_unusable_marker_fails_closed(self, tmp_path, payload):
        """An uninterpretable marker is treated as a revocation.

        The marker is a single file, in a directory the operator's account can
        write, recording the one verdict that must survive process death.  If
        content we cannot interpret meant "no evidence", corrupting one byte —
        or a write torn by a crash — would silently disarm it and the gateway
        would drive a session WhatsApp has already destroyed forever.

        Failing closed is recoverable in one explicit step (the destructive
        reset `hermes whatsapp` and the dashboard already offer); failing open
        is not recoverable at all, because nothing is left to notice.
        """
        from plugins.platforms.whatsapp.adapter import classify_saved_credentials

        (tmp_path / "creds.json").write_text('{"registered": true}')
        _write_marker(tmp_path, payload)

        result = classify_saved_credentials(tmp_path / "creds.json")

        assert result is not None, "an unusable marker must not be read as permission to connect"
        code, message = result
        assert code == "whatsapp_logged_out"
        assert "hermes whatsapp" in message, "the operator needs the recovery route"

    @pytest.mark.parametrize(
        "payload",
        ["", "{", '{"revoked": false}', "[]"],
        ids=["empty", "truncated", "explicitly-false", "array"],
    )
    def test_a_fail_closed_verdict_reflects_nothing_from_the_marker(self, tmp_path, payload):
        """The bytes we refused to trust must not come back out.

        Marker content is untrusted by assumption — that is *why* it is not
        trusted — and this message is printed to the operator and logged.  The
        verdict is the whole output; the file's contents, its path and the read
        error are not part of it.
        """
        from plugins.platforms.whatsapp.adapter import classify_saved_credentials

        sentinel = "SENTINEL_MARKER_CONTENT_MUST_NOT_LEAK"
        (tmp_path / "creds.json").write_text('{"registered": true}')
        _write_marker(tmp_path, f'{payload}{sentinel}')

        result = classify_saved_credentials(tmp_path / "creds.json")
        assert result is not None
        _, message = result

        assert sentinel not in message
        assert str(tmp_path) not in message
        assert "revoked.json" not in message

    def test_an_unreadable_marker_fails_closed(self, tmp_path):
        """Anything except "it is not there" is an unreadable marker.

        A directory standing where the file belongs raises ``IsADirectoryError``
        — an ``OSError`` that is emphatically not ``ENOENT``.  Treating every
        ``OSError`` as absence let any read failure disarm the verdict.
        """
        from gateway.platforms.whatsapp_common import is_whatsapp_session_revoked

        (tmp_path / "revoked.json").mkdir()

        assert is_whatsapp_session_revoked(tmp_path) is True

    @pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits only")
    def test_a_marker_that_cannot_be_read_fails_closed(self, tmp_path):
        """The most plausible way to disarm the verdict: make it unreadable."""
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            pytest.skip("root ignores file permissions")

        from gateway.platforms.whatsapp_common import is_whatsapp_session_revoked

        marker = _write_marker(tmp_path, _VALID_MARKER)
        marker.chmod(0o000)
        try:
            assert is_whatsapp_session_revoked(tmp_path) is True
        finally:
            marker.chmod(0o600)

    def test_an_absent_marker_is_the_only_no_evidence_state(self, tmp_path):
        """ENOENT — no marker, and no directory at all — means not revoked.

        This is what a freshly paired session looks like, and what an explicit
        reset leaves behind, so it must never be blocked.
        """
        from gateway.platforms.whatsapp_common import is_whatsapp_session_revoked

        assert is_whatsapp_session_revoked(tmp_path) is False
        assert is_whatsapp_session_revoked(tmp_path / "never-created") is False

    def test_marker_inspection_never_reads_present_contents(self, tmp_path):
        from gateway.platforms.whatsapp_common import is_whatsapp_session_revoked

        marker = tmp_path / "revoked.json"
        marker.write_bytes(b"content must remain untouched")

        with patch.object(
            Path,
            "read_text",
            side_effect=AssertionError("marker contents were read"),
        ):
            assert is_whatsapp_session_revoked(tmp_path) is True

    def test_invalid_utf8_marker_is_terminal_presence(self, tmp_path):
        from gateway.platforms.whatsapp_common import is_whatsapp_session_revoked

        (tmp_path / "revoked.json").write_bytes(b"\xff\xfe\x80")

        assert is_whatsapp_session_revoked(tmp_path) is True

    @pytest.mark.require_symlinks
    def test_dangling_marker_symlink_is_terminal_presence(self, tmp_path):
        from gateway.platforms.whatsapp_common import is_whatsapp_session_revoked

        (tmp_path / "revoked.json").symlink_to(tmp_path / "missing-target")

        assert is_whatsapp_session_revoked(tmp_path) is True

    @pytest.mark.skipif(os.name == "nt", reason="POSIX FIFO semantics")
    def test_fifo_marker_is_terminal_without_blocking(self, tmp_path):
        marker = tmp_path / "revoked.json"
        os.mkfifo(marker)
        probe = textwrap.dedent(
            """
            import sys
            from pathlib import Path
            from gateway.platforms.whatsapp_common import is_whatsapp_session_revoked

            raise SystemExit(
                0 if is_whatsapp_session_revoked(Path(sys.argv[1])) else 2
            )
            """
        )
        try:
            result = subprocess.run(
                [sys.executable, "-c", probe, str(tmp_path)],
                env=os.environ.copy(),
                timeout=1,
            )
        except subprocess.TimeoutExpired:
            pytest.fail("revoked marker FIFO inspection blocked")
        assert result.returncode == 0

    def test_marker_is_read_from_the_session_directory(self, tmp_path):
        """The marker is a *sibling* of creds.json, not somewhere global."""
        from plugins.platforms.whatsapp.adapter import classify_saved_credentials

        session = tmp_path / "session"
        session.mkdir()
        (session / "creds.json").write_text('{"registered": true}')
        _write_marker(tmp_path, _VALID_MARKER)  # parent dir — wrong place

        assert classify_saved_credentials(session / "creds.json") is None

    def test_marker_alone_without_creds_still_reports_revoked(self, tmp_path):
        """A revoked session whose creds were also removed is still revoked,
        and the clearer of the two messages should win."""
        from plugins.platforms.whatsapp.adapter import classify_saved_credentials

        _write_marker(tmp_path, _VALID_MARKER)
        result = classify_saved_credentials(tmp_path / "creds.json")
        assert result is not None
        code, _ = result
        assert code == "whatsapp_logged_out"

    # -- runtime: the reused / external bridge path ------------------------

    @staticmethod
    def _runtime_adapter(tmp_path, bridge_process):
        adapter = _make_adapter(session_path=tmp_path)
        adapter.set_fatal_error_handler(AsyncMock())
        adapter._running = True
        adapter._http_session = MagicMock()
        adapter._bridge_log_fh = MagicMock()
        adapter._bridge_process = bridge_process
        return adapter

    @pytest.mark.asyncio
    async def test_marker_is_fatal_when_the_bridge_is_not_ours(self, tmp_path):
        """``_bridge_process is None`` is the supported reuse path.

        There is no ``returncode`` to inspect here — the bridge belongs to
        somebody else.  Before the marker, this returned None forever and the
        gateway kept driving a revoked session.
        """
        adapter = self._runtime_adapter(tmp_path, bridge_process=None)
        _write_marker(tmp_path, _VALID_MARKER)

        message = await adapter._check_managed_bridge_exit()

        assert message, "a reused bridge on a revoked session must report fatal"
        assert adapter.fatal_error_code == "whatsapp_logged_out"
        assert adapter.fatal_error_retryable is False
        assert "hermes whatsapp" in adapter.fatal_error_message
        adapter._fatal_error_handler.assert_awaited_once()
        adapter._bridge_log_fh is None

    @pytest.mark.asyncio
    async def test_reused_bridge_send_fails_on_a_revoked_session(self, tmp_path):
        """End-to-end through a caller: send() consults the same helper, so a
        revoked session stops accepting work even with no child process."""
        adapter = self._runtime_adapter(tmp_path, bridge_process=None)
        _write_marker(tmp_path, _VALID_MARKER)

        result = await adapter.send("chat-123", "hello")

        assert result.success is False
        assert adapter.fatal_error_code == "whatsapp_logged_out"
        assert adapter.fatal_error_retryable is False

    @pytest.mark.asyncio
    async def test_reused_bridge_without_a_marker_is_not_fatal(self, tmp_path):
        """The overwhelmingly common case: no marker, nothing to report."""
        adapter = self._runtime_adapter(tmp_path, bridge_process=None)

        assert await adapter._check_managed_bridge_exit() is None
        assert adapter.fatal_error_code is None
        adapter._fatal_error_handler.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unusable_marker_is_fatal_at_runtime_too(self, tmp_path):
        """The runtime path reads the same marker, so it fails closed the same
        way — otherwise a corrupted marker would stop preflight and then let
        the very next poll keep driving the session anyway."""
        adapter = self._runtime_adapter(tmp_path, bridge_process=None)
        _write_marker(tmp_path, '{"revoked": fal')

        assert await adapter._check_managed_bridge_exit()
        assert adapter.fatal_error_code == "whatsapp_logged_out"
        assert adapter.fatal_error_retryable is False

    @pytest.mark.asyncio
    async def test_operator_is_notified_only_once(self, tmp_path):
        """The poll loop and every send() call this helper; a revoked session
        must not spray one notification per call."""
        adapter = self._runtime_adapter(tmp_path, bridge_process=None)
        _write_marker(tmp_path, _VALID_MARKER)

        for _ in range(5):
            assert await adapter._check_managed_bridge_exit()

        adapter._fatal_error_handler.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_marker_is_fatal_even_while_a_managed_bridge_still_runs(self, tmp_path):
        """A live child that has already been revoked (it writes the marker
        immediately *before* exiting) must not be treated as healthy just
        because ``poll()`` has not caught up yet."""
        live = MagicMock()
        live.poll.return_value = None
        adapter = self._runtime_adapter(tmp_path, bridge_process=live)
        _write_marker(tmp_path, _VALID_MARKER)

        assert await adapter._check_managed_bridge_exit()
        assert adapter.fatal_error_code == "whatsapp_logged_out"

    @pytest.mark.asyncio
    async def test_shutdown_does_not_manufacture_a_revoked_fatal(self, tmp_path):
        """Planned shutdown is not a failure to report, marker or not."""
        adapter = self._runtime_adapter(tmp_path, bridge_process=None)
        adapter._shutting_down = True
        _write_marker(tmp_path, _VALID_MARKER)

        assert await adapter._check_managed_bridge_exit() is None
        adapter._fatal_error_handler.assert_not_awaited()


# ---------------------------------------------------------------------------
# Cross-language contract
# ---------------------------------------------------------------------------

class TestCrossLanguageMarkerContract:
    """The Node writer and the Python parser must agree, in practice.

    The marker is a two-language contract: Node writes it, Python decides a
    session is unrecoverable because of it.  Asserting the same literals in
    both suites proves nothing — two independently-wrong sides pass both.
    So this runs the bridge's *real* ``writeSessionRevokedMarker`` against a
    throwaway directory and hands the result to the *real* Python parser,
    with nothing about the file's name, location or shape restated here.

    Nothing about the marker's contents is read or printed by these tests;
    only the parser's boolean verdict and the file's mode are asserted.
    """

    #: bridge_helpers.js imports nothing but Node builtins, so this needs no
    #: ``npm install`` — only a Node binary.
    _WRITER = (
        "const { writeSessionRevokedMarker } = await import(process.argv[1]);\n"
        "writeSessionRevokedMarker(process.argv[2], "
        "{ statusCode: 401, reason: 'conflict', detail: 'device_removed' }, "
        "{ log: () => {} });\n"
    )

    @staticmethod
    def _write_marker_with_node(session_dir: Path) -> None:
        """Have the real Node writer mark ``session_dir`` revoked."""
        import shutil
        import subprocess
        from urllib.request import pathname2url

        node = shutil.which("node")
        if node is None:
            pytest.skip("node is not installed")

        helpers = (
            Path(__file__).resolve().parents[2]
            / "scripts" / "whatsapp-bridge" / "bridge_helpers.js"
        )
        result = subprocess.run(
            [
                node, "--input-type=module", "-e", TestCrossLanguageMarkerContract._WRITER,
                f"file://{pathname2url(str(helpers))}", str(session_dir),
            ],
            capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, f"the Node marker writer failed: {result.stderr}"

    def test_python_recognizes_a_marker_written_by_node(self, tmp_path):
        from gateway.platforms.whatsapp_common import is_whatsapp_session_revoked

        session_dir = tmp_path / "session"
        session_dir.mkdir()
        assert is_whatsapp_session_revoked(session_dir) is False, "precondition"

        self._write_marker_with_node(session_dir)

        assert is_whatsapp_session_revoked(session_dir) is True

    def test_the_adapter_refuses_a_session_node_marked(self, tmp_path):
        """The consequence that matters: a Node-written marker is terminal."""
        from plugins.platforms.whatsapp.adapter import classify_saved_credentials

        session_dir = tmp_path / "session"
        session_dir.mkdir()
        (session_dir / "creds.json").write_text('{"registered": true}', encoding="utf-8")
        self._write_marker_with_node(session_dir)

        result = classify_saved_credentials(session_dir / "creds.json")

        assert result is not None
        assert result[0] == "whatsapp_logged_out"

    @pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits only")
    def test_the_node_written_marker_is_owner_only(self, tmp_path):
        """It sits beside creds.json, so it inherits the same expectation."""
        session_dir = tmp_path / "session"
        session_dir.mkdir()
        self._write_marker_with_node(session_dir)

        written = list(session_dir.iterdir())
        assert len(written) == 1, "the writer must leave exactly the marker behind"
        assert written[0].stat().st_mode & 0o777 == 0o600


class TestResetWhatsAppSessionDir:
    """The recovery lease is the one destructive session-reset path.

    Its whole job is to leave *no* auth state behind, because what follows it
    is a fresh pairing.  So it must either succeed completely or fail loudly:
    swallowing an error and returning normally would start pairing on top of
    credentials that are still on disk, which is the state the caller was
    trying to escape.
    """

    @staticmethod
    def _acquire_lease(tmp_path, monkeypatch, session_dir):
        from gateway.platforms import whatsapp_recovery as recovery

        monkeypatch.setenv("HERMES_GATEWAY_LOCK_DIR", str(tmp_path / "locks"))
        monkeypatch.setattr(recovery, "_listener_pids_on_port", lambda _port: [])
        return recovery.acquire_whatsapp_recovery_lease(
            session_dir,
            timeout=0.2,
            poll_interval=0.001,
        )

    def test_removes_credentials_and_marker_together(self, tmp_path, monkeypatch):
        from gateway.platforms.whatsapp_common import is_whatsapp_session_revoked

        session_dir = tmp_path / "session"
        _write_marker(session_dir, _VALID_MARKER)
        (session_dir / "creds.json").write_text("{}", encoding="utf-8")
        (session_dir / "app-state-sync-key-AAA.json").write_text("{}", encoding="utf-8")

        with self._acquire_lease(tmp_path, monkeypatch, session_dir) as lease:
            lease.reset_session(session_dir)

        assert session_dir.is_dir(), "the directory itself must be ready for re-pairing"
        assert list(session_dir.iterdir()) == []
        assert is_whatsapp_session_revoked(session_dir) is False

    @pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits only")
    def test_recreates_the_directory_owner_only(self, tmp_path, monkeypatch):
        """The directory is about to hold key material, so it is 0700.

        ``mkdir()`` without a mode takes the process umask, which on a typical
        install yields 0755 — a world-readable directory for Baileys' key
        files.  The bridge already writes its own files 0600; the directory
        that holds them must not be laxer.
        """
        session_dir = tmp_path / "session"
        session_dir.mkdir()
        previous_stat = session_dir.stat()

        with self._acquire_lease(tmp_path, monkeypatch, session_dir) as lease:
            lease.reset_session()

        recreated_stat = session_dir.stat()
        assert recreated_stat.st_mode & 0o777 == 0o700
        assert recreated_stat.st_uid == previous_stat.st_uid
        assert recreated_stat.st_gid == previous_stat.st_gid

    @pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits only")
    def test_a_failed_preflight_is_raised_without_deletion(
        self, tmp_path, monkeypatch
    ):
        """A reset that cannot prove deletion is safe must not report success.

        Returning normally here told `hermes whatsapp` and the dashboard to
        print "Session cleared" and start pairing over auth state that is
        still there.
        """
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            pytest.skip("root ignores directory permissions")

        from gateway.platforms import whatsapp_recovery as recovery

        session_dir = tmp_path / "session"
        session_dir.mkdir()
        (session_dir / "creds.json").write_text("{}", encoding="utf-8")
        session_dir.chmod(0o500)  # entries cannot be unlinked from a read-only dir
        lease = self._acquire_lease(tmp_path, monkeypatch, session_dir)
        try:
            with pytest.raises(recovery.WhatsAppRecoveryError):
                lease.reset_session()
            assert (session_dir / "creds.json").exists(), (
                "the credentials are still there — the caller must not be told otherwise"
            )
        finally:
            lease.release()
            session_dir.chmod(0o700)


class TestWhatsAppDiagnosticPrivacy:
    """Private bridge values never cross user-visible diagnostic boundaries."""

    @pytest.mark.asyncio
    async def test_missing_bridge_path_is_absent_from_logger_and_fatal_state(
        self, caplog
    ):
        adapter = _make_adapter()
        adapter._bridge_script = f"{_PRIVATE_PATH}/bridge.js"

        with caplog.at_level("WARNING"), patch(
            "plugins.platforms.whatsapp.adapter.check_whatsapp_requirements",
            return_value=True,
        ), patch.object(Path, "exists", return_value=False):
            result = await adapter.connect()

        assert result is False
        assert adapter.fatal_error_code == "whatsapp_bridge_missing"
        assert adapter.fatal_error_retryable is False
        diagnostics = "\n".join([*caplog.messages, adapter.fatal_error_message or ""])
        _assert_private_values_absent(diagnostics)

    @pytest.mark.asyncio
    async def test_found_bridge_path_is_absent_from_logger(self, caplog):
        adapter = _make_adapter()
        adapter._bridge_script = f"{_PRIVATE_PATH}/bridge.js"
        adapter._acquire_platform_lock = MagicMock(return_value=False)
        connect_gate = MagicMock()

        with caplog.at_level("INFO"), patch(
            "plugins.platforms.whatsapp.adapter.check_whatsapp_requirements",
            return_value=True,
        ), patch(
            "plugins.platforms.whatsapp.adapter.whatsapp_bridge_runtime_hash",
            return_value="a" * 64,
        ), patch.object(Path, "exists", return_value=True), patch(
            "plugins.platforms.whatsapp.adapter.acquire_whatsapp_connect_gate",
            return_value=connect_gate,
        ), patch(
            "plugins.platforms.whatsapp.adapter.classify_saved_credentials",
            return_value=None,
        ):
            result = await adapter.connect()

        assert result is False
        assert any("Bridge found" in message for message in caplog.messages)
        _assert_private_values_absent("\n".join(caplog.messages))
        connect_gate.release.assert_called_once()

    @pytest.mark.asyncio
    async def test_recovery_gate_exception_value_is_private_but_class_is_logged(
        self, caplog
    ):
        adapter = _make_adapter()
        adapter._bridge_script = f"{_PRIVATE_PATH}/bridge.js"

        with caplog.at_level("WARNING"), patch(
            "plugins.platforms.whatsapp.adapter.check_whatsapp_requirements",
            return_value=True,
        ), patch(
            "plugins.platforms.whatsapp.adapter.whatsapp_bridge_runtime_hash",
            return_value="a" * 64,
        ), patch.object(Path, "exists", return_value=True), patch(
            "plugins.platforms.whatsapp.adapter.acquire_whatsapp_connect_gate",
            side_effect=DiagnosticProbeError(_PRIVATE_VALUE),
        ):
            result = await adapter.connect()

        assert result is False
        assert adapter.fatal_error_code == "whatsapp_recovery_lock"
        assert adapter.fatal_error_retryable is True
        diagnostics = "\n".join([*caplog.messages, adapter.fatal_error_message or ""])
        _assert_private_values_absent(diagnostics)
        assert "DiagnosticProbeError" in diagnostics

    @pytest.mark.asyncio
    async def test_session_lock_exception_value_and_paths_are_private(self, caplog):
        adapter = _make_adapter(session_path=_PRIVATE_PATH)
        adapter._bridge_script = f"{_PRIVATE_PATH}/bridge.js"
        adapter._acquire_platform_lock = MagicMock(
            side_effect=DiagnosticProbeError(_PRIVATE_VALUE)
        )
        connect_gate = MagicMock()

        with caplog.at_level("WARNING"), patch(
            "plugins.platforms.whatsapp.adapter.check_whatsapp_requirements",
            return_value=True,
        ), patch(
            "plugins.platforms.whatsapp.adapter.whatsapp_bridge_runtime_hash",
            return_value="a" * 64,
        ), patch.object(Path, "exists", return_value=True), patch(
            "plugins.platforms.whatsapp.adapter.acquire_whatsapp_connect_gate",
            return_value=connect_gate,
        ), patch(
            "plugins.platforms.whatsapp.adapter.classify_saved_credentials",
            return_value=None,
        ):
            result = await adapter.connect()

        assert result is False
        _assert_private_values_absent("\n".join(caplog.messages))
        assert "DiagnosticProbeError" in "\n".join(caplog.messages)
        connect_gate.release.assert_called_once()

    @pytest.mark.asyncio
    async def test_npm_stderr_paths_and_exception_values_are_private(
        self, caplog
    ):
        adapter = _make_adapter(session_path=f"{_PRIVATE_PATH}/auth")
        adapter._bridge_script = f"{_PRIVATE_PATH}/bridge.js"
        install_result = MagicMock(returncode=1, stderr=_PRIVATE_VALUE)
        connect_gate = MagicMock()

        with caplog.at_level("WARNING"), patch("builtins.print") as mock_print, patch(
            "plugins.platforms.whatsapp.adapter.check_whatsapp_requirements",
            return_value=True,
        ), patch(
            "plugins.platforms.whatsapp.adapter.whatsapp_bridge_runtime_hash",
            return_value="a" * 64,
        ), patch.object(Path, "exists", return_value=True), patch(
            "plugins.platforms.whatsapp.adapter.acquire_whatsapp_connect_gate",
            return_value=connect_gate,
        ), patch(
            "plugins.platforms.whatsapp.adapter.classify_saved_credentials",
            return_value=None,
        ), patch(
            "plugins.platforms.whatsapp.adapter.whatsapp_bridge_dependencies_fresh",
            return_value=False,
        ), patch(
            "gateway.status.acquire_scoped_lock", return_value=(True, None)
        ), patch("subprocess.run", return_value=install_result):
            result = await adapter.connect()

        assert result is False
        assert adapter.fatal_error_code == "whatsapp_npm_install_failed"
        assert adapter.fatal_error_retryable is False
        diagnostics = "\n".join(
            [*caplog.messages, _printed_output(mock_print), adapter.fatal_error_message or ""]
        )
        _assert_private_values_absent(diagnostics)
        connect_gate.release.assert_called_once()

    @pytest.mark.asyncio
    async def test_npm_exception_value_is_private_but_class_is_logged(self, caplog):
        adapter = _make_adapter(session_path=f"{_PRIVATE_PATH}/auth")
        adapter._bridge_script = f"{_PRIVATE_PATH}/bridge.js"
        connect_gate = MagicMock()

        with caplog.at_level("WARNING"), patch("builtins.print") as mock_print, patch(
            "plugins.platforms.whatsapp.adapter.check_whatsapp_requirements",
            return_value=True,
        ), patch(
            "plugins.platforms.whatsapp.adapter.whatsapp_bridge_runtime_hash",
            return_value="a" * 64,
        ), patch.object(Path, "exists", return_value=True), patch(
            "plugins.platforms.whatsapp.adapter.acquire_whatsapp_connect_gate",
            return_value=connect_gate,
        ), patch(
            "plugins.platforms.whatsapp.adapter.classify_saved_credentials",
            return_value=None,
        ), patch(
            "plugins.platforms.whatsapp.adapter.whatsapp_bridge_dependencies_fresh",
            return_value=False,
        ), patch(
            "gateway.status.acquire_scoped_lock", return_value=(True, None)
        ), patch(
            "subprocess.run", side_effect=DiagnosticProbeError(_PRIVATE_VALUE)
        ):
            result = await adapter.connect()

        assert result is False
        assert adapter.fatal_error_code == "whatsapp_npm_install_failed"
        assert adapter.fatal_error_retryable is False
        diagnostics = "\n".join(
            [*caplog.messages, _printed_output(mock_print), adapter.fatal_error_message or ""]
        )
        _assert_private_values_absent(diagnostics)
        assert "DiagnosticProbeError" in diagnostics

    @pytest.mark.asyncio
    async def test_startup_status_and_log_path_are_not_printed(self):
        adapter = _make_adapter(session_path=_PRIVATE_PATH)
        adapter._bridge_script = f"{_PRIVATE_PATH}/bridge.js"
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_client_cls = _mock_aiohttp(
            status=200, json_data={"status": _PRIVATE_VALUE}
        )

        with patch("builtins.print") as mock_print, _connect_patches(
            mock_proc, MagicMock(), mock_client_cls
        ), patch.object(type(adapter), "_poll_messages", return_value=MagicMock()):
            result = await adapter.connect()

        assert result is True
        _assert_private_values_absent(_printed_output(mock_print))

    @pytest.mark.asyncio
    async def test_startup_exception_value_is_private_but_class_is_logged(self, caplog):
        adapter = _make_adapter(session_path=_PRIVATE_PATH)
        adapter._bridge_script = f"{_PRIVATE_PATH}/bridge.js"

        with caplog.at_level("ERROR"), _connect_patches(
            MagicMock(), MagicMock()
        ), patch(
            "subprocess.Popen", side_effect=DiagnosticProbeError(_PRIVATE_VALUE)
        ):
            result = await adapter.connect()

        assert result is False
        _assert_private_values_absent("\n".join(caplog.messages))
        assert "DiagnosticProbeError" in "\n".join(caplog.messages)
        assert all(record.exc_info is None for record in caplog.records)

    @pytest.mark.asyncio
    async def test_runtime_exit_keeps_code_and_retryability_without_returncode(self, caplog):
        adapter = _make_adapter()
        adapter.set_fatal_error_handler(AsyncMock())
        adapter._running = True
        adapter._http_session = MagicMock()
        proc = MagicMock()
        proc.poll.return_value = 314159
        adapter._bridge_process = proc

        with caplog.at_level("ERROR"):
            result = await adapter.send(_PRIVATE_JID, "hello")

        assert result.success is False
        assert adapter.fatal_error_code == "whatsapp_bridge_exited"
        assert adapter.fatal_error_retryable is True
        assert "314159" not in (result.error or "")
        assert "314159" not in "\n".join(caplog.messages)

    @pytest.mark.asyncio
    async def test_disconnect_exception_value_is_not_printed(self, caplog):
        adapter = _make_adapter(session_path=_PRIVATE_PATH)
        adapter._running = True
        adapter._poll_task = None
        adapter._http_session = None
        adapter._bridge_process = MagicMock()

        with caplog.at_level("WARNING"), patch("builtins.print") as mock_print, patch(
            "plugins.platforms.whatsapp.adapter._terminate_bridge_process",
            side_effect=DiagnosticProbeError(_PRIVATE_VALUE),
        ):
            await adapter.disconnect()

        diagnostics = "\n".join([*caplog.messages, _printed_output(mock_print)])
        _assert_private_values_absent(diagnostics)
        assert "DiagnosticProbeError" in diagnostics

    @pytest.mark.asyncio
    async def test_poll_exception_value_is_not_printed(self, caplog):
        adapter = _make_adapter()
        adapter._running = True
        adapter._bridge_process = None
        adapter._http_session = MagicMock()
        adapter._http_session.get.side_effect = DiagnosticProbeError(_PRIVATE_VALUE)
        adapter._check_managed_bridge_exit = AsyncMock(return_value=None)

        async def stop_polling(*_args, **_kwargs):
            adapter._running = False

        with caplog.at_level("WARNING"), patch("builtins.print") as mock_print, patch(
            "plugins.platforms.whatsapp.adapter.asyncio.sleep",
            side_effect=stop_polling,
        ):
            await adapter._poll_messages()

        diagnostics = "\n".join([*caplog.messages, _printed_output(mock_print)])
        _assert_private_values_absent(diagnostics)
        assert "DiagnosticProbeError" in diagnostics

    @pytest.mark.asyncio
    async def test_chat_info_failure_omits_jid_and_exception_value(self, caplog):
        adapter = _make_adapter()
        adapter._running = True
        adapter._http_session = MagicMock()
        adapter._http_session.get.side_effect = DiagnosticProbeError(_PRIVATE_VALUE)
        adapter._check_managed_bridge_exit = AsyncMock(return_value=None)

        with caplog.at_level("DEBUG"):
            result = await adapter.get_chat_info(_PRIVATE_JID)

        assert result == {"name": "Unknown", "type": "dm"}
        diagnostics = "\n".join(caplog.messages)
        _assert_private_values_absent(diagnostics)
        assert "DiagnosticProbeError" in diagnostics

    @pytest.mark.asyncio
    async def test_read_receipt_http_and_exception_diagnostics_are_fixed(self, caplog):
        adapter = _make_adapter()
        adapter._send_read_receipts = True
        adapter._http_session = MagicMock()
        response = MagicMock(status=599)
        adapter._http_session.post.return_value = _AsyncCM(response)

        with caplog.at_level("WARNING"):
            await adapter._send_read_receipt({"readReceiptKey": {"id": _PRIVATE_VALUE}})

        assert "599" not in "\n".join(caplog.messages)
        _assert_private_values_absent("\n".join(caplog.messages))

        caplog.clear()
        adapter._http_session.post.side_effect = DiagnosticProbeError(_PRIVATE_VALUE)
        with caplog.at_level("WARNING"):
            await adapter._send_read_receipt({"readReceiptKey": {"id": _PRIVATE_VALUE}})

        diagnostics = "\n".join(caplog.messages)
        _assert_private_values_absent(diagnostics)
        assert "DiagnosticProbeError" in diagnostics

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("method_name", "args"),
        [
            ("send", (_PRIVATE_JID, "hello")),
            ("edit_message", (_PRIVATE_JID, "message-id", "hello")),
            ("_send_media_to_bridge", (_PRIVATE_JID, _PRIVATE_PATH, "image")),
            ("send_poll", (_PRIVATE_JID, "Question?", ["One", "Two"])),
            ("send_location", (_PRIVATE_JID, 1.25, 2.5)),
        ],
        ids=["send", "edit", "media", "poll", "location"],
    )
    async def test_http_error_body_is_not_returned_but_status_is_preserved(
        self, method_name, args
    ):
        adapter = _make_adapter()
        adapter._running = True
        adapter._bridge_process = None
        response = MagicMock(status=503)
        response.text = AsyncMock(
            return_value=f"{_PRIVATE_VALUE} {_PRIVATE_PATH} {_PRIVATE_JID}"
        )
        adapter._http_session = MagicMock()
        adapter._http_session.post.return_value = _AsyncCM(response)

        with patch("plugins.platforms.whatsapp.adapter.os.path.exists", return_value=True):
            result = await getattr(adapter, method_name)(*args)

        assert result.success is False
        assert result.retryable is False
        assert "HTTP 503" in (result.error or "")
        _assert_private_values_absent(result.error or "")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("method_name", "args"),
        [
            ("send", (_PRIVATE_JID, "hello")),
            ("edit_message", (_PRIVATE_JID, "message-id", "hello")),
            ("_send_media_to_bridge", (_PRIVATE_JID, _PRIVATE_PATH, "image")),
            ("send_poll", (_PRIVATE_JID, "Question?", ["One", "Two"])),
            ("send_location", (_PRIVATE_JID, 1.25, 2.5)),
        ],
        ids=["send", "edit", "media", "poll", "location"],
    )
    async def test_transport_exception_value_is_not_returned(self, method_name, args):
        adapter = _make_adapter()
        adapter._running = True
        adapter._bridge_process = None
        adapter._http_session = MagicMock()
        adapter._http_session.post.side_effect = DiagnosticProbeError(
            f"{_PRIVATE_VALUE} {_PRIVATE_PATH} {_PRIVATE_JID}"
        )

        with patch("plugins.platforms.whatsapp.adapter.os.path.exists", return_value=True):
            result = await getattr(adapter, method_name)(*args)

        assert result.success is False
        assert result.retryable is False
        assert result.error
        _assert_private_values_absent(result.error)

    @pytest.mark.asyncio
    async def test_missing_media_path_is_not_returned(self):
        adapter = _make_adapter()
        adapter._running = True
        adapter._bridge_process = None
        adapter._http_session = MagicMock()

        with patch("plugins.platforms.whatsapp.adapter.os.path.exists", return_value=False):
            result = await adapter._send_media_to_bridge(
                _PRIVATE_JID, _PRIVATE_PATH, "document"
            )

        assert result.success is False
        _assert_private_values_absent(result.error or "")

    @pytest.mark.asyncio
    async def test_clarify_fallback_omits_native_poll_http_body(self, caplog):
        adapter = _make_adapter()
        adapter._running = True
        adapter._bridge_process = None
        response = MagicMock(status=503)
        response.text = AsyncMock(
            return_value=f"{_PRIVATE_VALUE} {_PRIVATE_PATH} {_PRIVATE_JID}"
        )
        adapter._http_session = MagicMock()
        adapter._http_session.post.return_value = _AsyncCM(response)

        with caplog.at_level("WARNING"):
            result = await adapter.send_clarify(
                chat_id=_PRIVATE_JID,
                question="Choose",
                choices=["One", "Two"],
                clarify_id="privacy-probe-clarify",
                session_key="whatsapp:dm:privacy-probe",
            )

        assert result.success is False
        assert "HTTP 503" in (result.error or "")
        diagnostics = "\n".join([*caplog.messages, result.error or ""])
        _assert_private_values_absent(diagnostics)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("media", [False, True], ids=["text", "media"])
    async def test_standalone_http_body_is_not_returned(self, media):
        from plugins.platforms.whatsapp.adapter import _standalone_send

        response = MagicMock(status=503)
        response.text = AsyncMock(
            return_value=f"{_PRIVATE_VALUE} {_PRIVATE_PATH} {_PRIVATE_JID}"
        )
        session = MagicMock()
        session.post.return_value = _AsyncCM(response)
        media_files = [(_PRIVATE_PATH, False)] if media else None
        message = "" if media else "hello"

        with patch("aiohttp.ClientSession", return_value=_AsyncCM(session)), patch(
            "plugins.platforms.whatsapp.adapter.os.path.exists", return_value=True
        ):
            result = await _standalone_send(
                MagicMock(extra={}),
                _PRIVATE_JID,
                message,
                media_files=media_files,
            )

        assert "HTTP 503" in result["error"]
        _assert_private_values_absent(result["error"])

    @pytest.mark.asyncio
    async def test_standalone_exception_and_missing_path_are_not_returned(self):
        from plugins.platforms.whatsapp.adapter import _standalone_send

        session = MagicMock()
        session.post.side_effect = DiagnosticProbeError(
            f"{_PRIVATE_VALUE} {_PRIVATE_PATH} {_PRIVATE_JID}"
        )

        with patch("aiohttp.ClientSession", return_value=_AsyncCM(session)):
            exception_result = await _standalone_send(
                MagicMock(extra={}), _PRIVATE_JID, "hello"
            )

        with patch("aiohttp.ClientSession", return_value=_AsyncCM(session)), patch(
            "plugins.platforms.whatsapp.adapter.os.path.exists", return_value=False
        ):
            missing_result = await _standalone_send(
                MagicMock(extra={}),
                _PRIVATE_JID,
                "",
                media_files=[(_PRIVATE_PATH, False)],
            )

        _assert_private_values_absent(exception_result["error"])
        _assert_private_values_absent(missing_result["error"])
