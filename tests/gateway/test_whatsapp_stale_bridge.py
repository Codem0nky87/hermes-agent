"""Tests for the WhatsApp stale-bridge staleness handshake.

Regression tests for the stale-bridge trap: ``connect()`` reused any
already-running bridge with ``status: connected`` unconditionally, and
``disconnect()`` only kills bridges the adapter spawned itself.  A
long-lived bridge process therefore survived gateway restarts AND
``hermes update``, serving pre-update bridge.js behavior forever (e.g.
no inbound media download → images/voice notes arrive as placeholders).

The fix: the bridge reports a composite SHA-256 runtime fingerprint in
``/health`` (``runtimeHash`` plus the compatibility ``scriptHash`` field);
the adapter compares it against every required source/helper and dependency
manifest on disk, then restarts the bridge on mismatch. Bridges that predate
the handshake report no valid hash and are treated as stale by definition.

Also covers the npm dependency-refresh stamp: deps are reinstalled when
package.json or package-lock.json changes, not only when node_modules is missing.
"""

import asyncio
import json
import os
from pathlib import Path
import shutil
import subprocess
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway.config import Platform


class _AsyncCM:
    """Minimal async context manager returning a fixed value."""

    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *exc):
        return False


def _make_adapter(bridge_script: str = "/tmp/test-bridge.js",
                  session_path: Path = Path("/tmp/test-wa-session")):
    """Create a WhatsAppAdapter with test attributes (bypass __init__)."""
    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter

    adapter = WhatsAppAdapter.__new__(WhatsAppAdapter)
    adapter.platform = Platform.WHATSAPP
    adapter.config = MagicMock()
    adapter._bridge_port = 19876
    adapter._bridge_script = bridge_script
    adapter._session_path = session_path
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


def _mock_health(json_data):
    """Mock aiohttp.ClientSession whose GET returns 200 + *json_data*."""
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value=json_data)
    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=_AsyncCM(mock_resp))
    mock_session.close = AsyncMock()
    return MagicMock(return_value=_AsyncCM(mock_session))


def _setup_bridge_dir(tmp_path: Path) -> Path:
    """Create a real bridge dir with every required production input + creds."""
    from gateway.platforms.whatsapp_common import WHATSAPP_BRIDGE_RUNTIME_INPUTS

    bridge_dir = tmp_path / "whatsapp-bridge"
    bridge_dir.mkdir()
    for filename in WHATSAPP_BRIDGE_RUNTIME_INPUTS:
        (bridge_dir / filename).write_text(f"// current {filename}\n")
    (bridge_dir / "package.json").write_text('{"name": "bridge"}\n')
    (bridge_dir / "package-lock.json").write_text(
        '{"lockfileVersion":3,"packages":{}}\n'
    )
    session_path = tmp_path / "session"
    session_path.mkdir(mode=0o700)
    # A genuinely paired session: complete synthetic identity, owner-only.
    # `{}` parses as JSON but is not a paired session, and the managed-start
    # gate rejects it — these tests are about bridge staleness, not pairing.
    creds = session_path / "creds.json"
    creds.write_text('{"me": {"id": "15550000001@s.whatsapp.net"}}')
    os.chmod(creds, 0o600)
    return bridge_dir


def _fresh_node_modules(bridge_dir: Path) -> None:
    """Create node_modules with a stamp matching package.json + package-lock."""
    from gateway.platforms.whatsapp_common import (
        write_whatsapp_bridge_dependency_stamp,
    )

    nm = bridge_dir / "node_modules"
    nm.mkdir()
    write_whatsapp_bridge_dependency_stamp(bridge_dir)


class TestCompositeRuntimeHash:
    @staticmethod
    def _node_fingerprint(root: Path, names: tuple[str, ...]) -> dict:
        node = shutil.which("node")
        if not node:
            pytest.skip("node is required for the cross-language hash contract")
        helper = (
            Path(__file__).resolve().parents[2]
            / "scripts"
            / "whatsapp-bridge"
            / "bridge_helpers.js"
        )
        script = """
          const mod = await import(process.argv[1]);
          const result = mod.fingerprintBridgeInputs(
            process.argv[2], JSON.parse(process.argv[3])
          );
          process.stdout.write(JSON.stringify(result));
        """
        result = subprocess.run(
            [node, "--input-type=module", "-e", script, helper.as_uri(), str(root), json.dumps(names)],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        return json.loads(result.stdout)

    def test_python_and_node_hash_the_same_present_inputs(self, tmp_path):
        from gateway.platforms.whatsapp_common import (
            WHATSAPP_BRIDGE_RUNTIME_INPUTS,
            whatsapp_bridge_runtime_hash,
        )

        for filename in WHATSAPP_BRIDGE_RUNTIME_INPUTS:
            (tmp_path / filename).write_bytes(f"production:{filename}\n".encode())

        python_hash = whatsapp_bridge_runtime_hash(tmp_path)
        node_result = self._node_fingerprint(
            tmp_path, tuple(WHATSAPP_BRIDGE_RUNTIME_INPUTS)
        )

        assert node_result == {"hash": python_hash, "allPresent": True}
        assert len(python_hash) == 64

    def test_python_and_node_share_the_missing_file_sentinel(self, tmp_path):
        from gateway.platforms.whatsapp_common import (
            _whatsapp_bridge_input_fingerprint,
        )

        names = ("alpha.js", "missing.js")
        (tmp_path / "alpha.js").write_bytes(b"alpha")

        python_hash, all_present = _whatsapp_bridge_input_fingerprint(tmp_path, names)
        node_result = self._node_fingerprint(tmp_path, names)

        assert all_present is False
        assert node_result == {"hash": python_hash, "allPresent": False}

    @pytest.mark.skipif(os.name == "nt", reason="mkfifo is POSIX-only")
    def test_required_fifo_is_invalid_without_blocking(self, tmp_path):
        from gateway.platforms.whatsapp_common import (
            WHATSAPP_BRIDGE_RUNTIME_INPUTS,
            whatsapp_bridge_runtime_hash,
        )

        for filename in WHATSAPP_BRIDGE_RUNTIME_INPUTS:
            (tmp_path / filename).write_bytes(b"production")
        fifo = tmp_path / "bridge_helpers.js"
        fifo.unlink()
        os.mkfifo(fifo)

        assert whatsapp_bridge_runtime_hash(tmp_path) == ""


class TestStaleBridgeHandshake:


    @pytest.mark.asyncio
    async def test_restarts_bridge_when_read_receipt_config_changed(self, tmp_path):
        from gateway.platforms.whatsapp_common import whatsapp_bridge_runtime_hash

        bridge_dir = _setup_bridge_dir(tmp_path)
        _fresh_node_modules(bridge_dir)
        adapter = _make_adapter(
            bridge_script=str(bridge_dir / "bridge.js"),
            session_path=tmp_path / "session",
        )
        adapter._send_read_receipts = True
        disk_hash = whatsapp_bridge_runtime_hash(bridge_dir)
        mock_client = _mock_health(
            {
                "status": "connected",
                "scriptHash": disk_hash,
                "sendReadReceipts": False,
            }
        )
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 1
        mock_proc.returncode = 1

        with patch("plugins.platforms.whatsapp.adapter.check_whatsapp_requirements", return_value=True), \
             patch("aiohttp.ClientSession", mock_client), \
             patch("plugins.platforms.whatsapp.adapter.asyncio.sleep", new_callable=AsyncMock), \
             patch("plugins.platforms.whatsapp.adapter._kill_stale_bridge_by_pidfile"), \
             patch("plugins.platforms.whatsapp.adapter._kill_port_process"), \
             patch("subprocess.Popen", return_value=mock_proc) as mock_popen, \
             patch.object(adapter, "_acquire_platform_lock", return_value=True, create=True):
            await adapter.connect()

        mock_popen.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("hash_field", ["runtimeHash", "scriptHash"])
    async def test_reuses_one_healthy_bridge_only_for_exact_composite_hash(
        self, tmp_path, hash_field
    ):
        from gateway.platforms.whatsapp_common import whatsapp_bridge_runtime_hash

        bridge_dir = _setup_bridge_dir(tmp_path)
        _fresh_node_modules(bridge_dir)
        adapter = _make_adapter(
            bridge_script=str(bridge_dir / "bridge.js"),
            session_path=tmp_path / "session",
        )
        desired = whatsapp_bridge_runtime_hash(bridge_dir)
        mock_client = _mock_health(
            {
                "status": "connected",
                hash_field: desired,
                "sendReadReceipts": False,
            }
        )

        with patch(
            "plugins.platforms.whatsapp.adapter.check_whatsapp_requirements",
            return_value=True,
        ), patch("aiohttp.ClientSession", mock_client), patch(
            "plugins.platforms.whatsapp.adapter._kill_stale_bridge_by_pidfile"
        ) as kill_pid, patch(
            "plugins.platforms.whatsapp.adapter._kill_port_process"
        ) as kill_port, patch("subprocess.Popen") as popen, patch.object(
            adapter, "_acquire_platform_lock", return_value=True, create=True
        ), patch.object(adapter, "_poll_messages", new_callable=AsyncMock):
            result = await adapter.connect()

        assert result is True
        assert adapter._bridge_process is None
        popen.assert_not_called()
        kill_pid.assert_not_called()
        kill_port.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "health",
        [
            {"status": "connected"},
            {"status": "connected", "runtimeHash": "not-a-sha256"},
            {"status": "connected", "scriptHash": "a" * 16},
        ],
        ids=["missing", "malformed", "old-bridge-js-only-shape"],
    )
    async def test_missing_malformed_or_old_hash_restarts(self, tmp_path, health):
        bridge_dir = _setup_bridge_dir(tmp_path)
        _fresh_node_modules(bridge_dir)
        adapter = _make_adapter(
            bridge_script=str(bridge_dir / "bridge.js"),
            session_path=tmp_path / "session",
        )
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 1
        mock_proc.returncode = 1

        with patch(
            "plugins.platforms.whatsapp.adapter.check_whatsapp_requirements",
            return_value=True,
        ), patch("aiohttp.ClientSession", _mock_health(health)), patch(
            "plugins.platforms.whatsapp.adapter.asyncio.sleep", new_callable=AsyncMock
        ), patch(
            "plugins.platforms.whatsapp.adapter._kill_stale_bridge_by_pidfile"
        ), patch("plugins.platforms.whatsapp.adapter._kill_port_process"), patch(
            "subprocess.Popen", return_value=mock_proc
        ) as popen, patch.object(
            adapter, "_acquire_platform_lock", return_value=True, create=True
        ):
            await adapter.connect()

        popen.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "changed_file", ["bridge_helpers.js", "package-lock.json"],
        ids=["helper-only", "lock-only"],
    )
    async def test_helper_or_lock_change_restarts_exact_old_bridge(
        self, tmp_path, changed_file
    ):
        from gateway.platforms.whatsapp_common import whatsapp_bridge_runtime_hash

        bridge_dir = _setup_bridge_dir(tmp_path)
        _fresh_node_modules(bridge_dir)
        old_hash = whatsapp_bridge_runtime_hash(bridge_dir)
        (bridge_dir / changed_file).write_text(f"// changed {changed_file}\n")
        adapter = _make_adapter(
            bridge_script=str(bridge_dir / "bridge.js"),
            session_path=tmp_path / "session",
        )
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 1
        mock_proc.returncode = 1
        install_result = MagicMock(returncode=0)

        with patch(
            "plugins.platforms.whatsapp.adapter.check_whatsapp_requirements",
            return_value=True,
        ), patch(
            "aiohttp.ClientSession",
            _mock_health(
                {
                    "status": "connected",
                    "runtimeHash": old_hash,
                    "sendReadReceipts": False,
                }
            ),
        ), patch(
            "plugins.platforms.whatsapp.adapter.asyncio.sleep", new_callable=AsyncMock
        ), patch(
            "plugins.platforms.whatsapp.adapter._kill_stale_bridge_by_pidfile"
        ), patch("plugins.platforms.whatsapp.adapter._kill_port_process"), patch(
            "subprocess.run", return_value=install_result
        ) as install, patch(
            "subprocess.Popen", return_value=mock_proc
        ) as popen, patch.object(
            adapter, "_acquire_platform_lock", return_value=True, create=True
        ):
            await adapter.connect()

        if changed_file == "package-lock.json":
            install.assert_called_once()
        else:
            install.assert_not_called()
        popen.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.skipif(os.name == "nt", reason="symlink/FIFO setup is POSIX-only")
    @pytest.mark.parametrize(
        "hostile_kind", ["manifest-symlink", "helper-fifo"],
    )
    async def test_invalid_required_input_prevents_install_reuse_stamp_and_start(
        self, tmp_path, hostile_kind
    ):
        bridge_dir = _setup_bridge_dir(tmp_path)
        (bridge_dir / "node_modules").mkdir()
        target_name = "package.json" if hostile_kind == "manifest-symlink" else "bridge_helpers.js"
        target = bridge_dir / target_name
        target.unlink()
        if hostile_kind == "manifest-symlink":
            secret = bridge_dir / "private-manifest"
            secret.write_text('{"private":"content"}')
            target.symlink_to(secret)
        else:
            os.mkfifo(target)

        adapter = _make_adapter(
            bridge_script=str(bridge_dir / "bridge.js"),
            session_path=tmp_path / "session",
        )

        with patch(
            "plugins.platforms.whatsapp.adapter.check_whatsapp_requirements",
            return_value=True,
        ), patch("aiohttp.ClientSession") as http_client, patch(
            "subprocess.run"
        ) as install, patch("subprocess.Popen") as popen, patch(
            "plugins.platforms.whatsapp.adapter.write_whatsapp_bridge_dependency_stamp"
        ) as write_stamp:
            result = await adapter.connect()

        assert result is False
        assert adapter.fatal_error_code == "whatsapp_bridge_source_invalid"
        assert adapter.fatal_error_retryable is False
        install.assert_not_called()
        write_stamp.assert_not_called()
        http_client.assert_not_called()
        popen.assert_not_called()
        assert not (bridge_dir / "node_modules" / ".hermes-pkg-hash").exists()


class TestDepRefreshStamp:
    @pytest.mark.asyncio
    async def test_skips_install_when_stamp_fresh(self, tmp_path):
        bridge_dir = _setup_bridge_dir(tmp_path)
        _fresh_node_modules(bridge_dir)
        adapter = _make_adapter(
            bridge_script=str(bridge_dir / "bridge.js"),
            session_path=tmp_path / "session",
        )
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 1
        mock_proc.returncode = 1

        with patch("plugins.platforms.whatsapp.adapter.check_whatsapp_requirements", return_value=True), \
             patch("aiohttp.ClientSession", _mock_health({"status": "disconnected"})), \
             patch("plugins.platforms.whatsapp.adapter.asyncio.sleep", new_callable=AsyncMock), \
             patch("plugins.platforms.whatsapp.adapter._kill_stale_bridge_by_pidfile"), \
             patch("plugins.platforms.whatsapp.adapter._kill_port_process"), \
             patch("subprocess.run") as mock_run, \
             patch("subprocess.Popen", return_value=mock_proc), \
             patch.object(adapter, "_acquire_platform_lock", return_value=True, create=True):
            await adapter.connect()

        mock_run.assert_not_called()


class TestCacheDirEnvPassthrough:
    @pytest.mark.asyncio
    async def test_bridge_spawn_env_has_cache_dirs(self, tmp_path):
        bridge_dir = _setup_bridge_dir(tmp_path)
        _fresh_node_modules(bridge_dir)
        adapter = _make_adapter(
            bridge_script=str(bridge_dir / "bridge.js"),
            session_path=tmp_path / "session",
        )
        adapter._send_read_receipts = True
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 1
        mock_proc.returncode = 1

        with patch("plugins.platforms.whatsapp.adapter.check_whatsapp_requirements", return_value=True), \
             patch("aiohttp.ClientSession", _mock_health({"status": "disconnected"})), \
             patch("plugins.platforms.whatsapp.adapter.asyncio.sleep", new_callable=AsyncMock), \
             patch("plugins.platforms.whatsapp.adapter._kill_stale_bridge_by_pidfile"), \
             patch("plugins.platforms.whatsapp.adapter._kill_port_process"), \
             patch("subprocess.Popen", return_value=mock_proc) as mock_popen, \
             patch.object(adapter, "_acquire_platform_lock", return_value=True, create=True):
            await adapter.connect()

        env = mock_popen.call_args.kwargs["env"]
        from gateway.platforms.base import (
            get_audio_cache_dir,
            get_document_cache_dir,
            get_image_cache_dir,
        )
        assert env["HERMES_IMAGE_CACHE_DIR"] == str(get_image_cache_dir())
        assert env["HERMES_AUDIO_CACHE_DIR"] == str(get_audio_cache_dir())
        assert env["HERMES_DOCUMENT_CACHE_DIR"] == str(get_document_cache_dir())
        assert env["WHATSAPP_SEND_READ_RECEIPTS"] == "true"
