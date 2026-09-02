"""Process-safety tests for WhatsApp stale bridge cleanup."""

from __future__ import annotations

import os
from pathlib import Path
import socket
import subprocess
import sys
import textwrap
import time

import pytest

from gateway.status import _pid_exists, get_process_start_time
from plugins.platforms.whatsapp import adapter as whatsapp_adapter


def _unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _spawn_sleeper(*extra_argv: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)", *extra_argv]
    )


def _spawn_python_listener(port: int) -> subprocess.Popen:
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import socket,sys,time; "
                "s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1); "
                "s.bind(('127.0.0.1',int(sys.argv[1]))); s.listen(); time.sleep(60)"
            ),
            str(port),
        ]
    )


def _wait_for_listener(pid: int, port: int, timeout: float = 5.0) -> None:
    from gateway.platforms.whatsapp_recovery import _listener_pids_on_port

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        listeners = _listener_pids_on_port(port)
        if listeners is not None and pid in listeners:
            return
        time.sleep(0.02)
    raise AssertionError("the test process did not bind its listener")


def _spawn_node_bridge(
    tmp_path: Path, session_dir: Path, port: int
) -> tuple[subprocess.Popen, Path]:
    from hermes_constants import find_node_executable

    node = find_node_executable("node")
    if not node:
        pytest.skip("Node.js is required for the real bridge identity harness")
    script = tmp_path / "bridge.js"
    script.write_text(
        """
const net = require('net');
const args = process.argv.slice(2);
const port = Number(args[args.indexOf('--port') + 1]);
const server = net.createServer(() => {});
server.listen(port, '127.0.0.1');
process.on('SIGTERM', () => server.close(() => process.exit(0)));
setInterval(() => {}, 1000);
""".strip(),
        encoding="utf-8",
    )
    proc = subprocess.Popen(
        [
            node,
            str(script),
            "--port",
            str(port),
            "--session",
            str(session_dir),
        ]
    )
    _wait_for_listener(proc.pid, port)
    return proc, script


def _wait_dead(proc: subprocess.Popen, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return True
        time.sleep(0.02)
    return False


class TestWriteAndRoundTrip:
    def test_pidfile_records_pid_and_start_time(self, tmp_path):
        proc = _spawn_sleeper()
        try:
            whatsapp_adapter._write_bridge_pidfile(tmp_path, proc.pid)
            lines = (tmp_path / "bridge.pid").read_text().split("\n")
            assert int(lines[0]) == proc.pid
            assert int(lines[1]) == get_process_start_time(proc.pid)
        finally:
            proc.kill()
            proc.wait()


class TestIdentityGuard:
    def test_matching_start_time_alone_never_authorizes_a_signal(self, tmp_path):
        proc = _spawn_sleeper()
        try:
            start = get_process_start_time(proc.pid)
            assert start is not None
            assert whatsapp_adapter._bridge_pid_is_ours(
                proc.pid,
                tmp_path,
                start,
                _unused_port(),
                tmp_path / "bridge.js",
            ) is False
        finally:
            proc.kill()
            proc.wait()

    def test_spoofed_argv_is_not_the_node_bridge(self, tmp_path):
        port = _unused_port()
        proc = _spawn_sleeper(
            "node",
            str(tmp_path / "bridge.js"),
            "--port",
            str(port),
            "--session",
            str(tmp_path),
        )
        try:
            assert whatsapp_adapter._bridge_pid_is_ours(
                proc.pid,
                tmp_path,
                None,
                port,
                tmp_path / "bridge.js",
            ) is False
        finally:
            proc.kill()
            proc.wait()

    def test_corrupt_pidfile_pointing_at_unrelated_listener_is_preserved(
        self, tmp_path
    ):
        session_dir = tmp_path / "session"
        session_dir.mkdir()
        script = tmp_path / "bridge.js"
        script.write_text("// expected bridge", encoding="utf-8")
        port = _unused_port()
        proc = _spawn_python_listener(port)
        try:
            _wait_for_listener(proc.pid, port)
            whatsapp_adapter._write_bridge_pidfile(session_dir, proc.pid)

            with pytest.raises(whatsapp_adapter.WhatsAppBridgeOwnershipError):
                whatsapp_adapter._kill_stale_bridge_by_pidfile(
                    session_dir, port, script
                )

            assert proc.poll() is None
            assert (session_dir / "bridge.pid").exists()
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait()

    @pytest.mark.require_symlinks
    def test_symlinked_pidfile_is_rejected_and_preserved(self, tmp_path):
        session_dir = tmp_path / "session"
        session_dir.mkdir()
        target = tmp_path / "outside-pid"
        target.write_text("424242\n", encoding="utf-8")
        pid_file = session_dir / "bridge.pid"
        pid_file.symlink_to(target)

        with pytest.raises(whatsapp_adapter.WhatsAppBridgeOwnershipError):
            whatsapp_adapter._kill_stale_bridge_by_pidfile(
                session_dir, _unused_port(), tmp_path / "bridge.js"
            )

        assert pid_file.is_symlink()
        assert target.read_text(encoding="utf-8") == "424242\n"

    @pytest.mark.skipif(os.name == "nt", reason="POSIX FIFO semantics")
    def test_pidfile_fifo_is_rejected_without_blocking(self, tmp_path):
        session_dir = tmp_path / "session"
        session_dir.mkdir()
        os.mkfifo(session_dir / "bridge.pid")
        probe = textwrap.dedent(
            """
            import sys
            from pathlib import Path
            from plugins.platforms.whatsapp import adapter

            try:
                adapter._kill_stale_bridge_by_pidfile(
                    Path(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3])
                )
            except adapter.WhatsAppBridgeOwnershipError:
                raise SystemExit(0)
            raise SystemExit(2)
            """
        )
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    probe,
                    str(session_dir),
                    str(_unused_port()),
                    str(tmp_path / "bridge.js"),
                ],
                env=os.environ.copy(),
                timeout=1,
            )
        except subprocess.TimeoutExpired:
            pytest.fail("adapter bridge.pid FIFO inspection blocked")
        assert result.returncode == 0

    def test_exact_matching_node_bridge_is_reaped(self, tmp_path):
        session_dir = tmp_path / "session"
        session_dir.mkdir()
        port = _unused_port()
        proc, script = _spawn_node_bridge(tmp_path, session_dir, port)
        try:
            whatsapp_adapter._write_bridge_pidfile(session_dir, proc.pid)
            whatsapp_adapter._kill_stale_bridge_by_pidfile(session_dir, port, script)

            assert _wait_dead(proc), "the exact matching bridge should be stopped"
            assert not (session_dir / "bridge.pid").exists()
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait()


class TestKillPortProcess:
    def test_unrelated_listener_is_never_signalled(self, tmp_path):
        session_dir = tmp_path / "session"
        session_dir.mkdir()
        script = tmp_path / "bridge.js"
        script.write_text("// expected bridge", encoding="utf-8")
        port = _unused_port()
        proc = _spawn_python_listener(port)
        try:
            _wait_for_listener(proc.pid, port)

            with pytest.raises(whatsapp_adapter.WhatsAppBridgeOwnershipError):
                whatsapp_adapter._kill_port_process(port, session_dir, script)

            assert proc.poll() is None
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait()

    def test_exact_listener_without_pidfile_is_reaped(self, tmp_path):
        session_dir = tmp_path / "session"
        session_dir.mkdir()
        port = _unused_port()
        proc, script = _spawn_node_bridge(tmp_path, session_dir, port)
        try:
            whatsapp_adapter._kill_port_process(port, session_dir, script)
            assert _wait_dead(proc)
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait()

    def test_listener_lookup_excludes_client_process(self):
        if os.name == "nt":
            pytest.skip("adapter listener wrapper is exercised by Windows unit tests")
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        port = srv.getsockname()[1]
        srv.listen(5)
        client = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import socket,time; c=socket.create_connection(('127.0.0.1',%d)); time.sleep(60)"
                % port,
            ]
        )
        try:
            conn, _ = srv.accept()
            pids = whatsapp_adapter._listener_pids_on_port(port)
            if os.getpid() not in pids:
                pytest.skip("the platform could not attribute the listener")
            assert client.pid not in pids
            conn.close()
        finally:
            client.kill()
            client.wait()
            srv.close()


def test_pid_liveness_probe_does_not_signal_process():
    proc = _spawn_sleeper()
    try:
        assert _pid_exists(proc.pid)
        assert proc.poll() is None
    finally:
        proc.kill()
        proc.wait()
