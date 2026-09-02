"""Behaviour tests for exclusive WhatsApp reset/re-pair recovery leases."""

from __future__ import annotations

import json
import subprocess
import sys
import socket
import signal
import os
import textwrap
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def _spawn_sleeper() -> subprocess.Popen:
    return subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])


def _unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_listener(pid: int, port: int, timeout: float = 5.0) -> None:
    from gateway.platforms.whatsapp_recovery import _listener_pids_on_port

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        listeners = _listener_pids_on_port(port)
        if listeners is not None and pid in listeners:
            return
        time.sleep(0.02)
    raise AssertionError("the listener process did not bind its test port")


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


def _write_pidfile(session_dir: Path, proc: subprocess.Popen) -> None:
    from gateway.status import get_process_start_time

    start_time = get_process_start_time(proc.pid)
    assert start_time is not None
    (session_dir / "bridge.pid").write_text(
        f"{proc.pid}\n{start_time}", encoding="utf-8"
    )


def _wait_for_path(path: Path, proc: subprocess.Popen, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if proc.poll() is not None:
            raise AssertionError("the lock-owner process exited before reaching the barrier")
        time.sleep(0.01)
    raise AssertionError("the lock-owner process did not reach the barrier")


def _acquire_test_lease(recovery, tmp_path, monkeypatch, session_dir):
    monkeypatch.setenv("HERMES_GATEWAY_LOCK_DIR", str(tmp_path / "locks"))
    monkeypatch.setattr(recovery, "_listener_pids_on_port", lambda _port: [])
    return recovery.acquire_whatsapp_recovery_lease(
        session_dir,
        bridge_port=_unused_port(),
        timeout=0.2,
        poll_interval=0.001,
    )


def test_paused_first_gate_acquirer_cannot_be_stolen(tmp_path, monkeypatch):
    """The OS lock must be held before its diagnostic metadata is initialized."""
    from gateway.platforms.whatsapp_recovery import acquire_whatsapp_connect_gate

    monkeypatch.setenv("HERMES_GATEWAY_LOCK_DIR", str(tmp_path / "locks"))
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    ready = tmp_path / "owner-ready"
    release = tmp_path / "owner-release"
    child_code = textwrap.dedent(
        """
        import sys
        import time
        from pathlib import Path
        from gateway.platforms import whatsapp_recovery as recovery

        session_dir, ready, release = map(Path, sys.argv[1:4])

        def pause_before_metadata(original):
            def paused(*args, **kwargs):
                ready.write_text("ready", encoding="utf-8")
                while not release.exists():
                    time.sleep(0.01)
                return original(*args, **kwargs)
            return paused

        if hasattr(recovery, "_write_gate_metadata"):
            recovery._write_gate_metadata = pause_before_metadata(
                recovery._write_gate_metadata
            )
        else:
            # Reproduce the old JSON-PID lock's vulnerable O_EXCL -> json.dump
            # initialization window so this regression test is red before the
            # held advisory lock exists.
            from gateway import status
            status.json.dump = pause_before_metadata(status.json.dump)

        gate = recovery.acquire_whatsapp_connect_gate(session_dir)
        if gate is None:
            raise SystemExit(2)
        try:
            while not release.exists():
                time.sleep(0.01)
        finally:
            gate.release()
        """
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", child_code, str(session_dir), str(ready), str(release)],
        env=os.environ.copy(),
    )
    contender = None
    try:
        _wait_for_path(ready, proc)
        contender = acquire_whatsapp_connect_gate(session_dir)
        stolen = contender is not None
    finally:
        if contender is not None:
            contender.release()
        release.write_text("release", encoding="utf-8")
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    assert proc.returncode == 0
    assert stolen is False, "a contender acquired while the owner initialized metadata"


def test_malformed_session_lock_state_fails_closed(tmp_path, monkeypatch):
    from gateway.platforms import whatsapp_recovery as recovery
    from gateway.status import _get_scope_lock_path

    monkeypatch.setenv("HERMES_GATEWAY_LOCK_DIR", str(tmp_path / "locks"))
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    identity = str(session_dir.resolve())
    lock_path = _get_scope_lock_path(recovery.WHATSAPP_SESSION_SCOPE, identity)
    lock_path.parent.mkdir(parents=True)
    lock_path.write_text("{", encoding="utf-8")
    monkeypatch.setattr(recovery, "_listener_pids_on_port", lambda _port: [])

    lease = None
    try:
        with pytest.raises(recovery.WhatsAppRecoveryError):
            lease = recovery.acquire_whatsapp_recovery_lease(
                session_dir, bridge_port=_unused_port(), timeout=0.02
            )
    finally:
        if lease is not None:
            lease.release()

    assert lock_path.read_text(encoding="utf-8") == "{"


def test_free_gate_with_torn_diagnostic_metadata_is_reinitialized(
    tmp_path, monkeypatch
):
    """Once acquired, the OS lock—not stale diagnostic JSON—is authority."""
    from gateway.platforms import whatsapp_recovery as recovery

    monkeypatch.setenv("HERMES_GATEWAY_LOCK_DIR", str(tmp_path / "locks"))
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    identity = str(session_dir.resolve())
    first = recovery.acquire_whatsapp_connect_gate(session_dir)
    assert first is not None
    first.release()
    gate_path = recovery._gate_path(identity)
    gate_path.write_bytes(b"{")

    replacement = recovery.acquire_whatsapp_connect_gate(session_dir)

    assert replacement is not None
    try:
        payload = json.loads(gate_path.read_text(encoding="utf-8"))
        assert payload["identity_hash"] == recovery._scope_hash(identity)
        assert payload["operation"] == "connect"
    finally:
        replacement.release()


@pytest.mark.skipif(os.name == "nt", reason="POSIX FIFO semantics")
def test_recorded_bridge_fifo_is_rejected_without_blocking(tmp_path):
    """Opening a hostile bridge.pid FIFO must never park recovery."""
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    os.mkfifo(session_dir / "bridge.pid")
    probe = textwrap.dedent(
        """
        import sys
        from pathlib import Path
        from gateway.platforms.whatsapp_recovery import (
            WhatsAppRecoveryError,
            _read_recorded_bridge,
        )

        try:
            _read_recorded_bridge(Path(sys.argv[1]))
        except WhatsAppRecoveryError:
            raise SystemExit(0)
        raise SystemExit(2)
        """
    )
    try:
        result = subprocess.run(
            [sys.executable, "-c", probe, str(session_dir)],
            env=os.environ.copy(),
            timeout=1,
        )
    except subprocess.TimeoutExpired:
        pytest.fail("bridge.pid FIFO inspection blocked")
    assert result.returncode == 0


@pytest.mark.require_symlinks
def test_recorded_bridge_symlink_is_rejected(tmp_path):
    from gateway.platforms import whatsapp_recovery as recovery

    session_dir = tmp_path / "session"
    session_dir.mkdir()
    target = tmp_path / "outside-pid"
    target.write_text("424242\n", encoding="utf-8")
    (session_dir / "bridge.pid").symlink_to(target)

    with pytest.raises(recovery.WhatsAppRecoveryError):
        recovery._read_recorded_bridge(session_dir)


def test_recorded_bridge_invalid_utf8_is_fixed_failure(tmp_path):
    from gateway.platforms import whatsapp_recovery as recovery

    session_dir = tmp_path / "session"
    session_dir.mkdir()
    (session_dir / "bridge.pid").write_bytes(b"123\n\xff")

    with pytest.raises(
        recovery.WhatsAppRecoveryError,
        match="^The recorded WhatsApp bridge could not be verified\\.$",
    ):
        recovery._read_recorded_bridge(session_dir)


@pytest.mark.skipif(os.name == "nt", reason="POSIX FIFO semantics")
def test_session_lock_fifo_is_rejected_without_blocking(tmp_path, monkeypatch):
    """A direct FIFO at the generic session-lock path must fail closed quickly."""
    from gateway.platforms import whatsapp_recovery as recovery
    from gateway.status import _get_scope_lock_path

    lock_dir = tmp_path / "locks"
    monkeypatch.setenv("HERMES_GATEWAY_LOCK_DIR", str(lock_dir))
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    identity = str(session_dir.resolve())
    lock_path = _get_scope_lock_path(recovery.WHATSAPP_SESSION_SCOPE, identity)
    lock_path.parent.mkdir(parents=True)
    os.mkfifo(lock_path)
    probe = textwrap.dedent(
        """
        import sys
        from pathlib import Path
        from gateway.platforms import whatsapp_recovery as recovery

        recovery._listener_pids_on_port = lambda _port: []
        try:
            recovery.acquire_whatsapp_recovery_lease(
                Path(sys.argv[1]), bridge_port=int(sys.argv[2]), timeout=0.1
            )
        except recovery.WhatsAppRecoveryError:
            raise SystemExit(0)
        raise SystemExit(2)
        """
    )
    try:
        result = subprocess.run(
            [sys.executable, "-c", probe, str(session_dir), str(_unused_port())],
            env=os.environ.copy(),
            timeout=1,
        )
    except subprocess.TimeoutExpired:
        pytest.fail("session-lock FIFO inspection blocked")
    assert result.returncode == 0


@pytest.mark.skipif(os.name == "nt", reason="POSIX lsof fallback")
def test_listener_fallback_failure_with_stderr_is_unknown(monkeypatch):
    from gateway.platforms import whatsapp_recovery as recovery

    import psutil

    monkeypatch.setattr(
        psutil, "net_connections", MagicMock(side_effect=OSError("unavailable"))
    )
    monkeypatch.setattr(
        recovery.subprocess,
        "run",
        MagicMock(
            return_value=MagicMock(
                returncode=1,
                stdout="",
                stderr="permission denied: private diagnostic",
            )
        ),
    )

    assert recovery._listener_pids_on_port(_unused_port()) is None


@pytest.mark.skipif(os.name == "nt", reason="POSIX lsof fallback")
def test_listener_fallback_unambiguous_lsof_no_match_is_empty(monkeypatch):
    from gateway.platforms import whatsapp_recovery as recovery

    import psutil

    monkeypatch.setattr(
        psutil, "net_connections", MagicMock(side_effect=OSError("unavailable"))
    )
    run = MagicMock(
        return_value=MagicMock(returncode=1, stdout="", stderr="")
    )
    monkeypatch.setattr(recovery.subprocess, "run", run)

    assert recovery._listener_pids_on_port(_unused_port()) == []
    run.assert_called_once()


@pytest.mark.skipif(os.name == "nt", reason="POSIX pass_fds advisory-lock inheritance")
def test_pair_child_inherits_gate_until_it_exits(tmp_path, monkeypatch):
    """Abrupt parent death must not strand an orphan pair writer unlocked."""
    from gateway.platforms import whatsapp_recovery as recovery

    monkeypatch.setenv("HERMES_GATEWAY_LOCK_DIR", str(tmp_path / "locks"))
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    holder_ready = tmp_path / "holder-ready"
    child_ready = tmp_path / "pair-ready"
    child_pid_path = tmp_path / "pair-pid"
    finish = tmp_path / "pair-finish"
    holder_code = textwrap.dedent(
        """
        import os
        import subprocess
        import sys
        import time
        from pathlib import Path
        from gateway.platforms import whatsapp_recovery as recovery

        session_dir, holder_ready, child_ready, child_pid_path, finish = map(
            Path, sys.argv[1:6]
        )
        recovery._listener_pids_on_port = lambda _port: []
        lease = recovery.acquire_whatsapp_recovery_lease(
            session_dir, bridge_port=int(sys.argv[6]), timeout=0.2
        )
        child = subprocess.Popen(
            [
                sys.executable,
                "-c",
                (
                    "import pathlib,sys,time; "
                    "ready,finish=map(pathlib.Path,sys.argv[1:3]); "
                    "ready.write_text('ready'); "
                    "exec(\\\"while not finish.exists(): time.sleep(0.01)\\\")"
                ),
                str(child_ready),
                str(finish),
            ],
            **lease.pair_subprocess_kwargs(),
        )
        while not child_ready.exists():
            if child.poll() is not None:
                raise SystemExit(3)
            time.sleep(0.01)
        child_pid_path.write_text(str(child.pid), encoding="utf-8")
        holder_ready.write_text("ready", encoding="utf-8")
        os._exit(0)
        """
    )
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            holder_code,
            str(session_dir),
            str(holder_ready),
            str(child_ready),
            str(child_pid_path),
            str(finish),
            str(_unused_port()),
        ],
        env=os.environ.copy(),
    )
    contender = None
    child_pid = None
    try:
        _wait_for_path(holder_ready, holder)
        assert holder.wait(timeout=5) == 0
        child_pid = int(child_pid_path.read_text(encoding="utf-8"))

        contender = recovery.acquire_whatsapp_connect_gate(session_dir)
        assert contender is None, "the inherited child FD must retain the gate"

        finish.write_text("finish", encoding="utf-8")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            contender = recovery.acquire_whatsapp_connect_gate(session_dir)
            if contender is not None:
                break
            time.sleep(0.01)
        assert contender is not None, "child exit must release the inherited gate"
    finally:
        finish.write_text("finish", encoding="utf-8")
        if holder.poll() is None:
            holder.kill()
            holder.wait()
        if child_pid is not None:
            try:
                os.kill(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if contender is not None:
            contender.release()


def test_same_process_probe_cannot_steal_or_release_a_held_gate(
    tmp_path, monkeypatch
):
    from gateway.platforms import whatsapp_recovery as recovery

    monkeypatch.setenv("HERMES_GATEWAY_LOCK_DIR", str(tmp_path / "locks"))
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    gate = recovery.acquire_whatsapp_connect_gate(session_dir)
    assert gate is not None
    try:
        assert recovery.whatsapp_recovery_is_active(session_dir) is True
        assert recovery.acquire_whatsapp_connect_gate(session_dir) is None
        assert gate.active is True
    finally:
        gate.release()

    replacement = recovery.acquire_whatsapp_connect_gate(session_dir)
    assert replacement is not None
    replacement.release()


def test_recorded_start_time_never_authorizes_an_unrelated_listener(
    tmp_path, monkeypatch
):
    from gateway.platforms import whatsapp_recovery as recovery

    monkeypatch.setenv("HERMES_GATEWAY_LOCK_DIR", str(tmp_path / "locks"))
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    port = _unused_port()
    proc = _spawn_python_listener(port)
    try:
        _wait_for_listener(proc.pid, port)
        _write_pidfile(session_dir, proc)

        with pytest.raises(recovery.WhatsAppRecoveryError):
            recovery.acquire_whatsapp_recovery_lease(
                session_dir,
                bridge_port=port,
                timeout=0.2,
                poll_interval=0.01,
            )

        assert proc.poll() is None, "start-time equality must never authorize a signal"
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.wait()


def test_recovery_quiesces_an_exact_matching_node_bridge(tmp_path, monkeypatch):
    from gateway.platforms import whatsapp_recovery as recovery

    monkeypatch.setenv("HERMES_GATEWAY_LOCK_DIR", str(tmp_path / "locks"))
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    sentinel = session_dir / "creds.json"
    sentinel.write_text('{"registered": true}', encoding="utf-8")
    port = _unused_port()
    proc, script = _spawn_node_bridge(tmp_path, session_dir, port)
    try:
        _write_pidfile(session_dir, proc)

        lease = recovery.acquire_whatsapp_recovery_lease(
            session_dir,
            bridge_port=port,
            bridge_script=script,
            timeout=2.0,
            poll_interval=0.01,
        )
        try:
            assert proc.wait(timeout=2) == 0
            assert sentinel.exists(), "acquiring a lease must not alter auth state"
            assert lease.active is True
        finally:
            lease.release()
        assert lease.active is False
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def test_recovery_lease_can_quiesce_a_reused_listener_without_a_pidfile(
    tmp_path, monkeypatch
):
    from gateway.platforms import whatsapp_recovery as recovery

    monkeypatch.setenv("HERMES_GATEWAY_LOCK_DIR", str(tmp_path / "locks"))
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    alive = {43210}
    terminated = []

    monkeypatch.setattr(recovery, "_listener_pids_on_port", lambda _port: [43210])
    monkeypatch.setattr(recovery, "_pid_exists", lambda pid: pid in alive)
    monkeypatch.setattr(
        recovery,
        "bridge_process_is_exact_listener",
        lambda pid, path, port, script, **_kwargs: (
            pid == 43210 and path == session_dir and port == 3000
        ),
    )

    def terminate(pid, *, force=False):
        terminated.append((pid, force))
        alive.discard(pid)

    monkeypatch.setattr(recovery, "terminate_pid", terminate)

    lease = recovery.acquire_whatsapp_recovery_lease(
        session_dir, bridge_port=3000, timeout=0.2, poll_interval=0.001
    )
    try:
        assert terminated == [(43210, False)]
        assert lease.active is True
    finally:
        lease.release()


def test_recovery_fails_closed_when_listener_identity_cannot_be_proved(
    tmp_path, monkeypatch
):
    from gateway.platforms import whatsapp_recovery as recovery

    monkeypatch.setenv("HERMES_GATEWAY_LOCK_DIR", str(tmp_path / "locks"))
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    sentinel = session_dir / "creds.json"
    sentinel.write_text("keep", encoding="utf-8")
    terminate = MagicMock()

    monkeypatch.setattr(recovery, "_listener_pids_on_port", lambda _port: [9876])
    monkeypatch.setattr(recovery, "_pid_exists", lambda _pid: True)
    monkeypatch.setattr(
        recovery, "_bridge_cmdline_matches_session", lambda *_args: False
    )
    monkeypatch.setattr(recovery, "terminate_pid", terminate)

    with pytest.raises(recovery.WhatsAppRecoveryError):
        recovery.acquire_whatsapp_recovery_lease(
            session_dir, timeout=0.02, poll_interval=0.001
        )

    terminate.assert_not_called()
    assert sentinel.read_text(encoding="utf-8") == "keep"
    assert list((tmp_path / "locks").glob("*.lock")) == []


def test_recovery_stop_failure_releases_every_lock_without_touching_session(
    tmp_path, monkeypatch
):
    from gateway.platforms import whatsapp_recovery as recovery

    monkeypatch.setenv("HERMES_GATEWAY_LOCK_DIR", str(tmp_path / "locks"))
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    sentinel = session_dir / "creds.json"
    sentinel.write_text("keep", encoding="utf-8")
    proc = _spawn_sleeper()
    try:
        _write_pidfile(session_dir, proc)
        monkeypatch.setattr(recovery, "terminate_pid", lambda *_a, **_kw: None)

        with pytest.raises(recovery.WhatsAppRecoveryError):
            recovery.acquire_whatsapp_recovery_lease(
                session_dir, timeout=0.02, poll_interval=0.001
            )

        assert sentinel.read_text(encoding="utf-8") == "keep"
        assert list((tmp_path / "locks").glob("*.lock")) == []
    finally:
        proc.kill()
        proc.wait()


def test_session_lock_contention_fails_closed_and_releases_recovery_scope(
    tmp_path, monkeypatch
):
    from gateway.platforms import whatsapp_recovery as recovery

    session_dir = tmp_path / "session"
    session_dir.mkdir()
    sentinel = session_dir / "creds.json"
    sentinel.write_text("keep", encoding="utf-8")
    monkeypatch.setenv("HERMES_GATEWAY_LOCK_DIR", str(tmp_path / "locks"))

    def acquire(scope, identity, metadata=None):
        assert scope == recovery.WHATSAPP_SESSION_SCOPE
        return False, {"pid": 4242}

    monkeypatch.setattr(recovery, "acquire_scoped_lock", acquire)
    monkeypatch.setattr(recovery, "_listener_pids_on_port", lambda _port: [])

    with pytest.raises(recovery.WhatsAppRecoveryError):
        recovery.acquire_whatsapp_recovery_lease(
            session_dir, timeout=0.01, poll_interval=0.001
        )

    assert sentinel.read_text(encoding="utf-8") == "keep"
    contender = recovery.acquire_whatsapp_connect_gate(session_dir)
    assert contender is not None, "a failed recovery must close its OS-held gate"
    contender.release()


def test_lease_exposes_canonical_identity_and_owns_reset(tmp_path, monkeypatch):
    from gateway.platforms import whatsapp_recovery as recovery

    session_dir = tmp_path / "account" / "session"
    session_dir.mkdir(parents=True)
    (session_dir / "creds.json").write_text("{}", encoding="utf-8")
    lease = _acquire_test_lease(recovery, tmp_path, monkeypatch, session_dir)
    try:
        assert lease.canonical_session_path == session_dir.resolve()
        assert lease.canonical_session_identity == str(session_dir.resolve())

        lease.reset_session()

        assert session_dir.is_dir()
        assert list(session_dir.iterdir()) == []
    finally:
        lease.release()


def test_reset_refuses_an_inactive_lease_before_touching_session(
    tmp_path, monkeypatch
):
    from gateway.platforms import whatsapp_recovery as recovery

    session_dir = tmp_path / "account" / "session"
    session_dir.mkdir(parents=True)
    sentinel = session_dir / "must-survive"
    sentinel.write_text("keep", encoding="utf-8")
    lease = _acquire_test_lease(recovery, tmp_path, monkeypatch, session_dir)
    lease.release()

    with pytest.raises(
        recovery.WhatsAppRecoveryError,
        match="^The WhatsApp recovery lease is not active\\.$",
    ):
        lease.reset_session()

    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_reset_rejects_a_requested_path_mismatch_before_deleting(
    tmp_path, monkeypatch
):
    from gateway.platforms import whatsapp_recovery as recovery

    session_dir = tmp_path / "account" / "session"
    other_session = tmp_path / "other" / "session"
    session_dir.mkdir(parents=True)
    other_session.mkdir(parents=True)
    original = session_dir / "original-secret"
    other = other_session / "other-secret"
    original.write_text("original", encoding="utf-8")
    other.write_text("other", encoding="utf-8")
    lease = _acquire_test_lease(recovery, tmp_path, monkeypatch, session_dir)
    try:
        with pytest.raises(recovery.WhatsAppRecoveryError):
            lease.reset_session(other_session)

        assert original.read_text(encoding="utf-8") == "original"
        assert other.read_text(encoding="utf-8") == "other"
    finally:
        lease.release()


@pytest.mark.require_symlinks
def test_reset_rejects_substituted_session_symlink_before_deleting(
    tmp_path, monkeypatch
):
    from gateway.platforms import whatsapp_recovery as recovery

    session_dir = tmp_path / "account" / "session"
    session_dir.mkdir(parents=True)
    original = tmp_path / "original-session"
    victim = tmp_path / "victim"
    victim.mkdir()
    victim_secret = victim / "must-survive"
    victim_secret.write_text("victim", encoding="utf-8")
    (session_dir / "original-secret").write_text("original", encoding="utf-8")
    lease = _acquire_test_lease(recovery, tmp_path, monkeypatch, session_dir)
    try:
        session_dir.rename(original)
        session_dir.symlink_to(victim, target_is_directory=True)

        with pytest.raises(recovery.WhatsAppRecoveryError):
            lease.reset_session()

        assert session_dir.is_symlink()
        assert (original / "original-secret").read_text(encoding="utf-8") == "original"
        assert victim_secret.read_text(encoding="utf-8") == "victim"
    finally:
        lease.release()


def test_reset_rejects_session_directory_identity_swap_before_deleting(
    tmp_path, monkeypatch
):
    from gateway.platforms import whatsapp_recovery as recovery

    session_dir = tmp_path / "account" / "session"
    session_dir.mkdir(parents=True)
    original_session = tmp_path / "original-session"
    original_secret = session_dir / "original-secret"
    original_secret.write_text("original", encoding="utf-8")
    lease = _acquire_test_lease(recovery, tmp_path, monkeypatch, session_dir)
    try:
        session_dir.rename(original_session)
        session_dir.mkdir()
        replacement_secret = session_dir / "replacement-secret"
        replacement_secret.write_text("replacement", encoding="utf-8")

        with pytest.raises(recovery.WhatsAppRecoveryError):
            lease.reset_session()

        assert (original_session / "original-secret").read_text(
            encoding="utf-8"
        ) == "original"
        assert replacement_secret.read_text(encoding="utf-8") == "replacement"
    finally:
        lease.release()


def test_reset_rejects_substituted_ancestor_before_deleting(tmp_path, monkeypatch):
    from gateway.platforms import whatsapp_recovery as recovery

    ancestor = tmp_path / "account"
    session_dir = ancestor / "session"
    session_dir.mkdir(parents=True)
    original_ancestor = tmp_path / "original-account"
    (session_dir / "original-secret").write_text("original", encoding="utf-8")
    lease = _acquire_test_lease(recovery, tmp_path, monkeypatch, session_dir)
    try:
        ancestor.rename(original_ancestor)
        replacement = ancestor / "session"
        replacement.mkdir(parents=True)
        replacement_secret = replacement / "must-survive"
        replacement_secret.write_text("replacement", encoding="utf-8")

        with pytest.raises(recovery.WhatsAppRecoveryError):
            lease.reset_session()

        assert (original_ancestor / "session" / "original-secret").exists()
        assert replacement_secret.read_text(encoding="utf-8") == "replacement"
    finally:
        lease.release()


@pytest.mark.require_symlinks
def test_reset_rejects_symlinked_ancestor_before_deleting(tmp_path, monkeypatch):
    from gateway.platforms import whatsapp_recovery as recovery

    ancestor = tmp_path / "account"
    session_dir = ancestor / "session"
    session_dir.mkdir(parents=True)
    original_ancestor = tmp_path / "original-account"
    target_ancestor = tmp_path / "target-account"
    target_session = target_ancestor / "session"
    target_session.mkdir(parents=True)
    (session_dir / "original-secret").write_text("original", encoding="utf-8")
    target_secret = target_session / "target-secret"
    target_secret.write_text("target", encoding="utf-8")
    lease = _acquire_test_lease(recovery, tmp_path, monkeypatch, session_dir)
    try:
        ancestor.rename(original_ancestor)
        ancestor.symlink_to(target_ancestor, target_is_directory=True)

        with pytest.raises(recovery.WhatsAppRecoveryError):
            lease.reset_session()

        assert (original_ancestor / "session" / "original-secret").read_text(
            encoding="utf-8"
        ) == "original"
        assert target_secret.read_text(encoding="utf-8") == "target"
    finally:
        lease.release()


@pytest.mark.require_symlinks
def test_reset_unlinks_session_symlinks_without_touching_targets(
    tmp_path, monkeypatch
):
    from gateway.platforms import whatsapp_recovery as recovery

    session_dir = tmp_path / "account" / "session"
    session_dir.mkdir(parents=True)
    outside = tmp_path / "outside-secret"
    outside.write_text("keep", encoding="utf-8")
    (session_dir / "auth-link").symlink_to(outside)
    lease = _acquire_test_lease(recovery, tmp_path, monkeypatch, session_dir)
    try:
        lease.reset_session()
        assert outside.read_text(encoding="utf-8") == "keep"
        assert list(session_dir.iterdir()) == []
    finally:
        lease.release()


@pytest.mark.skipif(os.name == "nt", reason="POSIX FIFO semantics")
def test_reset_preflight_failure_leaves_every_entry_untouched(tmp_path, monkeypatch):
    """A late hostile entry must not permit partial, order-dependent deletion."""
    from gateway.platforms import whatsapp_recovery as recovery

    session_dir = tmp_path / "account" / "session"
    session_dir.mkdir(parents=True)
    early_secret = session_dir / "a-would-be-deleted-first"
    early_secret.write_text("keep", encoding="utf-8")
    hostile = session_dir / "z-hostile-fifo"
    os.mkfifo(hostile)
    lease = _acquire_test_lease(recovery, tmp_path, monkeypatch, session_dir)
    try:
        with pytest.raises(recovery.WhatsAppRecoveryError):
            lease.reset_session()

        assert early_secret.read_text(encoding="utf-8") == "keep"
        assert hostile.exists()
        assert sorted(path.name for path in session_dir.iterdir()) == [
            "a-would-be-deleted-first",
            "z-hostile-fifo",
        ]
    finally:
        lease.release()


def test_reset_keeps_both_locks_until_explicit_idempotent_release(
    tmp_path, monkeypatch
):
    from gateway.platforms import whatsapp_recovery as recovery

    session_dir = tmp_path / "account" / "session"
    session_dir.mkdir(parents=True)
    (session_dir / "creds.json").write_text("{}", encoding="utf-8")
    lease = _acquire_test_lease(recovery, tmp_path, monkeypatch, session_dir)

    lease.reset_session()

    assert lease.active is True
    assert recovery.acquire_whatsapp_connect_gate(session_dir) is None
    lease.release()
    lease.release()
    assert lease.active is False

    replacement = recovery.acquire_whatsapp_connect_gate(session_dir)
    assert replacement is not None
    replacement.release()


def test_legacy_reset_helper_fails_closed_without_lease(tmp_path):
    from gateway.platforms.whatsapp_common import reset_whatsapp_session_dir

    session_dir = tmp_path / "session"
    session_dir.mkdir()
    sentinel = session_dir / "must-survive"
    sentinel.write_text("keep", encoding="utf-8")

    with pytest.raises(RuntimeError):
        reset_whatsapp_session_dir(session_dir)

    assert sentinel.read_text(encoding="utf-8") == "keep"
