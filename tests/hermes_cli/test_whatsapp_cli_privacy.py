"""CLI privacy boundary for `hermes whatsapp` pairing and status output.

The supported one-shot pairing command runs in the human owner's direct local,
non-recording terminal.  The QR/pairing payload must stay on that PTY and in
memory: never in ``bridge.log``, any other file, model context, clipboard,
screenshot, terminal transcript, or a Herdr event payload.  CLI status must
never echo a raw allowlist value.

Every identifier here is synthetic — fictional ``555-0000`` phone shapes and
explicit ``SYNTHETIC``/``CANARY``/``FAKE`` markers.  No real JID, allowlist,
credential, or pairing payload is used, read, or produced.  The pairing child
is a fake ``python -c`` process driven through a synthetic PTY; the real
one-shot pairing command is never run.
"""

from __future__ import annotations

import os
import pty
import selectors
import stat
import subprocess
import sys
from pathlib import Path

import pytest

# --------------------------------------------------------------------------
# Synthetic canaries.
# --------------------------------------------------------------------------
SYNTHETIC_USER_JID = "15550000001@s.whatsapp.net"
SYNTHETIC_GROUP_JID = "15550000001-1600000000@g.us"
SYNTHETIC_ALLOWLIST = "15550000001,15550000002"
SYNTHETIC_USER_NAME = "SyntheticCanaryUser-DO-NOT-LOG"
# Shaped like a Baileys QR payload (ref,noise,publicKey,identityKey) but wholly
# fabricated — this is not a usable pairing credential.
FAKE_QR_PAYLOAD = (
    "2@FAKEQRCANARY0000/SyntheticPairingRef,"
    "FAKEPUBKEYCANARY0000=,FAKEIDKEYCANARY0000=,FAKEADVCANARY0000="
)
FAKE_PAIRING_CODE = "FAKE-CANARY-PAIR-0000"

CLI_CANARIES = (
    SYNTHETIC_USER_JID,
    SYNTHETIC_GROUP_JID,
    SYNTHETIC_ALLOWLIST,
    SYNTHETIC_USER_NAME,
    FAKE_QR_PAYLOAD,
    FAKE_PAIRING_CODE,
)


def _assert_no_canaries(text: str, *, where: str) -> None:
    for canary in CLI_CANARIES:
        assert canary not in text, f"raw canary reached {where}: {canary!r}"


def _tree_contents(root: Path) -> str:
    """Concatenate every regular file under ``root`` (persistence sweep)."""
    chunks: list[str] = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and not path.is_symlink():
            chunks.append(path.read_text(encoding="utf-8", errors="replace"))
    return "\n".join(chunks)


# ---------------------------------------------------------------------------
# The pairing child must inherit the operator PTY, never a captured pipe.
# ---------------------------------------------------------------------------


@pytest.mark.skipif(os.name != "posix", reason="POSIX pairing lease semantics")
def test_pair_subprocess_kwargs_never_capture_child_output(tmp_path):
    """The lease's pairing kwargs must not redirect the child's stdout/stderr.

    Redirecting would move the QR off the operator's terminal into a Python
    buffer that lands in model context, a transcript, or a file.  The only
    inheritance the pairing child may receive is the held lease FD.
    """
    import threading

    from gateway.platforms.whatsapp_recovery import WhatsAppRecoveryLease

    # Build the lease directly: acquiring a real one performs live bridge-port
    # ownership verification, which is environmental and unrelated to the
    # output-capture contract under test here.  The assertion is unchanged.
    session_dir = tmp_path / "session"
    session_dir.mkdir(mode=0o700)
    gate = tmp_path / "gate.lock"
    handle = gate.open("w")
    try:
        lease = WhatsAppRecoveryLease(
            identity=str(session_dir),
            requested_path=session_dir,
            handle=handle,
            guard=threading.Lock(),
        )
        kwargs = lease.pair_subprocess_kwargs()
    finally:
        handle.close()

    assert set(kwargs) <= {"pass_fds"}, (
        "pairing child received extra subprocess kwargs: " f"{sorted(kwargs)}"
    )
    for forbidden in ("stdout", "stderr", "capture_output", "text", "encoding"):
        assert forbidden not in kwargs, (
            f"pairing child output would be captured via {forbidden!r}"
        )


@pytest.mark.skipif(os.name != "posix", reason="requires a POSIX pty")
def test_fake_qr_reaches_only_the_pairing_pty_and_is_never_persisted(tmp_path):
    """A fake QR printed by a synthetic pairing child stays on the PTY.

    This models the supported direct local non-recording terminal: the child's
    stdout is the operator's tty, so the payload is visible there and nowhere
    else.  The session directory must stay free of it.
    """
    session_dir = tmp_path / "session"
    session_dir.mkdir(mode=0o700)

    master_fd, slave_fd = pty.openpty()
    try:
        child = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import sys;sys.stdout.write(sys.argv[1]+chr(10));sys.stdout.flush()",
                FAKE_QR_PAYLOAD,
            ],
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            cwd=str(session_dir),
        )
        os.close(slave_fd)
        slave_fd = -1

        captured = bytearray()
        selector = selectors.DefaultSelector()
        selector.register(master_fd, selectors.EVENT_READ)
        while True:
            if not selector.select(timeout=10):
                break
            try:
                chunk = os.read(master_fd, 4096)
            except OSError:
                break
            if not chunk:
                break
            captured.extend(chunk)
            if FAKE_QR_PAYLOAD.encode() in captured:
                break
        selector.close()
        child.wait(timeout=10)
    finally:
        if slave_fd >= 0:
            os.close(slave_fd)
        os.close(master_fd)

    # Present on the operator's PTY — that is the one permitted sink.
    assert FAKE_QR_PAYLOAD.encode() in captured

    # Absent from every persisted byte under the session directory.
    _assert_no_canaries(_tree_contents(session_dir), where="the session directory")
    assert not list(session_dir.rglob("bridge.log"))


BRIDGE_DIR = (
    Path(__file__).resolve().parents[2] / "scripts" / "whatsapp-bridge"
)


def _run_bridge_on_pty(session_dir: Path, timeout: float = 15.0) -> str:
    """Drive the bridge through a synthetic PTY and return what it printed.

    The child is the real bridge running against the fake Baileys loader, so
    nothing contacts WhatsApp and no real pairing is attempted.
    """
    master_fd, slave_fd = pty.openpty()
    env = dict(
        os.environ,
        FAKE_WA_QR=FAKE_QR_PAYLOAD,
        WHATSAPP_ALLOWED_USERS="",
        WHATSAPP_MODE="self-chat",
    )
    try:
        child = subprocess.Popen(
            [
                "node",
                "--import",
                str(BRIDGE_DIR / "bridge.fake-baileys-loader.mjs"),
                str(BRIDGE_DIR / "bridge.js"),
                "--pair-only",
                "--session",
                str(session_dir),
            ],
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            cwd=str(BRIDGE_DIR),
            env=env,
        )
        os.close(slave_fd)
        slave_fd = -1

        captured = bytearray()
        selector = selectors.DefaultSelector()
        selector.register(master_fd, selectors.EVENT_READ)
        while selector.select(timeout=timeout):
            try:
                chunk = os.read(master_fd, 4096)
            except OSError:
                break
            if not chunk:
                break
            captured.extend(chunk)
            if b"Waiting for scan" in captured:
                break
        selector.close()
        try:
            child.wait(timeout=3)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)
    finally:
        if slave_fd >= 0:
            os.close(slave_fd)
        os.close(master_fd)
    return captured.decode("utf-8", "replace")


@pytest.mark.skipif(os.name != "posix", reason="requires a POSIX pty")
def test_qr_still_renders_on_a_real_pairing_terminal(tmp_path):
    """Positive control: suppressing QR on non-ttys must not break pairing.

    The managed-path fix stops a QR from reaching bridge.log. This proves it
    did not simply disable the operator's supported one-shot pairing flow —
    on a genuine terminal the code is still displayed.
    """
    if not (BRIDGE_DIR / "bridge.js").exists():
        pytest.skip("bridge sources are not present")

    session_dir = tmp_path / "session"
    session_dir.mkdir(mode=0o700)
    out = _run_bridge_on_pty(session_dir)

    assert "Scan this QR code" in out, "pairing QR no longer renders on a tty"
    assert "needs to be paired again" not in out

    # Even on the pairing path the payload must not be persisted.
    _assert_no_canaries(_tree_contents(session_dir), where="the session directory")


# ---------------------------------------------------------------------------
# Fixed CLI diagnostics disclose no path or value.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "printer_name",
    [
        "_print_whatsapp_pairing_failed",
        "_print_whatsapp_disable_failed",
        "_print_whatsapp_enable_failed",
        "_print_whatsapp_bridge_missing",
        "_print_whatsapp_dependency_install_failed",
    ],
)
def test_failure_diagnostics_are_fixed_and_disclose_no_path_or_value(
    capsys, printer_name, tmp_path
):
    """Failure text must be a fixed string with no path, errno, or identity."""
    import hermes_cli.main as cli

    getattr(cli, printer_name)()
    out = capsys.readouterr().out

    _assert_no_canaries(out, where=f"{printer_name} output")
    assert str(tmp_path) not in out
    assert str(Path.home()) not in out
    assert "Errno" not in out
    assert "Traceback" not in out


def test_pairing_instructions_never_echo_allowlist_values(capsys, monkeypatch):
    """Pre-pairing instructions must not print configured allowlist entries."""
    import hermes_cli.main as cli

    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", SYNTHETIC_ALLOWLIST)
    cli._print_whatsapp_pairing_instructions("self-chat")
    cli._print_whatsapp_pairing_instructions("bot")
    out = capsys.readouterr().out

    _assert_no_canaries(out, where="pairing instructions")


# ---------------------------------------------------------------------------
# Hostile local state around the credential artifacts the CLI inspects.
# ---------------------------------------------------------------------------


def _creds(session_dir: Path) -> Path:
    return session_dir / "creds.json"


@pytest.mark.skipif(os.name != "posix", reason="POSIX local-state semantics")
@pytest.mark.parametrize(
    "hostile",
    ["symlink", "dangling_symlink", "directory", "fifo"],
)
def test_credential_reader_rejects_hostile_session_state(tmp_path, hostile):
    """A non-regular or symlinked credential entry is never followed or read."""
    from plugins.platforms.whatsapp.adapter import (
        _SavedCredentialsInvalid,
        _read_saved_credentials,
    )

    session_dir = tmp_path / "session"
    session_dir.mkdir(mode=0o700)
    target = _creds(session_dir)

    if hostile == "symlink":
        real = tmp_path / "elsewhere.json"
        real.write_text('{"me":{"id":"' + SYNTHETIC_USER_JID + '"}}')
        target.symlink_to(real)
    elif hostile == "dangling_symlink":
        target.symlink_to(tmp_path / "absent.json")
    elif hostile == "directory":
        target.mkdir()
    elif hostile == "fifo":
        os.mkfifo(target)

    with pytest.raises(_SavedCredentialsInvalid) as excinfo:
        _read_saved_credentials(target)

    # The exception must not carry the path or any credential value.
    _assert_no_canaries(str(excinfo.value), where="the credential error")
    assert str(target) not in str(excinfo.value)


@pytest.mark.skipif(os.name != "posix", reason="POSIX local-state semantics")
def test_credential_reader_rejects_replacement_between_lstat_and_open(
    tmp_path, monkeypatch
):
    """A regular file swapped for another between lstat and open is rejected.

    The pre/post ``fstat`` identity check exists for exactly this race: the
    entry that was inspected must be the entry that was opened.
    """
    from plugins.platforms.whatsapp import adapter as wa_adapter

    session_dir = tmp_path / "session"
    session_dir.mkdir(mode=0o700)
    target = _creds(session_dir)
    target.write_text('{"noiseKey":{"private":"FAKECANARY"}}')

    decoy = tmp_path / "decoy.json"
    decoy.write_text('{"me":{"id":"' + SYNTHETIC_USER_JID + '"}}')

    real_open = os.open

    def _swapping_open(path, flags, *args, **kwargs):
        # Replace the inspected entry after lstat, before the descriptor opens.
        if str(path) == str(target):
            os.unlink(target)
            os.link(decoy, target)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(wa_adapter.os, "open", _swapping_open)

    with pytest.raises(wa_adapter._SavedCredentialsInvalid):
        wa_adapter._read_saved_credentials(target)


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode semantics")
def test_paired_session_directory_is_exactly_0700(tmp_path):
    """The paired-session directory must be owner-only, exactly 0700."""
    from gateway.platforms.whatsapp_common import ensure_whatsapp_session_dir

    session_dir = tmp_path / "session"
    ensure_whatsapp_session_dir(session_dir)

    mode = stat.S_IMODE(session_dir.lstat().st_mode)
    assert mode == 0o700, f"session directory mode is {mode:04o}, expected 0700"
