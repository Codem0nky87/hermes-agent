"""Regression tests for ``cmd_whatsapp`` env-var write ordering.

Before the fix, ``hermes whatsapp`` wrote ``WHATSAPP_ENABLED=true`` at
step 2 — before npm install (step 4) and before QR pairing (step 6).
If the user Ctrl+C'd at any later step, ``.env`` claimed WhatsApp was
ready when the bridge still had no ``creds.json``.  Every subsequent
``hermes gateway`` then paid a 30s bridge-bootstrap timeout and queued
WhatsApp for indefinite retries — looking like "the gateway is broken."

The fix: only set ``WHATSAPP_ENABLED=true`` once pairing actually
succeeds (creds.json exists).  Aborted setup leaves no enabled state.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import subprocess
import sys
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def isolated_home(tmp_path, monkeypatch):
    home = tmp_path / "home"
    hermes = home / ".hermes"
    hermes.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: home)
    monkeypatch.setenv("HERMES_HOME", str(hermes))
    # Ensure get_env_value cache doesn't carry stale state.
    for key in list(os.environ):
        if key.startswith("WHATSAPP_"):
            monkeypatch.delenv(key, raising=False)
    return hermes


def _env_value(hermes_home: Path, key: str) -> str | None:
    env_file = hermes_home / ".env"
    if not env_file.exists():
        return None
    for line in env_file.read_text().splitlines():
        if "=" not in line:
            continue
        k, _, v = line.partition("=")
        if k.strip() == key:
            return v.strip().strip('"').strip("'")
    return None


def test_aborted_setup_does_not_enable_whatsapp(isolated_home, monkeypatch):
    """User picks mode 1, then Ctrl+C's at the allowed-users prompt.

    WHATSAPP_ENABLED must NOT be present in .env after abort.
    """
    from hermes_cli.main import cmd_whatsapp

    # First input() = mode choice, second input() = allowed-users prompt
    # We raise KeyboardInterrupt on the second call to simulate abort.
    inputs = iter(["1"])

    def fake_input(_prompt=""):
        try:
            return next(inputs)
        except StopIteration:
            raise KeyboardInterrupt

    monkeypatch.setattr("builtins.input", fake_input)
    # _require_tty calls sys.stdin.isatty — make it pass.
    monkeypatch.setattr("hermes_cli.main._require_tty", lambda *_a, **_kw: None)
    # No node, no bridge script — we shouldn't reach those steps anyway.

    buf = io.StringIO()
    with redirect_stdout(buf):
        try:
            cmd_whatsapp(MagicMock())
        except KeyboardInterrupt:
            pass

    assert _env_value(isolated_home, "WHATSAPP_ENABLED") is None, (
        "Setup aborted before pairing — WHATSAPP_ENABLED must not be set. "
        f"Got .env: {(isolated_home / '.env').read_text() if (isolated_home / '.env').exists() else '(missing)'}"
    )


def test_existing_complete_pairing_decline_enables_whatsapp(
    isolated_home, monkeypatch
):
    """Declining re-pair preserves verified auth and restores enablement."""
    from hermes_cli.main import cmd_whatsapp

    # Pre-create a complete paired session WITHOUT WHATSAPP_ENABLED in .env.
    session = isolated_home / "whatsapp" / "session"
    session.mkdir(parents=True)
    (session / "creds.json").write_text(
        json.dumps({"me": {"id": "20000000000:7@s.whatsapp.net"}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("WHATSAPP_MODE", "bot")
    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", "15551234567")

    # mode already set → skip mode prompt; users already set → skip update
    # prompt with "no"; pairing exists → "no, keep session" → return.
    inputs = iter(["n", "n"])

    def fake_input(_prompt=""):
        try:
            return next(inputs)
        except StopIteration:
            return "n"

    monkeypatch.setattr("builtins.input", fake_input)
    monkeypatch.setattr("hermes_cli.main._require_tty", lambda *_a, **_kw: None)
    # Skip the bridge npm install — we're testing setup-ordering, not bridge
    # bootstrapping.  Pretend node_modules exists (Path.exists -> True for that
    # specific check is hard to scope, so instead pretend npm install would
    # succeed silently if reached).
    monkeypatch.setattr(
        "subprocess.run",
        lambda *_a, **_kw: MagicMock(returncode=0, stderr=""),
    )
    monkeypatch.setattr("shutil.which", lambda _name: "/usr/bin/npm")
    # Patch (bridge_dir / "node_modules").exists() by stubbing Path.exists
    # to True for that one specific subpath.  Easier: pre-create it as a
    # symlink to /tmp.  But we can't write to the repo.  Instead, stub
    # Path.exists wholesale to True for node_modules; the creds.json check
    # in the same function still works because we wrote it ourselves.
    _orig_exists = Path.exists
    def _stub_exists(self):
        if self.name == "node_modules":
            return True
        return _orig_exists(self)
    monkeypatch.setattr(Path, "exists", _stub_exists)

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())

    assert _env_value(isolated_home, "WHATSAPP_ENABLED") == "true"


def test_existing_incomplete_pairing_decline_stays_disabled(
    isolated_home, monkeypatch
):
    """A partial creds file is never promoted by the non-destructive branch."""
    from hermes_cli.main import cmd_whatsapp

    session = isolated_home / "whatsapp" / "session"
    session.mkdir(parents=True)
    (session / "creds.json").write_text("{}", encoding="utf-8")
    monkeypatch.setenv("WHATSAPP_MODE", "bot")
    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", "15551234567")
    _fixed_inputs(monkeypatch, ["n", "n"])
    _skip_bridge_bootstrap(monkeypatch)

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())
    out = buf.getvalue()

    assert _env_value(isolated_home, "WHATSAPP_ENABLED") is None
    assert "configured and paired" not in out
    assert "incomplete" in out.lower()


@pytest.mark.parametrize("stamp_state, expected_installs", [("missing", 1), ("fresh", 0)])
def test_cli_dependency_install_follows_resolved_manifest_stamp(
    isolated_home, monkeypatch, tmp_path, stamp_state, expected_installs
):
    from gateway.platforms import whatsapp_common
    from hermes_cli.main import cmd_whatsapp

    bridge_dir = tmp_path / "bridge"
    node_modules = bridge_dir / "node_modules"
    node_modules.mkdir(parents=True)
    (bridge_dir / "bridge.js").write_text("// bridge\n")
    (bridge_dir / "package.json").write_text(
        '{"dependencies":{"bridge":"whatever-this-manifest-declares"}}',
        encoding="utf-8",
    )
    (bridge_dir / "package-lock.json").write_text(
        '{"lockfileVersion":3,"packages":{}}',
        encoding="utf-8",
    )
    if stamp_state == "fresh":
        whatsapp_common.write_whatsapp_bridge_dependency_stamp(bridge_dir)

    session = isolated_home / "whatsapp" / "session"
    monkeypatch.setenv("WHATSAPP_MODE", "bot")
    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", "15551234567")
    monkeypatch.setattr("hermes_cli.main._require_tty", lambda *_a, **_kw: None)
    monkeypatch.setattr(
        whatsapp_common, "resolve_whatsapp_bridge_dir", lambda: bridge_dir
    )
    monkeypatch.setattr(
        "hermes_constants.find_node_executable", lambda name: f"/test/{name}"
    )
    _fixed_inputs(monkeypatch, ["n"])
    installs = []

    class NoopLease:
        def pair_subprocess_kwargs(self):
            return {}

        def reset_session(self, _path=None):
            pytest.fail("an unmarked fresh pairing must not reset the session")

        def release(self):
            pass

    monkeypatch.setattr(
        "gateway.platforms.whatsapp_recovery.acquire_whatsapp_recovery_lease",
        lambda *_args, **_kwargs: NoopLease(),
    )

    def run(cmd, *_args, **_kwargs):
        if "install" in cmd:
            installs.append(cmd)
        if "--pair-only" in cmd:
            session.mkdir(parents=True)
            (session / "creds.json").write_text(
                '{"me":{"id":"1@s.whatsapp.net"}}', encoding="utf-8"
            )
        return MagicMock(returncode=0, stderr="")

    monkeypatch.setattr("subprocess.run", run)

    cmd_whatsapp(MagicMock())

    assert len(installs) == expected_installs
    assert whatsapp_common.whatsapp_bridge_dependencies_fresh(bridge_dir) is True


# ── Revoked-session recovery ────────────────────────────────────────────────
#
# When the bridge records a durable revocation verdict in the session
# directory, the credentials there can never be accepted again.  The old
# "Re-pair? This will clear the existing session. [y/N]" prompt defaults to
# *no* and only appears when creds.json exists — so a user hitting Enter was
# told "WhatsApp is configured and paired!" about a session WhatsApp had
# already killed, and a marker left without creds.json skipped the prompt
# entirely.  Clearing credentials is destructive, so it still requires an
# explicit yes; what changes is that the user is told the truth and is asked
# at all.


def _fixed_inputs(monkeypatch, answers, default="n"):
    it = iter(answers)

    def fake_input(_prompt=""):
        try:
            return next(it)
        except StopIteration:
            return default

    monkeypatch.setattr("builtins.input", fake_input)


def _skip_bridge_bootstrap(monkeypatch, *, reset_failure=None):
    """Get cmd_whatsapp past mode/allowlist/npm to the session step."""
    monkeypatch.setattr("hermes_cli.main._require_tty", lambda *_a, **_kw: None)
    monkeypatch.setattr("shutil.which", lambda _name: "/usr/bin/npm")
    monkeypatch.setattr(
        "gateway.platforms.whatsapp_common.whatsapp_bridge_dependencies_fresh",
        lambda _bridge_dir: True,
    )
    calls = []

    def fake_run(cmd, *_a, **_kw):
        calls.append(list(cmd))
        return MagicMock(returncode=0, stderr="")

    monkeypatch.setattr("subprocess.run", fake_run)
    _orig_exists = Path.exists
    monkeypatch.setattr(
        Path, "exists", lambda self: True if self.name == "node_modules" else _orig_exists(self)
    )

    class NoopLease:
        active = True

        def pair_subprocess_kwargs(self):
            return {}

        def reset_session(self, path: Path | None = None):
            assert self.active
            assert path is not None
            if reset_failure is not None:
                reset_failure(path)
                return
            shutil.rmtree(path)
            path.mkdir(parents=True)

        def release(self):
            self.active = False

    monkeypatch.setattr(
        "gateway.platforms.whatsapp_recovery.acquire_whatsapp_recovery_lease",
        lambda *_args, **_kwargs: NoopLease(),
    )
    return calls


def _revoked_cli_session(isolated_home, monkeypatch, marker, *, creds=True):
    session = isolated_home / "whatsapp" / "session"
    session.mkdir(parents=True)
    if creds:
        (session / "creds.json").write_text('{"me":{"id":"1@s.whatsapp.net"}}')
    if marker is not None:
        (session / "revoked.json").write_text(marker)
    monkeypatch.setenv("WHATSAPP_MODE", "bot")
    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", "15551234567")
    return session


def test_revoked_session_declined_leaves_everything_intact(isolated_home, monkeypatch):
    """Declining must not clear anything, and must not claim we are paired."""
    from hermes_cli.main import cmd_whatsapp

    session = _revoked_cli_session(isolated_home, monkeypatch, '{"revoked": true}')
    creds_before = (session / "creds.json").read_bytes()
    marker_before = (session / "revoked.json").read_bytes()
    run_calls = _skip_bridge_bootstrap(monkeypatch)
    _fixed_inputs(monkeypatch, ["n", "n"])

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())
    out = buf.getvalue()

    assert (session / "creds.json").read_bytes() == creds_before
    assert (session / "revoked.json").read_bytes() == marker_before
    assert _env_value(isolated_home, "WHATSAPP_ENABLED") is None
    assert "configured and paired" not in out
    assert "revoked" in out.lower()
    assert not any("--pair-only" in c for c in run_calls)


def test_revoked_session_confirmed_clears_and_pairs(isolated_home, monkeypatch):
    """A confirmed yes replaces the whole session directory, then pairs."""
    from hermes_cli.main import cmd_whatsapp

    session = _revoked_cli_session(isolated_home, monkeypatch, '{"revoked": true}')
    run_calls = _skip_bridge_bootstrap(monkeypatch)
    _fixed_inputs(monkeypatch, ["n", "y"])

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())
    out = buf.getvalue()

    # The user must be told *why* they are being asked, not just asked.
    assert "revoked" in out.lower()
    assert session.is_dir()
    assert not (session / "creds.json").exists()
    assert not (session / "revoked.json").exists()
    assert any("--pair-only" in c for c in run_calls), "must reach QR pairing"


def test_confirmed_reset_holds_recovery_lease_through_pair_process(
    isolated_home, monkeypatch
):
    """The reset and pair-only child are one indivisible recovery operation."""
    from gateway.platforms import whatsapp_recovery as recovery
    from hermes_cli.main import cmd_whatsapp

    session = _revoked_cli_session(
        isolated_home, monkeypatch, '{"revoked": true}'
    )
    _fixed_inputs(monkeypatch, ["n", "y"])
    _skip_bridge_bootstrap(monkeypatch)
    events = []

    class FakeLease:
        active = True

        def pair_subprocess_kwargs(self):
            return {}

        def reset_session(self, path: Path | None = None):
            assert self.active
            assert path is not None
            assert path == session
            events.append("reset")
            for child in list(path.iterdir()):
                child.unlink()

        def release(self):
            events.append("release")
            self.active = False

    lease = FakeLease()
    monkeypatch.setattr(
        recovery,
        "acquire_whatsapp_recovery_lease",
        lambda *_args, **_kwargs: lease,
    )
    monkeypatch.setattr(
        "gateway.platforms.whatsapp_common.reset_whatsapp_session_dir",
        lambda _path: pytest.fail("CLI reset must use the active lease method"),
    )

    def run(cmd, *_args, **_kwargs):
        if "--pair-only" in cmd:
            assert lease.active
            events.append("pair")
        return MagicMock(returncode=0, stderr="")

    monkeypatch.setattr("subprocess.run", run)

    cmd_whatsapp(MagicMock())

    assert events == ["reset", "pair", "release"]
    assert lease.active is False
    assert session.is_dir()


def test_cli_pair_process_launch_error_is_fixed_private_and_releases_lease(
    isolated_home, monkeypatch
):
    from gateway.platforms import whatsapp_recovery as recovery
    from hermes_cli.main import cmd_whatsapp

    session = _revoked_cli_session(isolated_home, monkeypatch, '{"revoked": true}')
    _fixed_inputs(monkeypatch, ["n", "y"])
    _skip_bridge_bootstrap(monkeypatch)
    releases = []
    failure_sentinel = "/private/pair-launch-jid@s.whatsapp.net"

    class FakeLease:
        active = True

        def pair_subprocess_kwargs(self):
            return {}

        def reset_session(self, path: Path | None = None):
            assert self.active
            assert path == session
            shutil.rmtree(session)
            session.mkdir(parents=True)

        def release(self):
            releases.append("release")
            self.active = False

    monkeypatch.setattr(
        recovery,
        "acquire_whatsapp_recovery_lease",
        lambda *_args, **_kwargs: FakeLease(),
    )

    def run(cmd, *_args, **_kwargs):
        if "--pair-only" in cmd:
            raise OSError(failure_sentinel)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr("subprocess.run", run)

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())
    out = buf.getvalue()

    assert releases == ["release"]
    assert "WhatsApp pairing failed." in out
    assert "paired successfully" not in out
    assert "configured and paired" not in out
    assert failure_sentinel not in out
    assert _env_value(isolated_home, "WHATSAPP_ENABLED") == "false"


def test_revoked_marker_without_creds_still_prompts_and_recovers(isolated_home, monkeypatch):
    """A marker with no creds.json used to skip the prompt entirely."""
    from hermes_cli.main import cmd_whatsapp

    session = _revoked_cli_session(isolated_home, monkeypatch, '{"revoked": true}', creds=False)
    run_calls = _skip_bridge_bootstrap(monkeypatch)
    _fixed_inputs(monkeypatch, ["n", "y"])

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())
    out = buf.getvalue()

    assert "revoked" in out.lower()
    assert session.is_dir()
    assert not (session / "revoked.json").exists()
    assert any("--pair-only" in c for c in run_calls)


@pytest.mark.parametrize("marker", ['{"revoked": false}', "[]", "{not json", ""])
def test_invalid_marker_fails_closed_into_the_revoked_prompt(isolated_home, monkeypatch, marker):
    """An uninterpretable marker takes the revoked path, not the ordinary one.

    The ordinary path ends in "✓ WhatsApp is configured and paired!" and sets
    ``WHATSAPP_ENABLED=true``.  Saying that about a session whose verdict could
    not be read is how a corrupted marker silently disarms the whole mechanism,
    so an unusable marker is treated as evidence of revocation instead.

    Declining still changes nothing on disk: failing closed picks which
    question the user is asked, never what is deleted without them.
    """
    from hermes_cli.main import cmd_whatsapp

    session = _revoked_cli_session(isolated_home, monkeypatch, marker)
    creds_before = (session / "creds.json").read_bytes()
    run_calls = _skip_bridge_bootstrap(monkeypatch)
    _fixed_inputs(monkeypatch, ["n", "n"])

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())
    out = buf.getvalue()

    assert "revoked" in out.lower(), "the user must be told why they are being asked"
    assert "configured and paired" not in out
    assert (session / "creds.json").read_bytes() == creds_before
    assert (session / "revoked.json").read_bytes() == marker.encode()
    assert _env_value(isolated_home, "WHATSAPP_ENABLED") is None
    assert not any("--pair-only" in c for c in run_calls)


def test_invalid_marker_confirmed_clears_and_pairs(isolated_home, monkeypatch):
    """The explicit destructive reset is the way out of a fail-closed verdict.

    An unreadable marker would otherwise be unrecoverable — there is nothing
    to fix in a file we refuse to interpret — so the same yes that recovers a
    genuine revocation has to recover this too.
    """
    from hermes_cli.main import cmd_whatsapp

    session = _revoked_cli_session(isolated_home, monkeypatch, "{not json")
    run_calls = _skip_bridge_bootstrap(monkeypatch)
    _fixed_inputs(monkeypatch, ["n", "y"])

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())

    assert session.is_dir()
    assert not (session / "creds.json").exists()
    assert not (session / "revoked.json").exists()
    assert any("--pair-only" in c for c in run_calls), "must reach QR pairing"


def test_revoked_session_reports_a_failed_clear_instead_of_pairing(isolated_home, monkeypatch):
    """A clear that could not happen must stop, not proceed to pairing.

    Pairing on top of auth state still on disk is exactly what clearing the
    session exists to prevent, so a failure has to be reported and the command
    has to stop — without a traceback, which tells the user nothing they can
    act on.
    """
    from hermes_cli.main import cmd_whatsapp

    session = _revoked_cli_session(isolated_home, monkeypatch, '{"revoked": true}')
    def _boom(path):
        raise PermissionError(13, "Permission denied")

    run_calls = _skip_bridge_bootstrap(
        monkeypatch,
        reset_failure=_boom,
    )
    _fixed_inputs(monkeypatch, ["n", "y"])

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())
    out = buf.getvalue()

    assert "Session cleared" not in out, "a failed clear must not claim success"
    assert "could not be cleared" in out.lower()
    assert not any("--pair-only" in c for c in run_calls), "pairing must not start"
    assert (session / "creds.json").exists()
    assert _env_value(isolated_home, "WHATSAPP_ENABLED") == "false"


# ── What a failed clear may say ─────────────────────────────────────────────
#
# An OSError from the filesystem stringifies to something like
# ``[Errno 13] Permission denied: '/Users/alice/.hermes/whatsapp/session'``.
# Interpolating it into the failure line puts the operator's home directory —
# and therefore their OS username — on a terminal that gets screenshotted into
# bug reports and pasted into chat threads.  The path is also useless to the
# reader: they just ran ``hermes whatsapp``, so they know which session it is.
#
# Both reset call sites (revoked-session recovery, ordinary re-pair) go through
# the same rule, because the second is the one people hit every day.

#: An errno payload carrying something that must not be echoed back.
_SECRET_RESET_PATH = "/Users/sentinel-operator/.hermes/whatsapp/session"


def _permission_error(*_args, **_kwargs):
    raise PermissionError(13, "Permission denied", _SECRET_RESET_PATH)


def _assert_generic_clear_failure(out):
    """The failure is reported, actionably, and reveals nothing."""
    assert "could not be cleared" in out.lower(), "the failure must still be reported"
    assert "permissions" in out.lower(), "the actionable repair guidance must survive"
    assert "Session cleared" not in out, "a failed clear must not claim success"

    assert _SECRET_RESET_PATH not in out
    assert "sentinel-operator" not in out, "the OS user must not be disclosed"
    assert ".hermes" not in out, "no absolute session path may be printed"
    assert "Errno" not in out, "raw exception text must not be printed"
    assert "Permission denied" not in out, "raw exception text must not be printed"
    assert "Traceback" not in out


def test_failed_clear_of_a_revoked_session_reveals_no_path_or_errno(
    isolated_home, monkeypatch
):
    from hermes_cli.main import cmd_whatsapp

    session = _revoked_cli_session(isolated_home, monkeypatch, '{"revoked": true}')
    run_calls = _skip_bridge_bootstrap(
        monkeypatch,
        reset_failure=_permission_error,
    )
    _fixed_inputs(monkeypatch, ["n", "y"])

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())
    out = buf.getvalue()

    _assert_generic_clear_failure(out)
    assert not any("--pair-only" in c for c in run_calls), "pairing must not start"
    assert (session / "creds.json").exists(), "the session must be left as found"


def test_failed_clear_of_an_existing_session_reveals_no_path_or_errno(
    isolated_home, monkeypatch
):
    """The ordinary re-pair prompt — no revocation involved — obeys the
    same rule.  This is the path a user takes to re-pair a healthy session,
    so it is the one that gets shown to other people most often."""
    from hermes_cli.main import cmd_whatsapp

    session = _revoked_cli_session(isolated_home, monkeypatch, None)
    run_calls = _skip_bridge_bootstrap(
        monkeypatch,
        reset_failure=_permission_error,
    )
    _fixed_inputs(monkeypatch, ["n", "y"])

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())
    out = buf.getvalue()

    _assert_generic_clear_failure(out)
    assert not any("--pair-only" in c for c in run_calls), "pairing must not start"
    assert (session / "creds.json").exists(), "the session must be left as found"


def _write_complete_cli_credentials(session: Path) -> None:
    (session / "creds.json").write_text(
        json.dumps(
            {
                "me": {
                    "id": "20000000000:7@s.whatsapp.net",
                    "name": "Local CLI Device",
                }
            }
        ),
        encoding="utf-8",
    )


def _run_fresh_cli_pairing(
    isolated_home,
    monkeypatch,
    pair_run,
    *,
    recovery_lease=None,
):
    from gateway.platforms import whatsapp_recovery as recovery
    from hermes_cli.main import cmd_whatsapp

    session = _revoked_cli_session(
        isolated_home,
        monkeypatch,
        None,
        creds=False,
    )
    _fixed_inputs(monkeypatch, ["n"])
    _skip_bridge_bootstrap(monkeypatch)
    if recovery_lease is not None:
        def acquire(path, *, bridge_script, bridge_port):
            assert path == session
            assert bridge_script.name == "bridge.js"
            assert bridge_port == 3000
            return recovery_lease

        monkeypatch.setattr(
            recovery,
            "acquire_whatsapp_recovery_lease",
            acquire,
        )

    def run(cmd, *_args, **_kwargs):
        if "--pair-only" in cmd:
            return pair_run(session, cmd)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr("subprocess.run", run)
    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())
    return session, buf.getvalue()


@pytest.mark.parametrize("returncode", [78, 1, 23])
def test_pair_only_nonzero_with_complete_creds_fails_closed(
    isolated_home, monkeypatch, returncode
):
    """A stale/new creds file cannot turn any nonzero child exit into success."""
    sentinel = "/private/nonzero-child-account@s.whatsapp.net"

    def pair_run(session, cmd):
        _write_complete_cli_credentials(session)
        return subprocess.CompletedProcess(
            cmd,
            returncode,
            stdout=sentinel,
            stderr=sentinel,
        )

    session, out = _run_fresh_cli_pairing(
        isolated_home,
        monkeypatch,
        pair_run,
    )

    assert (session / "creds.json").exists()
    assert _env_value(isolated_home, "WHATSAPP_ENABLED") == "false"
    assert "WhatsApp pairing failed." in out
    assert "paired successfully" not in out
    assert "configured and paired" not in out
    assert sentinel not in out


@pytest.mark.parametrize(
    "artifact",
    [
        "missing",
        "empty-object",
        "malformed-creds",
        "revoked",
        "malformed-marker",
        "unreadable-marker",
    ],
)
def test_pair_only_zero_requires_complete_creds_and_absent_marker(
    isolated_home, monkeypatch, artifact
):
    private_sentinel = "/private/partial-auth-account@s.whatsapp.net"

    def pair_run(session, cmd):
        if artifact == "empty-object":
            (session / "creds.json").write_text("{}", encoding="utf-8")
        elif artifact == "malformed-creds":
            (session / "creds.json").write_text(
                private_sentinel,
                encoding="utf-8",
            )
        elif artifact in {"revoked", "malformed-marker", "unreadable-marker"}:
            _write_complete_cli_credentials(session)
            marker_path = session / "revoked.json"
            if artifact == "unreadable-marker":
                marker_path.mkdir()
            else:
                marker = (
                    '{"revoked": true}'
                    if artifact == "revoked"
                    else "{not-json"
                )
                marker_path.write_text(marker, encoding="utf-8")
        return subprocess.CompletedProcess(cmd, 0)

    _session, out = _run_fresh_cli_pairing(
        isolated_home,
        monkeypatch,
        pair_run,
    )

    assert _env_value(isolated_home, "WHATSAPP_ENABLED") == "false"
    assert "WhatsApp pairing failed." in out
    assert "paired successfully" not in out
    assert private_sentinel not in out


def test_pair_only_success_requires_complete_local_credentials(
    isolated_home, monkeypatch
):
    def pair_run(session, cmd):
        _write_complete_cli_credentials(session)
        return subprocess.CompletedProcess(cmd, 0)

    _session, out = _run_fresh_cli_pairing(
        isolated_home,
        monkeypatch,
        pair_run,
    )

    assert _env_value(isolated_home, "WHATSAPP_ENABLED") == "true"
    assert "WhatsApp paired successfully!" in out
    assert "WhatsApp pairing failed." not in out


def test_cli_disables_before_reset_and_holds_lease_through_validation(
    isolated_home, monkeypatch
):
    from gateway.platforms import whatsapp_recovery as recovery
    from hermes_cli import config
    from hermes_cli.main import cmd_whatsapp

    session = _revoked_cli_session(
        isolated_home,
        monkeypatch,
        '{"revoked": true}',
    )
    monkeypatch.setenv("WHATSAPP_ENABLED", "true")
    _fixed_inputs(monkeypatch, ["n", "y"])
    _skip_bridge_bootstrap(monkeypatch)
    events = []
    real_save_env_value = config.save_env_value

    class Lease:
        active = True

        def pair_subprocess_kwargs(self):
            assert self.active
            return {}

        def reset_session(self, path: Path | None = None):
            assert self.active
            assert path == session
            assert path is not None
            assert events[-1] == "enabled:false"
            events.append("reset")
            for child in list(path.iterdir()):
                child.unlink()

        def release(self):
            assert self.active
            events.append("release")
            self.active = False

    lease = Lease()

    def acquire(path, *, bridge_script, bridge_port):
        assert path == session
        assert bridge_script.name == "bridge.js"
        assert bridge_port == 3000
        events.append("acquire")
        return lease

    def save_env_value(key, value):
        if key == "WHATSAPP_ENABLED":
            events.append(f"enabled:{value}")
        return real_save_env_value(key, value)

    def run(cmd, *_args, **_kwargs):
        if "--pair-only" in cmd:
            assert lease.active
            events.append("pair")
            _write_complete_cli_credentials(session)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(recovery, "acquire_whatsapp_recovery_lease", acquire)
    monkeypatch.setattr(config, "save_env_value", save_env_value)
    monkeypatch.setattr("subprocess.run", run)

    cmd_whatsapp(MagicMock())

    assert events == [
        "acquire",
        "enabled:false",
        "reset",
        "pair",
        "release",
        "enabled:true",
    ]


def test_cli_disable_failure_aborts_before_reset_or_launch_and_releases(
    isolated_home, monkeypatch
):
    from gateway.platforms import whatsapp_recovery as recovery
    from hermes_cli import config
    from hermes_cli.main import cmd_whatsapp

    session = _revoked_cli_session(
        isolated_home,
        monkeypatch,
        '{"revoked": true}',
    )
    creds_before = (session / "creds.json").read_bytes()
    marker_before = (session / "revoked.json").read_bytes()
    _fixed_inputs(monkeypatch, ["n", "y"])
    _skip_bridge_bootstrap(monkeypatch)
    events = []
    failure_sentinel = "/private/disable-write-account@s.whatsapp.net"
    real_save_env_value = config.save_env_value

    class Lease:
        active = True

        def pair_subprocess_kwargs(self):
            pytest.fail("disable failure must precede pair child launch")

        def reset_session(self, _path=None):
            events.append("reset")
            pytest.fail("disable failure must precede reset")

        def release(self):
            assert self.active
            events.append("release")
            self.active = False

    def acquire(path, *, bridge_script, bridge_port):
        assert path == session
        assert bridge_script.name == "bridge.js"
        assert bridge_port == 3000
        events.append("acquire")
        return Lease()

    def fail_closed_save(key, value):
        if key == "WHATSAPP_ENABLED" and value == "false":
            events.append("disable")
            raise OSError(failure_sentinel)
        return real_save_env_value(key, value)

    monkeypatch.setattr(
        recovery,
        "acquire_whatsapp_recovery_lease",
        acquire,
    )
    monkeypatch.setattr(config, "save_env_value", fail_closed_save)
    monkeypatch.setattr(
        "subprocess.run",
        lambda cmd, *_args, **_kwargs: events.append("pair")
        or subprocess.CompletedProcess(cmd, 0),
    )

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())
    out = buf.getvalue()

    assert events == ["acquire", "disable", "release"]
    assert (session / "creds.json").read_bytes() == creds_before
    assert (session / "revoked.json").read_bytes() == marker_before
    assert "could not be disabled" in out.lower()
    assert "Session cleared" not in out
    assert "paired successfully" not in out
    assert failure_sentinel not in out


def test_cli_keyboard_interrupt_keeps_disabled_and_releases_once(
    isolated_home, monkeypatch
):
    releases = []

    class Lease:
        def pair_subprocess_kwargs(self):
            return {}

        def reset_session(self, _path=None):
            pytest.fail("an unmarked fresh pairing must not reset the session")

        def release(self):
            releases.append("release")

    def pair_run(_session, _cmd):
        raise KeyboardInterrupt

    _session, out = _run_fresh_cli_pairing(
        isolated_home,
        monkeypatch,
        pair_run,
        recovery_lease=Lease(),
    )

    assert releases == ["release"]
    assert _env_value(isolated_home, "WHATSAPP_ENABLED") == "false"
    assert "paired successfully" not in out
    assert "WhatsApp pairing failed." in out


@pytest.mark.parametrize("artifact", ["symlink", "fifo", "directory", "oversize"])
def test_cli_credential_reader_rejects_blocking_or_unbounded_files(
    tmp_path, artifact
):
    session = tmp_path / "session"
    session.mkdir()
    creds = session / "creds.json"
    valid_payload = json.dumps({"me": {"id": "20000000000:7@s.whatsapp.net"}})
    if artifact == "symlink":
        target = tmp_path / "outside-creds.json"
        target.write_text(valid_payload, encoding="utf-8")
        creds.symlink_to(target)
    elif artifact == "fifo":
        if not hasattr(os, "mkfifo"):
            pytest.skip("FIFO creation is unavailable")
        os.mkfifo(creds)
    elif artifact == "directory":
        creds.mkdir()
    else:
        creds.write_text(
            json.dumps(
                {
                    "me": {"id": "20000000000:7@s.whatsapp.net"},
                    "padding": "x" * (1024 * 1024),
                }
            ),
            encoding="utf-8",
        )

    script = (
        "from pathlib import Path; "
        "from hermes_cli.main import _whatsapp_pairing_credentials_complete; "
        f"print(_whatsapp_pairing_credentials_complete(Path({str(session)!r})))"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(Path(__file__).resolve().parents[2]),
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == "False"


def test_cli_pair_child_inherits_only_lease_fd_and_declares_port(
    isolated_home, monkeypatch
):
    from gateway.platforms import whatsapp_recovery as recovery
    from hermes_cli.main import cmd_whatsapp

    session = _revoked_cli_session(
        isolated_home,
        monkeypatch,
        None,
        creds=False,
    )
    _fixed_inputs(monkeypatch, ["n"])
    _skip_bridge_bootstrap(monkeypatch)
    captured = {}

    class Lease:
        active = True

        def pair_subprocess_kwargs(self):
            return {"pass_fds": (91,)}

        def reset_session(self, _path=None):
            pytest.fail("an unmarked fresh pairing must not reset the session")

        def release(self):
            self.active = False

    lease = Lease()
    monkeypatch.setattr(
        recovery,
        "acquire_whatsapp_recovery_lease",
        lambda *_args, **_kwargs: lease,
    )

    def run(cmd, *_args, **kwargs):
        if "--pair-only" in cmd:
            captured["cmd"] = list(cmd)
            captured["kwargs"] = kwargs
            _write_complete_cli_credentials(session)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr("subprocess.run", run)
    cmd_whatsapp(MagicMock())

    assert captured["cmd"][-2:] == ["--port", "3000"]
    assert captured["kwargs"]["pass_fds"] == (91,)
    assert "close_fds" not in captured["kwargs"]


def test_cli_final_enable_failure_is_fixed_private_and_stays_disabled(
    isolated_home, monkeypatch
):
    from hermes_cli import config

    sentinel = "/private/final-enable-account@s.whatsapp.net"
    real_save = config.save_env_value

    def pair_run(session, cmd):
        _write_complete_cli_credentials(session)
        return subprocess.CompletedProcess(cmd, 0)

    def save(key, value):
        if key == "WHATSAPP_ENABLED" and value == "true":
            raise OSError(sentinel)
        return real_save(key, value)

    monkeypatch.setattr(config, "save_env_value", save)
    _session, out = _run_fresh_cli_pairing(isolated_home, monkeypatch, pair_run)

    assert _env_value(isolated_home, "WHATSAPP_ENABLED") == "false"
    assert "could not be enabled" in out.lower()
    assert "paired successfully" not in out
    assert sentinel not in out


def test_cli_never_echoes_configured_or_entered_allowlists(
    isolated_home, monkeypatch
):
    from hermes_cli.main import cmd_whatsapp

    configured = "configured-private-phone-sentinel"
    entered = "entered-private-phone-sentinel"
    session = isolated_home / "whatsapp" / "session"
    session.mkdir(parents=True)
    _write_complete_cli_credentials(session)
    monkeypatch.setenv("WHATSAPP_MODE", "bot")
    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", configured)
    _fixed_inputs(monkeypatch, ["y", entered, "n"])
    _skip_bridge_bootstrap(monkeypatch)

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())
    out = buf.getvalue()

    assert configured not in out
    assert entered not in out
    assert _env_value(isolated_home, "WHATSAPP_ENABLED") == "true"


def test_cli_status_path_never_echoes_a_jid_shaped_allowlist(
    isolated_home, monkeypatch
):
    """The configured/status path must not echo JID-shaped allowlist entries.

    The sibling test above covers opaque sentinels. WhatsApp allowlists are
    normally phone numbers or bare JIDs, and those are the values that both
    identify a person and look innocuous enough to print — so they are
    asserted separately here, on the decline branch that renders the
    "already configured" status rather than the update prompt.
    """
    from hermes_cli.main import cmd_whatsapp

    # Fictional 555-0000 range plus an explicit synthetic marker.
    configured = "15550000001@s.whatsapp.net,15550000002,SYNTHETIC-ALLOW-CANARY"
    session = isolated_home / "whatsapp" / "session"
    session.mkdir(parents=True)
    _write_complete_cli_credentials(session)
    monkeypatch.setenv("WHATSAPP_MODE", "bot")
    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", configured)
    # "n" declines the update, so the CLI takes the already-configured branch.
    _fixed_inputs(monkeypatch, ["n"])
    _skip_bridge_bootstrap(monkeypatch)

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())
    out = buf.getvalue()

    for fragment in (
        configured,
        "15550000001@s.whatsapp.net",
        "15550000002",
        "SYNTHETIC-ALLOW-CANARY",
    ):
        assert fragment not in out, f"CLI echoed an allowlist value: {fragment!r}"
    # The status itself must still be reported.
    assert "Allowed users are configured" in out


def test_cli_bridge_missing_failure_does_not_disclose_path(
    isolated_home, monkeypatch, tmp_path
):
    from gateway.platforms import whatsapp_common
    from hermes_cli.main import cmd_whatsapp

    private_dir = tmp_path / "private-bridge-path-sentinel"
    monkeypatch.setenv("WHATSAPP_MODE", "bot")
    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", "")
    _fixed_inputs(monkeypatch, [""])
    monkeypatch.setattr("hermes_cli.main._require_tty", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        whatsapp_common,
        "resolve_whatsapp_bridge_dir",
        lambda: private_dir,
    )

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())
    out = buf.getvalue()

    assert "bridge script" in out.lower()
    assert str(private_dir) not in out
    assert "private-bridge-path-sentinel" not in out


def test_existing_complete_session_skips_dependency_install(
    isolated_home, monkeypatch, tmp_path
):
    from gateway.platforms import whatsapp_common
    from hermes_cli.main import cmd_whatsapp

    bridge_dir = tmp_path / "bridge"
    bridge_dir.mkdir()
    (bridge_dir / "bridge.js").write_text("// bridge\n", encoding="utf-8")
    (bridge_dir / "package.json").write_text("{}", encoding="utf-8")
    session = isolated_home / "whatsapp" / "session"
    session.mkdir(parents=True)
    _write_complete_cli_credentials(session)
    monkeypatch.setenv("WHATSAPP_MODE", "bot")
    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", "20000000000")
    _fixed_inputs(monkeypatch, ["n", "n"])
    monkeypatch.setattr("hermes_cli.main._require_tty", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(whatsapp_common, "resolve_whatsapp_bridge_dir", lambda: bridge_dir)
    monkeypatch.setattr(
        "hermes_constants.find_node_executable", lambda name: f"/test/{name}"
    )
    installs = []

    def run(cmd, *_args, **_kwargs):
        if "install" in cmd:
            installs.append(cmd)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr("subprocess.run", run)
    cmd_whatsapp(MagicMock())

    assert installs == []
    assert _env_value(isolated_home, "WHATSAPP_ENABLED") == "true"


def test_pair_dependency_install_is_private_disabled_and_under_lease(
    isolated_home, monkeypatch, tmp_path
):
    from gateway.platforms import whatsapp_common, whatsapp_recovery
    from hermes_cli import config
    from hermes_cli.main import cmd_whatsapp

    bridge_dir = tmp_path / "bridge"
    bridge_dir.mkdir()
    (bridge_dir / "bridge.js").write_text("// bridge\n", encoding="utf-8")
    (bridge_dir / "package.json").write_text("{}", encoding="utf-8")
    session = isolated_home / "whatsapp" / "session"
    monkeypatch.setenv("WHATSAPP_MODE", "bot")
    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", "")
    _fixed_inputs(monkeypatch, [""])
    monkeypatch.setattr("hermes_cli.main._require_tty", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(whatsapp_common, "resolve_whatsapp_bridge_dir", lambda: bridge_dir)
    monkeypatch.setattr(
        "hermes_constants.find_node_executable", lambda name: f"/test/{name}"
    )
    events = []
    private_stderr = "/private/npm-stderr-account@s.whatsapp.net"
    real_save = config.save_env_value

    class Lease:
        active = True

        def pair_subprocess_kwargs(self):
            return {}

        def reset_session(self, _path=None):
            pytest.fail("dependency failure must precede session reset")

        def release(self):
            events.append("release")
            self.active = False

    lease = Lease()

    def acquire(*_args, **_kwargs):
        events.append("acquire")
        return lease

    def save(key, value):
        if key == "WHATSAPP_ENABLED":
            events.append(f"enabled:{value}")
        return real_save(key, value)

    def run(cmd, *_args, **_kwargs):
        if "install" in cmd:
            assert lease.active
            assert events[-1] == "enabled:false"
            events.append("install")
            return subprocess.CompletedProcess(cmd, 1, stderr=private_stderr)
        pytest.fail("pair child must not launch after dependency failure")

    monkeypatch.setattr(whatsapp_recovery, "acquire_whatsapp_recovery_lease", acquire)
    monkeypatch.setattr(config, "save_env_value", save)
    monkeypatch.setattr("subprocess.run", run)

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_whatsapp(MagicMock())
    out = buf.getvalue()

    assert events == ["acquire", "enabled:false", "install", "release"]
    assert _env_value(isolated_home, "WHATSAPP_ENABLED") == "false"
    assert "dependencies could not be installed" in out.lower()
    assert private_stderr not in out
