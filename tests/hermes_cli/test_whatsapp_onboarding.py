import asyncio
import json
import os
import shutil
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import pytest


class _FakeProc:
    def __init__(self, lines=None, returncode=0, wait_error=None):
        self.lines = list(lines or [])
        self.stdout = iter(self.lines)
        self._returncode = returncode
        self._wait_error = wait_error
        self.wait_calls = 0
        self.terminated = False
        self.killed = False
        self.pid = 12345

    def poll(self):
        return None if not self.terminated and not self.killed else self._returncode

    def wait(self, timeout=None):
        self.wait_calls += 1
        if self._wait_error is not None:
            raise self._wait_error
        return self._returncode

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True


class _CountingLease:
    def __init__(self, events=None, reset_hook=None):
        self.active = True
        self.events = events if events is not None else []
        self.release_count = 0
        self.reset_hook = reset_hook

    def pair_subprocess_kwargs(self):
        return {}

    def reset_session(self, path: Path | None = None):
        assert self.active
        if self.reset_hook is None:
            pytest.fail("this recovery lease must not reset the session")
        assert path is not None
        self.reset_hook(path)

    def release(self):
        self.events.append("release")
        self.release_count += 1
        self.active = False


def _onboarding_record(ws, session_path, *, lease=None, allowed_users=""):
    return ws._WhatsAppOnboardingSession(
        proc=None,
        mode="bot",
        allowed_users=allowed_users,
        session_path=str(session_path),
        expires_at="2099-01-01T00:00:00Z",
        expires_at_ts=time.time() + 600,
        recovery_lease=lease,
    )


def _write_valid_dashboard_credentials(
    session_path,
    *,
    account_id="20000000000:7@s.whatsapp.net",
    account_name="Dashboard Test Device",
):
    session_path.mkdir(parents=True, exist_ok=True)
    (session_path / "creds.json").write_text(
        json.dumps({"me": {"id": account_id, "name": account_name}}),
        encoding="utf-8",
    )


@pytest.fixture(autouse=True)
def _no_real_whatsapp_finalization_delay(monkeypatch):
    """Keep unit tests fast; the flush-specific test installs its own hook."""
    from hermes_cli import web_server as ws

    monkeypatch.setattr(
        ws,
        "_WHATSAPP_PAIRING_FINALIZATION_DELAY_SECONDS",
        0,
        raising=False,
    )


def test_background_pairing_retains_recovery_lease_until_watcher_finishes(
    monkeypatch, tmp_path
):
    from hermes_cli import web_server as ws

    events = []
    session_path = tmp_path / "session"
    session_path.mkdir()
    raw_jid = "20000000000:7@s.whatsapp.net"
    account_name = "Dashboard Test Device"
    child_identity_sentinel = "child-stdout-private-identity@s.whatsapp.net"
    _write_valid_dashboard_credentials(
        session_path,
        account_id=raw_jid,
        account_name=account_name,
    )
    proc = _FakeProc(
        lines=[
            json.dumps(
                {
                    "event": "connected",
                    "account_id": child_identity_sentinel,
                    "account_name": child_identity_sentinel,
                }
            )
            + "\n"
        ]
    )
    lease = _CountingLease(events)
    record = _onboarding_record(ws, session_path, lease=lease)
    ws._whatsapp_onboarding_sessions.clear()
    ws._whatsapp_onboarding_sessions["pairing"] = record

    def spawn(*_args):
        assert lease.active
        events.append("spawn")
        return proc

    monkeypatch.setattr(ws, "_spawn_whatsapp_pairing_process", spawn)

    ws._run_whatsapp_pairing("pairing", session_path, "bot", lease)

    assert events == ["spawn", "release"]
    assert record.status == "connected"
    assert raw_jid not in "".join(proc.lines)
    assert account_name not in "".join(proc.lines)
    assert record.account_id == "20000000000"
    assert record.account_name == account_name
    assert record.account_phone == "20000000000"
    payload_text = json.dumps(ws._whatsapp_onboarding_payload("pairing", record))
    assert raw_jid not in payload_text
    assert child_identity_sentinel not in payload_text
    assert lease.release_count == 1
    assert lease.active is False
    ws._whatsapp_onboarding_sessions.clear()


def test_background_pairing_start_failure_releases_recovery_lease(
    monkeypatch, tmp_path, caplog, capsys
):
    from fastapi import HTTPException
    from hermes_cli import web_server as ws

    events = []
    failure_sentinel = "thread-start-private-path-sentinel"
    lease = _CountingLease(events)

    class FailingThread:
        def start(self):
            raise RuntimeError(failure_sentinel)

    monkeypatch.setattr(ws.threading, "Thread", lambda **_kwargs: FailingThread())
    ws._whatsapp_onboarding_sessions.clear()

    with pytest.raises(HTTPException) as exc_info:
        ws._begin_whatsapp_pairing_background(
            tmp_path / "session",
            "bot",
            "",
            "2099-01-01T00:00:00Z",
            time.time() + 600,
            None,
            lease,
        )

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "WhatsApp pairing could not be started."
    assert events == ["release"]
    assert lease.release_count == 1
    assert ws._whatsapp_onboarding_sessions == {}
    assert failure_sentinel not in caplog.text
    captured = capsys.readouterr()
    assert failure_sentinel not in captured.out
    assert failure_sentinel not in captured.err


def test_pairing_process_spawn_failure_is_fixed_and_private(
    monkeypatch, tmp_path, caplog, capsys
):
    from hermes_cli import web_server as ws

    failure_sentinel = "spawn-private-path-and-identity-sentinel"
    lease = _CountingLease()
    record = _onboarding_record(ws, tmp_path / "session", lease=lease)
    ws._whatsapp_onboarding_sessions.clear()
    ws._whatsapp_onboarding_sessions["pairing"] = record

    def fail_spawn(*_args):
        raise RuntimeError(failure_sentinel)

    monkeypatch.setattr(ws, "_spawn_whatsapp_pairing_process", fail_spawn)

    with caplog.at_level("ERROR"):
        ws._run_whatsapp_pairing("pairing", tmp_path / "session", "bot", lease)

    assert record.status == "error"
    assert record.error == "WhatsApp pairing could not be started."
    assert lease.release_count == 1
    assert "RuntimeError" in caplog.text
    assert failure_sentinel not in caplog.text
    captured = capsys.readouterr()
    assert failure_sentinel not in captured.out
    assert failure_sentinel not in captured.err
    ws._whatsapp_onboarding_sessions.clear()


def test_pairing_watcher_rejects_raw_child_failure_text(monkeypatch, tmp_path):
    from hermes_cli import web_server as ws

    child_failure_sentinel = "child-stderr-private-identity-sentinel"
    proc = _FakeProc(
        lines=[
            json.dumps(
                {"event": "error", "error": child_failure_sentinel, "jid": "private-jid"}
            )
            + "\n"
        ]
    )
    lease = _CountingLease()
    record = _onboarding_record(ws, tmp_path / "session", lease=lease)
    ws._whatsapp_onboarding_sessions.clear()
    ws._whatsapp_onboarding_sessions["pairing"] = record
    monkeypatch.setattr(ws, "_spawn_whatsapp_pairing_process", lambda *_args: proc)

    ws._run_whatsapp_pairing("pairing", tmp_path / "session", "bot", lease)

    payload = ws._whatsapp_onboarding_payload("pairing", record)
    assert record.status == "error"
    assert record.error == "WhatsApp pairing failed."
    assert child_failure_sentinel not in json.dumps(payload)
    assert "private-jid" not in json.dumps(payload)
    assert lease.release_count == 1
    ws._whatsapp_onboarding_sessions.clear()


@pytest.mark.parametrize("returncode", [78, 1, 23])
def test_connected_event_is_provisional_until_zero_child_exit(
    monkeypatch, tmp_path, returncode
):
    from hermes_cli import web_server as ws

    session_path = tmp_path / "session"
    _write_valid_dashboard_credentials(session_path)
    proc = _FakeProc(lines=['{"event":"connected"}\n'], returncode=returncode)
    lease = _CountingLease()
    record = _onboarding_record(ws, session_path, lease=lease)
    ws._whatsapp_onboarding_sessions.clear()
    ws._whatsapp_onboarding_sessions["pairing"] = record
    monkeypatch.setattr(ws, "_spawn_whatsapp_pairing_process", lambda *_args: proc)

    ws._run_whatsapp_pairing("pairing", session_path, "bot", lease)

    assert proc.wait_calls == 1
    assert record.status == "error"
    assert record.error == "WhatsApp pairing failed."
    assert record.account_id is None
    assert record.account_name is None
    assert record.account_phone is None
    assert lease.release_count == 1
    ws._whatsapp_onboarding_sessions.clear()


@pytest.mark.parametrize("artifact", ["missing", "partial", "malformed"])
def test_connected_event_requires_complete_local_credentials(
    monkeypatch, tmp_path, artifact, caplog, capsys
):
    from hermes_cli import web_server as ws

    private_sentinel = "/private/incomplete-creds-jid@s.whatsapp.net"
    session_path = tmp_path / "session"
    session_path.mkdir()
    if artifact == "partial":
        (session_path / "creds.json").write_text("{}", encoding="utf-8")
    elif artifact == "malformed":
        (session_path / "creds.json").write_text(
            private_sentinel,
            encoding="utf-8",
        )
    proc = _FakeProc(lines=['{"event":"connected"}\n'])
    lease = _CountingLease()
    record = _onboarding_record(ws, session_path, lease=lease)
    ws._whatsapp_onboarding_sessions.clear()
    ws._whatsapp_onboarding_sessions["pairing"] = record
    monkeypatch.setattr(ws, "_spawn_whatsapp_pairing_process", lambda *_args: proc)

    ws._run_whatsapp_pairing("pairing", session_path, "bot", lease)

    payload = ws._whatsapp_onboarding_payload("pairing", record)
    assert record.status == "error"
    assert record.error == "WhatsApp pairing failed."
    assert private_sentinel not in json.dumps(payload)
    assert private_sentinel not in caplog.text
    captured = capsys.readouterr()
    assert private_sentinel not in captured.out
    assert private_sentinel not in captured.err
    assert lease.release_count == 1
    ws._whatsapp_onboarding_sessions.clear()


def test_marker_created_during_finalization_flush_blocks_connected(
    monkeypatch, tmp_path
):
    from hermes_cli import web_server as ws

    events = []
    session_path = tmp_path / "session"
    _write_valid_dashboard_credentials(session_path)
    proc = _FakeProc(lines=['{"event":"connected"}\n'])
    lease = _CountingLease(events)
    record = _onboarding_record(ws, session_path, lease=lease)
    ws._whatsapp_onboarding_sessions.clear()
    ws._whatsapp_onboarding_sessions["pairing"] = record
    monkeypatch.setattr(ws, "_spawn_whatsapp_pairing_process", lambda *_args: proc)
    monkeypatch.setattr(ws, "_WHATSAPP_PAIRING_FINALIZATION_DELAY_SECONDS", 2)

    def marker_flush(delay):
        assert delay == 2
        assert lease.active
        events.append("flush")
        (session_path / "revoked.json").write_text(
            '{"revoked": true}',
            encoding="utf-8",
        )

    monkeypatch.setattr(ws.time, "sleep", marker_flush)

    ws._run_whatsapp_pairing("pairing", session_path, "bot", lease)

    assert events == ["flush", "release"]
    assert record.status == "error"
    assert record.error == "WhatsApp pairing failed."
    assert record.account_id is None
    assert lease.release_count == 1
    ws._whatsapp_onboarding_sessions.clear()


def test_process_wait_failure_cannot_preserve_provisional_connected(
    monkeypatch, tmp_path, caplog, capsys
):
    from hermes_cli import web_server as ws

    failure_sentinel = "/private/wait-failure-account@s.whatsapp.net"
    session_path = tmp_path / "session"
    _write_valid_dashboard_credentials(session_path)
    proc = _FakeProc(
        lines=['{"event":"connected"}\n'],
        wait_error=RuntimeError(failure_sentinel),
    )
    lease = _CountingLease()
    record = _onboarding_record(ws, session_path, lease=lease)
    ws._whatsapp_onboarding_sessions.clear()
    ws._whatsapp_onboarding_sessions["pairing"] = record
    monkeypatch.setattr(ws, "_spawn_whatsapp_pairing_process", lambda *_args: proc)

    with caplog.at_level("ERROR"):
        ws._run_whatsapp_pairing("pairing", session_path, "bot", lease)

    assert record.status == "error"
    assert record.error == "WhatsApp pairing failed."
    assert lease.release_count == 1
    assert "RuntimeError" in caplog.text
    assert failure_sentinel not in caplog.text
    captured = capsys.readouterr()
    assert failure_sentinel not in captured.out
    assert failure_sentinel not in captured.err
    ws._whatsapp_onboarding_sessions.clear()


def test_status_payload_excludes_allowed_users_and_recovery_lease(tmp_path):
    from hermes_cli import web_server as ws

    allowed_users_sentinel = "20000000000,private-account-sentinel"
    lease = _CountingLease()
    record = _onboarding_record(
        ws,
        tmp_path / "session",
        lease=lease,
        allowed_users=allowed_users_sentinel,
    )

    payload = ws._whatsapp_onboarding_payload("pairing", record)
    payload_text = json.dumps(payload)

    assert "allowed_users" not in payload
    assert "recovery_lease" not in payload
    assert allowed_users_sentinel not in payload_text


def test_pairing_watcher_exception_is_fixed_and_releases_once(
    monkeypatch, tmp_path, caplog, capsys
):
    from hermes_cli import web_server as ws

    failure_sentinel = "watcher-private-traceback-sentinel"

    class ExplodingStream:
        def __iter__(self):
            return self

        def __next__(self):
            raise RuntimeError(failure_sentinel)

    proc = _FakeProc()
    proc.stdout = ExplodingStream()
    lease = _CountingLease()
    record = _onboarding_record(ws, tmp_path / "session", lease=lease)
    ws._whatsapp_onboarding_sessions.clear()
    ws._whatsapp_onboarding_sessions["pairing"] = record
    monkeypatch.setattr(ws, "_spawn_whatsapp_pairing_process", lambda *_args: proc)

    with caplog.at_level("ERROR"):
        ws._run_whatsapp_pairing("pairing", tmp_path / "session", "bot", lease)

    assert proc.terminated is True
    assert record.status == "error"
    assert record.error == "WhatsApp pairing failed."
    assert lease.release_count == 1
    assert "RuntimeError" in caplog.text
    assert failure_sentinel not in caplog.text
    captured = capsys.readouterr()
    assert failure_sentinel not in captured.out
    assert failure_sentinel not in captured.err
    ws._whatsapp_onboarding_sessions.clear()


def test_pairing_lease_release_failure_is_private(
    monkeypatch, tmp_path, caplog, capsys
):
    from hermes_cli import web_server as ws

    failure_sentinel = "lease-release-private-lock-sentinel"

    class FailingLease:
        release_count = 0

        def pair_subprocess_kwargs(self):
            return {}

        def reset_session(self, _path=None):
            pytest.fail("pairing finalization must not reset the session")

        def release(self):
            self.release_count += 1
            raise RuntimeError(failure_sentinel)

    lease = FailingLease()
    session_path = tmp_path / "session"
    _write_valid_dashboard_credentials(session_path)
    proc = _FakeProc(lines=['{"event":"connected"}\n'])
    record = _onboarding_record(ws, session_path, lease=lease)
    ws._whatsapp_onboarding_sessions.clear()
    ws._whatsapp_onboarding_sessions["pairing"] = record
    monkeypatch.setattr(ws, "_spawn_whatsapp_pairing_process", lambda *_args: proc)

    with caplog.at_level("ERROR"):
        ws._run_whatsapp_pairing("pairing", tmp_path / "session", "bot", lease)

    assert record.status == "connected"
    assert lease.release_count == 1
    assert "RuntimeError" in caplog.text
    assert failure_sentinel not in caplog.text
    captured = capsys.readouterr()
    assert failure_sentinel not in captured.out
    assert failure_sentinel not in captured.err
    ws._whatsapp_onboarding_sessions.clear()


def test_cancelled_pairing_releases_lease_once_when_worker_observes_cancel(
    monkeypatch, tmp_path
):
    from hermes_cli import web_server as ws

    captured_threads = []

    class CapturedThread:
        def __init__(self, target=None, args=(), **_kwargs):
            self.target = target
            self.args = args

        def start(self):
            captured_threads.append(self)

    lease = _CountingLease()
    ws._whatsapp_onboarding_sessions.clear()
    monkeypatch.setattr(ws.threading, "Thread", CapturedThread)

    result = ws._begin_whatsapp_pairing_background(
        tmp_path / "session",
        "bot",
        "",
        "2099-01-01T00:00:00Z",
        time.time() + 600,
        None,
        lease,
    )
    asyncio.run(ws.cancel_whatsapp_onboarding(result["pairing_id"]))

    assert lease.release_count == 0
    assert len(captured_threads) == 1
    captured_threads[0].target(*captured_threads[0].args)
    assert lease.release_count == 1
    assert ws._whatsapp_onboarding_sessions == {}








def test_apply_whatsapp_onboarding_uses_server_side_pairing_policy(
    monkeypatch, tmp_path
):
    from hermes_cli import web_server as ws

    saved = {}
    enabled = []
    allowed_users_sentinel = "20000000000,server-side-only-account"
    session_path = tmp_path / "session"
    _write_valid_dashboard_credentials(session_path)

    monkeypatch.setattr(
        ws,
        "save_env_value",
        lambda key, value: saved.setdefault(key, value),
    )
    monkeypatch.setattr(
        ws,
        "_write_platform_enabled",
        lambda platform, value: enabled.append((platform, value)),
    )
    monkeypatch.setattr(
        ws,
        "_restart_gateway_after_whatsapp_onboarding",
        lambda profile=None: {"restart_started": True, "restart_pid": 12345},
    )

    record = ws._WhatsAppOnboardingSession(
        proc=None,
        mode="bot",
        allowed_users=allowed_users_sentinel,
        session_path=str(session_path),
        expires_at="2099-01-01T00:00:00Z",
        expires_at_ts=time.time() + 600,
        status="connected",
    )
    ws._whatsapp_onboarding_sessions.clear()
    ws._whatsapp_onboarding_sessions["pairing"] = record

    result = asyncio.run(
        ws.apply_whatsapp_onboarding(
            "pairing",
            ws.WhatsAppOnboardingApply(mode="bot"),
        )
    )

    assert result["ok"] is True
    assert saved["WHATSAPP_MODE"] == "bot"
    assert saved["WHATSAPP_DM_POLICY"] == "pairing"
    assert saved["WHATSAPP_ALLOWED_USERS"] == allowed_users_sentinel
    assert saved["WHATSAPP_ENABLED"] == "true"
    assert allowed_users_sentinel not in json.dumps(result)
    assert enabled == [("whatsapp", True)]
    assert "pairing" not in ws._whatsapp_onboarding_sessions


@pytest.mark.parametrize(
    ("exception", "status_code", "detail"),
    [
        (
            ValueError("/private/apply-value-jid@s.whatsapp.net"),
            400,
            "WhatsApp setup values are invalid.",
        ),
        (
            RuntimeError("/private/apply-runtime-jid@s.whatsapp.net"),
            500,
            "Failed to save WhatsApp setup.",
        ),
    ],
)
def test_apply_failures_are_fixed_and_private(
    monkeypatch,
    tmp_path,
    caplog,
    capsys,
    exception,
    status_code,
    detail,
):
    from fastapi import HTTPException
    from hermes_cli import web_server as ws

    session_path = tmp_path / "session"
    _write_valid_dashboard_credentials(session_path)
    record = _onboarding_record(ws, session_path)
    record.status = "connected"
    ws._whatsapp_onboarding_sessions.clear()
    ws._whatsapp_onboarding_sessions["pairing"] = record
    monkeypatch.setattr(
        ws,
        "save_env_value",
        lambda *_args: (_ for _ in ()).throw(exception),
    )

    with caplog.at_level("ERROR"):
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(
                ws.apply_whatsapp_onboarding(
                    "pairing",
                    ws.WhatsAppOnboardingApply(mode="bot"),
                )
            )

    private_sentinel = str(exception)
    assert exc_info.value.status_code == status_code
    assert exc_info.value.detail == detail
    assert private_sentinel not in caplog.text
    assert "Traceback" not in caplog.text
    if isinstance(exception, RuntimeError):
        assert "RuntimeError" in caplog.text
    captured = capsys.readouterr()
    assert private_sentinel not in captured.out
    assert private_sentinel not in captured.err
    ws._whatsapp_onboarding_sessions.clear()


@pytest.mark.parametrize("artifact", ["missing", "partial", "revoked"])
def test_apply_revalidates_local_session_before_enabling(
    monkeypatch, tmp_path, artifact
):
    from fastapi import HTTPException
    from hermes_cli import web_server as ws

    session_path = tmp_path / "session"
    session_path.mkdir()
    if artifact == "partial":
        (session_path / "creds.json").write_text("{}", encoding="utf-8")
    elif artifact == "revoked":
        _write_valid_dashboard_credentials(session_path)
        (session_path / "revoked.json").write_text(
            '{"revoked": true}',
            encoding="utf-8",
        )
    record = _onboarding_record(ws, session_path)
    record.status = "connected"
    ws._whatsapp_onboarding_sessions.clear()
    ws._whatsapp_onboarding_sessions["pairing"] = record
    mutations = []
    monkeypatch.setattr(
        ws,
        "save_env_value",
        lambda *_args: mutations.append("env"),
    )
    monkeypatch.setattr(
        ws,
        "_write_platform_enabled",
        lambda *_args: mutations.append("platform"),
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            ws.apply_whatsapp_onboarding(
                "pairing",
                ws.WhatsAppOnboardingApply(mode="bot"),
            )
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == (
        "WhatsApp setup is no longer valid. Start a new setup."
    )
    assert mutations == []
    assert record.status == "error"
    assert record.error == "WhatsApp pairing failed."
    ws._whatsapp_onboarding_sessions.clear()


def test_start_whatsapp_onboarding_existing_creds_returns_linked_account(monkeypatch, tmp_path):
    from hermes_cli import web_server as ws

    session_dir = tmp_path / "session"
    session_dir.mkdir()
    raw_jid = "20000000000:7@s.whatsapp.net"
    (session_dir / "creds.json").write_text(
        json.dumps({"me": {"id": raw_jid, "name": "Dashboard Test Device"}}),
        encoding="utf-8",
    )

    old_proc = _FakeProc(returncode=1)
    old_record = ws._WhatsAppOnboardingSession(
        proc=old_proc,
        mode="bot",
        allowed_users="",
        session_path=str(session_dir),
        expires_at="2099-01-01T00:00:00Z",
        expires_at_ts=time.time() + 600,
    )
    ws._whatsapp_onboarding_sessions.clear()
    ws._whatsapp_onboarding_sessions["old"] = old_record
    monkeypatch.setattr(ws, "_whatsapp_session_path", lambda: session_dir)
    monkeypatch.setattr(ws.secrets, "token_urlsafe", lambda size: "existing-creds")
    monkeypatch.setattr(
        "gateway.platforms.whatsapp_recovery.acquire_whatsapp_recovery_lease",
        lambda *_args, **_kwargs: pytest.fail(
            "an intact linked session must not acquire a recovery lease"
        ),
    )

    result = asyncio.run(
        ws.start_whatsapp_onboarding(
            ws.WhatsAppOnboardingStart(mode="self-chat", allowed_users="")
        )
    )

    assert result["pairing_id"] == "existing-creds"
    assert result["status"] == "connected"
    assert result["qr_payload"] is None
    assert result["account_id"] == "20000000000"
    assert result["account_name"] == "Dashboard Test Device"
    assert result["account_phone"] == "20000000000"
    assert raw_jid not in json.dumps(result)
    assert old_record.status == "cancelled"
    assert old_proc.terminated is True
    assert (session_dir / "creds.json").exists()
    assert ws._whatsapp_onboarding_sessions["existing-creds"].account_phone == "20000000000"
    ws._whatsapp_onboarding_sessions.clear()


def test_linked_account_reader_ignores_non_object_creds_without_leaking(
    tmp_path, caplog, capsys
):
    from hermes_cli import web_server as ws

    failure_sentinel = "creds-structure-private-path-sentinel"
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    (session_dir / "creds.json").write_text(
        json.dumps([failure_sentinel]),
        encoding="utf-8",
    )

    assert ws._whatsapp_linked_account_from_session(session_dir) == (
        None,
        None,
        None,
    )
    assert failure_sentinel not in caplog.text
    captured = capsys.readouterr()
    assert failure_sentinel not in captured.out
    assert failure_sentinel not in captured.err


def test_linked_account_reader_drops_path_or_jid_shaped_display_name(tmp_path):
    from hermes_cli import web_server as ws

    raw_name = "/private/dashboard-user@s.whatsapp.net"
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    (session_dir / "creds.json").write_text(
        json.dumps(
            {
                "me": {
                    "id": "20000000000:7@s.whatsapp.net",
                    "name": raw_name,
                }
            }
        ),
        encoding="utf-8",
    )

    account_id, account_name, account_phone = (
        ws._whatsapp_linked_account_from_session(session_dir)
    )

    assert account_id == "20000000000"
    assert account_name is None
    assert account_phone == "20000000000"
    assert raw_name not in json.dumps([account_id, account_name, account_phone])


@pytest.mark.parametrize("artifact", ["symlink", "fifo", "directory", "oversize"])
def test_dashboard_credential_reader_rejects_blocking_or_unbounded_files(
    tmp_path, artifact
):
    """Credential inspection must never follow or block on hostile file types."""
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    creds_path = session_dir / "creds.json"
    valid_payload = json.dumps(
        {"me": {"id": "20000000000:7@s.whatsapp.net", "name": "Local Device"}}
    )
    if artifact == "symlink":
        target = tmp_path / "outside-creds.json"
        target.write_text(valid_payload, encoding="utf-8")
        creds_path.symlink_to(target)
    elif artifact == "fifo":
        if not hasattr(os, "mkfifo"):
            pytest.skip("FIFO creation is unavailable")
        os.mkfifo(creds_path)
    elif artifact == "directory":
        creds_path.mkdir()
    else:
        creds_path.write_text(
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
        "from hermes_cli.web_server import _whatsapp_linked_account_from_session; "
        f"print(_whatsapp_linked_account_from_session(Path({str(session_dir)!r})))"
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
    assert result.stdout.strip() == "(None, None, None)"


def test_dashboard_pair_child_gets_only_lease_inheritance_and_explicit_port(
    monkeypatch, tmp_path
):
    from hermes_cli import web_server as ws

    bridge_dir = tmp_path / "bridge"
    bridge_dir.mkdir()
    (bridge_dir / "bridge.js").write_text("// bridge\n", encoding="utf-8")
    session_dir = tmp_path / "session"
    captured = {}

    class Lease:
        def pair_subprocess_kwargs(self):
            return {"pass_fds": (91,)}

        def reset_session(self, _path=None):
            pytest.fail("pair subprocess construction must not reset the session")

    def popen(argv, **kwargs):
        captured["argv"] = argv
        captured["kwargs"] = kwargs
        return _FakeProc()

    monkeypatch.setattr(
        "gateway.platforms.whatsapp_common.resolve_whatsapp_bridge_dir",
        lambda: bridge_dir,
    )
    monkeypatch.setattr(
        "hermes_constants.find_node_executable", lambda name: f"/test/{name}"
    )
    monkeypatch.setattr(ws, "_ensure_whatsapp_bridge_dependencies", lambda _path: None)
    monkeypatch.setattr(ws.subprocess, "Popen", popen)

    ws._spawn_whatsapp_pairing_process(session_dir, "bot", Lease())

    assert captured["argv"] == [
        "/test/node",
        str(bridge_dir / "bridge.js"),
        "--pair-only",
        "--pair-json",
        "--session",
        str(session_dir),
        "--port",
        "3000",
    ]
    assert captured["kwargs"]["pass_fds"] == (91,)
    assert "close_fds" not in captured["kwargs"]


def test_dashboard_reinstalls_only_when_resolved_manifest_stamp_is_stale(
    monkeypatch, tmp_path
):
    from gateway.platforms.whatsapp_common import (
        whatsapp_bridge_dependencies_fresh,
    )
    from hermes_cli import web_server as ws

    bridge_dir = tmp_path / "bridge"
    node_modules = bridge_dir / "node_modules"
    node_modules.mkdir(parents=True)
    manifest = bridge_dir / "package.json"
    manifest.write_text(
        '{"dependencies":{"bridge":"declared"}}',
        encoding="utf-8",
    )
    (bridge_dir / "package-lock.json").write_text(
        '{"lockfileVersion":3,"packages":{}}',
        encoding="utf-8",
    )
    installs = []

    monkeypatch.setattr(
        ws.subprocess,
        "run",
        lambda *args, **kwargs: installs.append((args, kwargs))
        or type("Result", (), {"returncode": 0, "stderr": "", "stdout": ""})(),
    )
    monkeypatch.setattr(
        "hermes_constants.find_node_executable", lambda name: f"/test/{name}"
    )

    ws._ensure_whatsapp_bridge_dependencies(bridge_dir)
    assert len(installs) == 1
    assert whatsapp_bridge_dependencies_fresh(bridge_dir) is True

    ws._ensure_whatsapp_bridge_dependencies(bridge_dir)
    assert len(installs) == 1, "matching manifest and stamp must preserve the install"

    manifest.write_text(
        '{"dependencies":{"bridge":"new-declared-value"}}',
        encoding="utf-8",
    )
    ws._ensure_whatsapp_bridge_dependencies(bridge_dir)
    assert len(installs) == 2
    assert whatsapp_bridge_dependencies_fresh(bridge_dir) is True


def test_dashboard_dependency_failure_does_not_return_subprocess_output(
    monkeypatch, tmp_path, caplog, capsys
):
    from fastapi import HTTPException
    from hermes_cli import web_server as ws

    failure_sentinel = "npm-stderr-private-path-and-jid-sentinel"
    bridge_dir = tmp_path / "bridge"
    bridge_dir.mkdir()
    (bridge_dir / "package.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        ws.subprocess,
        "run",
        lambda *_args, **_kwargs: type(
            "Result",
            (),
            {"returncode": 1, "stderr": failure_sentinel, "stdout": failure_sentinel},
        )(),
    )
    monkeypatch.setattr(
        "hermes_constants.find_node_executable", lambda _name: "/test/npm"
    )

    with pytest.raises(HTTPException) as exc_info:
        ws._ensure_whatsapp_bridge_dependencies(bridge_dir)

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "WhatsApp bridge dependencies could not be installed."
    assert failure_sentinel not in caplog.text
    captured = capsys.readouterr()
    assert failure_sentinel not in captured.out
    assert failure_sentinel not in captured.err


def test_post_onboarding_restart_failure_is_fixed_and_private(
    monkeypatch, caplog, capsys
):
    from hermes_cli import web_server as ws

    failure_sentinel = "restart-private-traceback-path-sentinel"

    def fail_restart(_profile):
        raise RuntimeError(failure_sentinel)

    monkeypatch.setattr(ws, "_spawn_gateway_restart", fail_restart)

    with caplog.at_level("ERROR"):
        result = ws._restart_gateway_after_whatsapp_onboarding()

    assert result == {
        "restart_started": False,
        "restart_error": "Gateway restart could not be started.",
    }
    assert "RuntimeError" in caplog.text
    assert failure_sentinel not in caplog.text
    captured = capsys.readouterr()
    assert failure_sentinel not in captured.out
    assert failure_sentinel not in captured.err






# ── Revoked-session recovery ────────────────────────────────────────────────
#
# Marker presence fails closed, but destructive recovery still requires a
# second, explicit authorization. The first request is a read-only probe:
# valid, malformed and unreadable markers all receive the same stable 409,
# and only a retry carrying reset_revoked_session=true may quiesce/reset.


_RESET_CONFIRMATION_DETAIL = "WhatsApp session reset requires confirmation."


class _FakeThread:
    """Capture the pairing thread instead of running it."""

    started = []

    def __init__(self, target=None, args=(), daemon=None, **kwargs):
        self.target = target
        self.args = args
        self.daemon = daemon

    def start(self):
        _FakeThread.started.append((self.target, self.args))


def _revoked_session(tmp_path, marker: str | None, *, creds: bool = True):
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    if creds:
        (session_dir / "creds.json").write_text(
            json.dumps(
                {
                    "me": {
                        "id": "20000000000:7@s.whatsapp.net",
                        "name": "Dashboard Test Device",
                    }
                }
            ),
            encoding="utf-8",
        )
    if marker == "__unreadable__":
        (session_dir / "revoked.json").mkdir()
    elif marker is not None:
        (session_dir / "revoked.json").write_text(marker, encoding="utf-8")
    return session_dir


def _start_onboarding(
    monkeypatch,
    session_dir,
    pairing_id="new-pairing",
    *,
    reset_revoked_session=False,
):
    from hermes_cli import web_server as ws

    _FakeThread.started = []
    ws._whatsapp_onboarding_sessions.clear()
    monkeypatch.setattr(ws, "_whatsapp_session_path", lambda: session_dir)
    monkeypatch.setattr(ws.secrets, "token_urlsafe", lambda size: pairing_id)
    monkeypatch.setattr(ws.threading, "Thread", _FakeThread)
    monkeypatch.setattr(ws, "save_env_value", lambda *_args: None)
    monkeypatch.setattr(ws, "_write_platform_enabled", lambda *_args: None)

    class NoopLease:
        active = True

        def pair_subprocess_kwargs(self):
            assert self.active
            return {}

        def reset_session(self, path: Path | None = None):
            assert self.active
            assert path == session_dir
            assert path is not None
            shutil.rmtree(path)
            path.mkdir(parents=True)

        def release(self):
            self.active = False

    def acquire(path, *, bridge_script, bridge_port):
        assert path == session_dir
        assert bridge_script.name == "bridge.js"
        assert bridge_port == 3000
        return NoopLease()

    monkeypatch.setattr(
        "gateway.platforms.whatsapp_recovery.acquire_whatsapp_recovery_lease",
        acquire,
    )
    try:
        return asyncio.run(
            ws.start_whatsapp_onboarding(
                ws.WhatsAppOnboardingStart(
                    mode="self-chat",
                    allowed_users="",
                    reset_revoked_session=reset_revoked_session,
                )
            )
        )
    finally:
        for _target, args in _FakeThread.started:
            lease = args[3]
            if getattr(lease, "active", False):
                lease.release()
        ws._whatsapp_onboarding_sessions.clear()


def test_start_onboarding_revoked_creds_require_confirmation(monkeypatch, tmp_path):
    from fastapi import HTTPException

    session_dir = _revoked_session(tmp_path, '{"revoked": true, "reason": "device_removed"}')
    creds_before = (session_dir / "creds.json").read_bytes()

    with pytest.raises(HTTPException) as exc_info:
        _start_onboarding(monkeypatch, session_dir)

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == _RESET_CONFIRMATION_DETAIL
    assert (session_dir / "creds.json").read_bytes() == creds_before
    assert (session_dir / "revoked.json").exists()
    assert _FakeThread.started == []


@pytest.mark.parametrize("artifact", ["partial", "malformed", "unreadable"])
def test_start_onboarding_invalid_existing_creds_require_confirmation_without_mutation(
    monkeypatch, tmp_path, artifact
):
    from fastapi import HTTPException
    from hermes_cli import web_server as ws

    session_dir = tmp_path / "session"
    session_dir.mkdir()
    creds_path = session_dir / "creds.json"
    if artifact == "partial":
        creds_path.write_text("{}", encoding="utf-8")
    elif artifact == "malformed":
        creds_path.write_text("private-invalid-creds-sentinel", encoding="utf-8")
    else:
        creds_path.mkdir()
    creds_was_dir = creds_path.is_dir()
    creds_before = None if creds_was_dir else creds_path.read_bytes()
    calls = []
    ws._whatsapp_onboarding_sessions.clear()
    monkeypatch.setattr(ws, "_whatsapp_session_path", lambda: session_dir)
    monkeypatch.setattr(
        "gateway.platforms.whatsapp_recovery.acquire_whatsapp_recovery_lease",
        lambda *_args, **_kwargs: calls.append("acquire"),
    )
    monkeypatch.setattr(
        ws,
        "save_env_value",
        lambda *_args: calls.append("env"),
    )
    monkeypatch.setattr(
        ws,
        "_write_platform_enabled",
        lambda *_args: calls.append("platform"),
    )
    monkeypatch.setattr(
        ws,
        "_terminate_whatsapp_pairing",
        lambda *_args: calls.append("terminate"),
    )

    class ForbiddenThread:
        def __init__(self, **_kwargs):
            calls.append("thread")

        def start(self):
            calls.append("thread-start")

    monkeypatch.setattr(ws.threading, "Thread", ForbiddenThread)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            ws.start_whatsapp_onboarding(
                ws.WhatsAppOnboardingStart(mode="bot", allowed_users="")
            )
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == _RESET_CONFIRMATION_DETAIL
    assert calls == []
    assert creds_path.is_dir() is creds_was_dir
    if creds_before is not None:
        assert creds_path.read_bytes() == creds_before
    assert ws._whatsapp_onboarding_sessions == {}


def test_authorized_invalid_creds_disable_then_reset_in_profile_scope(
    monkeypatch, tmp_path
):
    from hermes_cli import web_server as ws

    session_dir = tmp_path / "session"
    session_dir.mkdir()
    (session_dir / "creds.json").write_text("{}", encoding="utf-8")
    events = []
    active_profiles = []
    lease = _CountingLease(events)
    _FakeThread.started = []
    ws._whatsapp_onboarding_sessions.clear()
    monkeypatch.setattr(ws, "_whatsapp_session_path", lambda: session_dir)
    monkeypatch.setattr(ws.secrets, "token_urlsafe", lambda _size: "invalid-reset")
    monkeypatch.setattr(ws.threading, "Thread", _FakeThread)

    @contextmanager
    def profile_scope(profile):
        events.append(f"profile-enter:{profile}")
        active_profiles.append(profile)
        try:
            yield
        finally:
            active_profiles.pop()
            events.append(f"profile-exit:{profile}")

    def acquire(path, *, bridge_script, bridge_port):
        assert path == session_dir
        assert bridge_script.name == "bridge.js"
        assert bridge_port == 3000
        assert active_profiles == ["isolated-profile"]
        events.append("acquire")
        return lease

    def save_env_value(key, value):
        assert active_profiles == ["isolated-profile"]
        assert (key, value) == ("WHATSAPP_ENABLED", "false")
        events.append("env:false")

    def write_enabled(platform, value):
        assert active_profiles == ["isolated-profile"]
        assert (platform, value) == ("whatsapp", False)
        events.append("platform:false")

    def reset(path):
        assert lease.active
        assert events[-2:] == ["env:false", "platform:false"]
        events.append("reset")
        for child in list(path.iterdir()):
            child.rmdir() if child.is_dir() else child.unlink()

    lease.reset_hook = reset

    monkeypatch.setattr(ws, "_config_profile_scope", profile_scope)
    monkeypatch.setattr(
        "gateway.platforms.whatsapp_recovery.acquire_whatsapp_recovery_lease",
        acquire,
    )
    monkeypatch.setattr(ws, "save_env_value", save_env_value)
    monkeypatch.setattr(ws, "_write_platform_enabled", write_enabled)

    result = asyncio.run(
        ws.start_whatsapp_onboarding(
            ws.WhatsAppOnboardingStart(
                mode="bot",
                allowed_users="20000000000",
                profile="isolated-profile",
                reset_revoked_session=True,
            )
        )
    )

    assert result["status"] == "starting"
    assert "allowed_users" not in result
    assert events == [
        "profile-enter:isolated-profile",
        "acquire",
        "env:false",
        "platform:false",
        "reset",
        "profile-exit:isolated-profile",
    ]
    assert lease.active is True
    assert len(_FakeThread.started) == 1
    _target, args = _FakeThread.started[0]
    args[3].release()
    ws._whatsapp_onboarding_sessions.clear()


@pytest.mark.parametrize("failure_at", ["env", "platform"])
def test_disable_failure_aborts_before_reset_and_releases_lease(
    monkeypatch, tmp_path, failure_at, caplog, capsys
):
    from fastapi import HTTPException
    from hermes_cli import web_server as ws

    failure_sentinel = f"/private/{failure_at}-disable-jid@s.whatsapp.net"
    session_dir = _revoked_session(tmp_path, '{"revoked": true}')
    creds_before = (session_dir / "creds.json").read_bytes()
    marker_before = (session_dir / "revoked.json").read_bytes()
    events = []
    lease = _CountingLease(events)
    ws._whatsapp_onboarding_sessions.clear()
    monkeypatch.setattr(ws, "_whatsapp_session_path", lambda: session_dir)

    def acquire(path, *, bridge_script, bridge_port):
        assert path == session_dir
        assert bridge_script.name == "bridge.js"
        assert bridge_port == 3000
        events.append("acquire")
        return lease

    monkeypatch.setattr(
        "gateway.platforms.whatsapp_recovery.acquire_whatsapp_recovery_lease",
        acquire,
    )

    def save_env_value(key, value):
        assert (key, value) == ("WHATSAPP_ENABLED", "false")
        events.append("env")
        if failure_at == "env":
            raise OSError(failure_sentinel)

    def write_enabled(platform, value):
        assert (platform, value) == ("whatsapp", False)
        events.append("platform")
        if failure_at == "platform":
            raise RuntimeError(failure_sentinel)

    monkeypatch.setattr(ws, "save_env_value", save_env_value)
    monkeypatch.setattr(ws, "_write_platform_enabled", write_enabled)

    with caplog.at_level("ERROR"):
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(
                ws.start_whatsapp_onboarding(
                    ws.WhatsAppOnboardingStart(
                        mode="bot",
                        reset_revoked_session=True,
                    )
                )
            )

    expected_events = ["acquire", "env"]
    if failure_at == "platform":
        expected_events.append("platform")
    expected_events.append("release")
    assert events == expected_events
    assert lease.release_count == 1
    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == (
        "WhatsApp could not be disabled, so pairing was not started."
    )
    assert (session_dir / "creds.json").read_bytes() == creds_before
    assert (session_dir / "revoked.json").read_bytes() == marker_before
    assert failure_sentinel not in caplog.text
    captured = capsys.readouterr()
    assert failure_sentinel not in captured.out
    assert failure_sentinel not in captured.err
    assert ws._whatsapp_onboarding_sessions == {}


def test_start_onboarding_authorized_marker_without_creds_is_recoverable(
    monkeypatch, tmp_path
):
    """A marker left behind with no creds.json must still pair cleanly."""
    session_dir = _revoked_session(tmp_path, '{"revoked": true}', creds=False)

    result = _start_onboarding(
        monkeypatch, session_dir, reset_revoked_session=True
    )

    assert result["status"] != "connected"
    assert session_dir.is_dir()
    assert not (session_dir / "revoked.json").exists()
    assert len(_FakeThread.started) == 1


@pytest.mark.parametrize(
    "marker",
    [
        '{"revoked": false}',
        '{"revoked": 1}',
        '{"revoked": "true"}',
        "[]",
        '"revoked"',
        "{not json",
        "",
        "__unreadable__",
    ],
)
def test_start_onboarding_invalid_marker_requires_confirmation(
    monkeypatch, tmp_path, marker
):
    from fastapi import HTTPException

    session_dir = _revoked_session(tmp_path, marker)
    creds_before = (session_dir / "creds.json").read_bytes()

    with pytest.raises(HTTPException) as exc_info:
        _start_onboarding(monkeypatch, session_dir)

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == _RESET_CONFIRMATION_DETAIL
    assert (session_dir / "creds.json").read_bytes() == creds_before
    assert (session_dir / "revoked.json").exists()
    assert _FakeThread.started == []


def test_start_onboarding_reports_a_failed_reset_instead_of_pairing(
    monkeypatch, tmp_path, caplog, capsys
):
    from fastapi import HTTPException
    from hermes_cli import web_server as ws

    failure_sentinel = "reset-private-permission-path-sentinel"
    session_dir = _revoked_session(
        tmp_path, '{"revoked": true, "reason": "device_removed"}'
    )
    lease = _CountingLease()
    _FakeThread.started = []
    ws._whatsapp_onboarding_sessions.clear()
    monkeypatch.setattr(ws, "_whatsapp_session_path", lambda: session_dir)
    monkeypatch.setattr(ws.threading, "Thread", _FakeThread)

    def acquire(path, *, bridge_script, bridge_port):
        assert path == session_dir
        assert bridge_script.name == "bridge.js"
        assert bridge_port == 3000
        return lease

    monkeypatch.setattr(
        "gateway.platforms.whatsapp_recovery.acquire_whatsapp_recovery_lease",
        acquire,
    )

    def fail_reset(_path):
        raise RuntimeError(failure_sentinel)

    lease.reset_hook = fail_reset

    with caplog.at_level("ERROR"):
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(
                ws.start_whatsapp_onboarding(
                    ws.WhatsAppOnboardingStart(
                        mode="self-chat",
                        allowed_users="",
                        reset_revoked_session=True,
                    )
                )
            )

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == (
        "The WhatsApp session could not be reset, so pairing was not started."
    )
    assert lease.release_count == 1
    assert _FakeThread.started == []
    assert (session_dir / "creds.json").exists()
    assert "RuntimeError" in caplog.text
    assert failure_sentinel not in caplog.text
    assert str(session_dir) not in caplog.text
    captured = capsys.readouterr()
    assert failure_sentinel not in captured.out
    assert failure_sentinel not in captured.err


def test_whatsapp_onboarding_reset_authorization_defaults_false():
    from hermes_cli.web_models import WhatsAppOnboardingStart

    assert WhatsAppOnboardingStart().reset_revoked_session is False


@pytest.mark.parametrize(
    "marker",
    [
        '{"revoked": true, "reason": "device_removed"}',
        "{not json",
        "__unreadable__",
    ],
)
def test_marker_confirmation_precedes_every_mutating_operation(
    monkeypatch, tmp_path, marker
):
    from fastapi import HTTPException
    from hermes_cli import web_server as ws

    session_dir = _revoked_session(tmp_path, marker)
    creds_before = (session_dir / "creds.json").read_bytes()
    marker_path = session_dir / "revoked.json"
    marker_was_dir = marker_path.is_dir()
    marker_before = None if marker_was_dir else marker_path.read_bytes()
    old_proc = _FakeProc()
    old_record = _onboarding_record(ws, session_dir)
    old_record.proc = old_proc
    calls = []

    ws._whatsapp_onboarding_sessions.clear()
    ws._whatsapp_onboarding_sessions["old"] = old_record
    monkeypatch.setattr(ws, "_whatsapp_session_path", lambda: session_dir)
    monkeypatch.setattr(
        ws,
        "_terminate_whatsapp_pairing",
        lambda _proc: calls.append("terminate"),
    )
    monkeypatch.setattr(
        "gateway.platforms.whatsapp_recovery.acquire_whatsapp_recovery_lease",
        lambda *_args, **_kwargs: calls.append("acquire"),
    )
    monkeypatch.setattr(
        ws,
        "save_env_value",
        lambda *_args: calls.append("env"),
    )
    monkeypatch.setattr(
        ws,
        "_write_platform_enabled",
        lambda *_args: calls.append("platform"),
    )

    class ForbiddenThread:
        def __init__(self, **_kwargs):
            calls.append("thread-created")

        def start(self):
            calls.append("thread-started")

    monkeypatch.setattr(ws.threading, "Thread", ForbiddenThread)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            ws.start_whatsapp_onboarding(
                ws.WhatsAppOnboardingStart(mode="self-chat", allowed_users="")
            )
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == _RESET_CONFIRMATION_DETAIL
    assert calls == []
    assert old_record.status == "starting"
    assert old_proc.terminated is False
    assert (session_dir / "creds.json").read_bytes() == creds_before
    assert marker_path.is_dir() is marker_was_dir
    if marker_before is not None:
        assert marker_path.read_bytes() == marker_before
    ws._whatsapp_onboarding_sessions.clear()


@pytest.mark.parametrize(
    "marker",
    [
        '{"revoked": true, "reason": "device_removed"}',
        "{not json",
        "__unreadable__",
    ],
)
def test_authorized_marker_reset_holds_lease_through_identity_free_watcher(
    monkeypatch, tmp_path, marker
):
    from hermes_cli import web_server as ws

    session_dir = _revoked_session(tmp_path, marker)
    events = []
    lease = _CountingLease(events)
    _FakeThread.started = []
    ws._whatsapp_onboarding_sessions.clear()
    monkeypatch.setattr(ws, "_whatsapp_session_path", lambda: session_dir)
    monkeypatch.setattr(ws.secrets, "token_urlsafe", lambda _size: "new-pairing")
    monkeypatch.setattr(ws.threading, "Thread", _FakeThread)

    def acquire(path, *, bridge_script, bridge_port):
        assert path == session_dir
        assert bridge_script.name == "bridge.js"
        assert bridge_port == 3000
        events.append("acquire")
        return lease

    def reset(path):
        assert lease.active
        assert events[-2:] == ["env:false", "platform:false"]
        events.append("reset")
        shutil.rmtree(path)
        path.mkdir(parents=True)

    lease.reset_hook = reset

    monkeypatch.setattr(
        ws,
        "save_env_value",
        lambda key, value: events.append(f"env:{value}"),
    )
    monkeypatch.setattr(
        ws,
        "_write_platform_enabled",
        lambda platform, value: events.append(f"platform:{str(value).lower()}"),
    )
    monkeypatch.setattr(
        "gateway.platforms.whatsapp_recovery.acquire_whatsapp_recovery_lease",
        acquire,
    )

    result = asyncio.run(
        ws.start_whatsapp_onboarding(
            ws.WhatsAppOnboardingStart(
                mode="self-chat",
                allowed_users="",
                reset_revoked_session=True,
            )
        )
    )

    assert result["status"] == "starting"
    assert events == ["acquire", "env:false", "platform:false", "reset"]
    assert lease.active is True
    assert lease.release_count == 0
    assert session_dir.is_dir()
    assert list(session_dir.iterdir()) == []
    assert len(_FakeThread.started) == 1

    raw_jid = "20000000000:7@s.whatsapp.net"

    def spawn(path, _mode, child_lease):
        assert child_lease is lease
        assert lease.active
        events.append("spawn")
        (path / "creds.json").write_text(
            json.dumps(
                {"me": {"id": raw_jid, "name": "Dashboard Test Device"}}
            ),
            encoding="utf-8",
        )
        return _FakeProc(lines=['{"event":"connected","ts":"test-time"}\n'])

    monkeypatch.setattr(ws, "_spawn_whatsapp_pairing_process", spawn)
    target, args = _FakeThread.started[0]
    target(*args)

    record = ws._whatsapp_onboarding_sessions["new-pairing"]
    assert events == [
        "acquire",
        "env:false",
        "platform:false",
        "reset",
        "spawn",
        "release",
    ]
    assert lease.release_count == 1
    assert record.recovery_lease is None
    assert record.status == "connected"
    assert record.account_id == "20000000000"
    assert record.account_name == "Dashboard Test Device"
    assert record.account_phone == "20000000000"
    assert raw_jid not in json.dumps(
        ws._whatsapp_onboarding_payload("new-pairing", record)
    )
    ws._whatsapp_onboarding_sessions.clear()


def test_unmarked_pairing_uses_lifecycle_lease_without_reset(monkeypatch, tmp_path):
    from hermes_cli import web_server as ws

    session_dir = tmp_path / "session"
    session_dir.mkdir()
    sidecar = session_dir / "keep-during-pairing.txt"
    sidecar.write_text("keep", encoding="utf-8")
    events = []
    lease = _CountingLease(events)
    _FakeThread.started = []
    ws._whatsapp_onboarding_sessions.clear()
    monkeypatch.setattr(ws, "_whatsapp_session_path", lambda: session_dir)
    monkeypatch.setattr(ws.secrets, "token_urlsafe", lambda _size: "ordinary-pairing")
    monkeypatch.setattr(ws.threading, "Thread", _FakeThread)

    def acquire(path, *, bridge_script, bridge_port):
        assert path == session_dir
        assert bridge_script.name == "bridge.js"
        assert bridge_port == 3000
        events.append("acquire")
        return lease

    monkeypatch.setattr(
        "gateway.platforms.whatsapp_recovery.acquire_whatsapp_recovery_lease",
        acquire,
    )

    result = asyncio.run(
        ws.start_whatsapp_onboarding(
            ws.WhatsAppOnboardingStart(mode="bot", allowed_users="")
        )
    )

    assert result["status"] == "starting"
    assert events == ["acquire"]
    assert lease.active is True
    assert sidecar.read_text(encoding="utf-8") == "keep"
    target, args = _FakeThread.started[0]
    monkeypatch.setattr(
        ws,
        "_spawn_whatsapp_pairing_process",
        lambda *_args: _FakeProc(lines=['{"event":"connected"}\n']),
    )
    target(*args)
    assert events == ["acquire", "release"]
    assert lease.release_count == 1
    assert sidecar.read_text(encoding="utf-8") == "keep"
    ws._whatsapp_onboarding_sessions.clear()


# ---------------------------------------------------------------------------
# repair_required is a stable terminal status, distinct from a generic error.
#
# The bridge stops with BRIDGE_EXIT_REPAIR_REQUIRED (79) rather than sending a
# QR over a stream the dashboard would store and re-serve. That state has to
# survive finalization: overwriting it with "error" loses the one actionable
# instruction the operator needs, and re-serving a QR would defeat the reason
# the child stopped in the first place.
# ---------------------------------------------------------------------------

_REPAIR_STATUS = "repair_required"
_REPAIR_MESSAGE = (
    "WhatsApp needs to be paired again. Run 'hermes whatsapp' in a local terminal."
)
# Shaped like a Baileys QR payload but wholly fabricated.
_HOSTILE_FAKE_QR = "2@FAKEQRCANARY0002/SyntheticLegacyRef,FAKEPUBKEYCANARY0002="


@pytest.mark.parametrize(
    "line",
    [
        '{"event":"repair_required"}\n',
        # A stale or hostile child may still speak the old protocol; the
        # payload must be dropped rather than stored.
        '{"event":"qr","qr":"' + _HOSTILE_FAKE_QR + '"}\n',
    ],
    ids=["repair-required-event", "hostile-legacy-qr-event"],
)
def test_repair_required_is_terminal_and_never_stores_a_qr(
    monkeypatch, tmp_path, line
):
    from hermes_cli import web_server as ws

    session_path = tmp_path / "session"
    _write_valid_dashboard_credentials(session_path)
    proc = _FakeProc(lines=[line], returncode=79)
    lease = _CountingLease()
    record = _onboarding_record(ws, session_path, lease=lease)
    ws._whatsapp_onboarding_sessions.clear()
    ws._whatsapp_onboarding_sessions["pairing"] = record
    monkeypatch.setattr(ws, "_spawn_whatsapp_pairing_process", lambda *_args: proc)

    ws._run_whatsapp_pairing("pairing", session_path, "bot", lease)

    assert record.qr_payload is None, "a QR payload was stored for the dashboard"
    assert record.status == _REPAIR_STATUS, (
        f"finalization overwrote the terminal status with {record.status!r}"
    )
    assert record.error == _REPAIR_MESSAGE
    assert _HOSTILE_FAKE_QR not in json.dumps(
        ws._whatsapp_onboarding_status_payload(record)
        if hasattr(ws, "_whatsapp_onboarding_status_payload")
        else {"status": record.status, "error": record.error,
              "qr_payload": record.qr_payload}
    )
    assert lease.release_count == 1
    ws._whatsapp_onboarding_sessions.clear()


def test_repair_required_is_registered_as_a_terminal_status():
    """Lifecycle paths (cancel/cleanup/run) must treat it as terminal."""
    from hermes_cli import web_server as ws

    assert _REPAIR_STATUS in ws._WHATSAPP_ONBOARDING_TERMINAL_STATUSES
    # It must stay distinct from a generic operational failure.
    assert _REPAIR_STATUS != "error"


def test_repair_required_survives_a_zero_exit_finalization(monkeypatch, tmp_path):
    """Even a zero exit must not downgrade an already-terminal repair state."""
    from hermes_cli import web_server as ws

    session_path = tmp_path / "session"
    _write_valid_dashboard_credentials(session_path)
    proc = _FakeProc(lines=['{"event":"repair_required"}\n'], returncode=0)
    lease = _CountingLease()
    record = _onboarding_record(ws, session_path, lease=lease)
    ws._whatsapp_onboarding_sessions.clear()
    ws._whatsapp_onboarding_sessions["pairing"] = record
    monkeypatch.setattr(ws, "_spawn_whatsapp_pairing_process", lambda *_args: proc)

    ws._run_whatsapp_pairing("pairing", session_path, "bot", lease)

    assert record.status == _REPAIR_STATUS
    assert record.qr_payload is None
    ws._whatsapp_onboarding_sessions.clear()
