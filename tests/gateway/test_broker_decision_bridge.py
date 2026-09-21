"""Broker decisions ride the question registry: gated, critical-capable,
broker-rejection surfaces to the owner."""
import pytest

from gateway.question_registry import QuestionRegistry


@pytest.mark.asyncio
async def test_poller_submits_broker_decisions_gated(tmp_path):
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))
    sent = []

    async def send(sk, text):
        sent.append(text)

    decisions = [{"decision_id": "abc", "question": "codex blocked: apply?",
                  "critical_class": None},
                 {"decision_id": "def", "question": "second",
                  "critical_class": None}]
    await run_mod._submit_broker_decisions(reg, send, "owner-session",
                                           decisions)
    assert len(sent) == 1                       # one on the wire
    assert "codex blocked" in sent[0]
    assert reg.pending() is not None and len(reg.held_ids()) == 1


@pytest.mark.asyncio
async def test_critical_broker_decision_preempts(tmp_path):
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))
    sent = []

    async def send(sk, text):
        sent.append(text)

    await run_mod._submit_broker_decisions(reg, send, "s", [
        {"decision_id": "n1", "question": "normal", "critical_class": None}])
    await run_mod._submit_broker_decisions(reg, send, "s", [
        {"decision_id": "c1", "question": "codex hit usage limit",
         "critical_class": "auth_expiry"}])
    assert any("[CRITICAL]" in t for t in sent)


@pytest.mark.asyncio
async def test_unknown_critical_class_degrades_not_raises(tmp_path):
    """The broker releases separately: an unrecognized class must become an
    ordinary question, not an exception that loses the decision."""
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))
    sent = []

    async def send(sk, text):
        sent.append(text)

    await run_mod._submit_broker_decisions(reg, send, "s", [
        {"decision_id": "x1", "question": "who knows",
         "critical_class": "from_the_future"}])
    assert len(sent) == 1 and "[CRITICAL]" not in sent[0]


@pytest.mark.asyncio
async def test_malformed_decision_never_reaches_the_wire(tmp_path):
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))
    sent = []

    async def send(sk, text):
        sent.append(text)

    await run_mod._submit_broker_decisions(reg, send, "s", [
        {"decision_id": "", "question": "no id"},
        {"decision_id": "ok1", "question": "   "},
        None,
        {"decision_id": "ok2", "question": "a real one"}])
    assert len(sent) == 1 and "a real one" in sent[0]


@pytest.mark.asyncio
async def test_broker_reply_forwarding_surfaces_rejection(tmp_path,
                                                          monkeypatch):
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))
    sent = []

    async def send(sk, text):
        sent.append(text)

    await run_mod._submit_broker_decisions(reg, send, "s", [
        {"decision_id": "abc", "question": "q?", "critical_class": None}])
    qid = reg.pending()
    monkeypatch.setattr(
        run_mod, "_woodhouse_broker_call",
        lambda payload: {"ok": False, "error": "agent no longer blocked"})
    routed = reg.route_reply(f"{qid} 1")
    out = await run_mod._forward_broker_reply(reg, send, routed)
    assert out is True  # handled (not passed to clarify)
    assert any("no longer blocked" in t for t in sent)


@pytest.mark.asyncio
async def test_broker_reply_forwarding_types_the_answer(tmp_path, monkeypatch):
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))
    sent = []
    calls = []

    async def send(sk, text):
        sent.append(text)

    await run_mod._submit_broker_decisions(reg, send, "s", [
        {"decision_id": "abc", "question": "q?", "critical_class": None},
        {"decision_id": "next", "question": "held one",
         "critical_class": None}])
    qid = reg.pending()

    def fake_call(payload):
        calls.append(payload)
        return {"ok": True}

    monkeypatch.setattr(run_mod, "_woodhouse_broker_call", fake_call)
    assert await run_mod._forward_broker_reply(
        reg, send, reg.route_reply(f"{qid} option 2")) is True
    assert calls == [{"method": "decision.submit",
                      "params": {"decision_id": "abc", "reply": "option 2"}}]
    # The wire is freed and the held decision takes its place.
    assert reg.pending() is not None and reg.pending() != qid
    assert any("held one" in t for t in sent)


@pytest.mark.asyncio
async def test_non_broker_reply_is_left_for_clarify(tmp_path):
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))
    sent = []

    async def send(sk, text):
        sent.append(text)

    await run_mod._send_question_gated(reg, send, task_ref="task-42",
                                       session_key="s", body="deploy?")
    qid = reg.pending()
    assert await run_mod._forward_broker_reply(
        reg, send, reg.route_reply(f"{qid} yes")) is False
    assert reg.pending() == qid  # untouched — clarify still owns it


@pytest.mark.asyncio
async def test_unreachable_broker_is_reported_not_raised(tmp_path,
                                                         monkeypatch):
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))
    sent = []

    async def send(sk, text):
        sent.append(text)

    await run_mod._submit_broker_decisions(reg, send, "s", [
        {"decision_id": "abc", "question": "q?", "critical_class": None}])
    qid = reg.pending()

    def boom(payload):
        raise OSError("no such socket")

    monkeypatch.setattr(run_mod, "_woodhouse_broker_call", boom)
    assert await run_mod._forward_broker_reply(
        reg, send, reg.route_reply(f"{qid} 1")) is True
    assert any("broker unreachable" in t for t in sent)


def test_poll_seconds_defaults_to_disabled(monkeypatch):
    """The relay ships dark — Task 10 turns it on."""
    from gateway import run as run_mod
    assert run_mod._woodhouse_decision_poll_seconds({}) == 0.0
    assert run_mod._woodhouse_decision_poll_seconds(
        {"woodhouse": {"decision_poll_seconds": 15}}) == 15.0
    assert run_mod._woodhouse_decision_poll_seconds(
        {"woodhouse": {"decision_poll_seconds": "nonsense"}}) == 0.0


@pytest.mark.asyncio
async def test_poller_returns_immediately_when_disabled(monkeypatch):
    """A gated-off watcher must exit cleanly (never loop, never respawn)."""
    from gateway import run as run_mod

    class _Runner:
        _running = True
        _question_registry = object()
        _woodhouse_owner_session_key = staticmethod(lambda *a, **k: "s")

    monkeypatch.setattr(run_mod, "_load_gateway_runtime_config", lambda: {})

    def never(*a, **k):
        raise AssertionError("disabled poller must not call the broker")

    monkeypatch.setattr(run_mod, "_woodhouse_broker_call", never)
    await run_mod.GatewayRunner._poll_broker_decisions(_Runner())


@pytest.mark.asyncio
async def test_poller_stays_idle_without_the_plugin(monkeypatch):
    from gateway import run as run_mod

    class _Runner:
        _running = True
        _question_registry = object()

    monkeypatch.setattr(
        run_mod, "_load_gateway_runtime_config",
        lambda: {"woodhouse": {"decision_poll_seconds": 5},
                 "plugins": {"enabled": ["something-else"]}})

    def never(*a, **k):
        raise AssertionError("poller ran without the plugin enabled")

    monkeypatch.setattr(run_mod, "_woodhouse_broker_call", never)
    await run_mod.GatewayRunner._poll_broker_decisions(_Runner())


def test_owner_session_key_prefers_config_then_sole_sender():
    from gateway import run as run_mod

    class _Runner:
        _question_senders = {}

    runner = _Runner()
    key = run_mod.GatewayRunner._woodhouse_owner_session_key
    assert key(runner, {"woodhouse": {"owner_session_key": "wa:owner"}}) == \
        "wa:owner"
    assert key(runner, {}) == ""
    runner._question_senders = {"only-chat": lambda t: None}
    assert key(runner, {}) == "only-chat"
    runner._question_senders["other-chat"] = lambda t: None
    # Ambiguous without config: never guess which human owns the broker.
    assert key(runner, {}) == ""
