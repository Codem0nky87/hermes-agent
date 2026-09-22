"""Broker decisions ride the question registry: gated, critical-capable,
broker-rejection surfaces to the owner, owner chat only."""
import pytest

from gateway.question_registry import QuestionRegistry

OWNER = "s"


async def _forward(run_mod, reg, send, routed, session_key=OWNER,
                   owner=OWNER):
    """Forward a routed reply as the owner chat unless told otherwise."""
    return await run_mod._forward_broker_reply(
        reg, send, routed, session_key=session_key, owner_session_key=owner)


@pytest.mark.asyncio
async def test_poller_submits_broker_decisions_gated(tmp_path):
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))
    sent = []

    async def send(sk, text):
        sent.append(text)
        return True

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
        return True

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
        return True

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
        return True

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
        return True

    await run_mod._submit_broker_decisions(reg, send, "s", [
        {"decision_id": "abc", "question": "q?", "critical_class": None}])
    qid = reg.pending()
    monkeypatch.setattr(
        run_mod, "_woodhouse_broker_call",
        lambda payload: {"ok": False, "error": "agent no longer blocked"})
    routed = reg.route_reply(f"{qid} 1")
    out = await _forward(run_mod, reg, send, routed)
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
        return True

    await run_mod._submit_broker_decisions(reg, send, "s", [
        {"decision_id": "abc", "question": "q?", "critical_class": None},
        {"decision_id": "next", "question": "held one",
         "critical_class": None}])
    qid = reg.pending()

    def fake_call(payload):
        calls.append(payload)
        return {"ok": True}

    monkeypatch.setattr(run_mod, "_woodhouse_broker_call", fake_call)
    assert await _forward(
        run_mod, reg, send, reg.route_reply(f"{qid} option 2")) is True
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
        return True

    await run_mod._send_question_gated(reg, send, task_ref="task-42",
                                       session_key="s", body="deploy?")
    qid = reg.pending()
    assert await _forward(
        run_mod, reg, send, reg.route_reply(f"{qid} yes")) is False
    assert reg.pending() == qid  # untouched — clarify still owns it


@pytest.mark.asyncio
async def test_unreachable_broker_is_reported_not_raised(tmp_path,
                                                         monkeypatch):
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))
    sent = []

    async def send(sk, text):
        sent.append(text)
        return True

    await run_mod._submit_broker_decisions(reg, send, "s", [
        {"decision_id": "abc", "question": "q?", "critical_class": None}])
    qid = reg.pending()

    def boom(payload):
        raise OSError("no such socket")

    monkeypatch.setattr(run_mod, "_woodhouse_broker_call", boom)
    assert await _forward(
        run_mod, reg, send, reg.route_reply(f"{qid} 1")) is True
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


# --- Fix round 1 -------------------------------------------------------


@pytest.mark.asyncio
async def test_submitted_ids_are_returned_for_acknowledgement(tmp_path):
    """I1: the poller acknowledges exactly the decisions the registry took
    ownership of — held counts, undelivered does not."""
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))

    async def send(sk, text):
        return True

    owned = await run_mod._submit_broker_decisions(reg, send, OWNER, [
        {"decision_id": "d1", "question": "first", "critical_class": None},
        {"decision_id": "d2", "question": "held", "critical_class": None}])
    assert owned == ["d1", "d2"]  # d2 is held, still the registry's


@pytest.mark.asyncio
async def test_undelivered_question_never_jams_the_wire(tmp_path):
    """I5: a typo'd owner_session_key must not leave a pending question
    holding the one-question wire against every other question for 4h."""
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))

    async def undeliverable(sk, text):
        return False  # what _send_question_text reports with no sender

    owned = await run_mod._submit_broker_decisions(reg, undeliverable,
                                                   "typo-key", [
        {"decision_id": "d1", "question": "nobody sees this",
         "critical_class": None}])
    assert owned == []                      # never acknowledged to the broker
    assert reg.pending() is None            # wire is free
    assert reg.held_ids() == []


@pytest.mark.asyncio
async def test_raising_send_also_leaves_no_pending_question(tmp_path):
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))

    async def boom(sk, text):
        raise RuntimeError("adapter is gone")

    owned = await run_mod._submit_broker_decisions(reg, boom, OWNER, [
        {"decision_id": "d1", "question": "q", "critical_class": None}])
    assert owned == [] and reg.pending() is None


@pytest.mark.asyncio
async def test_send_question_text_reports_missing_sender():
    """I5: the failure signal the cleanup above depends on."""
    from gateway import run as run_mod

    class _Runner:
        # No ``config`` at all ⇒ no home channel to fall back to either.
        _question_senders = {}
        _question_home_channel_fallback = \
            run_mod.GatewayRunner._question_home_channel_fallback
        _send_question_text_via_home_channel = \
            run_mod.GatewayRunner._send_question_text_via_home_channel

    assert await run_mod.GatewayRunner._send_question_text(
        _Runner(), "nobody", "text") is False

    delivered = []

    async def sender(text):
        delivered.append(text)

    _Runner._question_senders = {"chat": sender}
    assert await run_mod.GatewayRunner._send_question_text(
        _Runner(), "chat", "text") is True
    assert delivered == ["text"]


@pytest.mark.asyncio
async def test_only_the_owner_chat_can_answer_a_decision(tmp_path,
                                                         monkeypatch):
    """I7: the registry lets any authorized session answer by explicit id —
    right for a clarify question, wrong for keystrokes in a terminal."""
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))
    sent = []

    async def send(sk, text):
        sent.append((sk, text))
        return True

    await run_mod._submit_broker_decisions(reg, send, OWNER, [
        {"decision_id": "abc", "question": "q?", "critical_class": None}])
    qid = reg.pending()

    def never(payload):
        raise AssertionError("broker called from a non-owner chat")

    monkeypatch.setattr(run_mod, "_woodhouse_broker_call", never)
    handled = await _forward(run_mod, reg, send, reg.route_reply(f"{qid} 1"),
                             session_key="some-other-chat")
    assert handled is True                    # consumed, never sent to clarify
    assert reg.pending() == qid               # still answerable by the owner
    assert sent[-1][0] == "some-other-chat"
    assert "owner chat" in sent[-1][1]


@pytest.mark.asyncio
async def test_unconfigured_owner_key_refuses_every_reply(tmp_path,
                                                          monkeypatch):
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))
    sent = []

    async def send(sk, text):
        sent.append(text)
        return True

    await run_mod._submit_broker_decisions(reg, send, OWNER, [
        {"decision_id": "abc", "question": "q?", "critical_class": None}])
    qid = reg.pending()

    def never(payload):
        raise AssertionError("broker called with no configured owner")

    monkeypatch.setattr(run_mod, "_woodhouse_broker_call", never)
    assert await _forward(run_mod, reg, send, reg.route_reply(f"{qid} 1"),
                          owner="") is True
    assert reg.pending() == qid


def test_owner_key_must_have_a_registered_sender():
    """I5: a configured key nobody can be reached at is no key at all.

    No ``config`` on the stub ⇒ no home channel either, so the only way to
    reach a key here is a registered ask-time sender.
    """
    from gateway import run as run_mod

    class _Runner:
        _question_senders = {"wa:real": lambda t: None}
        _question_home_channel_fallback = \
            run_mod.GatewayRunner._question_home_channel_fallback

    runner = _Runner()
    key = run_mod.GatewayRunner._woodhouse_owner_session_key
    assert key(runner, {"woodhouse": {"owner_session_key": "wa:typo"}}) == ""
    assert key(runner, {"woodhouse": {"owner_session_key": "wa:real"}}) == \
        "wa:real"
    # Before any chat has asked a question there is nothing to check against;
    # delivery failure then retires the question instead.
    runner._question_senders = {}
    assert key(runner, {"woodhouse": {"owner_session_key": "wa:real"}}) == \
        "wa:real"


# --- Fix round 2: direct home-channel fallback for broker delivery -----
#
# A broker decision comes from the poller, not from an agent turn, so
# nothing ever registers an ask-time question sender for the owner's
# session. Without a fallback every broker question is registered,
# dropped, retired, re-offered and dropped again — forever.

WA_OWNER = "agent:main:whatsapp:dm:27825323250"
WA_HOME = "36490075709622@lid"


class _FakeAdapter:
    """Minimal platform adapter: records sends, or refuses them."""

    def __init__(self, working=True):
        self.sent = []
        self.working = working

    async def send(self, chat_id, text, metadata=None):
        if not self.working:
            raise RuntimeError("whatsapp bridge is down")
        self.sent.append((chat_id, text, metadata))
        return None


def _fallback_runner(run_mod, monkeypatch, adapter, *, owner=WA_OWNER,
                     home_chat_id=WA_HOME):
    """A runner stub with a home channel but no ask-time senders."""
    from gateway.config import HomeChannel, Platform

    home = HomeChannel(platform=Platform.WHATSAPP, chat_id=home_chat_id,
                       name="Home")

    class _Config:
        platforms = {}

        @staticmethod
        def get_home_channel(platform):
            return home if platform == Platform.WHATSAPP else None

    class _Runner:
        config = _Config()
        adapters = {Platform.WHATSAPP: adapter}
        _question_senders = {}
        _thread_metadata_for_target = \
            run_mod.GatewayRunner._thread_metadata_for_target
        _is_telegram_dm_topic_target = \
            run_mod.GatewayRunner._is_telegram_dm_topic_target
        _question_home_channel_fallback = \
            run_mod.GatewayRunner._question_home_channel_fallback
        _send_question_text_via_home_channel = \
            run_mod.GatewayRunner._send_question_text_via_home_channel
        _send_question_text = run_mod.GatewayRunner._send_question_text

    monkeypatch.setattr(
        run_mod, "_load_gateway_runtime_config",
        lambda: {"woodhouse": {"owner_session_key": owner}})
    return _Runner()


@pytest.mark.asyncio
async def test_broker_question_delivered_without_an_asktime_sender(
        tmp_path, monkeypatch):
    """The live defect: no sender for the owner session must NOT mean the
    owner never sees the question."""
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))
    adapter = _FakeAdapter()
    runner = _fallback_runner(run_mod, monkeypatch, adapter)

    async def send(sk, text):
        return await runner._send_question_text(sk, text)

    owned = await run_mod._submit_broker_decisions(reg, send, WA_OWNER, [
        {"decision_id": "e31382f8", "question": "codex blocked: apply?",
         "critical_class": "auth_expiry"}])

    assert len(adapter.sent) == 1
    chat_id, text, _meta = adapter.sent[0]
    assert chat_id == WA_HOME
    assert "codex blocked: apply?" in text and "[CRITICAL]" in text
    assert owned == ["e31382f8"]            # broker gets its decision.ack
    assert reg.pending() is not None        # the question is live, not retired


@pytest.mark.asyncio
async def test_fallback_delivery_logs_that_it_was_used(tmp_path, monkeypatch,
                                                       caplog):
    from gateway import run as run_mod
    adapter = _FakeAdapter()
    runner = _fallback_runner(run_mod, monkeypatch, adapter)
    with caplog.at_level("INFO", logger="gateway.run"):
        assert await runner._send_question_text(WA_OWNER, "[D-AAA] q?") is True
    assert any("home channel" in r.getMessage()
               for r in caplog.records if r.levelname == "INFO")


@pytest.mark.asyncio
async def test_fallback_failure_still_retires_the_question(tmp_path,
                                                           monkeypatch):
    """I5 still holds: if the direct send also fails, nothing jams the wire
    and the broker is never acknowledged."""
    from gateway import run as run_mod
    reg = QuestionRegistry(str(tmp_path / "q.db"))
    adapter = _FakeAdapter(working=False)
    runner = _fallback_runner(run_mod, monkeypatch, adapter)

    async def send(sk, text):
        return await runner._send_question_text(sk, text)

    owned = await run_mod._submit_broker_decisions(reg, send, WA_OWNER, [
        {"decision_id": "d1", "question": "nobody can see this",
         "critical_class": None}])

    assert owned == []
    assert reg.pending() is None and reg.held_ids() == []


@pytest.mark.asyncio
async def test_fallback_never_leaks_another_chats_question(monkeypatch):
    """Only the owner's own chat is eligible — a question asked in some
    other session must not surface in the owner's home channel."""
    from gateway import run as run_mod
    adapter = _FakeAdapter()
    runner = _fallback_runner(run_mod, monkeypatch, adapter)
    assert await runner._send_question_text(
        "agent:main:whatsapp:dm:27999999999", "[D-BBB] private") is False
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_fallback_matches_a_home_channel_that_owns_the_session(
        monkeypatch):
    """No woodhouse config needed when the home channel IS the session's
    own chat — the ordinary post-restart case."""
    from gateway import run as run_mod
    adapter = _FakeAdapter()
    runner = _fallback_runner(run_mod, monkeypatch, adapter, owner="",
                              home_chat_id="27825323250@s.whatsapp.net")
    assert await runner._send_question_text(WA_OWNER, "[D-CCC] q?") is True
    assert adapter.sent[0][0] == "27825323250@s.whatsapp.net"


def test_owner_key_is_kept_when_only_the_fallback_can_reach_it(monkeypatch):
    """A configured owner key with no ask-time sender is still deliverable
    once the home-channel fallback exists — the relay must not go dark just
    because some other chat asked a question first."""
    from gateway import run as run_mod
    runner = _fallback_runner(run_mod, monkeypatch, _FakeAdapter())
    runner._question_senders = {"some-other-chat": lambda t: None}
    key = run_mod.GatewayRunner._woodhouse_owner_session_key
    assert key(runner, {"woodhouse": {"owner_session_key": WA_OWNER}}) == \
        WA_OWNER
