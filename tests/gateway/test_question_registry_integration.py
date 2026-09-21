"""Outbound questions pass through the registry gate; replies route before
LLM dispatch; critical resolution re-sends the preempted question."""
import uuid
from types import SimpleNamespace

import pytest

from gateway.question_registry import QuestionRegistry


class _Sent:
    def __init__(self):
        self.messages = []

    async def send(self, session_key, text):
        self.messages.append((session_key, text))


@pytest.mark.asyncio
async def test_outbound_gate_holds_second_question(tmp_path):
    from gateway import run as run_mod

    reg = QuestionRegistry(str(tmp_path / "q.db"))
    out = _Sent()
    await run_mod._send_question_gated(reg, out.send, task_ref="t1",
                                       session_key="s1", body="deploy?")
    await run_mod._send_question_gated(reg, out.send, task_ref="t2",
                                       session_key="s2", body="rename?")
    assert len(out.messages) == 1
    assert "deploy?" in out.messages[0][1]
    assert out.messages[0][1].startswith("[")  # question ID prefix "[D-XXX]"


@pytest.mark.asyncio
async def test_critical_send_tagged_and_reask_after_resolve(tmp_path):
    from gateway import run as run_mod

    reg = QuestionRegistry(str(tmp_path / "q.db"))
    out = _Sent()
    await run_mod._send_question_gated(reg, out.send, task_ref="t1",
                                       session_key="s1", body="deploy?")
    await run_mod._send_question_gated(reg, out.send, task_ref="t9",
                                       session_key="s9", body="auth expired",
                                       critical_class="auth_expiry")
    assert "[CRITICAL]" in out.messages[1][1]
    crit_id = reg.pending()
    await run_mod._resolve_question_gated(reg, out.send, crit_id)
    assert any("Re-asking — still pending from task t1" in m[1]
               for m in out.messages)


@pytest.mark.asyncio
async def test_resolve_of_unknown_id_sends_nothing(tmp_path):
    from gateway import run as run_mod

    reg = QuestionRegistry(str(tmp_path / "q.db"))
    out = _Sent()
    assert await run_mod._resolve_question_gated(reg, out.send, "D-ZZZ") is None
    assert out.messages == []


# --- gateway wiring -------------------------------------------------------


def _runner(tmp_path, *, db_name="q.db"):
    """A bare GatewayRunner carrying only the question-registry wiring."""
    from gateway.run import GatewayRunner

    runner = GatewayRunner.__new__(GatewayRunner)
    runner._question_registry = QuestionRegistry(str(tmp_path / db_name))
    sent = []

    async def _sender(text):
        sent.append(text)

    runner._question_senders = {}
    runner._question_sent = sent
    return runner, sent


def _clarify(session_key, question="deploy?", choices=None):
    """Register a real pending clarify entry and return it."""
    from tools import clarify_gateway

    return clarify_gateway.register(
        clarify_id=uuid.uuid4().hex[:10],
        session_key=session_key,
        question=question,
        choices=list(choices) if choices else None,
    )


def _event(text, allow_control=True):
    return SimpleNamespace(text=text, allow_gateway_control=allow_control)


@pytest.fixture(autouse=True)
def _clean_clarify_registry():
    from tools import clarify_gateway

    yield
    for key in list(clarify_gateway._session_index):
        clarify_gateway.clear_session(key)


@pytest.mark.asyncio
async def test_reply_with_id_resolves_owning_session_and_skips_dispatch(tmp_path):
    runner, sent = _runner(tmp_path)
    entry = _clarify("owner")

    async def _sender(text):
        sent.append(text)

    runner._question_senders["owner"] = _sender
    result = runner._question_registry.submit(
        task_ref="t1", session_key="owner", body="deploy?")

    # Reply arrives in a DIFFERENT session — routing is by question id.
    out = await runner._intercept_question_reply(
        _event(f"{result.question_id} yes"), "someone-else")

    assert out == ""  # consumed: no LLM turn
    assert entry.event.is_set()
    assert entry.response == "yes"


@pytest.mark.asyncio
async def test_resolving_a_reply_promotes_and_delivers_the_held_question(tmp_path):
    runner, sent = _runner(tmp_path)
    _clarify("owner")

    async def _sender(text):
        sent.append(text)

    runner._question_senders["owner"] = _sender
    runner._question_senders["other"] = _sender
    first = runner._question_registry.submit(
        task_ref="t1", session_key="owner", body="deploy?")
    held = runner._question_registry.submit(
        task_ref="t2", session_key="other", body="rename?")
    assert runner._question_registry.held_ids() == [held.question_id]

    await runner._intercept_question_reply(
        _event(f"{first.question_id} yes"), "owner")

    assert runner._question_registry.pending() == held.question_id
    assert any("rename?" in text for text in sent)


@pytest.mark.asyncio
async def test_expired_reply_is_answered_without_starting_a_turn(tmp_path):
    clock = [1000.0]
    from gateway.run import GatewayRunner

    runner = GatewayRunner.__new__(GatewayRunner)
    runner._question_registry = QuestionRegistry(
        str(tmp_path / "qx.db"), now=lambda: clock[0], default_ttl=10)
    runner._question_senders = {}
    result = runner._question_registry.submit(
        task_ref="t1", session_key="owner", body="deploy?")
    clock[0] += 60
    runner._question_registry.expire_stale()

    out = await runner._intercept_question_reply(
        _event(f"{result.question_id} yes"), "owner")

    assert out is not None and "expired" in out


@pytest.mark.asyncio
async def test_unmatched_reply_falls_through_to_normal_dispatch(tmp_path):
    runner, _sent = _runner(tmp_path)
    assert await runner._intercept_question_reply(
        _event("what's the weather"), "owner") is None
    # A slash command is never consumed as an answer.
    runner._question_registry.submit(
        task_ref="t1", session_key="owner", body="deploy?")
    assert await runner._intercept_question_reply(
        _event("/status"), "owner") is None


@pytest.mark.asyncio
async def test_coalesced_multiline_reply_still_routes_by_id(tmp_path):
    """The coalescer merges fragments with newlines; "D-XXX" + "2" typed as
    two WhatsApp messages arrives here as one event."""
    runner, sent = _runner(tmp_path)
    entry = _clarify("owner", choices=["staging", "prod"])

    async def _sender(text):
        sent.append(text)

    runner._question_senders["owner"] = _sender
    result = runner._question_registry.submit(
        task_ref="t1", session_key="owner", body="where?")

    out = await runner._intercept_question_reply(
        _event(f"{result.question_id}\n2"), "owner")

    assert out == ""
    assert entry.response == "prod"


@pytest.mark.asyncio
async def test_bare_reply_answers_only_its_own_session(tmp_path):
    """Without an id, a reply is routed purely because one question is on the
    wire — it must not answer an open-ended prompt asked in another chat."""
    runner, sent = _runner(tmp_path)
    entry = _clarify("owner")  # open-ended: accepts any prose

    async def _sender(text):
        sent.append(text)

    runner._question_senders["owner"] = _sender
    runner._question_registry.submit(
        task_ref="t1", session_key="owner", body="deploy?")

    # Unrelated chatter in a different chat falls through untouched.
    assert await runner._intercept_question_reply(
        _event("remind me to buy milk"), "elsewhere") is None
    assert not entry.event.is_set()

    # The same words in the asking session do answer it.
    assert await runner._intercept_question_reply(
        _event("ship it"), "owner") == ""
    assert entry.response == "ship it"


@pytest.mark.asyncio
async def test_registry_absent_is_a_no_op(tmp_path):
    from gateway.run import GatewayRunner

    runner = GatewayRunner.__new__(GatewayRunner)
    assert await runner._intercept_question_reply(_event("anything"), "s") is None


@pytest.mark.asyncio
async def test_release_question_is_idempotent_after_a_text_answer(tmp_path):
    """The agent thread frees the wire when its clarify wait ends. If the
    inbound text path already resolved that question, the release must not
    promote (and re-send) the held question a second time."""
    runner, sent = _runner(tmp_path)
    _clarify("owner")

    async def _sender(text):
        sent.append(text)

    runner._question_senders["owner"] = _sender
    runner._question_senders["other"] = _sender
    first = runner._question_registry.submit(
        task_ref="t1", session_key="owner", body="deploy?")
    runner._question_registry.submit(
        task_ref="t2", session_key="other", body="rename?")

    await runner._intercept_question_reply(
        _event(f"{first.question_id} yes"), "owner")
    delivered = list(sent)
    await runner._release_question(first.question_id)

    assert sent == delivered  # no duplicate re-ask


@pytest.mark.asyncio
async def test_release_question_frees_the_wire_after_a_button_answer(tmp_path):
    """Buttons resolve the clarify without passing through _handle_message,
    so the agent-thread release is what frees the wire."""
    runner, sent = _runner(tmp_path)

    async def _sender(text):
        sent.append(text)

    runner._question_senders["other"] = _sender
    first = runner._question_registry.submit(
        task_ref="t1", session_key="owner", body="deploy?")
    held = runner._question_registry.submit(
        task_ref="t2", session_key="other", body="rename?")

    await runner._release_question(first.question_id)

    assert runner._question_registry.pending() == held.question_id
    assert any("rename?" in text for text in sent)


@pytest.mark.asyncio
async def test_missing_sender_is_logged_not_raised(tmp_path, caplog):
    runner, _sent = _runner(tmp_path)
    await runner._send_question_text("nobody", "[D-AAA] hello?")
    assert any("nobody" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_answer_survives_a_failing_resolve(tmp_path):
    """The waiting agent already has the answer; a registry bookkeeping
    failure must not also turn that answer into a fresh LLM turn."""
    runner, sent = _runner(tmp_path)
    entry = _clarify("owner")

    async def _sender(text):
        sent.append(text)

    runner._question_senders["owner"] = _sender
    result = runner._question_registry.submit(
        task_ref="t1", session_key="owner", body="deploy?")

    def _boom(_qid):
        raise RuntimeError("db gone")

    runner._question_registry.resolve = _boom  # type: ignore[method-assign]
    out = await runner._intercept_question_reply(
        _event(f"{result.question_id} yes"), "owner")

    assert out == ""
    assert entry.response == "yes"


# --- construction ---------------------------------------------------------


def _runner_for_platforms(tmp_path, monkeypatch, platforms):
    from gateway import run as run_mod
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner

    monkeypatch.setattr(run_mod, "_hermes_home", tmp_path)
    return GatewayRunner(GatewayConfig(
        platforms=platforms, sessions_dir=tmp_path / "sessions",
    ))


def test_registry_is_built_only_when_whatsapp_is_enabled(tmp_path, monkeypatch):
    from gateway.config import Platform, PlatformConfig
    from gateway.question_registry import QuestionRegistry

    runner = _runner_for_platforms(tmp_path, monkeypatch, {
        Platform.WHATSAPP: PlatformConfig(enabled=True),
    })
    assert isinstance(runner._question_registry, QuestionRegistry)
    assert (tmp_path / "state" / "question_registry.db").exists()

    other = _runner_for_platforms(tmp_path, monkeypatch, {
        Platform.TELEGRAM: PlatformConfig(enabled=True, token="***"),
    })
    assert other._question_registry is None

    disabled = _runner_for_platforms(tmp_path, monkeypatch, {
        Platform.WHATSAPP: PlatformConfig(enabled=False),
    })
    assert disabled._question_registry is None
