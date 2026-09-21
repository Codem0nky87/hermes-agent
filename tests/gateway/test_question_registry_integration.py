"""Outbound questions pass through the registry gate; replies route before
LLM dispatch; critical resolution re-sends the preempted question."""
import asyncio
import contextlib
import threading
import time
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
async def test_promoted_held_question_is_not_worded_as_a_reask(tmp_path):
    """Fix round 1, controller ruling: only a preempted question has been
    seen before. A first delivery off the held queue is just a question."""
    from gateway import run as run_mod

    reg = QuestionRegistry(str(tmp_path / "q.db"))
    out = _Sent()
    first = await run_mod._send_question_gated(reg, out.send, task_ref="t1",
                                               session_key="s1", body="deploy?")
    held = await run_mod._send_question_gated(reg, out.send, task_ref="t2",
                                              session_key="s2", body="rename?")
    await run_mod._resolve_question_gated(reg, out.send, first.question_id)

    assert len(out.messages) == 2
    session_key, text = out.messages[1]
    assert session_key == "s2"
    assert text == f"[{held.question_id}] rename?"
    assert "Re-asking" not in text


@pytest.mark.asyncio
async def test_send_that_never_lands_cancels_the_question(tmp_path):
    """Fix round 1, I2: the id must survive a send that hangs or raises, or
    the question can never be cancelled and holds the wire until its TTL."""
    from gateway import run as run_mod

    reg = QuestionRegistry(str(tmp_path / "q.db"))
    out = _Sent()

    def _hangs_then_times_out(_text):
        raise TimeoutError("send future timed out")

    question_id, sent_ok = run_mod._dispatch_gated_question(
        reg, _hangs_then_times_out, task_ref="t1", session_key="s1",
        body="deploy?")

    assert question_id is not None and sent_ok is False
    assert reg.pending() == question_id  # registered despite the failed send
    await run_mod._cancel_question_gated(reg, out.send, question_id)
    assert reg.pending() is None
    assert reg.submit(task_ref="t2", session_key="s2",
                      body="rename?").action == "send"

    # A send that merely reports failure (no exception) behaves the same.
    reg2 = QuestionRegistry(str(tmp_path / "q2.db"))
    qid2, ok2 = run_mod._dispatch_gated_question(
        reg2, lambda _text: False, task_ref="t1", session_key="s1", body="q")
    assert qid2 is not None and ok2 is False
    await run_mod._cancel_question_gated(reg2, out.send, qid2)
    assert reg2.pending() is None


@pytest.mark.asyncio
async def test_dispatch_sends_the_wire_text_and_holds_the_second(tmp_path):
    from gateway import run as run_mod

    reg = QuestionRegistry(str(tmp_path / "q.db"))
    seen = []
    first, ok = run_mod._dispatch_gated_question(
        reg, lambda text: seen.append(text) or True,
        task_ref="t1", session_key="s1", body="deploy?")
    assert ok is True and seen == [f"[{first}] deploy?"]

    second, ok2 = run_mod._dispatch_gated_question(
        reg, lambda text: seen.append(text) or True,
        task_ref="t2", session_key="s2", body="rename?")
    # Held: nothing sent, but the caller still holds an id it can cancel.
    assert ok2 is True and second is not None and len(seen) == 1
    assert reg.held_ids() == [second]


async def _loop_responsiveness_during(coro, *, tick=0.01):
    """Run *coro*, counting how many times a plain heartbeat task gets to run.

    A blocking call left on the event loop freezes every other coroutine for
    its whole duration, so the heartbeat count is a direct measurement of
    whether the loop stayed alive.
    """
    ticks = 0

    async def heartbeat():
        nonlocal ticks
        while True:
            await asyncio.sleep(tick)
            ticks += 1

    beat = asyncio.ensure_future(heartbeat())
    try:
        result = await coro
    finally:
        beat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await beat
    return result, ticks


@pytest.mark.asyncio
async def test_slow_scorer_does_not_block_the_gateway_loop(tmp_path):
    """Final review, Important 2: the Jev scorer does synchronous urllib I/O
    (3 s timeout plus one retry ⇒ up to ~6 s) from inside the registry lock
    and an open sqlite transaction. Called inline from the gateway's async
    paths it stalls typing indicators, heartbeats and every adapter, for every
    chat. The async call sites must hand it to a thread."""
    from gateway import run as run_mod

    reg = QuestionRegistry(str(tmp_path / "q.db"))
    out = _Sent()
    on_the_wire = reg.submit(task_ref="t1", session_key="s1", body="deploy?")
    held = reg.submit(task_ref="t2", session_key="s2", body="rename?")

    scored = threading.Event()

    def slow_scorer(feats):
        scored.set()
        time.sleep(0.5)  # a slow / unreachable Jev
        return [f["question_id"] for f in feats]

    reg.set_scorer(slow_scorer)

    reask, ticks = await _loop_responsiveness_during(
        run_mod._resolve_question_gated(reg, out.send, on_the_wire.question_id)
    )

    assert scored.is_set()  # the slow scorer really did run
    # Inline on the loop this would be 0: nothing else gets to run for 0.5 s.
    assert ticks >= 10, f"event loop stalled during resolve (ticks={ticks})"
    assert reask is not None and reask.question_id == held.question_id
    assert len(out.messages) == 1  # promotion still happened


@pytest.mark.asyncio
async def test_slow_scorer_does_not_block_the_loop_on_cancel(tmp_path):
    """``cancel`` reaches the same scorer through the promotion path — the
    release path (button answers, clarify timeouts, failed sends) must stay
    off the loop too."""
    from gateway import run as run_mod

    reg = QuestionRegistry(str(tmp_path / "q.db"))
    out = _Sent()
    on_the_wire = reg.submit(task_ref="t1", session_key="s1", body="deploy?")
    held = reg.submit(task_ref="t2", session_key="s2", body="rename?")
    reg.set_scorer(lambda feats: time.sleep(0.5) or
                   [f["question_id"] for f in feats])

    reask, ticks = await _loop_responsiveness_during(
        run_mod._cancel_question_gated(reg, out.send, on_the_wire.question_id)
    )

    assert ticks >= 10, f"event loop stalled during cancel (ticks={ticks})"
    assert reask is not None and reask.question_id == held.question_id


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
    assert sent == [f"[{held.question_id}] rename?"]  # first showing, not a re-ask


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
async def test_releasing_a_held_question_leaves_the_wire_alone(tmp_path):
    """Fix round 1, I1: the question being released is not necessarily the
    one on the wire. Retiring a held one must not promote a sibling over the
    question the user is still looking at."""
    runner, sent = _runner(tmp_path)

    async def _sender(text):
        sent.append(text)

    for key in ("owner", "other", "third"):
        runner._question_senders[key] = _sender
    on_the_wire = runner._question_registry.submit(
        task_ref="t1", session_key="owner", body="deploy?")
    held = runner._question_registry.submit(
        task_ref="t2", session_key="other", body="rename?")
    also_held = runner._question_registry.submit(
        task_ref="t3", session_key="third", body="delete?")

    await runner._release_question(held.question_id)

    assert runner._question_registry.pending() == on_the_wire.question_id
    assert runner._question_registry.held_ids() == [also_held.question_id]
    assert sent == []  # nothing new reached anyone

    # Releasing the question that IS on the wire still promotes.
    await runner._release_question(on_the_wire.question_id)
    assert runner._question_registry.pending() == also_held.question_id
    assert sent == [f"[{also_held.question_id}] delete?"]


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


def test_construction_reconciles_questions_left_by_a_dead_process(
    tmp_path, monkeypatch,
):
    """Fix round 1, C1: the db outlives the process, the clarify waiters do
    not. A restart must not inherit a pending question that holds the wire."""
    from gateway.config import Platform, PlatformConfig
    from gateway.question_registry import QuestionRegistry

    db = tmp_path / "state" / "question_registry.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    before_restart = QuestionRegistry(str(db))
    stranded = before_restart.submit(
        task_ref="t1", session_key="owner", body="deploy?")
    before_restart.submit(task_ref="t2", session_key="other", body="rename?")

    runner = _runner_for_platforms(tmp_path, monkeypatch, {
        Platform.WHATSAPP: PlatformConfig(enabled=True),
    })

    assert runner._question_registry.pending() is None
    assert runner._question_registry.held_ids() == []
    assert runner._question_registry.submit(
        task_ref="t3", session_key="owner", body="fresh?").action == "send"
    assert before_restart.route_reply(
        f"{stranded.question_id} yes").status == "expired"
