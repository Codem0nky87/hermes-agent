"""Component B (spec §3): one question on the wire, criticality preemption,
automatic re-ask, reply routing by ID."""
import threading

import pytest

from gateway.question_registry import CRITICAL_CLASSES, QuestionRegistry


@pytest.fixture()
def reg(tmp_path):
    return QuestionRegistry(str(tmp_path / "q.db"))


def test_first_question_sends_second_holds(reg):
    a = reg.submit(task_ref="t1", session_key="s1", body="deploy now?")
    b = reg.submit(task_ref="t2", session_key="s2", body="rename module?")
    assert a.action == "send"
    assert b.action == "hold"
    assert reg.pending() == a.question_id


def test_critical_preempts_and_resolution_reasks(reg):
    normal = reg.submit(task_ref="t1", session_key="s1", body="deploy now?")
    crit = reg.submit(task_ref="t9", session_key="s9", body="claude auth expired",
                      critical_class="auth_expiry")
    assert crit.action == "send"
    assert crit.preempted_id == normal.question_id
    reask = reg.resolve(crit.question_id)
    assert reask is not None and reask.question_id == normal.question_id
    assert reg.pending() == normal.question_id


def test_unknown_critical_class_rejected(reg):
    with pytest.raises(ValueError):
        reg.submit(task_ref="t1", session_key="s1", body="x", critical_class="urgent")
    assert "auth_expiry" in CRITICAL_CLASSES


def test_resolve_promotes_next_held_fifo(reg):
    a = reg.submit(task_ref="t1", session_key="s1", body="q1")
    b = reg.submit(task_ref="t2", session_key="s2", body="q2")
    c = reg.submit(task_ref="t3", session_key="s3", body="q3")
    nxt = reg.resolve(a.question_id)
    assert nxt.question_id == b.question_id  # FIFO until a scorer is set
    assert reg.pending() == b.question_id


def test_reply_routing_by_id_and_bare(reg):
    a = reg.submit(task_ref="t1", session_key="s1", body="q1")
    r = reg.route_reply(f"{a.question_id} 2")
    assert r.status == "routed" and r.task_ref == "t1" and r.remainder == "2"
    b = reg.submit(task_ref="t2", session_key="s2", body="q2")  # held
    bare = reg.route_reply("yes")
    assert bare.status == "routed" and bare.question_id == a.question_id


def test_reply_to_preempted_question_still_routes(reg):
    # Review Focus #3
    normal = reg.submit(task_ref="t1", session_key="s1", body="deploy now?")
    reg.submit(task_ref="t9", session_key="s9", body="auth!", critical_class="auth_expiry")
    r = reg.route_reply(f"{normal.question_id} 1")
    assert r.status == "routed" and r.task_ref == "t1"


def test_expired_reply_gets_expired_status(reg, tmp_path):
    clock = [1000.0]
    reg2 = QuestionRegistry(str(tmp_path / "q2.db"), now=lambda: clock[0], default_ttl=10)
    a = reg2.submit(task_ref="t1", session_key="s1", body="q")
    clock[0] += 20
    reg2.expire_stale()
    r = reg2.route_reply(f"{a.question_id} 1")
    assert r.status == "expired"


def test_concurrent_submissions_yield_one_pending(tmp_path):
    # Review Focus #2
    reg2 = QuestionRegistry(str(tmp_path / "q3.db"))
    results = []

    def worker(i):
        results.append(reg2.submit(task_ref=f"t{i}", session_key=f"s{i}", body="q"))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(1 for r in results if r.action == "send") == 1
    assert sum(1 for r in results if r.action == "hold") == 7


def test_scorer_orders_held_queue(reg):
    a = reg.submit(task_ref="t1", session_key="s1", body="q1")
    b = reg.submit(task_ref="t2", session_key="s2", body="q2")
    c = reg.submit(task_ref="t3", session_key="s3", body="q3")
    # scorer returns held ids in preferred order; registry follows it
    reg.set_scorer(lambda feats: [c.question_id, b.question_id])
    nxt = reg.resolve(a.question_id)
    assert nxt.question_id == c.question_id


def test_reply_to_answered_id_gets_expired_status(reg):
    # Fix round 1, finding 3: an answered id must not route to an agent
    # again — it reports the same "expired" status as a TTL-expired id.
    a = reg.submit(task_ref="t1", session_key="s1", body="q1")
    reg.resolve(a.question_id)
    r = reg.route_reply(f"{a.question_id} thanks")
    assert r.status == "expired"
    assert r.question_id == a.question_id


def test_id_collision_retries_with_new_id(reg, monkeypatch):
    # Fix round 1, finding 2: a colliding candidate id must not surface an
    # uncaught sqlite3.IntegrityError from submit() — it should retry with
    # a fresh id and succeed.
    ids = iter(["D-AAA", "D-AAA", "D-BBB"])
    monkeypatch.setattr(
        "gateway.question_registry._new_id", lambda: next(ids)
    )
    first = reg.submit(task_ref="t1", session_key="s1", body="q1")
    assert first.question_id == "D-AAA"
    second = reg.submit(task_ref="t2", session_key="s2", body="q2")
    assert second.question_id == "D-BBB"
    assert second.action == "hold"


def test_id_collision_exhausted_raises_clear_error(reg, monkeypatch):
    # If every attempt collides, submit() must fail loudly (not silently
    # drop the question or raise a raw sqlite3.IntegrityError).
    monkeypatch.setattr(
        "gateway.question_registry._new_id", lambda: "D-AAA"
    )
    first = reg.submit(task_ref="t1", session_key="s1", body="q1")
    assert first.question_id == "D-AAA"
    # Every subsequent candidate is still "D-AAA", which now already
    # exists, so all _MAX_ID_ATTEMPTS retries collide.
    with pytest.raises(RuntimeError):
        reg.submit(task_ref="t2", session_key="s2", body="q2")


def test_gentle_reask_once_at_half_ttl(tmp_path):
    clock = [0.0]
    reg = QuestionRegistry(str(tmp_path / "q4.db"), now=lambda: clock[0], default_ttl=100)
    a = reg.submit(task_ref="t1", session_key="s1", body="q")
    assert reg.stale_for_reask() is None
    clock[0] = 60
    info = reg.stale_for_reask()
    assert info is not None and info.question_id == a.question_id
    assert reg.stale_for_reask() is None  # only one gentle re-ask


def test_gentle_reask_skips_critical_questions(tmp_path):
    clock = [0.0]
    reg = QuestionRegistry(str(tmp_path / "q6.db"), now=lambda: clock[0], default_ttl=100)
    reg.submit(task_ref="t9", session_key="s9", body="auth!",
               critical_class="auth_expiry")
    clock[0] = 60
    # Critical prompts are already loud; the gentle nudge is for the rest.
    assert reg.stale_for_reask() is None


def test_concurrent_expire_stale_no_transaction_errors(tmp_path):
    # Fix round 1, finding 1: expire_stale does its own read-modify-write
    # (SELECT stale ids, then UPDATE them) and must be lock-guarded like
    # submit/resolve, or concurrent callers hit sqlite3.OperationalError
    # ("cannot start a transaction within a transaction").
    clock = [1000.0]
    reg2 = QuestionRegistry(str(tmp_path / "q5.db"), now=lambda: clock[0],
                             default_ttl=10)
    for i in range(8):
        reg2.submit(task_ref=f"t{i}", session_key=f"s{i}", body="q")
    clock[0] += 20  # every question (pending + held) is now stale
    errors = []
    results = []

    def worker():
        try:
            results.append(reg2.expire_stale())
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    # Every question must end up expired exactly once, regardless of how
    # the eight expire_stale() calls interleaved.
    assert sorted(qid for batch in results for qid in batch) == sorted(
        set(qid for batch in results for qid in batch)
    )
