"""Jev scorer plugs into the registry; Jev failure degrades to FIFO.

Fake transports here use the REAL jev-ai.pro contract as verified against
the live API (score answers are {"type": "score", "score": <float>, ...}),
not the illustrative {"value": i} shape from the original amendment draft
— see agent/jev_client.py's module docstring and tests/agent/test_jev_client.py.
"""
from gateway.question_registry import QuestionRegistry
from agent.jev_client import JevClient


def _registry_with_scorer(tmp_path, transport):
    reg = QuestionRegistry(str(tmp_path / "q.db"))
    client = JevClient(api_key="k", transport=transport)
    reg.set_scorer(client.score_held_questions)
    return reg


def test_jev_orders_held_queue(tmp_path):
    # Question ids are random D-XXX tokens, so the fake can't pick a winner
    # by sorting alphabetically ahead of time — it forces a specific held
    # question's key to the top score once its id is known, guaranteeing the
    # pick differs from what FIFO would return (FIFO would pick `b`, the
    # first held submission).
    winner = {}

    def transport(body):
        qs = body["questions"]
        return {
            "answers": {
                k: {"score": 10.0 if k == winner.get("key") else 0.0}
                for k in qs
            }
        }

    reg = _registry_with_scorer(tmp_path, transport)
    a = reg.submit(task_ref="t1", session_key="s1", body="q1")
    b = reg.submit(task_ref="t2", session_key="s2", body="q2")
    c = reg.submit(task_ref="t3", session_key="s3", body="q3")
    winner["key"] = f"q_{c.question_id}"

    nxt = reg.resolve(a.question_id)
    assert nxt.question_id == c.question_id  # scorer consulted, not FIFO (would be b)


def test_jev_outage_degrades_to_fifo(tmp_path):
    def transport(body):
        raise TimeoutError("down")

    reg = _registry_with_scorer(tmp_path, transport)
    a = reg.submit(task_ref="t1", session_key="s1", body="q1")
    b = reg.submit(task_ref="t2", session_key="s2", body="q2")
    reg.submit(task_ref="t3", session_key="s3", body="q3")
    assert reg.resolve(a.question_id).question_id == b.question_id  # FIFO
