"""Component C (spec §4): privacy gate, request shape, deterministic fallback.

Question/answer shapes used by fake transports below (score/choice/noul,
"instructions", "criteria") mirror the live jev-ai.pro contract as verified
against the real API — not an earlier illustrative draft. `evaluate()` is a
generic passthrough and doesn't interpret these shapes itself; the
score/choice-specific parsing lives in score_held_questions/advise_task_class.
"""
import pytest

from agent.jev_client import (
    ALLOWED_FEATURE_KEYS,
    FeatureViolation,
    JevClient,
    JevResult,
    validate_features,
)


def test_allowlisted_features_pass():
    out = validate_features({"task_priority": "P1", "age_seconds": 90,
                             "queue_depth": 3, "agent_health": "VERIFIED"})
    assert out["queue_depth"] == 3


@pytest.mark.parametrize("bad", [
    {"task_text": "please fix the login bug"},          # unknown key
    {"project_alias": "/Users/rufus/Projects/woodhouse"},  # path-like value
    {"task_priority": "call me on +27821234567"},        # identifier-like
    {"agent_health": "x" * 64},                          # free text too long
    {"task_priority": "0821234567"},                     # identifier-like, no separators (hardening)
])
def test_violations_rejected(bad):
    with pytest.raises(FeatureViolation):
        validate_features(bad)


def test_evaluate_builds_pinned_request_and_parses_answers():
    seen = {}

    def fake_transport(body):
        seen.update(body)
        return {"answers": {"is_urgent": {"type": "noul", "noul": 0.91}}}

    c = JevClient(api_key="k", transport=fake_transport)
    r = c.evaluate({"queue_depth": 2},
                    {"is_urgent": {"type": "noul", "instructions": "Is this urgent?"}})
    assert r.ok and r.answers["is_urgent"]["noul"] == pytest.approx(0.91)
    assert seen["model"] == "jev-1.13.0"
    assert "queue_depth" in seen["state"]


def test_malformed_response_is_fallback_not_crash():
    # Review Focus #4
    c = JevClient(api_key="k", transport=lambda body: {"unexpected": []})
    r = c.evaluate({"queue_depth": 1}, {"q": {"type": "noul", "instructions": "x"}})
    assert r == JevResult(ok=False, answers={}, error=r.error)


def test_transport_exception_retries_once_then_fallback():
    attempts = []

    def boom(body):
        attempts.append(1)
        raise TimeoutError("slow")

    c = JevClient(api_key="k", transport=boom)
    r = c.evaluate({"queue_depth": 1}, {"q": {"type": "noul", "instructions": "x"}})
    assert not r.ok
    assert len(attempts) == 2  # spec §4.3: one retry, then deterministic fallback


def test_identical_request_served_from_cache():
    calls = []

    def t(body):
        calls.append(1)
        return {"answers": {"q": {"type": "score", "score": 1}}}

    c = JevClient(api_key="k", transport=t)
    questions = {"q": {"type": "score", "instructions": "x", "criteria": ["low", "high"]}}
    c.evaluate({"queue_depth": 1}, questions)
    c.evaluate({"queue_depth": 1}, questions)
    assert len(calls) == 1  # brief cache for identical feature sets (spec §4.3)


def test_missing_api_key_is_fallback():
    c = JevClient(api_key=None, transport=None)
    assert not c.evaluate({"queue_depth": 1}, {"q": {"type": "noul", "instructions": "x"}}).ok


def test_call_cap_enforced():
    calls = []
    c = JevClient(api_key="k", max_calls_per_minute=2,
                  transport=lambda b: (calls.append(1) or
                                       {"answers": {"q": {"type": "score", "score": 1}}}))
    questions = {"q": {"type": "score", "instructions": "x", "criteria": ["low", "high"]}}
    # NOTE: feature payload varies per iteration (queue_depth=i) so each
    # evaluate() call is a genuinely distinct request. A fixed payload here
    # would make calls 2-5 dedupe through the cache before ever reaching the
    # cap boundary, masking the cap check rather than exercising it.
    for i in range(5):
        c.evaluate({"queue_depth": i}, questions)
    assert len(calls) == 2  # further calls short-circuit to fallback


def test_score_held_questions_orders_by_score():
    def t(body):
        # two questions keyed q_<id>; higher score = more urgent
        return {"answers": {"q_D-AAA": {"type": "score", "score": 2},
                             "q_D-BBB": {"type": "score", "score": 9}}}
    c = JevClient(api_key="k", transport=t)
    order = c.score_held_questions([
        {"question_id": "D-AAA", "age_seconds": 10},
        {"question_id": "D-BBB", "age_seconds": 5},
    ])
    assert order == ["D-BBB", "D-AAA"]


def test_score_held_questions_none_on_failure():
    # realistically-wrong: a well-formed answers map, but the individual
    # answer is missing the "score" field a real score-type answer carries.
    c = JevClient(api_key="k", transport=lambda b: {"answers": {"q_D-AAA": {"type": "score"}}})
    assert c.score_held_questions([{"question_id": "D-AAA", "age_seconds": 1}]) is None


def test_advise_task_class_returns_choice():
    def t(body):
        q = body["questions"]["task_class"]
        assert q["type"] == "choice"
        assert q["criteria"]["quick-fix"]
        return {"answers": {"task_class": {"type": "choice", "choice": "quick-fix",
                                            "confidence": 0.6}}}

    c = JevClient(api_key="k", transport=t)
    assert c.advise_task_class({"queue_depth": 1}) == "quick-fix"


def test_advise_task_class_unknown_value_is_none():
    # realistically-wrong: well-formed choice answer, but a value outside
    # the four allowed task classes (e.g. a model drifting off-menu).
    c = JevClient(api_key="k",
                  transport=lambda b: {"answers": {"task_class": {"type": "choice",
                                                                   "choice": "not-a-real-class"}}})
    assert c.advise_task_class({"queue_depth": 1}) is None
