"""Jev advisory classifier client (amendment spec 2026-09-21 §4).

THE ONLY permitted code path to jev-ai.pro. Feature values (the request
"state") and any caller-supplied identifiers that get embedded into the
request on this module's own initiative (e.g. score_held_questions'
question_id) must pass the safe-token allowlist rule — bounded length,
plain [A-Za-z0-9_.-] charset, no bare long digit run even once
separators are stripped — before they reach the request builder; any
failure there aborts the call (JevResult(ok=False) / None) rather than
letting the value through. Question/answer *schema* (types,
instructions, criteria — authored by this module's code, not raw user
data) is passed through as-is; see evaluate()'s docstring. Every
failure degrades to a deterministic fallback (ok=False / None) —
callers must always have a non-Jev default, and evaluate() must never
raise.

Question/answer shapes below (score/choice/noul, "instructions",
"criteria") reflect the live jev-ai.pro contract, verified against the
real API — not the earlier illustrative draft. `evaluate()` itself is a
generic passthrough: it does not interpret question/answer semantics,
it only builds the pinned+allowlisted request and hands back whatever
"answers" mapping comes back (or a deterministic fallback).
"""
from __future__ import annotations

import json
import os
import re
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

ALLOWED_FEATURE_KEYS = frozenset({
    "task_priority", "age_seconds", "kanban_state", "queue_depth",
    "agent_health", "project_alias", "task_class", "diff_files",
    "diff_lines", "retry_count", "provider_changes", "blocked_state",
    "has_clean_checkpoint", "question_id",
})

_MAX_STR = 32
_SAFE_STR_RE = re.compile(r"^[A-Za-z0-9_\-\.]{1,32}$")
# Privacy hardening: a value can pass the character-class check above yet
# still be an identifier (phone number, account number, ...) if it has no
# separators — e.g. "0821234567" — or if its digits are merely broken up
# by the very separators the charset allows — e.g. "082-123-4567" or
# "082.123.4567". Strip [-._] before running the digit-run check so a
# formatted identifier can't hide between allowed punctuation. 7 digits is
# the conventional floor for "looks like a phone/account number" and no
# legitimate structural value in ALLOWED_FEATURE_KEYS needs one.
_IDENTIFIER_DIGIT_RUN_RE = re.compile(r"\d{7,}")
_SEPARATOR_RE = re.compile(r"[-._]")
_TASK_CLASSES = ("quick-fix", "feature", "review", "investigation")

_SCORE_CRITERIA = ("low", "medium", "high", "critical")
_SCORE_INSTRUCTIONS = (
    "Score how urgently this held question should be delivered next, "
    "given its age."
)
_TASK_CLASS_CRITERIA = {
    "quick-fix": "small bounded fix",
    "feature": "new functionality",
    "review": "code review pass",
    "investigation": "research or diagnosis",
}
_TASK_CLASS_INSTRUCTIONS = "Classify this coding task."


class FeatureViolation(ValueError):
    pass


def _is_safe_token(value: object) -> bool:
    """The allowlist's safe-token rule, as its own function so every call
    site that embeds a caller-supplied string into a request — not just
    validate_features' own dict values — is forced through the same gate.

    (A prior bypass let score_held_questions build request dict keys
    from feats[i]["question_id"] directly, without ever calling this —
    fixed by routing that value through here too.)
    """
    if not isinstance(value, str):
        return False
    # fullmatch (not match) — match()+"$" would let a value with a
    # trailing "\n" slip through, since "$" may match just before a
    # trailing newline instead of requiring true end-of-string.
    if len(value) > _MAX_STR or not _SAFE_STR_RE.fullmatch(value):
        return False
    # Strip allowed separators before the digit-run check so a phone
    # number formatted as "082-123-4567" / "082.123.4567" can't hide its
    # digits from a check that only looked for one contiguous run.
    stripped = _SEPARATOR_RE.sub("", value)
    if _IDENTIFIER_DIGIT_RUN_RE.search(stripped):
        return False
    return True


def validate_features(features: dict) -> dict:
    out: Dict[str, object] = {}
    for key, value in features.items():
        if key not in ALLOWED_FEATURE_KEYS:
            raise FeatureViolation(f"feature key not allowlisted: {key!r}")
        if isinstance(value, bool) or isinstance(value, (int, float)):
            out[key] = value
            continue
        if isinstance(value, str):
            if not _is_safe_token(value):
                raise FeatureViolation(
                    f"feature {key!r} value is not an allowlisted safe "
                    "token (unsafe charset/length, or looks like an "
                    "identifier)")
            out[key] = value
            continue
        raise FeatureViolation(f"feature {key!r} has unsupported type")
    return out


@dataclass
class JevResult:
    ok: bool
    answers: dict = field(default_factory=dict)
    error: Optional[str] = None


def _default_transport(base_url: str, api_key: str, timeout: float):
    def post(body: dict) -> dict:
        req = urllib.request.Request(
            base_url,
            data=json.dumps(body).encode(),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    return post


class JevClient:
    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        model: str = "jev-1.13.0",
        base_url: str = "https://jev-ai.pro/api/v1/systemone",
        timeout: float = 3.0,
        max_calls_per_minute: int = 30,
        transport: Optional[Callable[[dict], dict]] = None,
    ) -> None:
        self._api_key = api_key if api_key is not None else os.environ.get("JEV_AI_API_KEY")
        self._model = model
        self._timeout = timeout
        self._cap = max_calls_per_minute
        self._call_times: List[float] = []
        self._cache: Dict[str, tuple] = {}
        self._cache_ttl = 60.0
        if transport is not None:
            self._transport = transport
        elif self._api_key:
            self._transport = _default_transport(base_url, self._api_key, timeout)
        else:
            self._transport = None

    # -- core -------------------------------------------------------------
    def evaluate(self, features: dict, questions: dict) -> JevResult:
        if self._transport is None:
            return JevResult(ok=False, error="no api key / transport")
        if not self._under_cap():
            return JevResult(ok=False, error="per-minute call cap reached")
        try:
            safe = validate_features(features)
        except FeatureViolation as exc:
            return JevResult(ok=False, error=f"feature violation: {exc}")
        try:
            body = {
                "model": self._model,
                "state": json.dumps(safe, sort_keys=True),
                "questions": questions,
            }
            cache_key = json.dumps(body, sort_keys=True)
        except Exception as exc:
            # questions is caller-supplied and may contain values json
            # can't serialize (a set, a custom object, ...) — that must
            # degrade to a fallback like every other failure mode here,
            # not raise out of evaluate().
            return JevResult(ok=False, error=f"unserializable request: {exc}")
        now = time.monotonic()
        cached = self._cache.get(cache_key)
        if cached is not None and now - cached[0] < self._cache_ttl:
            return cached[1]
        last_error = "unknown"
        for _attempt in range(2):  # spec §4.3: one retry
            try:
                raw = self._transport(body)
                answers = raw["answers"]
                if not isinstance(answers, dict):
                    raise ValueError("answers not a dict")
                result = JevResult(ok=True, answers=answers)
                self._cache[cache_key] = (now, result)
                return result
            except Exception as exc:
                last_error = str(exc)
        return JevResult(ok=False, error=last_error)

    # -- decision-point helpers -------------------------------------------
    def score_held_questions(self, feats: List[dict]) -> Optional[List[str]]:
        # Every question_id becomes a request dict key below — route each
        # one through the same safe-token gate as any other value reaching
        # the request builder before it's embedded anywhere, or fall back
        # without ever calling evaluate()/transport.
        for f in feats:
            if not _is_safe_token(f.get("question_id")):
                return None
        questions = {
            f"q_{f['question_id']}": {
                "type": "score",
                "instructions": _SCORE_INSTRUCTIONS,
                "criteria": list(_SCORE_CRITERIA),
            }
            for f in feats
        }
        merged = {"queue_depth": len(feats)}
        result = self.evaluate(merged, questions)
        if not result.ok:
            return None
        scores: Dict[str, float] = {}
        for f in feats:
            ans = result.answers.get(f"q_{f['question_id']}")
            if not isinstance(ans, dict) or "score" not in ans:
                return None
            score = ans["score"]
            # bool is an int subclass in Python — exclude it explicitly so
            # a stray True/False can't silently sort as 0/1, and reject any
            # other non-numeric score (e.g. a str) instead of letting
            # sorted()'s key comparison raise TypeError out of this method.
            if isinstance(score, bool) or not isinstance(score, (int, float)):
                return None
            scores[f["question_id"]] = score
        return sorted(scores, key=scores.get, reverse=True)

    def advise_task_class(self, features: dict) -> Optional[str]:
        result = self.evaluate(
            features,
            {
                "task_class": {
                    "type": "choice",
                    "instructions": _TASK_CLASS_INSTRUCTIONS,
                    "criteria": dict(_TASK_CLASS_CRITERIA),
                }
            },
        )
        if not result.ok:
            return None
        ans = result.answers.get("task_class")
        value = ans.get("choice") if isinstance(ans, dict) else None
        return value if value in _TASK_CLASSES else None

    # -- internals --------------------------------------------------------
    def _under_cap(self) -> bool:
        now = time.monotonic()
        self._call_times = [t for t in self._call_times if now - t < 60]
        if len(self._call_times) >= self._cap:
            return False
        self._call_times.append(now)
        return True
