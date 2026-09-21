"""Jev advisory classifier client (amendment spec 2026-09-21 §4).

THE ONLY permitted code path to jev-ai.pro. Feature allowlist is the
privacy boundary: only enumerable structural values ever reach the
request builder. Every failure degrades to a deterministic fallback
(ok=False / None) — callers must always have a non-Jev default.

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
# separators — e.g. "0821234567". Reject long bare digit runs; 7 digits is
# the conventional floor for "looks like a phone/account number" and no
# legitimate structural value in ALLOWED_FEATURE_KEYS needs one.
_IDENTIFIER_DIGIT_RUN_RE = re.compile(r"\d{7,}")
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


def validate_features(features: dict) -> dict:
    out: Dict[str, object] = {}
    for key, value in features.items():
        if key not in ALLOWED_FEATURE_KEYS:
            raise FeatureViolation(f"feature key not allowlisted: {key!r}")
        if isinstance(value, bool) or isinstance(value, (int, float)):
            out[key] = value
            continue
        if isinstance(value, str):
            # fullmatch (not match) — match()+"$" would let a value with a
            # trailing "\n" slip through, since "$" may match just before a
            # trailing newline instead of requiring true end-of-string.
            if len(value) > _MAX_STR or not _SAFE_STR_RE.fullmatch(value):
                raise FeatureViolation(
                    f"feature {key!r} value is not a short safe token")
            if _IDENTIFIER_DIGIT_RUN_RE.search(value):
                raise FeatureViolation(
                    f"feature {key!r} value looks like a phone/account "
                    "identifier (long digit run)")
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
        body = {
            "model": self._model,
            "state": json.dumps(safe, sort_keys=True),
            "questions": questions,
        }
        cache_key = json.dumps(body, sort_keys=True)
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
            scores[f["question_id"]] = ans["score"]
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
