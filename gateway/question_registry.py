"""Pending-question registry (amendment spec 2026-09-21 §3).

One non-critical question on the wire at a time; fixed critical classes
preempt; resolving frees the wire and promotes the preempted question
first, then the held queue (scorer order if set, else FIFO). SQLite in
WAL mode; BEGIN IMMEDIATE serializes the send/hold race.
"""
from __future__ import annotations

import re
import secrets
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Callable, List, Optional

CRITICAL_CLASSES = frozenset(
    {"auth_expiry", "security_stop", "work_loss_risk", "infra_failure"}
)

_ID_RE = re.compile(r"^(D-[A-Z2-9]{3})\b\s*(.*)$", re.DOTALL)
_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
_MAX_ID_ATTEMPTS = 5

# Guards the submit/resolve/expire_stale transactions — every method that
# does a read-modify-write against the questions table. The sqlite
# connection is opened with check_same_thread=False so multiple gateway
# threads can share one QuestionRegistry instance; BEGIN IMMEDIATE alone
# still races under concurrent threads on a single connection (sqlite3's
# own implicit transaction handling can raise "cannot start a transaction
# within a transaction" when two threads interleave statements on one
# connection), so this lock serializes access at the Python level. The
# sqlite transaction is kept too — it still matters if this module is
# ever used from multiple processes against the same db file.
_LOCK = threading.Lock()


def _new_id() -> str:
    """Generate a candidate D-XXX question id. Module-level so tests can
    monkeypatch it to force id collisions deterministically."""
    return "D-" + "".join(secrets.choice(_ALPHABET) for _ in range(3))


@dataclass
class SubmitResult:
    question_id: str
    action: str  # "send" | "hold"
    preempted_id: Optional[str] = None


@dataclass
class ReaskInfo:
    question_id: str
    task_ref: str
    session_key: str
    body: str
    # True only when this question had already been sent and was pushed off
    # the wire by a critical one — i.e. the user has seen it before and is
    # being asked again. A question promoted straight from the held queue has
    # never been shown, so it must not be worded as a re-ask.
    was_preempted: bool = False


@dataclass
class RouteResult:
    status: str  # "routed" | "ambiguous" | "expired" | "unmatched"
    question_id: Optional[str]
    task_ref: Optional[str]
    session_key: Optional[str]
    remainder: str


class QuestionRegistry:
    def __init__(
        self,
        db_path: str,
        *,
        now: Callable[[], float] = time.time,
        default_ttl: float = 4 * 3600,
    ) -> None:
        self._now = now
        self._ttl = default_ttl
        self._scorer: Optional[Callable[[List[dict]], List[str]]] = None
        # Question ids that already had their one gentle half-TTL nudge.
        # Process-local by design: a gateway restart is allowed to nudge
        # once more rather than stay silent about a question it re-adopts.
        self._reasked: set[str] = set()
        self._db = sqlite3.connect(db_path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute(
            """CREATE TABLE IF NOT EXISTS questions (
                   question_id TEXT PRIMARY KEY,
                   task_ref TEXT NOT NULL,
                   session_key TEXT NOT NULL,
                   body TEXT NOT NULL,
                   critical_class TEXT,
                   status TEXT NOT NULL,
                   created_at REAL NOT NULL,
                   expires_at REAL NOT NULL
               )"""
        )
        self._db.commit()

    # -- public -----------------------------------------------------------
    def set_scorer(self, scorer) -> None:
        self._scorer = scorer

    def submit(self, *, task_ref: str, session_key: str, body: str,
               critical_class: Optional[str] = None) -> SubmitResult:
        if critical_class is not None and critical_class not in CRITICAL_CLASSES:
            raise ValueError(f"unknown critical class: {critical_class!r}")
        now = self._now()
        with _LOCK:
            cur = self._db.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                preempted_id = None
                if critical_class is not None:
                    row = cur.execute(
                        "SELECT question_id FROM questions "
                        "WHERE status='pending' AND critical_class IS NULL"
                    ).fetchone()
                    if row:
                        preempted_id = row[0]
                        cur.execute(
                            "UPDATE questions SET status='preempted' "
                            "WHERE question_id=?", (preempted_id,))
                    action = "send"
                    status = "pending"
                else:
                    busy = cur.execute(
                        "SELECT 1 FROM questions WHERE status='pending'"
                    ).fetchone()
                    action = "hold" if busy else "send"
                    status = "held" if busy else "pending"
                qid = None
                last_exc: Optional[BaseException] = None
                for _ in range(_MAX_ID_ATTEMPTS):
                    candidate = _new_id()
                    try:
                        cur.execute(
                            "INSERT INTO questions VALUES (?,?,?,?,?,?,?,?)",
                            (candidate, task_ref, session_key, body,
                             critical_class, status, now, now + self._ttl),
                        )
                        qid = candidate
                        break
                    except sqlite3.IntegrityError as exc:
                        last_exc = exc
                        continue
                if qid is None:
                    raise RuntimeError(
                        f"question id space exhausted: {_MAX_ID_ATTEMPTS} "
                        "consecutive id collisions"
                    ) from last_exc
                self._db.commit()
            except BaseException:
                self._db.rollback()
                raise
        return SubmitResult(qid, action, preempted_id)

    def resolve(self, question_id: str) -> Optional[ReaskInfo]:
        with _LOCK:
            cur = self._db.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                cur.execute(
                    "UPDATE questions SET status='answered' WHERE question_id=?",
                    (question_id,))
                nxt = self._pick_next(cur)
                if nxt is not None:
                    cur.execute(
                        "UPDATE questions SET status='pending' WHERE question_id=?",
                        (nxt.question_id,))
                self._db.commit()
            except BaseException:
                self._db.rollback()
                raise
        return nxt

    def cancel(self, question_id: str) -> Optional[ReaskInfo]:
        """Retire a question whose asker stopped waiting for an answer.

        Only a ``pending`` question occupies the wire, so only cancelling one
        of those may promote a successor — exactly as ``resolve`` does. A
        ``held`` or ``preempted`` question is NOT on the wire: expiring it
        must promote nothing, or a sibling would be pushed out alongside the
        question the user is actually looking at. Any other status (already
        answered or expired) is a no-op, which makes this safe to call from a
        release path racing an inbound answer.
        """
        with _LOCK:
            cur = self._db.cursor()
            cur.execute("BEGIN IMMEDIATE")
            nxt = None
            try:
                row = cur.execute(
                    "SELECT status FROM questions WHERE question_id=?",
                    (question_id,)).fetchone()
                if row is None or row[0] not in ("pending", "held", "preempted"):
                    self._db.commit()
                    return None
                was_on_the_wire = row[0] == "pending"
                cur.execute(
                    "UPDATE questions SET status='expired' WHERE question_id=?",
                    (question_id,))
                if was_on_the_wire:
                    nxt = self._pick_next(cur)
                    if nxt is not None:
                        cur.execute(
                            "UPDATE questions SET status='pending' "
                            "WHERE question_id=?", (nxt.question_id,))
                self._db.commit()
            except BaseException:
                self._db.rollback()
                raise
        return nxt

    def reconcile_startup(self) -> List[str]:
        """Expire every question left in flight by a previous process.

        Questions are agent-session-bound: the waiter that would receive an
        answer lives in the process that asked, and it does not survive a
        restart. An inherited ``pending`` row can therefore never be answered,
        and leaving it would hold the wire against every new question for the
        rest of its TTL. Held and preempted rows are retired for the same
        reason — their askers are gone too.
        """
        with _LOCK:
            cur = self._db.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                rows = cur.execute(
                    "SELECT question_id FROM questions "
                    "WHERE status IN ('pending','held','preempted')").fetchall()
                ids = [r[0] for r in rows]
                if ids:
                    cur.executemany(
                        "UPDATE questions SET status='expired' "
                        "WHERE question_id=?",
                        [(i,) for i in ids])
                self._db.commit()
            except BaseException:
                self._db.rollback()
                raise
        return ids

    def route_reply(self, text: str) -> RouteResult:
        text = (text or "").strip()
        m = _ID_RE.match(text)
        if m:
            qid, remainder = m.group(1), m.group(2)
            row = self._db.execute(
                "SELECT task_ref, session_key, status FROM questions "
                "WHERE question_id=?", (qid,)).fetchone()
            if row is None:
                return RouteResult("unmatched", None, None, None, text)
            task_ref, session_key, status = row
            if status in ("expired", "answered"):
                # An answered question is functionally expired for replies:
                # it already resolved, its slot may have been reassigned,
                # and a late reply must not reach an agent a second time.
                return RouteResult("expired", qid, task_ref, session_key, remainder)
            return RouteResult("routed", qid, task_ref, session_key, remainder)
        rows = self._db.execute(
            "SELECT question_id, task_ref, session_key FROM questions "
            "WHERE status='pending'").fetchall()
        if len(rows) == 1:
            qid, task_ref, session_key = rows[0]
            return RouteResult("routed", qid, task_ref, session_key, text)
        if len(rows) > 1:
            return RouteResult("ambiguous", None, None, None, text)
        return RouteResult("unmatched", None, None, None, text)

    def expire_stale(self) -> List[str]:
        now = self._now()
        with _LOCK:
            cur = self._db.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                rows = cur.execute(
                    "SELECT question_id FROM questions "
                    "WHERE status IN ('pending','held','preempted') "
                    "AND expires_at < ?",
                    (now,)).fetchall()
                ids = [r[0] for r in rows]
                if ids:
                    cur.executemany(
                        "UPDATE questions SET status='expired' "
                        "WHERE question_id=?",
                        [(i,) for i in ids])
                self._db.commit()
            except BaseException:
                self._db.rollback()
                raise
        return ids

    def stale_for_reask(self) -> Optional[ReaskInfo]:
        """One gentle re-ask at half-TTL: return the pending question that has
        been quiet past default_ttl/2 and has not been re-asked yet.

        Read-mostly, but the ``_reasked`` bookkeeping is a read-modify-write —
        two sweeps racing here would both claim the same question and send two
        nudges — so it takes the same lock as submit/resolve/expire_stale.
        """
        now = self._now()
        with _LOCK:
            row = self._db.execute(
                "SELECT question_id, task_ref, session_key, body, created_at "
                "FROM questions WHERE status='pending' AND critical_class IS NULL"
            ).fetchone()
            if row is None or now - row[4] < self._ttl / 2:
                return None
            if row[0] in self._reasked:
                return None
            self._reasked.add(row[0])
        return ReaskInfo(row[0], row[1], row[2], row[3])

    def pending(self) -> Optional[str]:
        row = self._db.execute(
            "SELECT question_id FROM questions WHERE status='pending' "
            "ORDER BY created_at LIMIT 1").fetchone()
        return row[0] if row else None

    def held_ids(self) -> List[str]:
        return [r[0] for r in self._db.execute(
            "SELECT question_id FROM questions WHERE status='held' "
            "ORDER BY created_at").fetchall()]

    # -- internals --------------------------------------------------------
    def _pick_next(self, cur) -> Optional[ReaskInfo]:
        row = cur.execute(
            "SELECT question_id, task_ref, session_key, body FROM questions "
            "WHERE status='preempted' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        was_preempted = row is not None
        if row is None:
            held = cur.execute(
                "SELECT question_id, task_ref, session_key, body, created_at "
                "FROM questions WHERE status='held' ORDER BY created_at"
            ).fetchall()
            if not held:
                return None
            order = [h[0] for h in held]
            if self._scorer is not None:
                feats = [
                    {"question_id": h[0], "age_seconds": self._now() - h[4]}
                    for h in held
                ]
                try:
                    scored = [q for q in self._scorer(feats) if q in order]
                    order = scored + [q for q in order if q not in scored]
                except Exception:
                    pass  # deterministic FIFO fallback
            by_id = {h[0]: h for h in held}
            row = by_id[order[0]][:4]
        return ReaskInfo(row[0], row[1], row[2], row[3], was_preempted)
