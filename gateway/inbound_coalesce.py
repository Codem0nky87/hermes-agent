"""Inbound burst coalescing (amendment spec 2026-09-21 §2).

Holds a just-arrived inbound message briefly; further messages from the
same chat inside the window merge into one combined turn. A bare
attachment whose window expires with no follow-up gets MEDIA_ONLY_NOTE
appended so the agent asks instead of guessing. Slash commands bypass;
stop-class commands also drop held work for that chat.

Windows disable per class: ``text_window=0`` means a text message opens no
window of its own and dispatches immediately *when nothing is held for that
chat* — but it still merges into an already-open window. That is how the
WhatsApp config runs, because the adapter there already batches text on its
own 5s quiet period and a second gateway-side text window would only stack
latency onto it.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Dict, List

logger = logging.getLogger(__name__)

MEDIA_ONLY_NOTE = (
    "[attachment arrived with no instruction — ask the user what to do "
    "with it; do not guess]"
)

_DEFAULT_STOP_COMMANDS = frozenset({"/stop", "/deny", "/pause"})


class _Held:
    __slots__ = ("events", "first_at", "timer")

    def __init__(self, event: Any, timer: asyncio.TimerHandle) -> None:
        self.events: List[Any] = [event]
        self.first_at: float = time.monotonic()
        self.timer: asyncio.TimerHandle = timer


class InboundCoalescer:
    def __init__(
        self,
        dispatch: Callable[[Any], Awaitable[None]],
        *,
        text_window: float = 4.0,
        media_window: float = 8.0,
        max_window: float = 10.0,
        stop_commands: frozenset = _DEFAULT_STOP_COMMANDS,
    ) -> None:
        self._dispatch = dispatch
        self._text_window = float(text_window)
        self._media_window = float(media_window)
        self._max_window = float(max_window)
        self._stop_commands = stop_commands
        self._held: Dict[str, _Held] = {}

    # -- public -----------------------------------------------------------
    async def submit(self, key: str, event: Any) -> None:
        text = (getattr(event, "text", "") or "").strip()
        if text.startswith("/"):
            command = text.split()[0].lower()
            if command in self._stop_commands:
                self._drop(key)
            await self._dispatch(event)
            return
        loop = asyncio.get_running_loop()
        held = self._held.get(key)
        if held is None:
            window = self._window_for(event)
            if window <= 0.0:
                # Per-window disable: this class of message opens no window of
                # its own, so with nothing already held for this chat it goes
                # straight through. Used for text on WhatsApp, where the
                # adapter already applies its own 5s quiet-period batching and
                # a second gateway-side window would just stack latency.
                await self._dispatch(event)
                return
            timer = loop.call_later(window, self._flush_soon, key)
            self._held[key] = _Held(event, timer)
            return

        # A window IS open for this chat: merge regardless of the arriving
        # message's own window setting, then re-arm from the merged turn's
        # shape (_window_for_held). This is what keeps the caption case
        # working with text_window=0 — media opens the media window, the next
        # photo of an album keeps it open, and the caption that follows joins
        # that turn (and, at text_window=0, closes it immediately) instead of
        # racing it.
        held.events.append(event)
        held.timer.cancel()
        elapsed = time.monotonic() - held.first_at
        remaining_cap = self._max_window - elapsed
        window = min(self._window_for_held(held), max(remaining_cap, 0.0))
        if window <= 0.0:
            await self._flush(key)
        else:
            held.timer = loop.call_later(window, self._flush_soon, key)

    # -- internals --------------------------------------------------------
    def _window_for(self, event: Any) -> float:
        """Opening window for *event*; ``<= 0`` disables holding for its class."""
        return self._window_class(
            bool(getattr(event, "media_urls", None)),
            bool((getattr(event, "text", "") or "").strip()),
        )

    def _window_for_held(self, held: "_Held") -> float:
        """Reset window for a turn that is still collecting fragments.

        Classified from the HELD TURN's aggregate content, not from the
        fragment that just arrived: the merged turn is what gets dispatched,
        so it is what decides how much longer to keep collecting. A turn that
        is still media-only keeps the (longer) media window open, and closes
        down to the text window the moment any instruction text joins it.

        Reusing ``_text_window`` here unconditionally was the album bug: with
        the live ``text_window: 0``, the second photo of an album computed a
        zero window and flushed a text-less turn on the spot — which then got
        MEDIA_ONLY_NOTE appended ("ask the user what to do with it") while the
        caption arrived afterwards as a separate, orphaned turn. That is
        exactly the split-caption failure the coalescer exists to prevent.
        """
        has_media = any(getattr(e, "media_urls", None) for e in held.events)
        has_text = any(
            (getattr(e, "text", "") or "").strip() for e in held.events
        )
        return self._window_class(has_media, has_text)

    def _window_class(self, has_media: bool, has_text: bool) -> float:
        """Window for content with the given shape — one rule, two callers."""
        if has_media and not has_text:
            return self._media_window
        return self._text_window

    def _drop(self, key: str) -> None:
        held = self._held.pop(key, None)
        if held is not None:
            held.timer.cancel()

    def _flush_soon(self, key: str) -> None:
        task = asyncio.ensure_future(self._flush(key))
        task.add_done_callback(self._log_flush_failure)

    @staticmethod
    def _log_flush_failure(task: "asyncio.Future[None]") -> None:
        # Timer-driven flushes run detached from any caller (unlike the
        # immediate-dispatch paths in submit(), where a raised exception
        # propagates straight back to whoever called submit()). Without this,
        # a dispatch() failure on the window-expiry path — the common path
        # for coalesced turns — would be silently swallowed by asyncio.
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error(
                "inbound coalescer: dispatch failed during window-expiry flush",
                exc_info=exc,
            )

    async def _flush(self, key: str) -> None:
        held = self._held.pop(key, None)
        if held is None:
            return
        held.timer.cancel()
        await self._dispatch(self._merge(held.events))

    @staticmethod
    def _merge(events: List[Any]) -> Any:
        base = events[0]
        texts = [t for t in ((getattr(e, "text", "") or "").strip() for e in events) if t]
        for extra in events[1:]:
            base.media_urls.extend(getattr(extra, "media_urls", []) or [])
            base.media_types.extend(getattr(extra, "media_types", []) or [])
        if not texts and getattr(base, "media_urls", None):
            base.text = MEDIA_ONLY_NOTE
        else:
            base.text = "\n".join(texts)
        return base
