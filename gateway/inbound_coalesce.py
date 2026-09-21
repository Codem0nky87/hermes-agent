"""Inbound burst coalescing (amendment spec 2026-09-21 §2).

Holds a just-arrived inbound message briefly; further messages from the
same chat inside the window merge into one combined turn. A bare
attachment whose window expires with no follow-up gets MEDIA_ONLY_NOTE
appended so the agent asks instead of guessing. Slash commands bypass;
stop-class commands also drop held work for that chat.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable, Dict, List

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
        if self._disabled():
            await self._dispatch(event)
            return

        loop = asyncio.get_running_loop()
        held = self._held.get(key)
        if held is None:
            window = self._window_for(event)
            timer = loop.call_later(window, self._flush_soon, key)
            self._held[key] = _Held(event, timer)
            return

        held.events.append(event)
        held.timer.cancel()
        elapsed = time.monotonic() - held.first_at
        remaining_cap = self._max_window - elapsed
        window = min(self._text_window, max(remaining_cap, 0.0))
        if window <= 0.0:
            await self._flush(key)
        else:
            held.timer = loop.call_later(window, self._flush_soon, key)

    # -- internals --------------------------------------------------------
    def _disabled(self) -> bool:
        return self._text_window <= 0 and self._media_window <= 0

    def _window_for(self, event: Any) -> float:
        has_media = bool(getattr(event, "media_urls", None))
        has_text = bool((getattr(event, "text", "") or "").strip())
        if has_media and not has_text:
            return self._media_window
        return self._text_window

    def _drop(self, key: str) -> None:
        held = self._held.pop(key, None)
        if held is not None:
            held.timer.cancel()

    def _flush_soon(self, key: str) -> None:
        asyncio.ensure_future(self._flush(key))

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
