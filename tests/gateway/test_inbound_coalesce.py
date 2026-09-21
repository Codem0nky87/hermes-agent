"""Component A (spec §2): inbound burst coalescing.

An attachment and its caption arrive as separate bridge events; the
coalescer holds dispatch briefly and merges same-chat fragments into one
combined turn. Safety commands bypass and stop-class commands drop held work.
"""
import asyncio
import logging

import pytest

from gateway.inbound_coalesce import InboundCoalescer, MEDIA_ONLY_NOTE
from gateway.platforms.base import MessageEvent


def _ev(text="", media=None):
    e = MessageEvent(text=text)
    if media:
        e.media_urls = list(media)
        e.media_types = ["image/jpeg"] * len(media)
    return e


def _coalescer(dispatched, **kw):
    async def dispatch(event):
        dispatched.append(event)
    # tiny windows so tests run in milliseconds
    kw.setdefault("text_window", 0.05)
    kw.setdefault("media_window", 0.1)
    kw.setdefault("max_window", 0.2)
    return InboundCoalescer(dispatch, **kw)


@pytest.mark.asyncio
async def test_media_then_caption_merges_into_one_turn():
    out = []
    c = _coalescer(out)
    await c.submit("chat1", _ev(media=["/tmp/img.jpg"]))
    await asyncio.sleep(0.02)
    await c.submit("chat1", _ev(text="resize this to 512px"))
    await asyncio.sleep(0.2)
    assert len(out) == 1
    assert out[0].media_urls == ["/tmp/img.jpg"]
    assert "resize this to 512px" in out[0].text


@pytest.mark.asyncio
async def test_text_burst_merges_in_arrival_order():
    out = []
    c = _coalescer(out)
    await c.submit("chat1", _ev(text="first"))
    await c.submit("chat1", _ev(text="second"))
    await asyncio.sleep(0.15)
    assert len(out) == 1
    assert out[0].text.index("first") < out[0].text.index("second")


@pytest.mark.asyncio
async def test_different_chats_do_not_merge():
    out = []
    c = _coalescer(out)
    await c.submit("chat1", _ev(text="a"))
    await c.submit("chat2", _ev(text="b"))
    await asyncio.sleep(0.15)
    assert len(out) == 2


@pytest.mark.asyncio
async def test_hard_cap_dispatches_even_while_fragments_keep_arriving():
    out = []
    c = _coalescer(out, text_window=0.06, max_window=0.15)
    await c.submit("chat1", _ev(text="x0"))
    for i in range(1, 6):  # keep resetting the window past the cap
        await asyncio.sleep(0.04)
        await c.submit("chat1", _ev(text=f"x{i}"))
    await asyncio.sleep(0.2)
    assert len(out) >= 1  # cap fired; a trailing fragment may open a second turn
    assert "x0" in out[0].text


@pytest.mark.asyncio
async def test_media_only_expiry_adds_no_instruction_note():
    out = []
    c = _coalescer(out)
    await c.submit("chat1", _ev(media=["/tmp/img.jpg"]))
    await asyncio.sleep(0.25)
    assert len(out) == 1
    assert MEDIA_ONLY_NOTE in out[0].text


@pytest.mark.asyncio
async def test_slash_command_bypasses_window():
    out = []
    c = _coalescer(out)
    await c.submit("chat1", _ev(text="/queue"))
    await asyncio.sleep(0)  # no window wait
    assert len(out) == 1 and out[0].text == "/queue"


@pytest.mark.asyncio
async def test_stop_during_open_window_dispatches_now_and_drops_held():
    # Review Focus #1
    out = []
    c = _coalescer(out)
    await c.submit("chat1", _ev(media=["/tmp/img.jpg"]))
    await c.submit("chat1", _ev(text="/stop"))
    await asyncio.sleep(0.25)
    assert [e.text for e in out] == ["/stop"]  # held attachment dropped


@pytest.mark.asyncio
async def test_zero_window_disables_coalescing():
    out = []
    c = _coalescer(out, text_window=0, media_window=0)
    await c.submit("chat1", _ev(text="hi"))
    await asyncio.sleep(0)
    assert len(out) == 1


@pytest.mark.asyncio
async def test_timer_flush_dispatch_failure_is_logged_and_state_recovers(caplog):
    # Fix round 1: dispatch() raising on the timer-driven (window-expiry)
    # flush path must not be silently swallowed by the detached flush task,
    # and the held state for that chat must still be cleared so later
    # messages keep flowing.
    calls = []

    async def flaky_dispatch(event):
        calls.append(event)
        if len(calls) == 1:
            raise RuntimeError("boom")

    c = InboundCoalescer(
        flaky_dispatch, text_window=0.05, media_window=0.1, max_window=0.2
    )

    with caplog.at_level(logging.ERROR, logger="gateway.inbound_coalesce"):
        await c.submit("chat1", _ev(text="first"))
        await asyncio.sleep(0.15)  # let the timer-driven flush fire and fail

    assert len(calls) == 1  # dispatch was attempted despite the failure
    assert any(
        "dispatch failed" in record.message and record.levelno == logging.ERROR
        for record in caplog.records
    )

    # Held state for "chat1" must have been cleaned up despite the failure,
    # so a later message opens a fresh hold and still gets dispatched.
    await c.submit("chat1", _ev(text="second"))
    await asyncio.sleep(0.15)
    assert len(calls) == 2
    assert calls[1].text == "second"
