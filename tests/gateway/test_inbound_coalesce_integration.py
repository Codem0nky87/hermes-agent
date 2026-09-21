"""Coalescer integration: gateway config controls the window, key is the
chat/session identity, non-listed platforms bypass entirely."""
import asyncio
from types import SimpleNamespace

import pytest

from gateway.config import GatewayConfig, Platform
from gateway.inbound_coalesce import InboundCoalescer
from gateway.platforms.base import MessageEvent


@pytest.mark.asyncio
async def test_gateway_builds_coalescer_from_config(monkeypatch):
    from gateway import run as run_mod

    cfg = {"gateway": {"inbound_coalesce": {
        "text_window": 0.05, "media_window": 0.1, "max": 0.2,
        "platforms": ["whatsapp"],
    }}}
    coalescer = run_mod._build_inbound_coalescer(cfg, dispatch=None)
    assert isinstance(coalescer, InboundCoalescer)
    assert coalescer._text_window == 0.05
    assert coalescer._media_window == 0.1


def test_platform_gate():
    from gateway import run as run_mod

    cfg = {"gateway": {"inbound_coalesce": {"platforms": ["whatsapp"]}}}
    assert run_mod._coalesce_applies(cfg, "whatsapp") is True
    assert run_mod._coalesce_applies(cfg, "discord") is False
    assert run_mod._coalesce_applies({}, "whatsapp") is True  # default on


# --- dispatch-path wiring -------------------------------------------------

_FAST_CFG = {"gateway": {"inbound_coalesce": {
    "text_window": 0.05, "media_window": 0.05, "max": 0.2,
    "platforms": ["whatsapp"],
}}}


def _event(text="", platform=Platform.WHATSAPP, media=None):
    event = MessageEvent(text=text)
    event.source = SimpleNamespace(platform=platform, chat_id="c1", profile=None)
    if media:
        event.media_urls = list(media)
        event.media_types = ["image/jpeg"] * len(media)
    return event


def _runner(monkeypatch, handled, injected, *, raises=None, adapter=True):
    from gateway import run as run_mod
    from gateway.run import GatewayRunner

    monkeypatch.setattr(run_mod, "_load_gateway_runtime_config", lambda: _FAST_CFG)

    class _Adapter:
        async def handle_message(self, event):
            injected.append(event)

    runner = GatewayRunner.__new__(GatewayRunner)
    runner.config = GatewayConfig()

    async def _handle_message(event):
        handled.append(event)
        if raises is not None:
            raise raises
        return "reply"

    _adapter = _Adapter() if adapter else None
    runner._handle_message = _handle_message  # type: ignore[method-assign]
    runner._session_key_for_source = lambda source: "wa:c1"  # type: ignore[method-assign]
    runner._adapter_for_source = lambda source: _adapter  # type: ignore[method-assign]
    return runner


@pytest.mark.asyncio
async def test_burst_is_held_then_reinjected_through_adapter_ingress(monkeypatch):
    """Fragments do not reach the handler; the merged turn re-enters ingress."""
    handled, injected = [], []
    handler = _runner(monkeypatch, handled, injected)._primary_message_handler()

    assert await handler(_event(media=["/tmp/a.jpg"])) is None
    assert await handler(_event(text="resize this")) is None
    assert handled == []  # still inside the window

    await asyncio.sleep(0.25)
    assert len(injected) == 1
    assert injected[0].media_urls == ["/tmp/a.jpg"]
    assert "resize this" in injected[0].text

    # The adapter now calls the same handler back; that pass must run the
    # real handler so its reply is delivered on the normal path.
    assert await handler(injected[0]) == "reply"
    assert handled == [injected[0]]


@pytest.mark.asyncio
async def test_slash_command_runs_inline_and_returns_its_response(monkeypatch):
    """Commands (e.g. the bridge /attach protocol) keep the original path."""
    handled, injected = [], []
    handler = _runner(monkeypatch, handled, injected)._primary_message_handler()

    assert await handler(_event(text="/attach 3")) == "reply"
    assert len(handled) == 1
    assert injected == []


@pytest.mark.asyncio
async def test_unlisted_platform_bypasses_the_coalescer(monkeypatch):
    handled, injected = [], []
    handler = _runner(monkeypatch, handled, injected)._primary_message_handler()

    assert await handler(_event(text="hi", platform=Platform.DISCORD)) == "reply"
    assert len(handled) == 1
    assert injected == []


# --- fix round 1 ----------------------------------------------------------


@pytest.mark.asyncio
async def test_failing_inline_command_handler_is_not_re_dispatched(monkeypatch, caplog):
    """A raising slash-command handler must run exactly once.

    The inline dispatch happens *inside* ``submit()``, so a handler failure
    surfaces as an exception out of the submit call. Retrying there would
    repeat side effects like /new or /approve.
    """
    import logging

    handled, injected = [], []
    handler = _runner(
        monkeypatch, handled, injected, raises=RuntimeError("boom")
    )._primary_message_handler()

    with caplog.at_level(logging.ERROR, logger="gateway.run"):
        with pytest.raises(RuntimeError, match="boom"):
            await handler(_event(text="/new"))

    assert len(handled) == 1  # ran once, not twice
    assert injected == []
    assert any(
        "after the message was handed over" in r.message and r.levelno == logging.ERROR
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_failure_before_handover_still_dispatches_directly(monkeypatch, caplog):
    """A failure with the message still un-handed-over falls back safely."""
    import logging

    handled, injected = [], []
    runner = _runner(monkeypatch, handled, injected)

    def _boom(source):
        raise RuntimeError("no session key")

    runner._session_key_for_source = _boom  # type: ignore[method-assign]
    handler = runner._primary_message_handler()

    with caplog.at_level(logging.ERROR, logger="gateway.run"):
        assert await handler(_event(text="hello")) == "reply"

    assert len(handled) == 1  # dispatched directly, exactly once
    assert any(
        "before the message was handed over" in r.message and r.levelno == logging.ERROR
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_missing_adapter_logs_error_and_still_processes(monkeypatch, caplog):
    """No adapter ⇒ the reply cannot be delivered; say so, but still process."""
    import logging

    handled, injected = [], []
    handler = _runner(
        monkeypatch, handled, injected, adapter=False
    )._primary_message_handler()

    assert await handler(_event(media=["/tmp/a.jpg"])) is None
    with caplog.at_level(logging.ERROR, logger="gateway.run"):
        await asyncio.sleep(0.25)

    assert injected == []
    assert len(handled) == 1  # work not dropped
    assert any(
        "reply cannot be delivered" in r.message and r.levelno == logging.ERROR
        for r in caplog.records
    )
