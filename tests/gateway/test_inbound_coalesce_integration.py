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


def _runner(monkeypatch, handled, injected):
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
        return "reply"

    runner._handle_message = _handle_message  # type: ignore[method-assign]
    runner._session_key_for_source = lambda source: "wa:c1"  # type: ignore[method-assign]
    runner._adapter_for_source = lambda source: _Adapter()  # type: ignore[method-assign]
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
