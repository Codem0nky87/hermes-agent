"""Woodhouse plugin: strict-tag parse, hook actions, broker tool calls."""
import importlib.util
import json
from pathlib import Path

PLUGIN = Path(__file__).resolve().parents[2] / "plugins/woodhouse-orchestration/__init__.py"
spec = importlib.util.spec_from_file_location("woodhouse_orchestration", PLUGIN)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def test_untagged_is_allowed_conversation():
    assert mod.parse_tag("hey how is my day looking") is None


def test_tag_grammar_variants():
    p = mod.parse_tag("[msam] Fix the reconnect race.")
    assert p.project == "msam" and p.workstream == "" and p.agent_override == ""
    p = mod.parse_tag("[MSAM][gateway] Reconcile supervision. @claude")
    assert (p.project, p.workstream, p.agent_override) == ("msam", "gateway", "claude")
    assert "Reconcile supervision." in p.body


def test_unknown_agent_suffix_not_parsed_as_override():
    p = mod.parse_tag("[msam] do it @cursor")
    assert p.agent_override == "" and "@cursor" in p.body


def test_hook_rewrites_tagged_and_allows_untagged():
    class Ev:
        text = "[msam] Fix it."
    out = mod.pre_gateway_dispatch(event=Ev(), gateway=None, session_store=None)
    assert out["action"] == "rewrite"
    assert out["text"].startswith("WOODHOUSE-DISPATCH project=msam")

    class Ev2:
        text = "just chatting"
    assert mod.pre_gateway_dispatch(event=Ev2(), gateway=None, session_store=None)["action"] == "allow"

    class Ev3:
        text = "/stop"
    assert mod.pre_gateway_dispatch(event=Ev3(), gateway=None, session_store=None)["action"] == "allow"


def test_broker_call_shapes_request(tmp_path, monkeypatch):
    captured = {}

    def fake_call(payload, timeout=10):
        captured.update(payload)
        return {"ok": True, "task_id": "t_1", "execution_id": "e1"}

    monkeypatch.setattr(mod, "_broker_call", fake_call)
    out = mod.woodhouse_dispatch(project="msam", objective="obj")
    assert out["ok"] is True
    assert captured == {"method": "task.dispatch",
                        "params": {"project": "msam", "objective": "obj",
                                   "workstream": "", "agent_override": "",
                                   "platform": "gateway", "message_id": ""}}


def test_task_status_shapes_request(monkeypatch):
    captured = {}

    def fake_call(payload, timeout=10):
        captured.update(payload)
        return {"ok": True, "status": "running"}

    monkeypatch.setattr(mod, "_broker_call", fake_call)
    out = mod.woodhouse_task_status(execution_id="e1")
    assert out["ok"] is True
    assert captured == {"method": "task.status", "params": {"execution_id": "e1"}}


def test_kanban_list_shapes_request(monkeypatch):
    """Controller Ruling 2: third tool, same shape/error-handling as the others."""
    captured = {}

    def fake_call(payload, timeout=10):
        captured.update(payload)
        return {"ok": True, "tasks": []}

    monkeypatch.setattr(mod, "_broker_call", fake_call)
    out = mod.woodhouse_kanban_list(board="orch-wave23")
    assert out["ok"] is True
    assert captured == {"method": "kanban.list", "params": {"board": "orch-wave23"}}


def test_fetch_codes_shapes_request(monkeypatch):
    """Wave-4 Task 6: the 4th tool delegates to the bounded email-read tool
    (agent.email_read.fetch_codes) — not the broker — with the provider and
    since_minutes, and passes its dict through unchanged."""
    captured = {}

    def fake_fetch(provider, since_minutes=10):
        captured["provider"] = provider
        captured["since_minutes"] = since_minutes
        return {"ok": True, "codes": [{"code_or_link": "482913",
                                       "received_at": "2026-09-22T07:00:00Z",
                                       "matched": "sign.?in"}], "error": None}

    monkeypatch.setattr(mod.email_read, "fetch_codes", fake_fetch)
    out = mod.woodhouse_fetch_codes("codex", since_minutes=5)
    assert out["ok"] is True and out["codes"][0]["code_or_link"] == "482913"
    assert captured == {"provider": "codex", "since_minutes": 5}


def test_fetch_codes_errors_are_translated_not_raised(monkeypatch):
    """Parity with the broker tools: an exception from the email tool becomes
    an {"ok": False, ...} dict rather than reaching the model."""
    def boom(provider, since_minutes=10):
        raise RuntimeError("graph down")

    monkeypatch.setattr(mod.email_read, "fetch_codes", boom)
    out = mod.woodhouse_fetch_codes("codex")
    assert out["ok"] is False and "email tool error" in out["error"]


def test_fetch_codes_handler_returns_json_and_requires_provider(monkeypatch):
    def fake_fetch(provider, since_minutes=10):
        return {"ok": True, "codes": [], "error": None}

    monkeypatch.setattr(mod.email_read, "fetch_codes", fake_fetch)
    out = json.loads(mod._handle_woodhouse_fetch_codes(
        {"provider": "claude", "since_minutes": 3}))
    assert out["ok"] is True
    missing = json.loads(mod._handle_woodhouse_fetch_codes({}))
    assert "error" in missing


def test_broker_call_errors_are_translated_not_raised(monkeypatch):
    """Every broker tool function must swallow OSError/ValueError from the
    socket layer into an {"ok": False, "error": ...} dict rather than
    letting an exception reach the model."""
    def boom(payload, timeout=10):
        raise OSError("no such file or directory")

    monkeypatch.setattr(mod, "_broker_call", boom)
    for fn, kwargs in (
        (mod.woodhouse_dispatch, {"project": "msam", "objective": "obj"}),
        (mod.woodhouse_task_status, {"execution_id": "e1"}),
        (mod.woodhouse_kanban_list, {"board": "orch-wave23"}),
    ):
        out = fn(**kwargs)
        assert out["ok"] is False
        assert "broker unreachable" in out["error"]


def test_register_wires_hook_and_toolset():
    """register(ctx) mirrors plugins/google_meet/__init__.py's test pattern:
    a fake ctx records what got registered, no real plugin manager needed."""
    calls = {"tools": [], "hooks": []}

    class _Ctx:
        def register_tool(self, **kw):
            calls["tools"].append(kw)

        def register_hook(self, name, fn):
            calls["hooks"].append(name)

    mod.register(_Ctx())

    assert calls["hooks"] == ["pre_gateway_dispatch"]
    tool_names = [kw["name"] for kw in calls["tools"]]
    assert tool_names == ["woodhouse_dispatch", "woodhouse_task_status",
                          "woodhouse_kanban_list", "woodhouse_fetch_codes"]
    for kw in calls["tools"]:
        assert kw["toolset"] == "woodhouse"
        assert callable(kw["handler"])
        assert kw["schema"]["name"] == kw["name"]


def test_registered_handlers_return_json_tool_results(monkeypatch):
    """Registered handlers use the registry calling convention
    (args: dict, **kwargs) -> JSON str, not the plain-kwarg functions
    directly — verify the adapter wiring end to end."""
    def fake_call(payload, timeout=10):
        return {"ok": True, "echo": payload}

    monkeypatch.setattr(mod, "_broker_call", fake_call)

    out = mod._handle_woodhouse_dispatch({"project": "msam", "objective": "obj"})
    parsed = json.loads(out)
    assert parsed["ok"] is True
    assert parsed["echo"]["method"] == "task.dispatch"

    missing = json.loads(mod._handle_woodhouse_dispatch({"project": "msam"}))
    assert "error" in missing

    out2 = mod._handle_woodhouse_kanban_list({"board": "orch-wave23"})
    assert json.loads(out2)["echo"]["params"] == {"board": "orch-wave23"}
