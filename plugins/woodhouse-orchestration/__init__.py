"""Woodhouse orchestration plugin: strict-tag intake + broker toolset.

Design refs (2026-09-21 Wave 2-3 orchestration plan):
  - D§4  plugin hook contract: ``pre_gateway_dispatch`` (declared
    ``hermes_cli/plugins.py``, fired ``gateway/run.py`` before auth/
    dispatch on every user-originated MessageEvent).
  - D§7.1 Woodhouse clarifies objectives before dispatch: this plugin only
    validates and normalizes strict-tag intake; the LLM turn that follows
    the rewrite decides when requirements are adequate and calls the
    broker tool deliberately.
  - D§7.4 validate tagged ingress; never rely on downstream auth for
    interception; never swallow safety commands.
  - D§10.1-2 tag grammar: ``[project]``, ``[project][workstream]``, optional
    trailing ``@claude|@codex|@agy`` (case-insensitive; alias resolution to
    a concrete agent identity is deferred to the broker).
  - D§10.3 safety commands starting with ``/`` always pass through
    untouched (``{"action": "allow"}``).

The broker socket (``~/.woodhouse/broker.sock``) is the only side-effect
channel this plugin uses — no shell, no file writes. Each tool call opens
one connection, sends one newline-terminated JSON request, and reads one
newline-terminated JSON response (Task 5 broker: one request per
connection).
"""

from __future__ import annotations

import json
import os
import re
import socket
from collections import namedtuple

from tools.registry import tool_error, tool_result

TagParse = namedtuple("TagParse", "project workstream agent_override body")

_TAG_RE = re.compile(
    r"^\s*\[(?P<project>[A-Za-z0-9_-]+)\]"
    r"(?:\[(?P<workstream>[A-Za-z0-9_-]+)\])?"
    r"\s*(?P<body>.*)$",
    re.DOTALL,
)
_AGENT_RE = re.compile(r"\s@(claude|codex|agy)\b", re.IGNORECASE)
_BROKER_SOCK = "~/.woodhouse/broker.sock"


def parse_tag(text):
    """Parse the strict-tag grammar off the first line of ``text``.

    Returns ``None`` for untagged text (ordinary personal conversation) and
    for anything starting with ``/`` (safety commands must never be parsed
    as a dispatch tag). Returns a :class:`TagParse` on a match; an unknown
    ``@agent`` suffix (anything other than claude/codex/agy) is left
    untouched in ``body`` rather than being consumed as an override.
    """
    if not text or text.lstrip().startswith("/"):
        return None
    m = _TAG_RE.match(text)
    if not m or not m.group("project"):
        return None
    body = m.group("body") or ""
    agent = ""
    am = _AGENT_RE.search(body)
    if am:
        agent = am.group(1).lower()
        body = _AGENT_RE.sub("", body, count=1)
    return TagParse(
        m.group("project").lower(),
        (m.group("workstream") or "").lower(),
        agent,
        body.strip(),
    )


def pre_gateway_dispatch(event=None, gateway=None, session_store=None, **_kw):
    """``pre_gateway_dispatch`` hook: normalize tagged intake, pass through
    everything else.

    - Untagged text -> ``{"action": "allow"}`` (personal conversation).
    - Safety commands (``/...``) -> ``{"action": "allow"}`` untouched.
    - Tagged text -> ``{"action": "rewrite", "text": <normalized>}`` so the
      LLM turn that follows sees an explicit ``WOODHOUSE-DISPATCH`` marker
      and calls the broker tool deliberately, rather than the plugin
      dispatching on the model's behalf.
    """
    text = getattr(event, "text", "") or ""
    if text.lstrip().startswith("/"):
        return {"action": "allow"}
    parsed = parse_tag(text)
    if parsed is None:
        return {"action": "allow"}
    normalized = (
        f"WOODHOUSE-DISPATCH project={parsed.project} "
        f"workstream={parsed.workstream or '-'} agent={parsed.agent_override or '-'}\n"
        f"{parsed.body}"
    )
    return {"action": "rewrite", "text": normalized}


def _broker_call(payload, timeout=10):
    """Send one JSON request to the Woodhouse broker and read one response.

    One connection per call (Task 5 broker contract) — never reused, never
    pooled. Raises ``OSError`` if the socket is unreachable and
    ``ValueError`` if the response isn't valid JSON; callers translate both
    into a ``{"ok": False, "error": ...}`` tool result rather than letting
    an exception reach the model.
    """
    path = os.path.expanduser(_BROKER_SOCK)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        s.connect(path)
        s.sendall((json.dumps(payload) + "\n").encode())
        buf = b""
        while not buf.endswith(b"\n"):
            chunk = s.recv(65536)
            if not chunk:
                break
            buf += chunk
    return json.loads(buf or b"{}")


def woodhouse_dispatch(project, objective, workstream="", agent_override=""):
    """Dispatch a task objective to the broker (``task.dispatch``)."""
    try:
        return _broker_call({
            "method": "task.dispatch",
            "params": {
                "project": project,
                "objective": objective,
                "workstream": workstream,
                "agent_override": agent_override,
                "platform": "gateway",
                "message_id": "",
            },
        })
    except (OSError, ValueError) as exc:
        return {"ok": False, "error": f"broker unreachable: {exc}"}


def woodhouse_task_status(execution_id):
    """Query the status of a dispatched execution (``task.status``)."""
    try:
        return _broker_call({
            "method": "task.status",
            "params": {"execution_id": execution_id},
        })
    except (OSError, ValueError) as exc:
        return {"ok": False, "error": f"broker unreachable: {exc}"}


def woodhouse_kanban_list(board):
    """List tasks on a Woodhouse kanban board (``kanban.list``).

    Controller addition beyond the original brief (Ruling 2): a third tool
    mirroring the error handling of ``woodhouse_dispatch`` /
    ``woodhouse_task_status`` exactly.
    """
    try:
        return _broker_call({
            "method": "kanban.list",
            "params": {"board": board},
        })
    except (OSError, ValueError) as exc:
        return {"ok": False, "error": f"broker unreachable: {exc}"}


# ── Tool registration ───────────────────────────────────────────────────
#
# Registered tool handlers must match the registry's calling convention
# (``tools/registry.py``: ``entry.handler(args, **kwargs)`` returning a
# JSON string built with ``tool_result``/``tool_error``) rather than the
# plain-keyword-argument signatures above. The plain functions
# (``woodhouse_dispatch`` etc.) stay directly unit-testable and are the
# thing the LLM-facing schema below maps onto; these ``_handle_*`` adapters
# are the thin registry-facing wrappers, mirroring the pattern in
# ``plugins/spotify/__init__.py`` and ``plugins/google_meet/__init__.py``.

WOODHOUSE_DISPATCH_SCHEMA = {
    "name": "woodhouse_dispatch",
    "description": (
        "Dispatch a task objective to the Woodhouse orchestration broker "
        "for execution by an agent."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "project": {"type": "string", "description": "Project slug the task belongs to."},
            "objective": {"type": "string", "description": "Objective / instructions for the dispatched task."},
            "workstream": {"type": "string", "description": "Optional workstream within the project."},
            "agent_override": {"type": "string", "description": "Optional explicit agent (claude, codex, agy)."},
        },
        "required": ["project", "objective"],
    },
}

WOODHOUSE_TASK_STATUS_SCHEMA = {
    "name": "woodhouse_task_status",
    "description": "Query the status of a previously dispatched Woodhouse execution.",
    "parameters": {
        "type": "object",
        "properties": {
            "execution_id": {"type": "string", "description": "Execution id returned by woodhouse_dispatch."},
        },
        "required": ["execution_id"],
    },
}

WOODHOUSE_KANBAN_LIST_SCHEMA = {
    "name": "woodhouse_kanban_list",
    "description": "List tasks on a Woodhouse kanban board via the broker.",
    "parameters": {
        "type": "object",
        "properties": {
            "board": {"type": "string", "description": "Kanban board name."},
        },
        "required": ["board"],
    },
}


def _handle_woodhouse_dispatch(args: dict, **_kw) -> str:
    project = str(args.get("project") or "").strip()
    objective = str(args.get("objective") or "").strip()
    if not project or not objective:
        return tool_error("project and objective are required")
    result = woodhouse_dispatch(
        project=project,
        objective=objective,
        workstream=str(args.get("workstream") or ""),
        agent_override=str(args.get("agent_override") or ""),
    )
    return tool_result(result)


def _handle_woodhouse_task_status(args: dict, **_kw) -> str:
    execution_id = str(args.get("execution_id") or "").strip()
    if not execution_id:
        return tool_error("execution_id is required")
    return tool_result(woodhouse_task_status(execution_id))


def _handle_woodhouse_kanban_list(args: dict, **_kw) -> str:
    board = str(args.get("board") or "").strip()
    if not board:
        return tool_error("board is required")
    return tool_result(woodhouse_kanban_list(board))


_TOOLS = (
    ("woodhouse_dispatch", WOODHOUSE_DISPATCH_SCHEMA, _handle_woodhouse_dispatch, "🛰️"),
    ("woodhouse_task_status", WOODHOUSE_TASK_STATUS_SCHEMA, _handle_woodhouse_task_status, "📋"),
    ("woodhouse_kanban_list", WOODHOUSE_KANBAN_LIST_SCHEMA, _handle_woodhouse_kanban_list, "🗂️"),
)


def register(ctx) -> None:
    """Register the pre_gateway_dispatch hook and the woodhouse toolset.

    Called once by the plugin loader when the plugin is enabled via
    ``plugins.enabled`` in config.yaml (mirrors
    ``plugins/google_meet/__init__.py::register`` and
    ``~/.hermes/plugins/herdr-agent-state/__init__.py::register``). Not
    installed/enabled anywhere by this task — Task 10 does the cutover.
    """
    ctx.register_hook("pre_gateway_dispatch", pre_gateway_dispatch)
    for name, schema, handler, emoji in _TOOLS:
        ctx.register_tool(
            name=name,
            toolset="woodhouse",
            schema=schema,
            handler=handler,
            description=schema["description"],
            emoji=emoji,
        )
