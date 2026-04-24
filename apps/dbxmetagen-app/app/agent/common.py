"""Shared utilities for dbxmetagen agents.

Consolidates duplicated helpers: SQL guardrails, LLM setup, ReAct graph
construction, message conversion, and profiling SQL fragments.
"""

import logging
import os
import re
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Annotated, Dict, List, Optional, TypedDict

from databricks_langchain import ChatDatabricks
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

from agent.guardrails import GuardrailConfig

logger = logging.getLogger(__name__)

CATALOG = os.environ.get("CATALOG_NAME", "")
SCHEMA = os.environ.get("SCHEMA_NAME", "")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "")
MODEL = os.environ.get("LLM_MODEL", "databricks-claude-sonnet-4-6")

_DML_KEYWORDS = {"INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"}


def fq(table: str) -> str:
    """Fully-qualify a table name with CATALOG.SCHEMA."""
    return f"{CATALOG}.{SCHEMA}.{table}"


# ---------------------------------------------------------------------------
# LLM singleton
# ---------------------------------------------------------------------------

_cached_llm = None


def get_llm() -> ChatDatabricks:
    global _cached_llm
    if _cached_llm is None:
        _cached_llm = ChatDatabricks(endpoint=MODEL, temperature=0, max_retries=6)
    return _cached_llm


# ---------------------------------------------------------------------------
# SQL guardrails
# ---------------------------------------------------------------------------

def check_select_only(query: str) -> Optional[str]:
    """Return an error string if the query is not a safe read-only statement, else None."""
    q_upper = query.strip().upper()
    if not q_upper.startswith(("SELECT", "DESCRIBE", "SHOW", "WITH")):
        return "Only SELECT, DESCRIBE, SHOW, and WITH (CTE) queries are allowed."
    for kw in _DML_KEYWORDS:
        if re.search(rf'\b{kw}\b', q_upper):
            return f"Blocked keyword: {kw}"
    return None


def check_table_allowlist(query: str, allowed_tables: set) -> Optional[str]:
    """Return an error string if the query references tables outside the allowed set."""
    q = query.lower()
    prefix = f"{CATALOG.lower()}.{SCHEMA.lower()}."
    for t in allowed_tables:
        q = q.replace(f"{prefix}{t}", t)
    refs = re.findall(r'\bfrom\s+(\w+)|\bjoin\s+(\w+)', q)
    for match in refs:
        name = match[0] or match[1]
        if name and name not in allowed_tables:
            return f"Table '{name}' is not in the allowed list: {', '.join(sorted(allowed_tables))}"
    return None


# ---------------------------------------------------------------------------
# ReAct graph builder
# ---------------------------------------------------------------------------

class ReactState(TypedDict):
    messages: Annotated[list, add_messages]


MAX_SAME_TOOL_CALLS = 2


def build_react_graph(
    tools: list,
    system_prompt: str,
    max_tool_rounds: int = GuardrailConfig.MAX_AGENT_ITERATIONS,
):
    """Build a generic ReAct agent graph with tool-round limiting.

    Uses dynamic tool rebinding: once a tool has been called MAX_SAME_TOOL_CALLS
    times, it is removed from the LLM's available tool list so it physically
    cannot be called again.
    """
    llm = get_llm()
    tools_by_name = {t.name: t for t in tools}
    tool_node = ToolNode(tools)

    def _count_tool_rounds(messages) -> int:
        return sum(1 for m in messages if hasattr(m, "tool_calls") and m.tool_calls)

    def _tool_call_counts(messages) -> dict:
        counts: dict = {}
        for m in messages:
            if hasattr(m, "tool_calls") and m.tool_calls:
                for tc in m.tool_calls:
                    counts[tc["name"]] = counts.get(tc["name"], 0) + 1
        return counts

    def agent_node(state):
        rounds = _count_tool_rounds(state["messages"])
        prompt = system_prompt
        budget_note = ""

        tool_counts = _tool_call_counts(state["messages"])
        overused = {n for n, c in tool_counts.items() if c >= MAX_SAME_TOOL_CALLS}
        if overused:
            budget_note += (
                f"\n\nYou have already called these tools {MAX_SAME_TOOL_CALLS}+ times: "
                f"{', '.join(overused)}. Do NOT call them again -- use what you have."
            )

        if rounds >= max_tool_rounds:
            prompt += (
                "\n\nIMPORTANT: You have reached the tool-call budget. "
                "Do NOT call any more tools. Synthesize your final answer NOW "
                "from the information you have gathered so far."
            )
            msgs = [SystemMessage(content=prompt + budget_note)] + state["messages"]
            return {"messages": [llm.invoke(msgs)]}

        available = [tools_by_name[n] for n in tools_by_name if n not in overused]
        if not available:
            msgs = [SystemMessage(content=prompt + "\n\nAll tools exhausted. Provide your final answer.")] + state["messages"]
            return {"messages": [llm.invoke(msgs)]}

        bound_llm = llm.bind_tools(available)
        msgs = [SystemMessage(content=prompt + budget_note)] + state["messages"]
        return {"messages": [bound_llm.invoke(msgs)]}

    def should_continue(state):
        last = state["messages"][-1]
        if hasattr(last, "tool_calls") and last.tool_calls:
            return "tools"
        return END

    graph = StateGraph(ReactState)
    graph.add_node("agent", agent_node)
    graph.add_node("tools", tool_node)
    graph.add_edge(START, "agent")
    graph.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})
    graph.add_edge("tools", "agent")
    return graph.compile()


# ---------------------------------------------------------------------------
# Message helpers
# ---------------------------------------------------------------------------

def history_to_messages(history: Optional[List[Dict[str, str]]], question: str) -> list:
    """Convert a list of {role, content} dicts + a new question into LangChain messages."""
    messages = []
    if history:
        for msg in history:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if role == "user":
                messages.append(HumanMessage(content=content))
            elif role == "assistant":
                messages.append(AIMessage(content=content))
    messages.append(HumanMessage(content=question))
    return messages


def extract_tool_calls(messages: list) -> List[str]:
    """Extract tool call names from a list of LangChain messages."""
    names = []
    for msg in messages:
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            names.extend(tc["name"] for tc in msg.tool_calls)
    return names


# ---------------------------------------------------------------------------
# Token usage extraction
# ---------------------------------------------------------------------------

def extract_token_usage(messages: list) -> Dict[str, int]:
    """Sum token usage across AIMessage instances in a message list."""
    usage = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    for msg in messages:
        if not isinstance(msg, AIMessage):
            continue
        if getattr(msg, "usage_metadata", None):
            u = msg.usage_metadata
            usage["input_tokens"] += (u.get("input_tokens", 0) if isinstance(u, dict) else getattr(u, "input_tokens", 0)) or 0
            usage["output_tokens"] += (u.get("output_tokens", 0) if isinstance(u, dict) else getattr(u, "output_tokens", 0)) or 0
            usage["total_tokens"] += (u.get("total_tokens", 0) if isinstance(u, dict) else getattr(u, "total_tokens", 0)) or 0
        elif getattr(msg, "response_metadata", None):
            rm = msg.response_metadata
            tu = rm.get("token_usage") or rm.get("usage") or {}
            if isinstance(tu, dict):
                usage["input_tokens"] += tu.get("prompt_tokens", 0) or tu.get("input_tokens", 0) or 0
                usage["output_tokens"] += tu.get("completion_tokens", 0) or tu.get("output_tokens", 0) or 0
                usage["total_tokens"] += tu.get("total_tokens", 0) or 0
    return usage


# ---------------------------------------------------------------------------
# SQL fragment: latest profiling snapshot join
# ---------------------------------------------------------------------------

def latest_profiling_join(stats_alias: str = "cs") -> str:
    """Return the INNER JOIN subquery that selects the latest profiling snapshot per table."""
    return f"""INNER JOIN (
    SELECT snapshot_id, table_name FROM (
        SELECT snapshot_id, table_name,
               ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY snapshot_time DESC) rn
        FROM {fq('profiling_snapshots')}
    ) WHERE rn = 1
) latest ON {stats_alias}.snapshot_id = latest.snapshot_id AND {stats_alias}.table_name = latest.table_name"""


# ---------------------------------------------------------------------------
# VS result cache (session-keyed)
# ---------------------------------------------------------------------------

_vs_cache: Dict[str, Dict] = {}
_vs_cache_lock = threading.Lock()
VS_CACHE_TTL = timedelta(minutes=10)
_cache_stats = {"hits": 0, "misses": 0}


VS_CACHE_MAX_ENTRIES = 1000


def vs_cache_get(session_id: str, intent_type: str = "new_question") -> Optional[str]:
    """Return cached VS result for a session on refinement/continuation, else None."""
    if intent_type not in ("refinement", "continuation"):
        return None
    with _vs_cache_lock:
        entry = _vs_cache.get(session_id)
        if entry and (time.time() - entry["timestamp"]) < VS_CACHE_TTL.total_seconds():
            _cache_stats["hits"] += 1
            logger.info("[vs_cache] HIT for session=%s", session_id[:12])
            return entry["result"]
        if entry:
            del _vs_cache[session_id]
        _cache_stats["misses"] += 1
        return None


def vs_cache_put(session_id: str, query: str, result: str):
    """Store a VS result in the session cache."""
    with _vs_cache_lock:
        _vs_cache[session_id] = {"query": query, "result": result, "timestamp": time.time()}
        cutoff = time.time() - VS_CACHE_TTL.total_seconds()
        stale = [k for k, v in _vs_cache.items() if v["timestamp"] < cutoff]
        for k in stale:
            del _vs_cache[k]
        # LRU eviction if over max entries
        if len(_vs_cache) > VS_CACHE_MAX_ENTRIES:
            oldest = sorted(_vs_cache, key=lambda k: _vs_cache[k]["timestamp"])
            for k in oldest[:len(_vs_cache) - VS_CACHE_MAX_ENTRIES]:
                del _vs_cache[k]


def vs_cache_stats() -> Dict[str, int]:
    return dict(_cache_stats)


# ---------------------------------------------------------------------------
# Plan cache -- avoids repeated _plan_retrieval LLM calls within a session
# ---------------------------------------------------------------------------

_plan_cache: dict[str, dict] = {}
_plan_cache_lock = threading.Lock()


def plan_cache_get(session_id: str, domain: str) -> Optional[dict]:
    """Return cached retrieval plan for (session, domain), or None."""
    if not session_id:
        return None
    key = f"{session_id}::{domain}"
    with _plan_cache_lock:
        entry = _plan_cache.get(key)
        if entry and (time.time() - entry["timestamp"]) < VS_CACHE_TTL.total_seconds():
            logger.info("[plan_cache] HIT session=%s domain=%s", session_id[:12], domain)
            return entry["plan"]
        return None


def plan_cache_put(session_id: str, domain: str, plan: dict):
    """Store a retrieval plan in the session cache."""
    if not session_id:
        return
    key = f"{session_id}::{domain}"
    with _plan_cache_lock:
        _plan_cache[key] = {"plan": plan, "timestamp": time.time()}
        cutoff = time.time() - VS_CACHE_TTL.total_seconds()
        stale = [k for k, v in _plan_cache.items() if v["timestamp"] < cutoff]
        for k in stale:
            del _plan_cache[k]


# ---------------------------------------------------------------------------
# Structured tool result type
# ---------------------------------------------------------------------------

@dataclass
class ToolResult:
    success: bool
    data: Optional[str]
    error_type: Optional[str] = None
    error_hint: Optional[str] = None
    elapsed_s: float = 0.0
    label: str = ""


@dataclass
class ConversationTurn:
    """Typed conversation state per turn. Scaffolding for future LangGraph checkpoint support."""
    turn_id: str
    query: str
    intent_type: str = "new_question"
    context_summary: Optional[str] = None
    domain: Optional[str] = None
    timestamp: str = ""


# ---------------------------------------------------------------------------
# Performance instrumentation
# ---------------------------------------------------------------------------

@contextmanager
def measure_phase(name: str, metrics: dict):
    """Context manager that records wall-clock time for a named phase."""
    t0 = time.time()
    try:
        yield
    finally:
        elapsed = round(time.time() - t0, 3)
        metrics[name] = elapsed
        logger.info("[perf] %s: %.3fs", name, elapsed)
