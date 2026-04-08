"""LangGraph-based deep analysis agent for GraphRAG and Baseline modes.

Replaces the plain function pipeline in deep_analysis.py with a proper
LangGraph StateGraph. Benefits:
  - Automatic MLflow tracing via langchain.autolog (every node + LLM call)
  - Token-level streaming via astream_events + llm.astream()
  - Fine-grained progress events via adispatch_custom_event
  - Structured state management

The retrieval logic is reused from deep_analysis.py helpers -- same tools,
same prompts, same timeouts, same parallelism.
"""

import asyncio
import json
import logging
import time
from contextlib import nullcontext
from functools import partial
from typing import Optional, TypedDict

from databricks_langchain import ChatDatabricks
from langchain_core.callbacks import adispatch_custom_event
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph

from agent.common import ToolResult, vs_cache_get, vs_cache_put
from agent.guardrails import EvidenceBudget, sanitize_output
from agent.intent import classify_and_contextualize
from agent.metadata_tools import (
    CATALOG, SCHEMA,
    execute_baseline_sql, execute_metadata_sql,
    expand_vs_hits, search_metadata, traverse_graph,
)
from agent.deep_analysis import (
    ANALYSIS_PROMPT, BASELINE_ANALYSIS_PROMPT,
    MODEL, TOOL_TIMEOUT,
    _build_baseline_queries, _build_relevance_sql,
    _discover_ontology_edge_types, _extract_keywords, _extract_node_ids,
    _extract_table_names, _merge_graph_data,
    _parse_json, _pick_traversal_nodes, _plan_retrieval,
    _safe_tool_call, _sparql_ontology_query, _sql_escape,
    _truncate_part,
)
from agent.tracing import _init_tracing, get_mlflow

logger = logging.getLogger(__name__)

MAX_HISTORY_TURNS = 10


async def _safe_progress(data: dict) -> None:
    """Dispatch a progress event, silently swallowing errors.

    adispatch_custom_event requires a LangChain RunnableConfig in the current
    contextvars context. If the context is missing (e.g., nested async call
    without proper propagation), we log and move on rather than crashing the node.
    """
    try:
        await adispatch_custom_event("progress", data)
    except Exception:
        logger.debug("[graph] progress event dispatch failed (non-critical): %s", data.get("message", ""))

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class DeepAnalysisState(TypedDict, total=False):
    query: str
    history: list[dict]
    session_id: str
    mode: str
    # classify_intent
    intent_type: str
    effective_query: str
    meta_answer: str
    # gather_evidence
    evidence_parts: list[str]
    graph_data: dict
    tool_calls: list[str]
    failed_sources: list[str]
    timing: dict
    # assemble_context
    context: str
    # analyze
    answer: str


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

def classify_intent(state: DeepAnalysisState) -> dict:
    """Classify intent and produce effective query. Sync -- auto-traced by autolog."""
    result = classify_and_contextualize(state["query"], state.get("history"))
    logger.info("[graph] intent=%s domain=%s", result.intent_type, result.domain)

    if result.intent_type in ("irrelevant", "meta"):
        return {
            "intent_type": result.intent_type,
            "effective_query": result.context_summary,
            "meta_answer": result.meta_answer or "I can help you analyze your data catalog metadata.",
        }
    return {
        "intent_type": result.intent_type,
        "effective_query": result.context_summary,
        "meta_answer": "",
    }


def quick_answer(state: DeepAnalysisState) -> dict:
    """Return the meta/irrelevant answer directly."""
    raw = state.get("meta_answer", "")
    # Guard: if the intent LLM leaked JSON instead of prose, extract the text field
    if raw.lstrip().startswith("{"):
        try:
            obj = json.loads(raw)
            raw = obj.get("meta_answer") or obj.get("context_summary") or raw
        except (json.JSONDecodeError, TypeError):
            pass
    return {
        "answer": raw,
        "tool_calls": [],
        "graph_data": {},
        "timing": {},
        "evidence_parts": [],
        "failed_sources": [],
        "context": "",
    }


def _should_continue(state: DeepAnalysisState) -> str:
    if state.get("intent_type") in ("irrelevant", "meta"):
        return "quick_answer"
    return "gather_evidence"


# -- Gather: GraphRAG ---

async def _gather_graphrag(query: str, session_id: Optional[str], intent_type: str,
                           parent_span_id: str = None, trace_request_id: str = None):
    """Async GraphRAG gathering with progress events and asyncio parallelism."""
    loop = asyncio.get_running_loop()
    parts: list[str] = []
    tools_used: list[str] = []
    graph_data: dict = {}
    failed: list[ToolResult] = []
    timing: dict = {}
    B = EvidenceBudget

    _tool = (partial(_safe_tool_call, parent_span_id=parent_span_id, trace_request_id=trace_request_id)
             if parent_span_id else _safe_tool_call)

    def collect(tr: ToolResult, source_label: str, limit: int):
        if tr.success and tr.data:
            tools_used.append(tr.label.split("(")[0].strip())
            parts.append(f"### Source: {source_label}\n{_truncate_part(tr.data, limit)}")
        elif not tr.success:
            failed.append(tr)

    # Step 0: discover ontology edge types (cached, fast)
    ontology_ets = await loop.run_in_executor(None, _discover_ontology_edge_types)

    # Step 1: retrieval planner
    await _safe_progress({"stage": "gathering", "message": "Step 1/7: Planning retrieval strategy..."})
    t0 = time.time()
    cancel = _make_cancel()
    plan = await loop.run_in_executor(None, _plan_retrieval, query, cancel, ontology_ets)
    timing["plan_retrieval"] = round(time.time() - t0, 3)

    planned_edge_types = plan["edge_types"]
    planned_traverse_et = plan["traverse_edge_type"]
    planned_direction = plan["direction"]

    # Step 2: vector search
    await _safe_progress({"stage": "gathering", "message": "Step 2/7: Searching knowledge base..."})
    t0 = time.time()
    vs_tr = None
    if session_id:
        cached = vs_cache_get(session_id, intent_type=intent_type)
        if cached:
            vs_tr = ToolResult(success=True, data=cached, label="search_metadata (cached)")
    if vs_tr is None:
        vs_tr = await loop.run_in_executor(
            None, _tool, search_metadata, {"query": query}, TOOL_TIMEOUT, "search_metadata", 2, 7,
        )
        if vs_tr.success and vs_tr.data and session_id:
            vs_cache_put(session_id, query, vs_tr.data)
    collect(vs_tr, "search_metadata (vector search)", B.VS_RESULTS)
    timing["vector_search"] = round(time.time() - t0, 3)

    node_ids = _extract_node_ids(vs_tr.data if vs_tr.success else None)
    table_names = _extract_table_names(vs_tr.data if vs_tr.success else None)

    # Fallback: keyword SQL if VS empty
    if not node_ids and not table_names:
        keywords = _extract_keywords(query)
        if keywords:
            like_clauses = " OR ".join(
                f"(LOWER(table_name) LIKE '%{_sql_escape(k)}%' OR LOWER(comment) LIKE '%{_sql_escape(k)}%')"
                for k in keywords
            )
            fb_sql = (
                f"SELECT table_name, domain, subdomain, comment, has_pii, row_count "
                f"FROM {CATALOG}.{SCHEMA}.table_knowledge_base WHERE {like_clauses} LIMIT 20"
            )
            fb_tr = await loop.run_in_executor(
                None, _tool, execute_metadata_sql, {"query": fb_sql}, TOOL_TIMEOUT,
                "execute_metadata_sql (keyword fallback)", 2, 7,
            )
            if fb_tr.success and fb_tr.data:
                collect(fb_tr, "execute_metadata_sql (keyword fallback)", B.VS_RESULTS)
                fb_data = _parse_json(fb_tr.data)
                if fb_data and fb_data.get("rows"):
                    table_names = [r["table_name"] for r in fb_data["rows"] if r.get("table_name")]
                    node_ids = table_names[:]

    traverse_et_list = [planned_traverse_et] + [
        et for et in planned_edge_types if et != planned_traverse_et
    ]

    # Steps 3-5: parallel graph expansion + traversal
    await _safe_progress({"stage": "gathering", "message": "Steps 3-5/7: Expanding graph connections..."})
    t0 = time.time()
    if node_ids:
        trav_nodes = _pick_traversal_nodes(node_ids, table_names)
        expand_coro = loop.run_in_executor(
            None, _tool, expand_vs_hits,
            {"node_ids": node_ids[:5], "edge_types": planned_edge_types},
            TOOL_TIMEOUT, "expand_vs_hits", 3, 7,
        )
        trav_coros = [
            loop.run_in_executor(
                None, _tool, traverse_graph,
                {"start_node": nid, "max_hops": 3, "edge_types": traverse_et_list, "direction": planned_direction},
                TOOL_TIMEOUT, f"traverse_graph({nid.split('.')[-1]})", 4 + i, 7,
            )
            for i, nid in enumerate(trav_nodes[:3])
        ]
        results = await asyncio.gather(expand_coro, *trav_coros, return_exceptions=True)

        exp_tr = results[0] if not isinstance(results[0], BaseException) else ToolResult(success=False, data=None, error_type="exception", error_hint=str(results[0]), label="expand_vs_hits")
        collect(exp_tr, "expand_vs_hits (graph expansion)", B.GRAPH_EXPANSION)

        for r in results[1:]:
            if isinstance(r, BaseException):
                continue
            trav_tr = r
            if trav_tr.success and trav_tr.data:
                trav_str = trav_tr.data if isinstance(trav_tr.data, str) else json.dumps(trav_tr.data)
                tools_used.append("traverse_graph")
                parts.append(f"### Source: traverse_graph\n{_truncate_part(trav_str, B.GRAPH_TRAVERSAL)}")
                chunk = _parse_json(trav_str)
                if chunk and isinstance(chunk, dict):
                    graph_data = _merge_graph_data(graph_data, chunk)
            elif not trav_tr.success:
                failed.append(trav_tr)
    timing["graph_expand_traverse"] = round(time.time() - t0, 3)

    # SPARQL ontology
    if table_names:
        try:
            sparql_result = await loop.run_in_executor(None, _sparql_ontology_query, table_names)
            if sparql_result:
                tools_used.append("sparql_ontology_query")
                parts.append(f"### Source: sparql_ontology_query (formal RDF ontology)\n{_truncate_part(sparql_result, B.GRAPH_TRAVERSAL)}")
        except Exception as e:
            logger.debug("SPARQL ontology query failed (non-critical): %s", e)

    # Steps 6-7: parallel FK + structured retrieval
    await _safe_progress({"stage": "gathering", "message": "Steps 6-7/7: Structured data retrieval..."})
    t0 = time.time()
    if table_names:
        fk_coro = loop.run_in_executor(
            None, _tool, execute_metadata_sql,
            {"query": _build_relevance_sql(table_names)}, TOOL_TIMEOUT,
            "execute_metadata_sql (FK predictions)", 6, 7,
        )
        sr_coro = None
        if plan.get("requires_structured_retrieval", True):
            from agent.deep_analysis import _structured_retrieval
            _sr = (partial(_structured_retrieval, parent_span_id=parent_span_id, trace_request_id=trace_request_id)
                   if parent_span_id else _structured_retrieval)
            sr_coro = loop.run_in_executor(None, _sr, query, table_names, cancel, 7, 7)

        fk_tr = await fk_coro
        collect(fk_tr, "execute_metadata_sql (FK predictions)", B.FK_PREDICTIONS)

        if sr_coro:
            sr_tr = await sr_coro
            if sr_tr.success and sr_tr.data:
                tools_used.extend(["sql_writer", "execute_data_sql"])
                parts.append(f"### Source: structured_retrieval (LLM-generated SQL on data)\n"
                             f"{_truncate_part(sr_tr.data, B.STRUCTURED_RETRIEVAL)}")
            elif not sr_tr.success:
                failed.append(sr_tr)
    timing["fk_and_structured"] = round(time.time() - t0, 3)

    return parts, graph_data, tools_used, failed, timing


async def _gather_baseline(query: str, parent_span_id: str = None, trace_request_id: str = None):
    """Async baseline gathering."""
    loop = asyncio.get_running_loop()
    parts: list[str] = []
    tools_used: list[str] = []
    failed: list[ToolResult] = []
    timing: dict = {}
    queries = _build_baseline_queries(query)

    _tool = (partial(_safe_tool_call, parent_span_id=parent_span_id, trace_request_id=trace_request_id)
             if parent_span_id else _safe_tool_call)

    await _safe_progress({"stage": "gathering", "message": f"Running {len(queries)} baseline queries..."})
    t0 = time.time()
    coros = [
        loop.run_in_executor(
            None, _tool, execute_baseline_sql, {"query": sql_q}, TOOL_TIMEOUT,
            f"execute_baseline_sql (query {i + 1})", i + 1, len(queries),
        )
        for i, sql_q in enumerate(queries)
    ]
    results = await asyncio.gather(*coros, return_exceptions=True)
    for r in results:
        if isinstance(r, BaseException):
            continue
        if r.success and r.data:
            tools_used.append("execute_baseline_sql")
            parts.append(f"### Source: execute_baseline_sql\n{r.data}")
        elif not r.success:
            failed.append(r)
    timing["baseline_queries"] = round(time.time() - t0, 3)

    return parts, tools_used, failed, timing


import threading

def _make_cancel() -> threading.Event:
    """Create a cancel event (not wired to anything yet -- placeholder for future use)."""
    return threading.Event()


async def gather_evidence(state: DeepAnalysisState) -> dict:
    """Async node: runs the full retrieval pipeline with progress events."""
    query = state["effective_query"]
    mode = state.get("mode", "graphrag")
    session_id = state.get("session_id")
    intent_type = state.get("intent_type", "new_question")

    await _safe_progress({"stage": "gathering", "message": "Gathering evidence from metadata catalog..."})

    # Create a manual span to bridge autolog context. mlflow.langchain.autolog()
    # manages spans via callbacks, not the context-var API, so
    # get_current_active_span() returns None inside nodes. A manual start_span()
    # DOES inherit the autolog context and gives us IDs to parent tool-call spans.
    mlflow = get_mlflow()
    _span_cm = mlflow.start_span("evidence_retrieval", span_type="RETRIEVER") if mlflow else nullcontext()

    with _span_cm as _parent:
        _pid = getattr(_parent, "span_id", None) if _parent else None
        _rid = getattr(_parent, "request_id", None) if _parent else None

        t0 = time.time()
        if mode == "graphrag":
            parts, graph_data, tools_used, failed_sources, timing = await _gather_graphrag(
                query, session_id, intent_type, _pid, _rid)
        else:
            parts, tools_used, failed_sources, timing = await _gather_baseline(query, _pid, _rid)
            graph_data = {}

        if _parent and hasattr(_parent, "set_attributes"):
            _parent.set_attributes({
                "mode": mode,
                "tools_used": ",".join(dict.fromkeys(tools_used)) if tools_used else "",
            })

    timing["gather_total"] = round(time.time() - t0, 3)

    failed_strs = []
    for fs in failed_sources:
        hint = f" -- {fs.error_hint}" if fs.error_hint else ""
        failed_strs.append(f"{fs.label}: {fs.error_type or 'failed'}{hint}")

    return {
        "evidence_parts": parts,
        "graph_data": graph_data,
        "tool_calls": list(dict.fromkeys(tools_used)),
        "failed_sources": failed_strs,
        "timing": timing,
    }


def assemble_context(state: DeepAnalysisState) -> dict:
    """Sync node: concatenate evidence parts, apply budget truncation."""
    parts = state.get("evidence_parts", [])
    failed = state.get("failed_sources", [])
    B = EvidenceBudget

    context = "\n\n".join(parts) if parts else ""
    if not context.strip():
        context = "No evidence could be gathered. The metadata catalog may be empty or inaccessible."

    if failed:
        context += "\n\n### Failed Evidence Sources\n" + "\n".join(f"- {f}" for f in failed)

    if len(context) > B.TOTAL:
        logger.warning("[graph] Context too large (%d chars), truncating to %d", len(context), B.TOTAL)
        context = context[:B.TOTAL] + f"\n...[context truncated from {len(context):,} to {B.TOTAL:,} chars]"

    return {"context": context}


async def analyze(state: DeepAnalysisState) -> dict:
    """Async node: final LLM analysis with token streaming via astream().

    Conversation history is included as proper HumanMessage/AIMessage pairs
    so the analysis model has multi-turn context.
    """
    mode = state.get("mode", "graphrag")
    query = state["effective_query"]
    context = state.get("context", "")
    history = state.get("history", [])

    await _safe_progress({"stage": "analyzing", "message": "Generating analysis..."})

    prompt = ANALYSIS_PROMPT if mode == "graphrag" else BASELINE_ANALYSIS_PROMPT

    # Build message list with conversation history
    messages: list = [SystemMessage(content=prompt)]
    if history:
        for h in history[-MAX_HISTORY_TURNS:]:
            role = h.get("role", "user")
            content = h.get("content", "")
            if not content:
                continue
            if role == "user":
                messages.append(HumanMessage(content=content))
            elif role == "assistant":
                messages.append(AIMessage(content=content))
    messages.append(HumanMessage(content=f"Current question: {query}\n\nGathered Evidence:\n{context}"))

    llm = ChatDatabricks(endpoint=MODEL, temperature=0, max_retries=1, request_timeout=120)

    t0 = time.time()
    token_usage: dict = {}
    try:
        answer = ""
        last_chunk = None
        async for chunk in llm.astream(messages):
            answer += chunk.content or ""
            last_chunk = chunk
        answer = answer or "Analysis produced no output."
        # Extract token usage from the final chunk (model-reported)
        if last_chunk:
            um = getattr(last_chunk, "usage_metadata", None)
            if um and isinstance(um, dict):
                token_usage = dict(um)
            elif hasattr(last_chunk, "response_metadata"):
                rm = last_chunk.response_metadata or {}
                if "usage" in rm:
                    token_usage = dict(rm["usage"])
        # Fallback: rough char-based estimate (~4 chars/token) when model doesn't report
        if not token_usage:
            prompt_chars = sum(len(m.content) for m in messages)
            token_usage = {
                "input_tokens": prompt_chars // 4,
                "output_tokens": len(answer) // 4,
                "total_tokens": (prompt_chars + len(answer)) // 4,
                "estimated": True,
            }
    except Exception as e:
        logger.error("[graph] Analysis LLM error: %s", e, exc_info=True)
        answer = f"Analysis LLM call failed: {e}. Evidence gathered:\n\n{context[:2000]}"

    elapsed = round(time.time() - t0, 3)
    timing = dict(state.get("timing", {}))
    timing["llm_analysis"] = elapsed
    timing["token_usage"] = token_usage
    timing["total"] = round(sum(v for v in timing.values() if isinstance(v, (int, float))), 3)
    logger.info("[graph] LLM analysis: %.1fs, tokens=%s", elapsed, token_usage)

    # Attach token usage to the MLflow span. Try context-var span first,
    # then fall back to MlflowClient trace search for the root span.
    mlflow = get_mlflow()
    _tok_attrs = {
        "llm.token_count.prompt": token_usage.get("input_tokens", 0),
        "llm.token_count.completion": token_usage.get("output_tokens", 0),
        "llm.token_count.total": token_usage.get("total_tokens", 0),
        "token_usage.estimated": token_usage.get("estimated", False),
    } if token_usage else {}
    if mlflow and _tok_attrs:
        try:
            active = mlflow.get_current_active_span()
            if active:
                active.set_attributes(_tok_attrs)
            else:
                # autolog manages spans via callbacks, not context vars --
                # walk up to the root span via MlflowClient
                client = mlflow.MlflowClient()
                trace = client.get_trace(mlflow.get_last_active_trace().info.request_id)
                if trace and trace.data and trace.data.spans:
                    root = trace.data.spans[0]
                    root.set_attributes(_tok_attrs)
        except Exception:
            logger.debug("[graph] Could not attach token usage to trace span")

    return {
        "answer": sanitize_output(answer),
        "timing": timing,
        "mode": mode,
    }


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def build_deep_analysis_graph() -> StateGraph:
    """Build and compile the deep analysis LangGraph."""
    graph = StateGraph(DeepAnalysisState)

    graph.add_node("classify_intent", classify_intent)
    graph.add_node("quick_answer", quick_answer)
    graph.add_node("gather_evidence", gather_evidence)
    graph.add_node("assemble_context", assemble_context)
    graph.add_node("analyze", analyze)

    graph.add_edge(START, "classify_intent")
    graph.add_conditional_edges("classify_intent", _should_continue, {
        "quick_answer": "quick_answer",
        "gather_evidence": "gather_evidence",
    })
    graph.add_edge("quick_answer", END)
    graph.add_edge("gather_evidence", "assemble_context")
    graph.add_edge("assemble_context", "analyze")
    graph.add_edge("analyze", END)

    return graph.compile()


# Singleton compiled graph
_compiled_graph = None


def get_graph():
    global _compiled_graph
    if _compiled_graph is None:
        _init_tracing()
        _compiled_graph = build_deep_analysis_graph()
    return _compiled_graph


# ---------------------------------------------------------------------------
# SSE event names for node mapping
# ---------------------------------------------------------------------------

NODE_STAGE_MAP = {
    "classify_intent": {"stage": "classifying_intent", "message": "Understanding your question..."},
    "gather_evidence": {"stage": "gathering", "message": "Gathering evidence from metadata catalog..."},
    "assemble_context": {"stage": "assembling", "message": "Assembling evidence..."},
    "analyze": {"stage": "analyzing", "message": "Generating analysis (streaming)..."},
    "quick_answer": {"stage": "responding", "message": "Responding..."},
}
