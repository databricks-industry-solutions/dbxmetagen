"""Multi-agent deep analysis for GraphRAG and Baseline modes.

Adapted from the hospital operations control tower pattern:
Supervisor -> Planner -> Retrieval -> Analyst -> Respond (with Clarify branch).
The agent_mode field ("graphrag" or "baseline") selects tools and prompts.
"""

import json
import logging
import os
import queue
import threading
from operator import add
from typing import Annotated, Dict, List, Optional

from databricks_langchain import ChatDatabricks
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langgraph.prebuilt import create_react_agent
from langgraph.graph import StateGraph, END
from typing_extensions import TypedDict

from agent.metadata_tools import (
    GRAPHRAG_TOOLS, BASELINE_TOOLS, CATALOG, SCHEMA,
    execute_metadata_sql, execute_baseline_sql,
)
from agent.graph_skill import GRAPH_SCHEMA_CONTEXT
from agent.guardrails import GuardrailConfig, SAFETY_PROMPT_BLOCK, sanitize_output
from agent.tracing import trace

logger = logging.getLogger(__name__)

MODEL = os.environ.get("LLM_MODEL", "databricks-claude-sonnet-4-6")
ROUTING_MODEL = os.environ.get("ROUTING_MODEL", "databricks-meta-llama-3-3-70b-instruct")
MAX_ITERATIONS = int(os.environ.get("MAX_SUPERVISOR_ITERATIONS", str(GuardrailConfig.MAX_DEEP_ITERATIONS)))

_local = threading.local()


def _emit(stage: str, message: str, agent: str = ""):
    trace = getattr(_local, "routing_trace", None)
    if agent and trace is not None:
        trace.append({"agent": agent, "action": stage, "message": message})
    q = getattr(_local, "progress_queue", None)
    if q is not None:
        q.put({"stage": stage, "message": message})


_cache_lock = threading.Lock()
_cached_llm = None


def _llm():
    global _cached_llm
    if _cached_llm is None:
        with _cache_lock:
            if _cached_llm is None:
                _cached_llm = ChatDatabricks(endpoint=MODEL, temperature=0, max_retries=3)
    return _cached_llm


_cached_routing_llm = None


def _routing_llm():
    global _cached_routing_llm
    if _cached_routing_llm is None:
        with _cache_lock:
            if _cached_routing_llm is None:
                _cached_routing_llm = ChatDatabricks(endpoint=ROUTING_MODEL, temperature=0, max_retries=3)
    return _cached_routing_llm


_cached_retrieval_agents: dict = {}
_cached_analyst_agents: dict = {}


def _get_retrieval_agent(mode: str):
    with _cache_lock:
        if mode not in _cached_retrieval_agents:
            tools = GRAPHRAG_TOOLS if mode == "graphrag" else BASELINE_TOOLS
            _cached_retrieval_agents[mode] = create_react_agent(_llm(), tools)
        return _cached_retrieval_agents[mode]


def _get_analyst_agent(mode: str):
    with _cache_lock:
        if mode not in _cached_analyst_agents:
            followup_tools = [execute_metadata_sql] if mode == "graphrag" else [execute_baseline_sql]
            _cached_analyst_agents[mode] = create_react_agent(_llm(), followup_tools)
        return _cached_analyst_agents[mode]


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class DeepAnalysisState(TypedDict):
    messages: Annotated[list, add]
    user_query: str
    plan: str
    retrieved_evidence: str
    analysis_result: str
    needs_clarification: bool
    clarification_question: str
    next_step: str
    iteration: Annotated[list, add]
    tool_calls_made: Annotated[list, add]
    agent_mode: str  # "graphrag" or "baseline"
    graph_data: dict


# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------

SUPERVISOR_PROMPT = """You are a supervisor coordinating a deep analysis of a data catalog's metadata.
""" + SAFETY_PROMPT_BLOCK + """
Given the user's question and the current analysis state, decide the single next step.
Respond with EXACTLY one word from: CLARIFY, PLAN, RETRIEVE, ANALYZE, RESPOND.

Decision rules:
- CLARIFY  -- the question is too vague to act on
- PLAN     -- no plan exists yet
- RETRIEVE -- a plan exists but evidence has not been gathered
- ANALYZE  -- evidence has been gathered and is ready for interpretation
- RESPOND  -- the analysis is complete OR you need to deliver a clarification

If the analysis result seems incomplete and iterations remain, you may route back to
RETRIEVE or PLAN for a second pass.

Current state will be provided. Return ONLY one word."""


def _planner_prompt(mode: str) -> str:
    base = """You are a planning specialist for data catalog metadata analysis.

Given the user question, produce a concise numbered plan of data-gathering steps
that the Retrieval agent should execute."""

    if mode == "graphrag":
        return base + f"""

{GRAPH_SCHEMA_CONTEXT}

Available tools the Retrieval agent can call:
- search_metadata (vector search over metadata documents)
- execute_metadata_sql (SQL on UC knowledge base tables in {CATALOG}.{SCHEMA})
- get_table_summary, get_data_quality (convenience lookups)
- query_graph_nodes, traverse_graph, find_similar_nodes, get_node_details (graph tools)
- expand_vs_hits (bridge VS results into graph via node_id)

STRATEGY:
1. Start with search_metadata for semantic discovery, then extract node_ids.
2. Use expand_vs_hits to bridge VS hits into graph for structural context.
3. Use traverse_graph with edge_type filters for targeted exploration.
4. Use execute_metadata_sql for precise counts/aggregations.
Keep the plan to 3-6 steps."""
    else:
        return base + f"""

The Retrieval agent can ONLY query these three tables in {CATALOG}.{SCHEMA}:
- table_knowledge_base: table_name, comment, domain, subdomain, has_pii, has_phi, row_count
- column_knowledge_base: table_name, column_name, comment, data_type, classification, classification_type
- schema_knowledge_base: catalog_name, schema_name, comment, tables_count

Available tool: execute_baseline_sql (read-only SELECT on the 3 tables above).
No vector search, no graph traversal, no ontology, no FK predictions.
Keep the plan to 3-5 steps."""


def _retrieval_prompt(mode: str) -> str:
    prefix = f"""You are a data retrieval specialist for a data catalog.

Execute the data-gathering plan provided. For EACH piece of data you retrieve,
note which tool provided it so the Analyst can cite sources.

Format your output as:
### Source: [tool_name]
[data / result summary]

Use {CATALOG}.{SCHEMA} as the catalog/schema for SQL queries."""

    if mode == "graphrag":
        return prefix + """

IMPORTANT: When using search_metadata, look at node_id in results.
Pass those node_ids to expand_vs_hits to bridge into graph.
Use traverse_graph for multi-hop exploration after identifying starting nodes."""
    return prefix


ANALYST_PROMPT = """You are a senior data catalog analyst. Interpret retrieved evidence
and produce a structured analysis.

## Key Findings
Numbered findings with inline source citations (e.g. "[Source: search_metadata]").

## Relationships & Patterns
Describe discovered structural relationships, join patterns, or entity mappings.

## Recommendations
- **Action**: What to do
- **Evidence**: Supporting data (cite source)
- **Priority**: High / Medium / Low

Be concise. Lead with the most impactful finding."""


# ---------------------------------------------------------------------------
# Sub-agent nodes
# ---------------------------------------------------------------------------

def supervisor_node(state: DeepAnalysisState) -> dict:
    _emit("routing", "Deciding next step...", agent="supervisor")
    try:
        parts = [f"User question: {state['user_query']}"]
        parts.append(f"Plan exists: {'yes' if state.get('plan') else 'no'}")
        parts.append("Evidence: gathered" if state.get("retrieved_evidence") else "Evidence: not yet gathered")
        parts.append("Analysis: complete" if state.get("analysis_result") else "Analysis: not complete")

        resp = _routing_llm().invoke([
            SystemMessage(content=SUPERVISOR_PROMPT),
            HumanMessage(content="\n".join(parts)),
        ])
        decision = resp.content.strip().upper().split()[0] if resp.content else "RESPOND"
        valid = {"CLARIFY", "PLAN", "RETRIEVE", "ANALYZE", "RESPOND"}
        if decision not in valid:
            decision = "RESPOND"
    except Exception as e:
        logger.warning("Supervisor LLM error (retries exhausted): %s", e)
        decision = "RESPOND"

    iters = len(state.get("iteration", []))
    if iters >= MAX_ITERATIONS:
        decision = "RESPOND" if state.get("analysis_result") else (
            "ANALYZE" if state.get("retrieved_evidence") else "RESPOND"
        )

    _emit("routing", f"Next: {decision}", agent="supervisor")
    return {"next_step": decision, "iteration": [1]}


def planner_node(state: DeepAnalysisState) -> dict:
    mode = state.get("agent_mode", "graphrag")
    _emit("planning", "Creating analysis plan...", agent="planner")
    try:
        resp = _routing_llm().invoke([
            SystemMessage(content=_planner_prompt(mode)),
            HumanMessage(content=f"User question: {state['user_query']}"),
        ])
        return {"plan": resp.content, "messages": []}
    except Exception as e:
        logger.warning("Planner LLM error (retries exhausted): %s", e)
        return {"plan": f"1. Gather relevant data for: {state['user_query']}", "messages": []}


_GRAPH_TOOLS = {"traverse_graph", "expand_vs_hits", "find_similar_nodes"}


def _merge_graph_data(existing: dict, new_chunk: dict) -> dict:
    """Merge a traverse_graph result into an accumulated graph_data dict."""
    nodes = dict(existing.get("nodes") or {})
    nodes.update(new_chunk.get("nodes") or {})
    edges = list(existing.get("edges") or [])
    seen = {(e.get("src"), e.get("dst"), e.get("relationship")) for e in edges}
    for e in new_chunk.get("edges") or []:
        key = (e.get("src"), e.get("dst"), e.get("relationship"))
        if key not in seen:
            edges.append(e)
            seen.add(key)
    start = existing.get("start_node") or new_chunk.get("start_node")
    return {"start_node": start, "nodes": nodes, "edges": edges}


def retrieval_node(state: DeepAnalysisState) -> dict:
    mode = state.get("agent_mode", "graphrag")
    _emit("retrieving", "Gathering evidence...", agent="retrieval")
    try:
        agent = _get_retrieval_agent(mode)
        prompt = f"Execute this plan:\n\n{state.get('plan', '')}\n\nUser question: {state['user_query']}"
        result = agent.invoke({
            "messages": [
                SystemMessage(content=_retrieval_prompt(mode)),
                HumanMessage(content=prompt),
            ]
        })
        evidence = ""
        tools_used = []
        graph_data = dict(state.get("graph_data") or {})
        for msg in result.get("messages", []):
            if isinstance(msg, AIMessage):
                if msg.content:
                    evidence = msg.content
                if hasattr(msg, "tool_calls") and msg.tool_calls:
                    tools_used.extend([tc.get("name", "unknown") for tc in msg.tool_calls])
            elif isinstance(msg, ToolMessage) and getattr(msg, "name", "") in _GRAPH_TOOLS:
                try:
                    chunk = json.loads(msg.content) if isinstance(msg.content, str) else msg.content
                    if isinstance(chunk, dict) and ("nodes" in chunk or "edges" in chunk):
                        graph_data = _merge_graph_data(graph_data, chunk)
                except (json.JSONDecodeError, TypeError):
                    pass
        return {"retrieved_evidence": evidence, "tool_calls_made": tools_used, "graph_data": graph_data, "messages": []}
    except Exception as e:
        logger.warning("Retrieval error (retries exhausted): %s", e)
        return {"retrieved_evidence": f"Error: {e}", "tool_calls_made": [], "messages": []}


def analyst_node(state: DeepAnalysisState) -> dict:
    mode = state.get("agent_mode", "graphrag")
    _emit("analyzing", "Interpreting results...", agent="analyst")
    try:
        agent = _get_analyst_agent(mode)
        context = (
            f"User question: {state['user_query']}\n\n"
            f"Plan:\n{state.get('plan', 'N/A')}\n\n"
            f"Retrieved Evidence:\n{state.get('retrieved_evidence', 'N/A')}"
        )
        result = agent.invoke({
            "messages": [SystemMessage(content=ANALYST_PROMPT), HumanMessage(content=context)]
        })
        analysis = ""
        tools_used = []
        for msg in result.get("messages", []):
            if isinstance(msg, AIMessage):
                if msg.content and len(msg.content) > len(analysis):
                    analysis = msg.content
                if hasattr(msg, "tool_calls") and msg.tool_calls:
                    tools_used.extend([tc.get("name", "unknown") for tc in msg.tool_calls])
        return {"analysis_result": analysis, "tool_calls_made": tools_used, "messages": []}
    except Exception as e:
        logger.warning("Analyst error (retries exhausted): %s", e)
        return {"analysis_result": "Analysis could not be completed. Please try again.", "tool_calls_made": [], "messages": []}


def respond_node(state: DeepAnalysisState) -> dict:
    _emit("responding", "Preparing final response...", agent="respond")
    if state.get("needs_clarification"):
        return {"messages": [AIMessage(content=state.get("clarification_question", "Could you clarify your question?"))]}
    if state.get("analysis_result"):
        return {"messages": [AIMessage(content=state["analysis_result"])]}
    try:
        resp = _llm().invoke([
            SystemMessage(content="Summarize what you know so far. Be helpful."),
            HumanMessage(content=f"Question: {state['user_query']}\nEvidence: {state.get('retrieved_evidence', 'none')}"),
        ])
        return {"messages": [AIMessage(content=resp.content)]}
    except Exception:
        return {"messages": [AIMessage(content="The AI model is temporarily unavailable. Please try again.")]}


def clarify_node(state: DeepAnalysisState) -> dict:
    _emit("clarifying", "Asking for clarification...", agent="clarify")
    try:
        resp = _llm().invoke([
            SystemMessage(content="The user's question is ambiguous. Ask a brief, specific clarifying question."),
            HumanMessage(content=f"User question: {state['user_query']}"),
        ])
        return {"needs_clarification": True, "clarification_question": resp.content, "next_step": "RESPOND", "messages": []}
    except Exception:
        return {"needs_clarification": True, "clarification_question": "Could you provide more details?", "next_step": "RESPOND", "messages": []}


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------

def _route_supervisor(state: DeepAnalysisState) -> str:
    return {
        "CLARIFY": "clarify", "PLAN": "planner", "RETRIEVE": "retrieval",
        "ANALYZE": "analyst", "RESPOND": "respond",
    }.get(state.get("next_step", "RESPOND"), "respond")


def build_deep_graph():
    graph = StateGraph(DeepAnalysisState)
    graph.add_node("supervisor", supervisor_node)
    graph.add_node("planner", planner_node)
    graph.add_node("retrieval", retrieval_node)
    graph.add_node("analyst", analyst_node)
    graph.add_node("respond", respond_node)
    graph.add_node("clarify", clarify_node)
    graph.set_entry_point("supervisor")
    graph.add_conditional_edges("supervisor", _route_supervisor)
    graph.add_edge("planner", "supervisor")
    graph.add_edge("retrieval", "supervisor")
    graph.add_edge("analyst", "supervisor")
    graph.add_edge("clarify", "respond")
    graph.add_edge("respond", END)
    return graph.compile()


_deep_graph = None


def _get_graph():
    global _deep_graph
    if _deep_graph is None:
        _deep_graph = build_deep_graph()
    return _deep_graph


def _init_state(query: str, mode: str) -> DeepAnalysisState:
    return {
        "messages": [],
        "user_query": query,
        "plan": "",
        "retrieved_evidence": "",
        "analysis_result": "",
        "needs_clarification": False,
        "clarification_question": "",
        "next_step": "",
        "iteration": [],
        "tool_calls_made": [],
        "agent_mode": mode,
        "graph_data": {},
    }


def _build_query(message: str, history: Optional[List[Dict]] = None) -> str:
    if history:
        lines = [f"{m.get('role', 'user')}: {m['content']}" for m in history[-6:]]
        lines.append(f"user: {message}")
        return "Conversation so far:\n" + "\n".join(lines) + "\n\nRespond to the latest user message."
    return message


def _extract_result(result: dict, mode: str) -> Dict:
    response = ""
    for msg in result.get("messages", []):
        if isinstance(msg, AIMessage) and msg.content and len(msg.content) > len(response):
            response = msg.content
    if not response:
        response = result.get("analysis_result", "Analysis complete but no output was generated.")
    tools = list(set(result.get("tool_calls_made", [])))
    out: Dict = {"answer": sanitize_output(response), "tool_calls": tools, "mode": mode, "steps": len(result.get("iteration", []))}
    gd = result.get("graph_data")
    if gd and (gd.get("nodes") or gd.get("edges")):
        out["graph_data"] = gd
    return out


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@trace(name="deep_analysis")
def run_deep_analysis(
    message: str,
    mode: str = "graphrag",
    history: Optional[List[Dict]] = None,
) -> Dict:
    """Non-streaming deep analysis. mode is 'graphrag' or 'baseline'."""
    query = _build_query(message, history)
    state = _init_state(query, mode)
    result = _get_graph().invoke(state, config={"recursion_limit": GuardrailConfig.MAX_RECURSION_LIMIT})
    return _extract_result(result, mode)


def run_deep_analysis_streaming(
    message: str,
    mode: str = "graphrag",
    history: Optional[List[Dict]] = None,
) -> queue.Queue:
    """Streaming deep analysis. Returns a Queue yielding progress events.

    Events:
        {"stage": "planning"|"retrieving"|"analyzing"|"responding", "message": "..."}
        {"stage": "done", "answer": "...", "tool_calls": [...], "mode": "..."}
        {"stage": "error", "message": "..."}
    """
    q: queue.Queue = queue.Queue()

    def _run():
        _local.progress_queue = q
        _local.routing_trace = []
        try:
            _emit("starting", "Starting analysis...", agent="system")
            query = _build_query(message, history)
            state = _init_state(query, mode)
            result = _get_graph().invoke(state, config={"recursion_limit": GuardrailConfig.MAX_RECURSION_LIMIT})
            extracted = _extract_result(result, mode)
            done_event = {
                "stage": "done",
                "answer": extracted["answer"],
                "tool_calls": extracted["tool_calls"],
                "mode": mode,
                "routing_trace": list(_local.routing_trace),
            }
            if "graph_data" in extracted:
                done_event["graph_data"] = extracted["graph_data"]
            q.put(done_event)
        except Exception as e:
            logger.error("Streaming deep analysis error: %s", e, exc_info=True)
            q.put({"stage": "error", "message": str(e)})
        finally:
            _local.progress_queue = None
            _local.routing_trace = []

    threading.Thread(target=_run, daemon=True).start()
    return q
