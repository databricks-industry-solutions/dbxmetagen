"""Metric view agent -- answers business questions using metric views as its semantic layer.

Purpose-built LangGraph with explicit phases: plan -> search -> query -> summarize.
Each node does one thing and routes deterministically to the next based on state
fields, eliminating the "I'll check this now" confabulation problem from ReAct loops.

Never touches raw tables -- metric views are the bounded query contract.
"""

import json
import logging
import queue
import threading
from typing import Annotated, Any, Dict, List, Optional, TypedDict

from langchain_core.messages import AIMessage, SystemMessage
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages

from agent.common import CATALOG, SCHEMA, get_llm, history_to_messages
from agent.guardrails import SAFETY_PROMPT_BLOCK, sanitize_output
from agent.metric_view_tools import (
    search_metric_views_direct, list_metric_views_direct,
    fetch_definition, execute_measure_query, execute_dimension_query,
)
from agent.tracing import trace, tag_trace

logger = logging.getLogger(__name__)

MAX_QUERY_ATTEMPTS = 3


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class MetricViewState(TypedDict):
    messages: Annotated[list, add_messages]
    question: str
    plan: Optional[str]
    candidates: list[dict]
    selected_view: Optional[str]
    selected_fqn: Optional[str]
    view_definition: Optional[dict]
    query_result: Optional[dict]
    query_sql: Optional[str]
    query_attempts: int
    error: Optional[str]
    stage: Optional[str]


# ---------------------------------------------------------------------------
# Prompts (per-node, focused)
# ---------------------------------------------------------------------------

_PLAN_PROMPT = f"""You are a data analyst that answers questions using metric views in {CATALOG}.{SCHEMA}.

Given the user's question (and any chat history), produce a 1-2 sentence PLAN:
- Which metric or dimension is needed?
- What query shape? (aggregation, filter, trend, comparison, count of distinct values)
- Is this a dimension-only question (e.g. "how many distinct X", "list all Y")?

Output ONLY the plan text, nothing else.
{SAFETY_PROMPT_BLOCK}"""

_SELECT_PROMPT = """Given these metric view search results and the user's plan, pick the BEST metric view.

PLAN: {plan}
QUESTION: {question}

SEARCH RESULTS:
{candidates_json}

Respond with ONLY a JSON object: {{"metric_view_name": "<name>", "reasoning": "<1 sentence>"}}
If none match, respond: {{"metric_view_name": null, "reasoning": "<why>"}}"""

_QUERY_PROMPT = """You must build a metric view query. Here is the definition:

METRIC VIEW: {view_name} (FQN: {fqn})
MEASURES: {measures}
DIMENSIONS: {dimensions}
JOINS: {joins}
FILTER: {filter}

USER QUESTION: {question}
PLAN: {plan}
{error_context}

Decide: is this a MEASURE query or a DIMENSION-ONLY query?

For MEASURE queries, respond with JSON:
{{"query_type": "measure", "measures": ["m1", "m2"], "dimensions": ["d1"], "filters": "optional WHERE clause or null", "order_by": "optional ORDER BY or null", "limit": 100}}

For DIMENSION-ONLY queries (listing unique values, counting distinct, etc.), respond with JSON:
{{"query_type": "dimension", "dimensions": ["d1"], "aggregation": "list|count_distinct|unique_values", "filters": "optional WHERE clause or null", "limit": 100}}

RULES:
- ONLY use measure and dimension names from the definition above.
- Do NOT fabricate names. If a name doesn't exist, pick the closest valid one.
- For trends, include a time dimension. For comparisons, include a categorical dimension.
- Respond with ONLY the JSON object."""

_SUMMARIZE_PROMPT = f"""Present the query results to the user clearly and concisely.

QUESTION: {{question}}
PLAN: {{plan}}
METRIC VIEW: {{view_name}}
SQL: {{sql}}
RESULT: {{result_json}}

Lead with the key insight, then show supporting data. Include the metric view name.
If the query errored, explain what went wrong and suggest alternatives.
Keep it concise.
{SAFETY_PROMPT_BLOCK}"""

_NO_DATA_PROMPT = """No metric views matched the user's question.

QUESTION: {question}
AVAILABLE VIEWS: {available_json}

Explain that no matching metric view was found. List what IS available so the user can refine their question."""


# ---------------------------------------------------------------------------
# Graph nodes
# ---------------------------------------------------------------------------

def plan_node(state: MetricViewState) -> dict:
    """LLM produces a 1-2 sentence plan from the question."""
    llm = get_llm()
    history = [m for m in state["messages"] if not isinstance(m, SystemMessage)]
    msgs = [SystemMessage(content=_PLAN_PROMPT)] + history
    response = llm.invoke(msgs)
    plan = (response.content or "").strip()
    logger.info("[mv_agent] plan: %s", plan[:200])
    return {"plan": plan, "stage": "planning"}


def search_node(state: MetricViewState) -> dict:
    """Call vector search directly (no LLM decision needed), then LLM picks the best candidate."""
    question = state["question"]
    plan = state.get("plan") or question

    candidates = search_metric_views_direct(question)
    if not candidates:
        return {"candidates": [], "stage": "searching"}

    # Deduplicate by metric_view_name, keep first occurrence
    seen = set()
    unique = []
    for c in candidates:
        name = c.get("metric_view_name", "")
        if name and name not in seen:
            seen.add(name)
            unique.append(c)

    # LLM picks the best candidate
    llm = get_llm()
    select_msg = _SELECT_PROMPT.format(
        plan=plan, question=question,
        candidates_json=json.dumps(unique[:8], indent=2),
    )
    response = llm.invoke([SystemMessage(content=select_msg)])
    content = (response.content or "").strip()

    selected_name = None
    try:
        parsed = json.loads(content)
        selected_name = parsed.get("metric_view_name")
    except (json.JSONDecodeError, TypeError):
        # Fallback: pick the first candidate with measures
        for c in unique:
            if c.get("measures"):
                selected_name = c["metric_view_name"]
                break

    selected_fqn = None
    view_definition = None
    if selected_name:
        defn, fqn, _err = fetch_definition(selected_name)
        if defn and fqn:
            view_definition = defn
            selected_fqn = fqn
        else:
            for c in unique:
                if c.get("metric_view_name") == selected_name and c.get("fqn"):
                    selected_fqn = c["fqn"]
                    break

    return {
        "candidates": unique,
        "selected_view": selected_name,
        "selected_fqn": selected_fqn,
        "view_definition": view_definition,
        "stage": "searching",
    }


def list_fallback_node(state: MetricViewState) -> dict:
    """Fallback when search returns nothing -- list all views and LLM picks."""
    question = state["question"]
    plan = state.get("plan") or question

    views = list_metric_views_direct()
    if not views:
        return {"candidates": [], "stage": "listing"}

    llm = get_llm()
    select_msg = _SELECT_PROMPT.format(
        plan=plan, question=question,
        candidates_json=json.dumps(views[:15], indent=2),
    )
    response = llm.invoke([SystemMessage(content=select_msg)])
    content = (response.content or "").strip()

    selected_name = None
    try:
        parsed = json.loads(content)
        selected_name = parsed.get("metric_view_name")
    except (json.JSONDecodeError, TypeError):
        pass

    selected_fqn = None
    view_definition = None
    if selected_name:
        defn, fqn, _err = fetch_definition(selected_name)
        if defn and fqn:
            view_definition = defn
            selected_fqn = fqn

    return {
        "candidates": views,
        "selected_view": selected_name,
        "selected_fqn": selected_fqn,
        "view_definition": view_definition,
        "stage": "listing",
    }


def query_node(state: MetricViewState) -> dict:
    """LLM builds query args from the definition, then execute directly."""
    view_name = state["selected_view"]
    fqn = state["selected_fqn"]
    defn = state.get("view_definition") or {}
    question = state["question"]
    plan = state.get("plan") or question
    attempts = state.get("query_attempts", 0)
    prev_error = state.get("error")

    error_context = ""
    if prev_error and attempts > 0:
        error_context = f"\nPREVIOUS ATTEMPT FAILED: {prev_error}\nFix the issue and try a different approach."

    measures_str = json.dumps([m["name"] for m in defn.get("measures", [])])
    dims_str = json.dumps([d["name"] for d in defn.get("dimensions", [])])
    joins_str = json.dumps([j.get("name", "") for j in defn.get("joins", [])])
    filter_str = defn.get("filter", "none")

    query_msg = _QUERY_PROMPT.format(
        view_name=view_name, fqn=fqn,
        measures=measures_str, dimensions=dims_str,
        joins=joins_str, filter=filter_str,
        question=question, plan=plan,
        error_context=error_context,
    )

    llm = get_llm()
    response = llm.invoke([SystemMessage(content=query_msg)])
    content = (response.content or "").strip()

    # Parse LLM's query specification
    try:
        spec = json.loads(content)
    except (json.JSONDecodeError, TypeError):
        return {
            "query_attempts": attempts + 1,
            "error": f"Failed to parse query spec from LLM: {content[:200]}",
            "stage": "querying",
        }

    query_type = spec.get("query_type", "measure")

    if query_type == "dimension":
        result = execute_dimension_query(
            metric_view_name=view_name,
            dimensions=spec.get("dimensions", []),
            aggregation=spec.get("aggregation", "list"),
            filters=spec.get("filters"),
            limit=spec.get("limit", 100),
        )
    else:
        result = execute_measure_query(
            metric_view_name=view_name,
            measures=spec.get("measures", []),
            dimensions=spec.get("dimensions", []),
            filters=spec.get("filters"),
            order_by=spec.get("order_by"),
            limit=spec.get("limit", 100),
        )

    sql = result.get("sql", "")
    if result.get("error"):
        return {
            "query_attempts": attempts + 1,
            "query_sql": sql,
            "error": result["error"],
            "query_result": None,
            "stage": "querying",
        }

    return {
        "query_attempts": attempts + 1,
        "query_sql": sql,
        "query_result": result,
        "error": None,
        "stage": "querying",
    }


def summarize_node(state: MetricViewState) -> dict:
    """LLM synthesizes the final user-facing answer from query results."""
    question = state["question"]
    plan = state.get("plan", "")
    view_name = state.get("selected_view", "unknown")
    sql = state.get("query_sql", "")
    result = state.get("query_result")
    error = state.get("error")

    if error and not result:
        result_json = json.dumps({"error": error})
    elif result:
        # Truncate large results for the LLM context
        rows = result.get("rows", [])
        truncated = {**result, "rows": rows[:50]}
        if len(rows) > 50:
            truncated["note"] = f"Showing 50 of {len(rows)} rows"
        result_json = json.dumps(truncated, default=str)
    else:
        result_json = json.dumps({"error": "No query was executed"})

    prompt = _SUMMARIZE_PROMPT.format(
        question=question, plan=plan, view_name=view_name,
        sql=sql, result_json=result_json,
    )
    llm = get_llm()
    response = llm.invoke([SystemMessage(content=prompt)])
    answer = sanitize_output((response.content or "").strip())
    return {"messages": [AIMessage(content=answer)], "stage": "summarizing"}


def answer_no_data_node(state: MetricViewState) -> dict:
    """No metric views found -- tell the user what IS available."""
    question = state["question"]

    available = list_metric_views_direct()
    available_json = json.dumps(available[:20], indent=2) if available else "[]"

    prompt = _NO_DATA_PROMPT.format(question=question, available_json=available_json)
    llm = get_llm()
    response = llm.invoke([SystemMessage(content=prompt)])
    answer = sanitize_output((response.content or "").strip())
    return {"messages": [AIMessage(content=answer)], "stage": "no_data"}


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------

def _route_after_search(state: MetricViewState) -> str:
    if state.get("selected_view") and state.get("selected_fqn"):
        return "query"
    if state.get("candidates"):
        # Had candidates but couldn't select -- still try query with first one
        return "list_fallback"
    return "list_fallback"


def _route_after_list(state: MetricViewState) -> str:
    if state.get("selected_view") and state.get("selected_fqn"):
        return "query"
    return "no_data"


def _route_after_query(state: MetricViewState) -> str:
    result = state.get("query_result")
    if result and not result.get("error"):
        return "summarize"
    if state.get("query_attempts", 0) < MAX_QUERY_ATTEMPTS:
        return "query"
    return "summarize"


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def _build_metric_view_graph():
    graph = StateGraph(MetricViewState)

    graph.add_node("plan", plan_node)
    graph.add_node("search", search_node)
    graph.add_node("list_fallback", list_fallback_node)
    graph.add_node("query", query_node)
    graph.add_node("summarize", summarize_node)
    graph.add_node("no_data", answer_no_data_node)

    graph.add_edge(START, "plan")
    graph.add_edge("plan", "search")
    graph.add_conditional_edges("search", _route_after_search, {
        "query": "query",
        "list_fallback": "list_fallback",
    })
    graph.add_conditional_edges("list_fallback", _route_after_list, {
        "query": "query",
        "no_data": "no_data",
    })
    graph.add_conditional_edges("query", _route_after_query, {
        "summarize": "summarize",
        "query": "query",
    })
    graph.add_edge("summarize", END)
    graph.add_edge("no_data", END)

    return graph.compile()


_compiled = None


def _get_graph():
    global _compiled
    if _compiled is None:
        _compiled = _build_metric_view_graph()
    return _compiled


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@trace(name="metric_view_chat", span_type="CHAIN")
async def run_metric_view_agent(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the metric view agent on a question. Returns answer + metadata."""
    tag_trace(agent="metric_view", session_id=session_id)
    graph = _get_graph()

    messages = history_to_messages(history, question)

    result = await graph.ainvoke(
        {
            "messages": messages,
            "question": question,
            "plan": None,
            "candidates": [],
            "selected_view": None,
            "selected_fqn": None,
            "view_definition": None,
            "query_result": None,
            "query_sql": None,
            "query_attempts": 0,
            "error": None,
            "stage": None,
        },
        config={"recursion_limit": 20},
    )

    final_msg = result["messages"][-1]
    return {
        "answer": sanitize_output(final_msg.content if hasattr(final_msg, "content") else str(final_msg)),
        "tool_calls": [],
        "steps": result.get("query_attempts", 0),
        "metric_view": result.get("selected_view"),
        "sql": result.get("query_sql"),
    }


def stream_metric_view_agent(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
    session_id: Optional[str] = None,
):
    """Generator that yields SSE events for streaming metric view agent responses."""
    from api_server import _obo_token_var

    def _sse(event: str, data: dict) -> str:
        return f"data: {json.dumps({'event': event, **data})}\n\n"

    obo_token = _obo_token_var.get(None)
    result_holder: Dict[str, Any] = {}
    error_holder: Dict[str, str] = {}
    done = queue.Queue()

    def _run():
        import asyncio
        if obo_token:
            _obo_token_var.set(obo_token)
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result_holder["result"] = loop.run_until_complete(
                run_metric_view_agent(question, history, session_id)
            )
        except Exception as exc:
            logger.exception("Metric view agent stream failed")
            error_holder["error"] = str(exc)
        finally:
            done.put(True)

    threading.Thread(target=_run, daemon=True).start()

    yield _sse("stage", {"stage": "planning"})

    # Poll for completion, emitting stage updates
    stage_sequence = ["planning", "searching", "querying", "summarizing"]
    stage_idx = 0
    while True:
        try:
            done.get(timeout=2)
            break
        except queue.Empty:
            # Emit next stage hint based on elapsed time
            if stage_idx < len(stage_sequence) - 1:
                stage_idx += 1
                yield _sse("stage", {"stage": stage_sequence[stage_idx]})

    if error_holder:
        yield _sse("error", {"error": error_holder["error"]})
    else:
        yield _sse("done", {"result": result_holder.get("result", {})})
