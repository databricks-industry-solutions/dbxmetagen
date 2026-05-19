"""Metric view agent -- answers business questions using metric views as its semantic layer.

Purpose-built LangGraph with explicit phases:
  plan -> search -> query -> assess -> (summarize | search again)

The assess node enables multi-view queries: after each query, the agent evaluates
whether the gathered data is sufficient. If not (and more views are available), it
loops back to search for another view, up to MAX_VIEW_ITERATIONS times.

Never touches raw tables -- metric views are the bounded query contract.
"""

import json
import logging
import queue
import threading
from typing import Annotated, Any, Dict, List, Optional, TypedDict

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
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
MAX_VIEW_ITERATIONS = 3


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
    completed_queries: list[dict]
    queried_views: list[str]
    iteration: int
    _assess_decision: Optional[str]


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
{exclusion_block}
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
{{queries_block}}

Lead with the key insight, then show supporting data. Reference each metric view by name.
If multiple metric views were queried, synthesize the findings into a unified answer.
If any query errored, explain what went wrong and suggest alternatives.
Keep it concise.
{SAFETY_PROMPT_BLOCK}"""

_ASSESS_PROMPT = """You have queried metric view(s) to answer a user's question. Decide if the data gathered so far is SUFFICIENT or if another metric view is needed.

QUESTION: {question}
PLAN: {plan}

DATA GATHERED SO FAR:
{gathered_json}

AVAILABLE METRIC VIEWS NOT YET QUERIED:
{available_json}

Rules:
- If the gathered data fully answers the question, respond sufficient.
- If the question clearly needs data from a different metric view AND one of the available views looks relevant, respond need_more with a focused plan for what to query next.
- Do NOT request another view just to add minor context -- only if the answer is materially incomplete.

Respond with ONLY a JSON object:
{{"sufficient": true, "reasoning": "<1 sentence>"}}
or
{{"sufficient": false, "next_plan": "<1-2 sentence plan for the next query>", "reasoning": "<1 sentence>"}}"""

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


def _exclusion_block(queried_views: list[str]) -> str:
    if not queried_views:
        return ""
    return f"\nALREADY QUERIED (do NOT pick these): {json.dumps(queried_views)}\n"


def search_node(state: MetricViewState) -> dict:
    """Call vector search directly (no LLM decision needed), then LLM picks the best candidate."""
    question = state["question"]
    plan = state.get("plan") or question
    queried = state.get("queried_views") or []

    candidates = search_metric_views_direct(question)
    if not candidates:
        return {"candidates": [], "stage": "searching"}

    # Deduplicate by metric_view_name and exclude already-queried views
    seen = set(queried)
    unique = []
    for c in candidates:
        name = c.get("metric_view_name", "")
        if name and name not in seen:
            seen.add(name)
            unique.append(c)

    if not unique:
        return {"candidates": [], "stage": "searching"}

    llm = get_llm()
    select_content = _SELECT_PROMPT.format(
        plan=plan, question=question,
        exclusion_block=_exclusion_block(queried),
        candidates_json=json.dumps(unique[:8], indent=2),
    )
    response = llm.invoke([HumanMessage(content=select_content)])
    content = (response.content or "").strip()

    selected_name = None
    try:
        parsed = json.loads(content)
        selected_name = parsed.get("metric_view_name")
    except (json.JSONDecodeError, TypeError):
        for c in unique:
            if c.get("measures"):
                selected_name = c["metric_view_name"]
                break

    if selected_name and selected_name in queried:
        selected_name = None

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
    queried = state.get("queried_views") or []

    views = list_metric_views_direct()
    views = [v for v in views if v.get("metric_view_name") not in queried]
    if not views:
        return {"candidates": [], "stage": "listing"}

    llm = get_llm()
    select_content = _SELECT_PROMPT.format(
        plan=plan, question=question,
        exclusion_block=_exclusion_block(queried),
        candidates_json=json.dumps(views[:15], indent=2),
    )
    response = llm.invoke([HumanMessage(content=select_content)])
    content = (response.content or "").strip()

    selected_name = None
    try:
        parsed = json.loads(content)
        selected_name = parsed.get("metric_view_name")
    except (json.JSONDecodeError, TypeError):
        pass

    if selected_name and selected_name in queried:
        selected_name = None

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

    query_content = _QUERY_PROMPT.format(
        view_name=view_name, fqn=fqn,
        measures=measures_str, dimensions=dims_str,
        joins=joins_str, filter=filter_str,
        question=question, plan=plan,
        error_context=error_context,
    )

    llm = get_llm()
    response = llm.invoke([HumanMessage(content=query_content)])
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


def _truncate_result(result: Optional[dict], error: Optional[str] = None) -> dict:
    """Build a JSON-safe truncated result dict for LLM context."""
    if error and not result:
        return {"error": error}
    if result:
        rows = result.get("rows", [])
        truncated = {**result, "rows": rows[:50]}
        if len(rows) > 50:
            truncated["note"] = f"Showing 50 of {len(rows)} rows"
        return truncated
    return {"error": "No query was executed"}


def assess_node(state: MetricViewState) -> dict:
    """Decide if gathered data is sufficient or another metric view is needed."""
    question = state["question"]
    plan = state.get("plan") or question
    view_name = state.get("selected_view", "unknown")
    iteration = state.get("iteration", 0) + 1

    current_query = {
        "view_name": view_name,
        "fqn": state.get("selected_fqn", ""),
        "sql": state.get("query_sql", ""),
        "result": _truncate_result(state.get("query_result"), state.get("error")),
    }
    completed = list(state.get("completed_queries") or []) + [current_query]
    queried = list(state.get("queried_views") or []) + [view_name]

    base_update = {
        "completed_queries": completed,
        "queried_views": queried,
        "iteration": iteration,
        "selected_view": None,
        "selected_fqn": None,
        "view_definition": None,
        "query_result": None,
        "query_sql": None,
        "query_attempts": 0,
        "error": None,
        "stage": "assessing",
    }

    if iteration >= MAX_VIEW_ITERATIONS:
        logger.info("[mv_agent] max iterations (%d) reached, proceeding to summarize", iteration)
        return {**base_update, "_assess_decision": "summarize"}

    available = list_metric_views_direct()
    available = [v for v in available if v.get("metric_view_name") not in queried]
    if not available:
        logger.info("[mv_agent] no more views available, proceeding to summarize")
        return {**base_update, "_assess_decision": "summarize"}

    gathered_json = json.dumps(
        [{"view_name": q["view_name"], "sql": q["sql"], "result_preview": q["result"]} for q in completed],
        default=str,
    )
    available_summary = json.dumps(
        [{"metric_view_name": v["metric_view_name"], "measures": v.get("measures", []), "dimensions": v.get("dimensions", [])} for v in available[:15]],
        indent=2,
    )

    llm = get_llm()
    content = _ASSESS_PROMPT.format(
        question=question, plan=plan,
        gathered_json=gathered_json, available_json=available_summary,
    )
    response = llm.invoke([HumanMessage(content=content)])
    raw = (response.content or "").strip()

    try:
        decision = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("[mv_agent] assess parse failed, defaulting to summarize: %s", raw[:200])
        return {**base_update, "_assess_decision": "summarize"}

    if decision.get("sufficient", True):
        logger.info("[mv_agent] assess: sufficient (%s)", decision.get("reasoning", ""))
        return {**base_update, "_assess_decision": "summarize"}

    next_plan = decision.get("next_plan", plan)
    logger.info("[mv_agent] assess: need_more, next_plan=%s", next_plan[:200])
    return {**base_update, "plan": next_plan, "_assess_decision": "search"}


def summarize_node(state: MetricViewState) -> dict:
    """LLM synthesizes the final user-facing answer from all query results."""
    question = state["question"]
    plan = state.get("plan", "")
    completed = state.get("completed_queries") or []

    if not completed:
        result = state.get("query_result")
        error = state.get("error")
        completed = [{
            "view_name": state.get("selected_view", "unknown"),
            "fqn": state.get("selected_fqn", ""),
            "sql": state.get("query_sql", ""),
            "result": _truncate_result(result, error),
        }]

    queries_parts = []
    for i, q in enumerate(completed, 1):
        queries_parts.append(
            f"--- Query {i}: {q['view_name']} ---\n"
            f"SQL: {q.get('sql', 'N/A')}\n"
            f"RESULT: {json.dumps(q.get('result', {}), default=str)}"
        )
    queries_block = "\n\n".join(queries_parts)

    summarize_content = _SUMMARIZE_PROMPT.format(
        question=question, plan=plan, queries_block=queries_block,
    )
    llm = get_llm()
    response = llm.invoke([HumanMessage(content=summarize_content)])
    answer = sanitize_output((response.content or "").strip())
    return {"messages": [AIMessage(content=answer)], "stage": "summarizing"}


def answer_no_data_node(state: MetricViewState) -> dict:
    """No metric views found -- tell the user what IS available."""
    question = state["question"]

    available = list_metric_views_direct()
    available_json = json.dumps(available[:20], indent=2) if available else "[]"

    no_data_content = _NO_DATA_PROMPT.format(question=question, available_json=available_json)
    llm = get_llm()
    response = llm.invoke([HumanMessage(content=no_data_content)])
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
        return "assess"
    if state.get("query_attempts", 0) < MAX_QUERY_ATTEMPTS:
        return "query"
    return "assess"


def _route_after_assess(state: MetricViewState) -> str:
    return state.get("_assess_decision", "summarize")


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def _build_metric_view_graph():
    graph = StateGraph(MetricViewState)

    graph.add_node("plan", plan_node)
    graph.add_node("search", search_node)
    graph.add_node("list_fallback", list_fallback_node)
    graph.add_node("query", query_node)
    graph.add_node("assess", assess_node)
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
        "assess": "assess",
        "query": "query",
    })
    graph.add_conditional_edges("assess", _route_after_assess, {
        "summarize": "summarize",
        "search": "search",
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
            "completed_queries": [],
            "queried_views": [],
            "iteration": 0,
            "_assess_decision": None,
        },
        config={"recursion_limit": 30},
    )

    completed = result.get("completed_queries") or []
    final_msg = result["messages"][-1]
    last_view = completed[-1]["view_name"] if completed else result.get("selected_view")
    last_sql = completed[-1].get("sql") if completed else result.get("query_sql")
    return {
        "answer": sanitize_output(final_msg.content if hasattr(final_msg, "content") else str(final_msg)),
        "tool_calls": [],
        "steps": result.get("iteration", 1),
        "metric_view": last_view,
        "sql": last_sql,
        "queries": [{"metric_view": q["view_name"], "sql": q.get("sql", "")} for q in completed],
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
    stage_sequence = ["planning", "searching", "querying", "assessing", "summarizing"]
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
