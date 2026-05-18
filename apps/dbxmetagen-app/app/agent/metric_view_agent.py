"""Metric view agent -- answers business questions using metric views as its semantic layer.

ReAct agent that searches for relevant metric views, inspects their definitions
(measures, dimensions, joins), and queries them using MEASURE() syntax.
Never touches raw tables -- metric views are the bounded query contract.
"""

import json
import logging
import queue
import threading
from typing import Any, Dict, List, Optional

from agent.common import (
    CATALOG, SCHEMA,
    build_react_graph, history_to_messages, extract_tool_calls,
)
from agent.guardrails import GuardrailConfig, SAFETY_PROMPT_BLOCK, sanitize_output
from agent.metric_view_tools import METRIC_VIEW_TOOLS
from agent.tracing import trace, tag_trace

logger = logging.getLogger(__name__)


METRIC_VIEW_SYSTEM_PROMPT = f"""You are a data analyst agent that answers business questions by querying metric views in Databricks Unity Catalog ({CATALOG}.{SCHEMA}).

Metric views are semantic layer objects that bundle governed measures, dimensions, and join paths. You MUST only query through metric views -- never access raw tables directly.

WORKFLOW:
1. **Plan**: Before calling any tools, state in 1-2 sentences what you need to answer the question: which metric or dimension you need, and what query shape (aggregation, filter, trend, comparison, count of distinct values). If the question needs a dimension-only query (e.g. "how many distinct X", "list all Y"), say so explicitly.
2. **Search**: Use search_metric_views to find relevant metric views. The results include measure and dimension names -- use these to pick the best candidate directly.
3. **Describe** (if needed): Call describe_metric_view only if the search results lack sufficient detail about measures, dimensions, or joins.
4. **Query**: Use the right tool for the job:
   - metric_view_query -- for measure aggregations (SUM, AVG, COUNT with measures).
   - metric_view_dimension_query -- for dimension-only questions: counting distinct values, listing unique values, or exploring dimension contents.
5. **Answer**: Present results clearly with context about what was measured and how. Include the metric view name so the user understands the data lineage.

CRITICAL RULES:
- If you say you will query or verify something, you MUST call a tool in the same response. Never promise a query and then answer without executing one.
- If no tool can answer the question, explicitly say so. Do NOT derive, estimate, or use arithmetic as a substitute for querying.
- NEVER fabricate measure or dimension names. Only use names from search or describe results.
- If no metric view matches the question, say so and list what IS available via list_metric_views.
- When the user asks about trends, include a time dimension. When they ask about comparisons, include a categorical dimension.
- For follow-up questions, reuse the same metric view if it covers the new angle; otherwise search again.
- If a query returns an error, explain what went wrong and suggest an alternative approach.
- Keep answers concise. Lead with the key insight, then show supporting data.
{SAFETY_PROMPT_BLOCK}"""


def _build_metric_view_graph():
    return build_react_graph(METRIC_VIEW_TOOLS, METRIC_VIEW_SYSTEM_PROMPT)


_compiled = None


def _get_graph():
    global _compiled
    if _compiled is None:
        _compiled = _build_metric_view_graph()
    return _compiled


@trace(name="metric_view_chat", span_type="CHAIN")
async def run_metric_view_agent(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the metric view agent on a question. Returns answer + tool call metadata."""
    tag_trace(agent="metric_view", session_id=session_id)
    graph = _get_graph()
    messages = history_to_messages(history, question)

    result = await graph.ainvoke(
        {"messages": messages},
        config={"recursion_limit": GuardrailConfig.MAX_RECURSION_LIMIT},
    )

    final_msg = result["messages"][-1]
    return {
        "answer": sanitize_output(final_msg.content),
        "tool_calls": list(set(extract_tool_calls(result["messages"]))),
        "steps": len(result["messages"]),
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

    yield _sse("stage", {"stage": "searching_metric_views"})

    # Capture OBO token from the request context so the background thread
    # can execute SQL as the user, not the app service principal.
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

    yield _sse("stage", {"stage": "processing"})
    try:
        done.get(timeout=300)
    except queue.Empty:
        yield _sse("error", {"error": "Agent timed out after 300 seconds"})
        return

    if error_holder:
        yield _sse("error", {"error": error_holder["error"]})
    else:
        yield _sse("done", {"result": result_holder.get("result", {})})
