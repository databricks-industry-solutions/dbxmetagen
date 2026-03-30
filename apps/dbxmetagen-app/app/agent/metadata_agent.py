"""LangGraph ReAct agent for metadata intelligence.

Combines vector search over metadata_documents, SQL queries on knowledge-base
tables, and Lakebase graph traversal to answer questions about the data catalog.
"""

import asyncio
import json as _json
import os
import logging
from typing import Annotated, Dict, Any, List, Optional, TypedDict

from agent.guardrails import GuardrailConfig, SAFETY_PROMPT_BLOCK, sanitize_output
from agent.common import extract_token_usage
from agent.intent import classify_and_contextualize, format_clarification, keyword_domain
from agent.tracing import trace

from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langchain_core.tools import BaseTool
from databricks_langchain import ChatDatabricks
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, create_react_agent

from agent.metadata_tools import (
    search_metadata, execute_metadata_sql, get_table_summary, get_data_quality,  # noqa: F401
    query_graph_nodes, get_node_details, find_similar_nodes, traverse_graph,  # noqa: F401
    ALL_METADATA_TOOLS, CATALOG, SCHEMA,
)

logger = logging.getLogger(__name__)

MODEL = os.environ.get("LLM_MODEL", "databricks-claude-sonnet-4-6")

QUICK_MAX_TOOL_ROUNDS = 4


class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    intent: str
    tools: list


# ---------------------------------------------------------------------------
# Tool selection
# ---------------------------------------------------------------------------

def select_tools(intent: str) -> List[BaseTool]:
    if intent == "query":
        return [search_metadata, execute_metadata_sql, get_table_summary]
    if intent == "discovery":
        return [search_metadata, get_table_summary, execute_metadata_sql]
    if intent == "relationship":
        return [search_metadata, execute_metadata_sql, query_graph_nodes, traverse_graph]
    if intent == "governance":
        return [search_metadata, execute_metadata_sql, get_data_quality]
    return ALL_METADATA_TOOLS


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

def get_system_prompt(intent: str, tools: List[BaseTool]) -> str:  # noqa: ARG001
    tool_names = [t.name for t in tools]
    prompt = f"""You are a metadata intelligence assistant for a Databricks data catalog.
You help data engineers, analysts, and governance teams understand their data.

Available knowledge base tables in {CATALOG}.{SCHEMA}:
- table_knowledge_base: table_name, comment, domain, subdomain, has_pii, has_phi, row_count
- column_knowledge_base: table_name, column_name, comment, data_type, classification
- ontology_entities: entity_name, entity_type, source_tables, confidence
- fk_predictions: src_table, src_column, dst_table, dst_column, final_confidence, cardinality
- metric_view_definitions: metric_view_name, source_table, json_definition, status
- profiling_results: table_name, column_name, distinct_count, null_count

EFFICIENCY RULES (you MUST follow these):
1. Answer in at most 3 tool rounds. Plan ahead.
2. Call ALL tools you need in PARALLEL in a single round -- do not call them one at a time.
3. On round 1, call search_metadata AND any SQL queries you already know you need simultaneously.
4. get_table_summary is expensive (4 SQL queries internally). Only call it when you truly need a full table picture, and for at most 1 table.
5. If search_metadata returns enough info, answer immediately without further SQL.
6. Be concise and data-driven. Cite specific table/column names.
7. Format answers in markdown with tables when presenting structured data.
""" + SAFETY_PROMPT_BLOCK

    if "execute_metadata_sql" in tool_names:
        prompt += f"\n\nWhen writing SQL, always use catalog/schema prefix: {CATALOG}.{SCHEMA}."

    return prompt


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def _build_graph(tools: List[BaseTool]):
    llm = ChatDatabricks(endpoint=MODEL, temperature=0, max_retries=3)
    tool_node = ToolNode(tools)

    def _count_tool_rounds(messages) -> int:
        return sum(1 for m in messages if hasattr(m, "tool_calls") and m.tool_calls)

    def agent_node(state: AgentState):
        intent = state.get("intent", "general")
        selected = state.get("tools") or select_tools(intent)
        sys_prompt = get_system_prompt(intent, selected)

        rounds = _count_tool_rounds(state["messages"])
        if rounds >= QUICK_MAX_TOOL_ROUNDS:
            sys_prompt += (
                "\n\nIMPORTANT: You have used all your tool rounds. "
                "Do NOT call any more tools. Synthesize your final answer "
                "from the information you have gathered so far."
            )
            msgs = [SystemMessage(content=sys_prompt)] + state["messages"]
            response = llm.invoke(msgs)
        else:
            remaining = QUICK_MAX_TOOL_ROUNDS - rounds
            sys_prompt += f"\n\nYou have {remaining} tool round(s) remaining. Use them wisely."
            bound = llm.bind_tools(selected)
            msgs = [SystemMessage(content=sys_prompt)] + state["messages"]
            response = bound.invoke(msgs)
        return {"messages": [response]}

    def should_continue(state: AgentState):
        last = state["messages"][-1]
        if hasattr(last, "tool_calls") and last.tool_calls:
            return "tools"
        return END

    graph = StateGraph(AgentState)
    graph.add_node("agent", agent_node)
    graph.add_node("tools", tool_node)
    graph.add_edge(START, "agent")
    graph.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})
    graph.add_edge("tools", "agent")
    return graph.compile()


_compiled = None


def _get_graph():
    global _compiled
    if _compiled is None:
        _compiled = _build_graph(ALL_METADATA_TOOLS)
    return _compiled


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

@trace(name="metadata_quick")
async def run_metadata_agent(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
    mode: str = "quick",
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the metadata intelligence agent and return the response.

    mode='quick': fast ReAct agent with intent-based tool selection.
    mode='graphrag': multi-agent (supervisor/planner/retrieval/analyst/respond) with full VS + graph.
    mode='baseline': same multi-agent but restricted to 3 KB tables only.
    mode='deep': legacy alias for 'graphrag'.
    """
    if mode in ("graphrag", "baseline", "deep"):
        from agent.deep_analysis import run_deep_analysis
        effective_mode = "graphrag" if mode == "deep" else mode
        return run_deep_analysis(question, mode=effective_mode, history=history, session_id=session_id)

    # Phase 1: Unified intent classification (run in thread to avoid blocking event loop)
    intent_result = await asyncio.to_thread(classify_and_contextualize, question, history)
    logger.info("[metadata_agent] intent=%s domain=%s clear=%s complexity=%s",
                intent_result.intent_type, intent_result.domain,
                intent_result.question_clear, intent_result.complexity)

    if intent_result.intent_type == "irrelevant":
        return {
            "answer": intent_result.meta_answer or "I'm a metadata analysis assistant. Please ask about your data catalog.",
            "tool_calls": [], "intent": "irrelevant", "steps": 0, "mode": mode, "token_usage": {},
        }
    if intent_result.intent_type == "meta":
        return {
            "answer": intent_result.meta_answer or "I can help you explore metadata across your catalog.",
            "tool_calls": [], "intent": "meta", "steps": 0, "mode": mode, "token_usage": {},
        }
    if not intent_result.question_clear:
        return {
            "answer": format_clarification(intent_result),
            "tool_calls": [], "intent": "clarification", "steps": 0, "mode": mode, "token_usage": {},
        }

    graph = _get_graph()

    messages = []
    if history:
        for msg in history:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if role == "user":
                messages.append(HumanMessage(content=content))
            elif role == "assistant":
                messages.append(AIMessage(content=content))
        messages = messages[-8:]

    intent = intent_result.domain or keyword_domain(question)
    selected_tools = select_tools(intent)
    effective_query = intent_result.context_summary
    messages.append(HumanMessage(content=effective_query))

    result = await graph.ainvoke(
        {"messages": messages, "intent": intent, "tools": selected_tools},
        config={"recursion_limit": GuardrailConfig.MAX_RECURSION_LIMIT},
    )

    final_msg = result["messages"][-1]

    tool_calls_used = []
    for msg in result["messages"]:
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            tool_calls_used.extend([tc["name"] for tc in msg.tool_calls])

    return {
        "answer": sanitize_output(final_msg.content),
        "tool_calls": list(set(tool_calls_used)),
        "intent": intent,
        "steps": len(result["messages"]),
        "mode": mode,
        "token_usage": extract_token_usage(result["messages"]),
    }


# ---------------------------------------------------------------------------
# Plot generation agent
# ---------------------------------------------------------------------------

PLOT_SYSTEM_PROMPT = f"""You are a data visualization agent for a metadata intelligence platform.

You will receive the text of an agent response about data catalog metadata.
Your job is to determine if the response contains data that can be plotted, and if so,
run a SQL query to get precise numbers and return a chart specification.

Available tables in {CATALOG}.{SCHEMA}:
- table_knowledge_base: table_name, comment, domain, subdomain, has_pii, has_phi, row_count
- column_knowledge_base: table_name, column_name, comment, data_type, classification, classification_type
- ontology_entities: entity_id, entity_name, entity_type, description, source_tables, confidence
- fk_predictions: src_table, src_column, dst_table, dst_column, final_confidence, cardinality
- metric_view_definitions: definition_id, metric_view_name, source_table, status
- profiling_results: table_name, column_name, distinct_count, null_count, min_value, max_value
- metadata_generation_log: table_name, mode, status, comment

Use the execute_metadata_sql tool to query the data, then return ONLY a JSON object (no markdown, no extra text) in this exact format:

If data IS suitable for plotting:
{{"type": "bar"|"line"|"pie"|"area", "title": "Chart Title", "data": [{{"name": "Label", "value": 123}}, ...], "xKey": "name", "yKeys": ["value"], "text": "Brief one-sentence caption"}}

For multi-series data use multiple yKeys:
{{"type": "bar", "title": "...", "data": [{{"name": "X", "series1": 10, "series2": 20}}, ...], "xKey": "name", "yKeys": ["series1", "series2"], "text": "..."}}

If data is NOT suitable for plotting:
{{"no_data": true, "reason": "Brief explanation why"}}

Rules:
- Always query the database for precise numbers -- do not invent data.
- Choose the chart type that best represents the data (bar for comparisons, line for time series, pie for proportions).
- Keep data arrays to 20 items or fewer. Aggregate if needed.
- Return ONLY the JSON object, nothing else."""


def create_plot_spec(content: str, history: Optional[List[Dict]] = None) -> Dict:
    """Use an LLM agent to query KB tables and return a chart spec."""
    try:
        llm = ChatDatabricks(endpoint=MODEL, temperature=0, max_retries=3)
        agent = create_react_agent(llm, [execute_metadata_sql])
        context = f"Agent response to visualize:\n\n{content}"
        if history:
            recent = history[-4:]
            context = "Recent conversation:\n" + "\n".join(
                f"{m['role']}: {m['content'][:300]}" for m in recent
            ) + f"\n\nAgent response to visualize:\n\n{content}"

        result = agent.invoke({
            "messages": [SystemMessage(content=PLOT_SYSTEM_PROMPT), HumanMessage(content=context)]
        })

        for msg in reversed(result.get("messages", [])):
            if isinstance(msg, AIMessage) and msg.content:
                text = msg.content.strip()
                if text.startswith("```"):
                    text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                    text = text.rsplit("```", 1)[0].strip()
                return _json.loads(text)
        return {"no_data": True, "reason": "Agent did not produce a chart specification."}
    except Exception as e:
        logger.error(f"Plot agent error: {e}", exc_info=True)
        return {"no_data": True, "reason": f"Error generating chart: {e}"}
