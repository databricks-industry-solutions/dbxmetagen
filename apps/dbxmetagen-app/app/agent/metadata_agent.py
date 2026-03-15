"""LangGraph ReAct agent for metadata intelligence.

Combines vector search over metadata_documents, SQL queries on knowledge-base
tables, and Lakebase graph traversal to answer questions about the data catalog.
"""

import os
import logging
from typing import Annotated, Dict, Any, List, Optional, TypedDict

from agent.guardrails import GuardrailConfig, SAFETY_PROMPT_BLOCK, sanitize_output

from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langchain_core.tools import BaseTool
from databricks_langchain import ChatDatabricks
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

from agent.metadata_tools import (
    search_metadata, execute_metadata_sql, get_table_summary, get_data_quality,  # noqa: F401
    query_graph_nodes, get_node_details, find_similar_nodes, traverse_graph,  # noqa: F401
    ALL_METADATA_TOOLS, CATALOG, SCHEMA,
)

logger = logging.getLogger(__name__)

MODEL = os.environ.get("LLM_MODEL", os.environ.get("GRAPHRAG_MODEL", "databricks-claude-sonnet-4-5"))


class AgentState(TypedDict):
    messages: Annotated[list, add_messages]


# ---------------------------------------------------------------------------
# Intent classification + tool selection
# ---------------------------------------------------------------------------

def classify_intent(message: str) -> str:
    m = message.lower()
    if any(kw in m for kw in ["pii", "phi", "sensitive", "classify", "governance", "security", "compliance"]):
        return "governance"
    if any(kw in m for kw in ["foreign key", "fk", "join", "relationship", "connected", "related to", "references"]):
        return "relationship"
    if any(kw in m for kw in ["how many", "count", "list", "show me", "select", "average", "total", "sum"]):
        return "query"
    if any(kw in m for kw in ["find", "search", "similar", "about", "describe", "what is", "what are", "explain"]):
        return "discovery"
    return "general"


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
You help data engineers, analysts, and governance teams understand their data:
- What tables and columns exist, what they mean, and how they relate
- Data quality and profiling insights
- Ontology entity classifications and domain structure
- Foreign key relationships and join patterns
- Metric view definitions and business KPIs

Available knowledge base tables in {CATALOG}.{SCHEMA}:
- table_knowledge_base: Table-level metadata (comment, domain, subdomain, has_pii, has_phi, row_count)
- column_knowledge_base: Column-level metadata (comment, data_type, classification)
- ontology_entities: Discovered entity types (entity_name, entity_type, source_tables, confidence)
- fk_predictions: FK relationships (src_table, src_column, dst_table, dst_column, final_confidence, cardinality)
- metric_view_definitions: Generated metric views (metric_view_name, source_table, source_questions, json_definition, status)
- profiling_results: Statistical profiling (table_name, column_name, distinct_count, null_count)

BEHAVIOR:
- Always start with search_metadata to gather relevant context before running SQL queries. You may call it multiple times with different queries or doc_type filters.
- Be concise and data-driven. Cite specific table/column names.
- Use execute_metadata_sql for precise counts, filters, or aggregations.
- Use get_table_summary for a full picture of one table.
- When asked about relationships, check both fk_predictions and the knowledge graph.
- Format answers in markdown with tables when presenting structured data.
""" + SAFETY_PROMPT_BLOCK

    if "execute_metadata_sql" in tool_names:
        prompt += f"\n\nWhen writing SQL, always use catalog/schema prefix: {CATALOG}.{SCHEMA}."

    return prompt


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

MAX_TOOL_ROUNDS = GuardrailConfig.MAX_AGENT_ITERATIONS

def _build_graph(tools: List[BaseTool]):
    llm = ChatDatabricks(endpoint=MODEL, temperature=0, max_retries=3)
    llm_with_tools = llm.bind_tools(tools)
    tool_node = ToolNode(tools)

    def _count_tool_rounds(messages) -> int:
        return sum(1 for m in messages if hasattr(m, "tool_calls") and m.tool_calls)

    def agent_node(state: AgentState):
        intent = classify_intent(state["messages"][-1].content if hasattr(state["messages"][-1], "content") else "")
        selected = select_tools(intent)
        sys_prompt = get_system_prompt(intent, selected)

        rounds = _count_tool_rounds(state["messages"])
        if rounds >= MAX_TOOL_ROUNDS:
            sys_prompt += (
                "\n\nIMPORTANT: You have already called tools many times. "
                "Do NOT call any more tools. Synthesize your final answer "
                "from the information you have gathered so far."
            )
            msgs = [SystemMessage(content=sys_prompt)] + state["messages"]
            response = llm.invoke(msgs)
        else:
            msgs = [SystemMessage(content=sys_prompt)] + state["messages"]
            response = llm_with_tools.invoke(msgs)
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

async def run_metadata_agent(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
    mode: str = "quick",
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
        return run_deep_analysis(question, mode=effective_mode, history=history)

    # Quick mode: existing ReAct agent
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

    intent = classify_intent(question)
    messages.append(HumanMessage(content=question))

    result = await graph.ainvoke(
        {"messages": messages},
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
    }
