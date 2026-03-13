"""DEPRECATED: Standalone Graph Explorer agent.

Superseded by the GraphRAG Analysis mode in agent/deep_analysis.py.
Kept for backward compatibility with any existing callers.
"""

import os
import logging
from typing import Annotated, TypedDict

from langchain_core.messages import HumanMessage, SystemMessage
from databricks_langchain import ChatDatabricks
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

from agent.tools import ALL_TOOLS

logger = logging.getLogger(__name__)

MODEL = os.environ.get("GRAPHRAG_MODEL", "databricks-claude-sonnet-4-5")


class AgentState(TypedDict):
    messages: Annotated[list, add_messages]


SYSTEM_PROMPT = """You are a knowledge graph analyst for a data catalog.
You have access to tools that let you query and traverse a knowledge graph
of tables, columns, schemas, and entities stored in a Lakebase-managed catalog.

When answering a question:
1. Start by searching for relevant nodes using query_graph_nodes.
2. Use traverse_graph to do multi-hop traversal from a starting node -- this
   returns connected nodes, edges, and paths in one call.
3. Use get_node_details for deeper info on specific nodes.
4. Use find_similar_nodes to discover related tables/columns via embeddings.
5. Synthesize your findings into a clear answer.

Always cite specific node ids and relationships in your answer."""


def _build_graph():
    llm = ChatDatabricks(endpoint=MODEL, temperature=0)
    llm_with_tools = llm.bind_tools(ALL_TOOLS)
    tool_node = ToolNode(ALL_TOOLS)

    def agent_node(state: AgentState):
        msgs = [SystemMessage(content=SYSTEM_PROMPT)] + state["messages"]
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
        _compiled = _build_graph()
    return _compiled


async def run_graph_agent(question: str, max_hops: int = 3) -> dict:
    """Run the GraphRAG agent and return the final answer."""
    graph = _get_graph()
    initial_state = {"messages": [HumanMessage(content=question)]}

    result = await graph.ainvoke(initial_state, config={"recursion_limit": max_hops * 2 + 2})

    # Extract the final assistant message
    final_msg = result["messages"][-1]
    return {
        "answer": final_msg.content,
        "steps": len(result["messages"]),
    }
