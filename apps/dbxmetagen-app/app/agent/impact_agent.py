"""Impact analysis agent using a supervisor StateGraph.

Coordinates: identify target -> trace dependencies -> assess severity -> generate report.
Uses graph traversal, FK predictions, lineage, metric views, and ontology.
"""

import logging
from operator import add
from typing import Annotated, Any, Dict, List, Optional

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.prebuilt import create_react_agent
from langgraph.graph import StateGraph, END
from typing_extensions import TypedDict

from agent.common import get_llm, history_to_messages, extract_tool_calls
from agent.guardrails import GuardrailConfig, SAFETY_PROMPT_BLOCK, sanitize_output
from agent.impact_tools import IMPACT_TOOLS
from agent.tracing import trace

logger = logging.getLogger(__name__)

MAX_ITERATIONS = GuardrailConfig.MAX_DEEP_ITERATIONS


class ImpactState(TypedDict):
    messages: Annotated[list, add]
    user_query: str
    target_description: str
    direct_deps: str
    transitive_deps: str
    metric_impact: str
    entity_impact: str
    severity_assessment: str
    next_step: str
    iteration: Annotated[list, add]


SUPERVISOR_PROMPT = f"""You are a schema impact analysis supervisor.
{SAFETY_PROMPT_BLOCK}
Given a user's "what if" question about changing a table, column, or relationship,
decide the SINGLE next step. Respond with EXACTLY one word:

IDENTIFY -- resolve the change target (table, column, entity)
TRACE -- trace direct and transitive dependencies
ASSESS -- evaluate impact on metric views, entities, and severity
RESPOND -- synthesize final impact report

Current state:
- Query: {{user_query}}
- Target: {{target_description}}
- Direct dependencies found: {{has_direct}}
- Metric/entity impact found: {{has_metrics}}
- Severity assessed: {{has_severity}}
- Iteration: {{iteration}}/{MAX_ITERATIONS}

If iteration >= {MAX_ITERATIONS}, you MUST say RESPOND."""


def _build_impact_graph():
    llm = get_llm()
    identifier = create_react_agent(llm, IMPACT_TOOLS[:4])
    tracer = create_react_agent(llm, IMPACT_TOOLS)
    assessor = create_react_agent(llm, IMPACT_TOOLS)

    def supervisor(state: ImpactState):
        iteration = len(state.get("iteration", []))
        prompt = SUPERVISOR_PROMPT.format(
            user_query=state.get("user_query", ""),
            target_description=state.get("target_description", "none"),
            has_direct="yes" if state.get("direct_deps") else "no",
            has_metrics="yes" if state.get("metric_impact") else "no",
            has_severity="yes" if state.get("severity_assessment") else "no",
            iteration=iteration,
        )
        if iteration >= MAX_ITERATIONS:
            return {"next_step": "RESPOND", "iteration": [1]}
        resp = llm.invoke([SystemMessage(content=prompt)] + state["messages"][-5:])
        step = resp.content.strip().upper().split()[0] if resp.content else "RESPOND"
        if step not in ("IDENTIFY", "TRACE", "ASSESS", "RESPOND"):
            step = "RESPOND"
        return {"next_step": step, "iteration": [1]}

    def identify_node(state: ImpactState):
        prompt = f"""Identify the exact table, column, or entity the user wants to change.
Use get_direct_dependencies and get_entity_impact to resolve the target.
Query: {state['user_query']}
Return a concise description of the target and its current role in the schema."""
        result = identifier.invoke({"messages": [HumanMessage(content=prompt)]})
        answer = result["messages"][-1].content if result["messages"] else ""
        return {
            "target_description": answer,
            "messages": [AIMessage(content=f"[Identifier] {answer}")],
        }

    def trace_node(state: ImpactState):
        prompt = f"""Trace all dependencies for this target:
Target: {state.get('target_description', state['user_query'])}

1. Use get_direct_dependencies for FK, lineage, and graph connections.
2. Use traverse_graph for multi-hop transitive dependencies (2-3 hops).
3. Summarize: which tables/columns are directly affected, which are transitively affected."""
        result = tracer.invoke({"messages": [HumanMessage(content=prompt)]})
        answer = result["messages"][-1].content if result["messages"] else ""
        return {
            "direct_deps": answer,
            "messages": [AIMessage(content=f"[Tracer] {answer}")],
        }

    def assess_node(state: ImpactState):
        target = state.get("target_description", state["user_query"])
        deps = state.get("direct_deps", "none found")
        prompt = f"""Assess the impact of changing: {target}
Dependencies found: {deps}

1. Use find_affected_metric_views to check KPI breakage.
2. Use get_entity_impact for ontology-level consequences.
3. Use assess_column_importance on key columns for severity scoring.
4. Find similar_columns that might need the same change.

Produce: metric view impact, entity impact, and severity for each dependency."""
        result = assessor.invoke({"messages": [HumanMessage(content=prompt)]})
        answer = result["messages"][-1].content if result["messages"] else ""
        return {
            "metric_impact": answer,
            "severity_assessment": answer,
            "messages": [AIMessage(content=f"[Assessor] {answer}")],
        }

    def respond_node(state: ImpactState):
        report_prompt = f"""Generate a structured impact analysis report.

## Change Request
{state['user_query']}

## Target
{state.get('target_description', 'Unknown')}

## Dependencies
{state.get('direct_deps', 'Not analyzed')}

## Metric & Entity Impact
{state.get('metric_impact', 'Not analyzed')}

## Severity Assessment
{state.get('severity_assessment', 'Not analyzed')}

FORMAT as a structured report with these sections:
1. **Target** -- what is being changed
2. **Direct Dependencies** -- tables/columns with direct connections (with severity badges)
3. **Transitive Dependencies** -- multi-hop impacts
4. **Metric View Impact** -- which KPIs break
5. **Entity Impact** -- ontology relationship chain
6. **Similar Assets** -- columns/tables needing the same change
7. **Severity Summary** -- HIGH/MEDIUM/LOW counts with justification"""

        resp = get_llm().invoke([SystemMessage(content=SAFETY_PROMPT_BLOCK), HumanMessage(content=report_prompt)])
        return {"messages": [AIMessage(content=resp.content)]}

    def route(state: ImpactState):
        step = state.get("next_step", "RESPOND")
        return {"IDENTIFY": "identify", "TRACE": "trace", "ASSESS": "assess", "RESPOND": "respond"}.get(step, "respond")

    graph = StateGraph(ImpactState)
    graph.add_node("supervisor", supervisor)
    graph.add_node("identify", identify_node)
    graph.add_node("trace", trace_node)
    graph.add_node("assess", assess_node)
    graph.add_node("respond", respond_node)

    graph.set_entry_point("supervisor")
    graph.add_conditional_edges("supervisor", route, {
        "identify": "identify", "trace": "trace",
        "assess": "assess", "respond": "respond",
    })
    graph.add_edge("identify", "supervisor")
    graph.add_edge("trace", "supervisor")
    graph.add_edge("assess", "supervisor")
    graph.add_edge("respond", END)

    return graph.compile()


_compiled = None


def _get_graph():
    global _compiled
    if _compiled is None:
        _compiled = _build_impact_graph()
    return _compiled


@trace(name="impact_analysis", span_type="CHAIN")
def run_impact_analysis(question: str) -> Dict[str, Any]:
    """Run the full impact analysis supervisor agent (synchronous, called from thread)."""
    graph = _get_graph()
    result = graph.invoke({
        "messages": [HumanMessage(content=question)],
        "user_query": question,
        "target_description": "",
        "direct_deps": "",
        "transitive_deps": "",
        "metric_impact": "",
        "entity_impact": "",
        "severity_assessment": "",
        "next_step": "",
        "iteration": [],
    })

    final_msg = result["messages"][-1] if result["messages"] else None
    return {
        "answer": sanitize_output(final_msg.content if final_msg else "No analysis produced."),
        "target": result.get("target_description", ""),
        "steps": len(result.get("iteration", [])),
    }


@trace(name="impact_chat", span_type="CHAIN")
async def run_impact_chat(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """Conversational follow-up for impact questions (lightweight ReAct)."""
    agent = create_react_agent(get_llm(), IMPACT_TOOLS[:6])
    messages = history_to_messages(history, question)

    result = agent.invoke({"messages": messages})
    final_msg = result["messages"][-1] if result["messages"] else None

    return {
        "answer": sanitize_output(final_msg.content if final_msg else ""),
        "tool_calls": list(set(extract_tool_calls(result["messages"]))),
    }
