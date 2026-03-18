"""Governance & compliance intelligence agent.

ReAct agent for data governance questions: sensitivity analysis, compliance gaps,
lineage of sensitive data, masking coverage, and re-identification risk assessment.
"""

import logging
from typing import Any, Dict, List, Optional

from agent.common import (
    CATALOG, SCHEMA,
    build_react_graph, history_to_messages, extract_tool_calls,
)
from agent.guardrails import GuardrailConfig, SAFETY_PROMPT_BLOCK, sanitize_output
from agent.governance_tools import GOVERNANCE_TOOLS
from agent.tracing import trace

logger = logging.getLogger(__name__)


GOVERNANCE_SYSTEM_PROMPT = f"""You are a data governance analyst for a Databricks data catalog ({CATALOG}.{SCHEMA}).
You help teams understand data sensitivity, compliance gaps, and protection coverage.

You have access to:
- **Sensitivity classifications** (PII, PHI, PCI) for every column in the catalog
- **Profiling pattern detection** that identifies columns looking like emails, phones, SSNs, etc.
- **Data lineage** showing upstream sources and downstream consumers of sensitive data
- **Column masking policies** showing which classified columns are protected
- **FK relationships** that could enable re-identification through table joins
- **Ontology entity types and column roles** (identifiers vs attributes vs measures)

WORKFLOW:
1. Use get_sensitivity_summary to understand the overall landscape.
2. Use find_classification_gaps to detect unclassified columns with suspicious patterns.
3. Use trace_sensitive_lineage to follow sensitive data through the pipeline.
4. Use check_masking_coverage to find protection gaps.
5. Use find_reidentification_paths to assess cross-table re-identification risks.
6. Use execute_governance_sql for custom queries on governance metadata.

When generating compliance reports, organize by regulation (HIPAA/GDPR/PCI-DSS).
Flag HIGH risks in bold. Be specific about table and column names.
{SAFETY_PROMPT_BLOCK}"""


def _build_governance_graph():
    return build_react_graph(GOVERNANCE_TOOLS, GOVERNANCE_SYSTEM_PROMPT)


_compiled = None


def _get_graph():
    global _compiled
    if _compiled is None:
        _compiled = _build_governance_graph()
    return _compiled


@trace(name="governance_chat", span_type="CHAIN")
async def run_governance_agent(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
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
