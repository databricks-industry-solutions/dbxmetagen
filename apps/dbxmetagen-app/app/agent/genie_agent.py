"""LangGraph ReAct agent for generating Genie space serialized_space JSON.

Receives pre-assembled context from GenieContextAssembler, then iteratively
generates the serialized_space JSON using test_sql and sample_values tools.
"""

import json
import logging
import queue
from typing import Any, Dict, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Format, Disposition
from langchain_community.chat_models import ChatDatabricks
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT_TEMPLATE = """You are a Genie Space configuration expert. Your job is to generate a
comprehensive `serialized_space` JSON for the Databricks Genie API based on the
metadata context below.

=== METADATA CONTEXT ===
{context_text}

=== PRE-BUILT SECTIONS (use as starting point) ===
data_sources: {data_sources}
join_specs: {join_specs}
sql_snippets (measures/filters/expressions from metric views): {sql_snippets}

=== USER QUESTIONS (optional guidance) ===
{questions}

=== REFERENCE: BEST PRACTICES ===
{reference_text}

=== OUTPUT REQUIREMENTS ===
Produce a JSON object with this simplified schema (post-processing will add IDs and restructure for the API):
{{
  "description": "<1-2 sentence description of what this Genie space helps users explore>",
  "data_sources": {{ "tables": [...], "metric_views": [...] }},
  "instructions": {{
    "text": "<markdown instructions describing the data, relationships, and business rules>",
    "example_sql": [ {{ "question": "...", "sql": "..." }}, ... ],
    "join_specs": [ {{ "left": {{ "identifier": "catalog.schema.table1" }}, "right": {{ "identifier": "catalog.schema.table2" }}, "sql": ["table1.col = table2.col"] }}, ... ],
    "sql_snippets": {{
      "filters": [ {{ "display_name": "...", "sql": ["..."] }}, ... ],
      "expressions": [ {{ "alias": "...", "display_name": "...", "sql": ["..."] }}, ... ],
      "measures": [ {{ "alias": "...", "display_name": "...", "sql": ["..."] }}, ... ]
    }}
  }},
  "sample_questions": [ "...", ... ]
}}

RULES:
1. Use the test_sql tool to validate every SQL expression BEFORE including it.
2. Use sample_values to get real values for filter suggestions.
3. Table references in SQL must be fully qualified (catalog.schema.table). Column references in sql_snippets (measures, filters, expressions) MUST use table.column format (e.g. `fact_ed_wait_times.wait_minutes`), never bare column names.
4. Generate at least 5 example_sql pairs, 3 filters, 3 measures.
5. Return the final JSON as your LAST message, wrapped in ```json ``` fences.
6. Do NOT add id fields -- post-processing handles that automatically.
7. METRIC VIEW ROUTING: metric_views listed in data_sources are APPLIED Unity Catalog objects -- Genie queries them natively via MEASURE(). sql_snippets contain measures decomposed from UNAPPLIED (validated-only) definitions plus filter suggestions. NEVER duplicate the same measure in both data_sources.metric_views and sql_snippets.measures.
8. JOINS: If the pre-built join_specs are empty but FOREIGN KEY RELATIONSHIPS or shared column names exist in the metadata context, you MUST generate join_specs. Each join_spec needs left/right identifiers (fully qualified table names) and a sql array with join conditions using table.column format.
9. DATE FUNCTIONS: Use Databricks/Spark SQL syntax for all date functions.
   - TIMESTAMPADD(MONTH, 1, col) -- unit is a bare keyword, singular, NO quotes
   - TIMESTAMPDIFF(MINUTE, start, end) -- same: bare keyword, no quotes
   - DATE_TRUNC('MONTH', col) -- interval IS single-quoted
   - EXTRACT(HOUR FROM col) -- use EXTRACT, not DATE_PART
   - DATEDIFF(end, start) returns days only (2 args). For other units use TIMESTAMPDIFF.
   NEVER use plural units (MONTHS, HOURS) or quoted units in TIMESTAMPADD/TIMESTAMPDIFF.
"""


def _make_tools(ws: WorkspaceClient, warehouse_id: str):
    """Build langchain tools bound to this warehouse."""

    @tool
    def test_sql(sql: str) -> str:
        """Test a SQL query for syntax/reference errors by running it with LIMIT 0.
        Returns 'OK' or the error message."""
        test_q = f"SELECT * FROM ({sql}) t LIMIT 0"
        try:
            r = ws.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=test_q,
                wait_timeout="20s",
                format=Format.JSON_ARRAY,
                disposition=Disposition.INLINE,
            )
            state = r.status.state.value if r.status and r.status.state else "UNKNOWN"
            if state in ("SUCCEEDED", "CLOSED"):
                return "OK"
            err = (
                r.status.error.message
                if r.status and r.status.error
                else "Unknown error"
            )
            return f"ERROR: {err}"
        except Exception as e:
            return f"ERROR: {e}"

    @tool
    def sample_values(table: str, column: str, limit: int = 20) -> str:
        """Get distinct values for a column. Useful for building filter suggestions."""
        q = f"SELECT DISTINCT `{column}` AS val FROM {table} WHERE `{column}` IS NOT NULL LIMIT {limit}"
        try:
            r = ws.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=q,
                wait_timeout="20s",
                format=Format.JSON_ARRAY,
                disposition=Disposition.INLINE,
            )
            state = r.status.state.value if r.status and r.status.state else "UNKNOWN"
            if state not in ("SUCCEEDED", "CLOSED"):
                err = (
                    r.status.error.message if r.status and r.status.error else "Unknown"
                )
                return f"ERROR: {err}"
            if r.result and r.result.data_array:
                vals = [row[0] for row in r.result.data_array if row[0] is not None]
                return json.dumps(vals)
            return "[]"
        except Exception as e:
            return f"ERROR: {e}"

    return [test_sql, sample_values]


def run_genie_agent(
    ws: WorkspaceClient,
    warehouse_id: str,
    context: Dict[str, Any],
    progress_queue: queue.Queue,
    model_endpoint: str = "databricks-claude-3-7-sonnet",
) -> Dict[str, Any]:
    """Run the Genie builder agent and emit progress events to the queue.

    Returns the final serialized_space dict or raises on failure.
    """
    progress_queue.put({"stage": "initializing"})

    tools = _make_tools(ws, warehouse_id)
    llm = ChatDatabricks(endpoint=model_endpoint, temperature=0.1, max_tokens=16384)

    questions_text = (
        "\n".join(f"- {q}" for q in context.get("questions", [])) or "None provided"
    )
    system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(
        context_text=context["context_text"],
        data_sources=json.dumps(context["data_sources"], indent=2),
        join_specs=json.dumps(context["join_specs"], indent=2),
        sql_snippets=json.dumps(context.get("sql_snippets", {}), indent=2),
        questions=questions_text,
        reference_text=context.get("reference_text", ""),
    )

    agent = create_react_agent(llm, tools, prompt=system_prompt)
    progress_queue.put({"stage": "generating"})

    try:
        result = agent.invoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": "Generate the complete serialized_space JSON now.",
                    }
                ]
            }
        )
    except Exception as e:
        progress_queue.put({"stage": "error", "message": str(e)})
        raise

    progress_queue.put({"stage": "parsing"})

    # Extract JSON from the final message
    final_content = ""
    for msg in reversed(result.get("messages", [])):
        if hasattr(msg, "content") and msg.content:
            final_content = msg.content
            break

    serialized = _extract_json(final_content)
    if serialized is None:
        progress_queue.put(
            {"stage": "error", "message": "Agent did not produce valid JSON"}
        )
        raise ValueError("Agent did not produce valid serialized_space JSON")

    progress_queue.put({"stage": "done", "result": serialized})
    return serialized


def _extract_json(text: str) -> Optional[dict]:
    """Extract a JSON object from text, looking for ```json fences first."""
    import re

    m = re.search(r"```(?:json)?\s*\n(.*?)```", text, re.DOTALL)
    candidate = m.group(1).strip() if m else text.strip()
    # Try to find the outermost { ... }
    start = candidate.find("{")
    if start == -1:
        return None
    depth, end = 0, start
    for i in range(start, len(candidate)):
        if candidate[i] == "{":
            depth += 1
        elif candidate[i] == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    try:
        return json.loads(candidate[start:end])
    except json.JSONDecodeError:
        return None
