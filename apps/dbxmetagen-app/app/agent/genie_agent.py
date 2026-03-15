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

from agent.guardrails import GuardrailConfig, SAFETY_PROMPT_BLOCK

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT_TEMPLATE = """You are a Genie Space configuration expert. Your job is to generate a
comprehensive, production-quality `serialized_space` JSON for the Databricks Genie API
based on the metadata context below. A great Genie space is one that business users
can immediately query in natural language and get correct, insightful answers.

=== METADATA CONTEXT ===
{context_text}

=== PRE-BUILT SECTIONS (use as starting point, enrich and expand) ===
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
      "expressions": [ {{ "alias": "...", "display_name": "...", "sql": ["..."], "synonyms": ["..."] }}, ... ],
      "measures": [ {{ "alias": "...", "display_name": "...", "sql": ["..."], "synonyms": ["..."], "description": "..." }}, ... ]
    }}
  }},
  "sample_questions": [ "...", ... ]
}}

RULES:
1. Use the test_sql tool to validate every SQL expression BEFORE including it.
2. Use sample_values to get real values for filter suggestions.
3. Use describe_columns to understand column semantics when metadata is sparse.
4. Table references in SQL must be fully qualified (catalog.schema.table). Column references in sql_snippets (measures, filters, expressions) MUST use table.column format (e.g. `fact_ed_wait_times.wait_minutes`), never bare column names.
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

QUALITY REQUIREMENTS (critical):
10. SYNONYMS: Every measure and expression MUST have a "synonyms" array with 1-4 business-friendly alternative names. Think about how non-technical users would ask for this metric. E.g. "total_revenue" -> ["revenue", "sales", "total sales"]. Pre-built synonyms are provided -- keep them and add more if useful.
11. DESCRIPTIONS: Every measure MUST have a "description" explaining what it calculates and when to use it.
12. COMPLEXITY: Generate at least 8 example_sql pairs covering: simple aggregation, multi-table join, time-series trend, top-N ranking, filtered aggregation, ratio/rate, comparison, and at least one window function. Generate at least 5 measures, 5 filters, and 3 expressions.
13. DISAMBIGUATION: In the text instructions, explicitly disambiguate columns that appear in multiple tables (e.g. "created_date in orders vs created_date in accounts"). Also explain which table to use for common questions.
14. FILTERS: Generate time-based filters (last 30 days, YTD, current month) for every date column, plus categorical filters for low-cardinality string columns. Each filter should have a clear display_name.
15. SAMPLE QUESTIONS: Generate 8-12 sample questions spanning different complexity levels and covering all major tables.
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

    @tool
    def describe_columns(table: str) -> str:
        """Get column names, types, and comments for a table. Useful when metadata
        context is sparse and you need to understand what columns are available."""
        q = f"DESCRIBE TABLE {table}"
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
                err = r.status.error.message if r.status and r.status.error else "Unknown"
                return f"ERROR: {err}"
            if r.result and r.result.data_array:
                cols = [c.name for c in r.manifest.schema.columns]
                rows = [dict(zip(cols, row)) for row in r.result.data_array]
                lines = []
                for row in rows:
                    name = row.get("col_name", "")
                    if not name or name.startswith("#"):
                        continue
                    dtype = row.get("data_type", "")
                    comment = row.get("comment", "")
                    line = f"  {name} ({dtype})"
                    if comment:
                        line += f" -- {comment}"
                    lines.append(line)
                return "\n".join(lines) if lines else "No columns found"
            return "No columns found"
        except Exception as e:
            return f"ERROR: {e}"

    return [test_sql, sample_values, describe_columns]


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
    llm = ChatDatabricks(endpoint=model_endpoint, temperature=0.1, max_tokens=16384, max_retries=3)

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
    system_prompt += SAFETY_PROMPT_BLOCK

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
            },
            config={"recursion_limit": GuardrailConfig.MAX_RECURSION_LIMIT},
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

    serialized = _backfill_synonyms(serialized)
    warnings = _validate_output(serialized)
    if warnings:
        logger.warning("Genie output quality warnings: %s", "; ".join(warnings))

    progress_queue.put({"stage": "done", "result": serialized, "warnings": warnings})
    return serialized


def _backfill_synonyms(raw: dict) -> dict:
    """Ensure every measure and expression has synonyms; generate them if missing."""
    from .genie_builder import _generate_synonyms

    snippets = raw.get("instructions", {}).get("sql_snippets", {})
    if not snippets:
        snippets = raw.get("sql_snippets", {})
    if not snippets:
        return raw
    for category in ("measures", "expressions"):
        for item in snippets.get(category, []):
            if not item.get("synonyms"):
                alias = item.get("alias") or item.get("name") or ""
                desc = item.get("description", "")
                item["synonyms"] = _generate_synonyms(alias, comment=desc) or None
    return raw


def _validate_output(raw: dict) -> list[str]:
    """Return a list of warnings about missing or thin sections."""
    warnings = []
    ds = raw.get("data_sources", {})
    if not ds.get("tables"):
        warnings.append("No tables in data_sources")
    inst = raw.get("instructions", {})
    examples = inst.get("example_sql") or inst.get("example_question_sqls") or []
    if len(examples) < 5:
        warnings.append(f"Only {len(examples)} example_sql pairs (target: 8+)")
    snip = inst.get("sql_snippets") or raw.get("sql_snippets") or {}
    if len(snip.get("measures", [])) < 3:
        warnings.append(f"Only {len(snip.get('measures', []))} measures (target: 5+)")
    if len(snip.get("filters", [])) < 3:
        warnings.append(f"Only {len(snip.get('filters', []))} filters (target: 5+)")
    if len(snip.get("expressions", [])) < 3:
        warnings.append(f"Only {len(snip.get('expressions', []))} expressions (target: 3+)")
    sqs = raw.get("sample_questions", [])
    if len(sqs) < 8:
        warnings.append(f"Only {len(sqs)} sample questions (target: 8+)")
    if not inst.get("text") and not inst.get("text_instructions"):
        warnings.append("Missing text instructions")
    return warnings


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
