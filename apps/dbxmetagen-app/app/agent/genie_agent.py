"""LangGraph ReAct agent for generating Genie space serialized_space JSON.

Receives pre-assembled context from GenieContextAssembler, then iteratively
generates the serialized_space JSON using test_sql and sample_values tools.
"""

import json
import logging
import os
import queue
import time
from typing import Any, Dict, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Format, Disposition
from langchain_community.chat_models import ChatDatabricks
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent
from agent.tracing import trace

from agent.guardrails import GuardrailConfig, SAFETY_PROMPT_BLOCK

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT_TEMPLATE = """You are a Genie Space configuration expert. Your job is to generate a
comprehensive, production-quality `serialized_space` JSON for the Databricks Genie API
based on the metadata context below. A great Genie space is one that business users
can immediately query in natural language and get correct, insightful answers.

=== METADATA CONTEXT ===
{context_text}

=== PRE-BUILT SECTIONS (FINALIZED -- do NOT regenerate these) ===
data_sources: {data_sources}
join_specs: {join_specs}

=== PRE-BUILT SQL SNIPPETS (FINALIZED -- these will be merged automatically) ===
The following measures, filters, and expressions are pre-built from validated metric views
and sampled data. They are ALREADY CORRECT and will be merged into your output automatically.
Do NOT include these in your sql_snippets -- only generate ADDITIONAL measures/filters/expressions
that are NOT already covered below:
{sql_snippets}

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
12. COMPLEXITY: Generate at least 8 example_sql pairs covering: simple aggregation, multi-table join, time-series trend, top-N ranking, filtered aggregation, ratio/rate, comparison, and at least one window function. Generate at least 5 NEW measures, 5 NEW filters, and 3 NEW expressions beyond the pre-built ones.
13. DISAMBIGUATION: In the text instructions, explicitly disambiguate columns that appear in multiple tables (e.g. "created_date in orders vs created_date in accounts"). Also explain which table to use for common questions.
14. FILTERS: Generate time-based filters (last 30 days, YTD, current month) for every date column, plus categorical filters for low-cardinality string columns. Each filter should have a clear display_name.
15. SAMPLE QUESTIONS vs EXAMPLE SQL (IMPORTANT -- these serve DIFFERENT purposes):
    - "sample_questions": Shown in the Genie UI as clickable starter questions for BUSINESS USERS. Must be plain English, non-technical, no SQL jargon. 8-12 questions like "What were total sales last quarter?" or "Who are our top customers?"
    - "example_sql": Few-shot examples that teach Genie HOW to translate natural language to SQL. Each needs a question AND working SQL. Cover diverse SQL patterns (joins, aggregations, window functions, filters). Do NOT duplicate the same question in both lists.

EFFICIENCY (important -- you have a limited number of tool-call rounds):
16. BATCH your tool calls: call test_sql, sample_values, and describe_columns in PARALLEL within a single round when possible, rather than one at a time.
17. Focus validation on the most complex SQL (joins, window functions, CASE expressions). Simple aggregations like COUNT(*) and SUM(col) don't need test_sql validation.
18. After finishing tool calls, your FINAL message MUST contain the complete JSON wrapped in ```json ``` fences. Budget your rounds so you always have at least one round left for the final output.
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


def _extract_json_from_messages(messages: list) -> Optional[dict]:
    """Search all AI messages (newest first) for a valid JSON object.

    First pass: require at least one expected key.
    Second pass: accept any non-trivial dict (fallback for unexpected schemas).
    """
    candidates: list[dict] = []
    for msg in reversed(messages):
        content = getattr(msg, "content", None) or ""
        if not content or not isinstance(content, str):
            continue
        parsed = _extract_json(content)
        if not parsed:
            continue
        if "data_sources" in parsed or "instructions" in parsed or "sample_questions" in parsed:
            return parsed
        if len(parsed) >= 2:
            candidates.append(parsed)
    if candidates:
        logger.warning(
            "No message had expected keys; returning best candidate with %d keys: %s",
            len(candidates[0]), list(candidates[0].keys())[:5],
        )
        return candidates[0]
    return None


def _log_message_diagnostics(messages: list):
    """Log information about agent messages to help debug JSON extraction failures."""
    if not messages:
        logger.warning("Agent produced 0 messages")
        return
    logger.warning("Agent produced %d messages", len(messages))
    for i, msg in enumerate(reversed(messages)):
        content = getattr(msg, "content", None) or ""
        msg_type = type(msg).__name__
        if not isinstance(content, str):
            logger.warning("  msg[-%d] %s: content is %s (not str)", i + 1, msg_type, type(content).__name__)
            continue
        has_json_fence = "```json" in content or "```\n{" in content
        has_brace = "{" in content
        logger.warning(
            "  msg[-%d] %s: %d chars, has_fence=%s, has_brace=%s, preview=%.200s",
            i + 1, msg_type, len(content), has_json_fence, has_brace,
            content[:200].replace("\n", "\\n"),
        )
        if i >= 4:
            break


GENIE_WALL_TIMEOUT = 600  # 10 minutes hard deadline


def _count_tool_messages(messages: list) -> int:
    """Count ToolMessage instances to determine the current tool round."""
    from langchain_core.messages import ToolMessage
    rounds = 0
    prev_was_tool = False
    for m in messages:
        is_tool = isinstance(m, ToolMessage)
        if is_tool and not prev_was_tool:
            rounds += 1
        prev_was_tool = is_tool
    return rounds


def _collect_stream_messages(
    agent,
    input_msg: dict,
    config: dict,
    progress_queue: queue.Queue | None = None,
    deadline: float | None = None,
) -> tuple[list, int, bool]:
    """Run agent via stream(), collecting accumulated messages.

    Returns (messages, tool_round_count, timed_out).
    """
    messages = []
    last_round = 0
    timed_out = False
    for chunk in agent.stream(input_msg, config=config, stream_mode="values"):
        messages = chunk.get("messages", messages)
        current_round = _count_tool_messages(messages)
        if current_round > last_round and progress_queue:
            progress_queue.put({"stage": "tool_round", "round": current_round})
            last_round = current_round
        if deadline and time.time() > deadline:
            timed_out = True
            logger.warning("Genie agent hit %ds wall-clock deadline at round %d", GENIE_WALL_TIMEOUT, current_round)
            break
    return messages, last_round, timed_out


def _messages_to_langchain(messages: list) -> list:
    """Convert LangGraph messages to simple dicts safe for a raw LLM invoke.

    Keeps only human/assistant turns (tool messages confuse a non-agent LLM).
    Truncates to the last N messages to stay within context limits.
    """
    from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
    converted = []
    for m in messages:
        if isinstance(m, SystemMessage):
            converted.append({"role": "system", "content": m.content or ""})
        elif isinstance(m, HumanMessage):
            converted.append({"role": "user", "content": m.content or ""})
        elif isinstance(m, AIMessage) and m.content and isinstance(m.content, str):
            converted.append({"role": "assistant", "content": m.content})
    # Keep system + last 6 turns to stay within context limits
    system_msgs = [m for m in converted if m["role"] == "system"]
    non_system = [m for m in converted if m["role"] != "system"]
    return system_msgs + non_system[-6:]


def _recovery_invoke(
    messages: list,
    model_endpoint: str,
) -> Optional[dict]:
    """One-shot LLM call asking the agent to output just the JSON."""
    try:
        converted = _messages_to_langchain(messages)
        converted.append({
            "role": "user",
            "content": (
                "Your previous response did not contain valid JSON. "
                "Output ONLY the complete serialized_space JSON object now, "
                "wrapped in ```json ``` fences. No explanation, no tool calls."
            ),
        })
        recovery_llm = ChatDatabricks(
            endpoint=model_endpoint, temperature=0.0, max_tokens=16384,
            max_retries=2, request_timeout=300,
        )
        result = recovery_llm.invoke(converted)
        content = getattr(result, "content", "") or ""
        logger.info("Recovery response length: %d chars", len(content))
        return _extract_json(content)
    except Exception as exc:
        logger.warning("Recovery invoke failed: %s", exc)
        return None


@trace(name="genie_generate")
def run_genie_agent(
    ws: WorkspaceClient,
    warehouse_id: str,
    context: Dict[str, Any],
    progress_queue: queue.Queue,
    model_endpoint: str = os.environ.get("LLM_MODEL", "databricks-claude-sonnet-4-6"),
) -> Dict[str, Any]:
    """Run the Genie builder agent and emit progress events to the queue.

    Returns the final serialized_space dict or raises on failure.
    """
    start_time = time.time()
    deadline = start_time + GENIE_WALL_TIMEOUT

    def _elapsed() -> float:
        return round(time.time() - start_time, 1)

    def _error_event(message: str, *, rounds: int = 0) -> dict:
        return {
            "stage": "error",
            "message": message,
            "elapsed_seconds": _elapsed(),
            "rounds_completed": rounds,
        }

    progress_queue.put({"stage": "initializing"})

    tools = _make_tools(ws, warehouse_id)
    llm = ChatDatabricks(
        endpoint=model_endpoint, temperature=0.1, max_tokens=16384,
        max_retries=2, request_timeout=300,
    )

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

    input_msg = {
        "messages": [
            {"role": "user", "content": "Generate the complete serialized_space JSON now."}
        ]
    }
    agent_config = {"recursion_limit": max(GuardrailConfig.MAX_RECURSION_LIMIT, 30)}

    messages: list = []
    timed_out = False
    round_count = 0
    try:
        messages, round_count, timed_out = _collect_stream_messages(
            agent, input_msg, agent_config,
            progress_queue=progress_queue,
            deadline=deadline,
        )
    except Exception as e:
        round_count = _count_tool_messages(messages) if messages else 0
        err_str = str(e).lower()
        if "timeout" in err_str or "timed out" in err_str or isinstance(e, TimeoutError):
            logger.warning("Agent exception timeout after %.1fs, %d rounds: %s", _elapsed(), round_count, e)
            timed_out = True
        else:
            progress_queue.put(_error_event(f"Agent error: {e}", rounds=round_count))
            raise

    progress_queue.put({"stage": "parsing"})

    serialized = _extract_json_from_messages(messages) if messages else None

    if serialized is None:
        _log_message_diagnostics(messages)
        if messages:
            reason = "timed out" if timed_out else "no valid JSON in output"
            logger.warning(
                "JSON extraction failed (%s) after %.1fs, %d rounds -- attempting recovery",
                reason, _elapsed(), round_count,
            )
            progress_queue.put({"stage": "recovering"})
            serialized = _recovery_invoke(messages, model_endpoint)

    if serialized is None:
        detail = (
            f"Agent did not produce valid JSON after {_elapsed():.0f}s "
            f"and {round_count} tool round(s)."
        )
        if timed_out:
            detail += " The 10-minute timeout was reached. Try selecting fewer tables."
        progress_queue.put(_error_event(detail, rounds=round_count))
        raise ValueError(detail)

    serialized = _merge_prebuilt_snippets(serialized, context.get("sql_snippets", {}))
    serialized = _dedup_sample_vs_example(serialized)
    serialized = _backfill_synonyms(serialized)
    warnings = _validate_output(serialized)
    if warnings:
        logger.warning("Genie output quality warnings: %s", "; ".join(warnings))

    progress_queue.put({
        "stage": "done",
        "result": serialized,
        "warnings": warnings,
        "elapsed_seconds": _elapsed(),
        "rounds_completed": round_count,
    })
    return serialized


def _merge_prebuilt_snippets(raw: dict, prebuilt: dict) -> dict:
    """Merge pre-built sql_snippets into agent output, deduplicating by alias/display_name."""
    if not prebuilt:
        return raw
    inst = raw.get("instructions", {})
    agent_snippets = inst.get("sql_snippets") or raw.get("sql_snippets") or {}

    for category, key_field in [("measures", "alias"), ("expressions", "alias"), ("filters", "display_name")]:
        prebuilt_items = prebuilt.get(category, [])
        if not prebuilt_items:
            continue
        agent_items = agent_snippets.get(category, [])
        existing_keys = {(item.get(key_field) or "").lower() for item in agent_items}
        for item in prebuilt_items:
            item_key = (item.get(key_field) or "").lower()
            if item_key and item_key not in existing_keys:
                agent_items.append(item)
                existing_keys.add(item_key)
        agent_snippets[category] = agent_items

    if "sql_snippets" in inst:
        inst["sql_snippets"] = agent_snippets
    elif "sql_snippets" in raw:
        raw["sql_snippets"] = agent_snippets
    else:
        inst["sql_snippets"] = agent_snippets
        raw["instructions"] = inst
    return raw


def _dedup_sample_vs_example(raw: dict) -> dict:
    """Remove sample_questions that are near-duplicates of example_sql questions."""
    inst = raw.get("instructions", {})
    examples = inst.get("example_sql") or inst.get("example_question_sqls") or []
    example_questions = set()
    for ex in examples:
        q = ex.get("question", "")
        if isinstance(q, list):
            q = q[0] if q else ""
        example_questions.add(q.lower().strip().rstrip("?"))

    sample_qs = raw.get("sample_questions", [])
    if not sample_qs or not example_questions:
        return raw

    filtered = []
    for sq in sample_qs:
        q = sq if isinstance(sq, str) else (sq.get("question", [""])[0] if isinstance(sq.get("question"), list) else sq.get("question", ""))
        if q.lower().strip().rstrip("?") not in example_questions:
            filtered.append(sq)
    raw["sample_questions"] = filtered
    return raw


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

    # Try fenced block (with or without newline after fence)
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if not m:
        m = re.search(r"```(?:json)?\s*\n(.*?)```", text, re.DOTALL)
    candidate = m.group(1).strip() if m else text.strip()

    # Try direct json.loads first (handles the common clean case)
    start = candidate.find("{")
    if start == -1:
        return None
    try:
        return json.loads(candidate[start:])
    except json.JSONDecodeError:
        pass

    # Fall back to brace-counting, but skip braces inside strings
    depth, end, in_str, escape = 0, start, False, False
    for i in range(start, len(candidate)):
        ch = candidate[i]
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"' and not escape:
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    try:
        return json.loads(candidate[start:end])
    except json.JSONDecodeError:
        return None
