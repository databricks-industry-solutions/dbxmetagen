"""Two-phase Genie space configuration generator.

Phase 1: Single LLM call generates the full serialized_space JSON.
Phase 2: Parallel SQL validation strips broken example_sql entries.
Post-processing merges pre-built joins, snippets, and data sources.
"""

import json
import logging
import os
import queue
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Format, Disposition
from langchain_community.chat_models import ChatDatabricks
from agent.tracing import trace

from agent.guardrails import SAFETY_PROMPT_BLOCK

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT_TEMPLATE = """You are a Genie Space configuration expert. Your job is to generate a
comprehensive, production-quality `serialized_space` JSON for the Databricks Genie API
based on the metadata context below. A great Genie space is one that business users
can immediately query in natural language and get correct, insightful answers.

=== METADATA CONTEXT ===
{context_text}

=== PRE-BUILT DATA SOURCES (FINALIZED -- do NOT regenerate) ===
data_sources: {data_sources}

=== PRE-BUILT JOIN SPECS (will be merged automatically -- do NOT duplicate) ===
The following join_specs are pre-built from FK predictions, ontology relationships,
and metric view joins. They will be merged into your output automatically.
Only generate ADDITIONAL join_specs for relationships NOT already covered below.
If no pre-built joins exist and FK relationships or shared columns appear in context,
you MUST generate join_specs.
{join_specs}

=== PRE-BUILT SQL SNIPPETS (will be merged automatically -- do NOT duplicate) ===
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
Generate the COMPLETE JSON object in a SINGLE response, wrapped in ```json ``` fences.
Post-processing will add IDs, validate SQL, and restructure for the API.

Schema:
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
1. Table references in SQL must be fully qualified (catalog.schema.table). Column references in sql_snippets (measures, filters, expressions) MUST use table.column format (e.g. `fact_ed_wait_times.wait_minutes`), never bare column names.
2. Output ONLY the JSON wrapped in ```json ``` fences. No explanation before or after.
3. Do NOT add id fields -- post-processing handles that automatically.
4. METRIC VIEW ROUTING: metric_views listed in data_sources are APPLIED Unity Catalog objects -- Genie queries them natively via MEASURE(). sql_snippets contain measures decomposed from UNAPPLIED (validated-only) definitions plus filter suggestions. NEVER duplicate the same measure in both data_sources.metric_views and sql_snippets.measures.
5. JOINS: Pre-built join_specs are merged automatically after generation. Generate ADDITIONAL join_specs only for relationships not already covered by the pre-built set. If no pre-built joins exist but FOREIGN KEY RELATIONSHIPS or shared column names exist in the metadata context, you MUST generate join_specs. Each join_spec needs left/right identifiers (fully qualified table names) and a sql array with join conditions using table.column format.
6. DATE FUNCTIONS: Use Databricks/Spark SQL syntax for all date functions.
   - TIMESTAMPADD(MONTH, 1, col) -- unit is a bare keyword, singular, NO quotes
   - TIMESTAMPDIFF(MINUTE, start, end) -- same: bare keyword, no quotes
   - DATE_TRUNC('MONTH', col) -- interval IS single-quoted
   - EXTRACT(HOUR FROM col) -- use EXTRACT, not DATE_PART
   - DATEDIFF(end, start) returns days only (2 args). For other units use TIMESTAMPDIFF.
   NEVER use plural units (MONTHS, HOURS) or quoted units in TIMESTAMPADD/TIMESTAMPDIFF.

QUALITY REQUIREMENTS (critical):
7. SYNONYMS: Every measure and expression MUST have a "synonyms" array with 1-4 business-friendly alternative names. Think about how non-technical users would ask for this metric. E.g. "total_revenue" -> ["revenue", "sales", "total sales"]. Pre-built synonyms are provided -- keep them and add more if useful.
8. DESCRIPTIONS: Every measure MUST have a "description" explaining what it calculates and when to use it.
9. COMPLEXITY: Generate at least 8 example_sql pairs covering: simple aggregation, multi-table join, time-series trend, top-N ranking, filtered aggregation, ratio/rate, comparison, and at least one window function. Generate at least 5 NEW measures, 5 NEW filters, and 3 NEW expressions beyond the pre-built ones.
10. DISAMBIGUATION: In the text instructions, explicitly disambiguate columns that appear in multiple tables (e.g. "created_date in orders vs created_date in accounts"). Also explain which table to use for common questions.
11. FILTERS: Generate time-based filters (last 30 days, YTD, current month) for every date column, plus categorical filters for low-cardinality string columns. Each filter should have a clear display_name.
12. SAMPLE QUESTIONS vs EXAMPLE SQL (IMPORTANT -- these serve DIFFERENT purposes):
    - "sample_questions": Shown in the Genie UI as clickable starter questions for BUSINESS USERS. Must be plain English, non-technical, no SQL jargon. 8-12 questions like "What were total sales last quarter?" or "Who are our top customers?"
    - "example_sql": Few-shot examples that teach Genie HOW to translate natural language to SQL. Each needs a question AND working SQL. Cover diverse SQL patterns (joins, aggregations, window functions, filters). Do NOT duplicate the same question in both lists.
13. SQL QUALITY: Write the best SQL you can. Post-processing will automatically validate all SQL and remove any broken entries, so aim for correctness but do not hold back on complexity.
"""


def _test_one_sql(ws: WorkspaceClient, warehouse_id: str, sql: str) -> str:
    """Test a SQL query for syntax/reference errors. Returns 'OK' or error string."""
    test_q = f"SELECT * FROM ({sql}) t LIMIT 0"
    try:
        r = ws.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=test_q,
            wait_timeout="20s", format=Format.JSON_ARRAY, disposition=Disposition.INLINE,
        )
        state = r.status.state.value if r.status and r.status.state else "UNKNOWN"
        if state in ("SUCCEEDED", "CLOSED"):
            return "OK"
        err = r.status.error.message if r.status and r.status.error else "Unknown error"
        return f"ERROR: {err}"
    except Exception as e:
        return f"ERROR: {e}"


def _validate_and_strip_sql(
    raw: dict, ws: WorkspaceClient, warehouse_id: str,
    progress_queue: queue.Queue | None = None,
) -> dict:
    """Phase 2: validate example_sql via test_sql in parallel, strip broken ones."""
    inst = raw.get("instructions", {})
    examples = inst.get("example_sql") or inst.get("example_question_sqls") or []
    if not examples:
        return raw

    sql_items: list[tuple[int, str]] = []
    for i, ex in enumerate(examples):
        sql_val = ex.get("sql", "")
        if isinstance(sql_val, list):
            sql_val = sql_val[0] if sql_val else ""
        if sql_val:
            sql_items.append((i, sql_val))

    if not sql_items:
        return raw

    results: dict[int, str] = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {
            pool.submit(_test_one_sql, ws, warehouse_id, sql): idx
            for idx, sql in sql_items
        }
        for f in as_completed(futures):
            idx = futures[f]
            try:
                results[idx] = f.result(timeout=25)
            except Exception:
                results[idx] = "ERROR: timeout"

    valid = []
    stripped = 0
    for i, ex in enumerate(examples):
        verdict = results.get(i, "OK")
        if verdict == "OK":
            valid.append(ex)
        else:
            stripped += 1
            q = ex.get("question", "?")
            if isinstance(q, list):
                q = q[0] if q else "?"
            logger.info("Stripped invalid example_sql: %s -- %s", q[:60], verdict[:100])

    if stripped:
        logger.info("SQL validation: %d/%d example_sql passed, %d stripped", len(valid), len(examples), stripped)
    if progress_queue:
        progress_queue.put({"stage": "validating_sql", "validated": len(valid), "total": len(examples)})

    if "example_sql" in inst:
        inst["example_sql"] = valid
    elif "example_question_sqls" in inst:
        inst["example_question_sqls"] = valid
    else:
        inst["example_sql"] = valid
    raw["instructions"] = inst
    return raw


GENIE_WALL_TIMEOUT = 540  # 9 minutes (LLM call up to 300s + recovery 300s + validation 60s)


def _recovery_invoke(
    messages: list[dict],
    model_endpoint: str,
    context: Optional[Dict[str, Any]] = None,
) -> Optional[dict]:
    """One-shot LLM call asking for just the JSON when first attempt failed."""
    try:
        hint = ""
        if context:
            if context.get("join_specs"):
                js_json = json.dumps(context["join_specs"])
                if len(js_json) < 3000:
                    hint += f"\nUse these pre-built join_specs: {js_json}"
                else:
                    hint += f"\nThere are {len(context['join_specs'])} pre-built join_specs. Include them in instructions.join_specs."
            if context.get("sql_snippets"):
                sn_json = json.dumps(context["sql_snippets"])
                if len(sn_json) < 3000:
                    hint += f"\nUse these pre-built sql_snippets: {sn_json}"
        recovery_msgs = list(messages) + [{
            "role": "user",
            "content": (
                "Your previous response did not contain valid JSON. "
                "Output ONLY the complete serialized_space JSON object now, "
                f"wrapped in ```json ``` fences. No explanation.{hint}"
            ),
        }]
        recovery_llm = ChatDatabricks(
            endpoint=model_endpoint, temperature=0.0, max_tokens=16384,
            max_retries=2, request_timeout=300,
        )
        result = recovery_llm.invoke(recovery_msgs)
        content = getattr(result, "content", "") or ""
        logger.info("Recovery response length: %d chars", len(content))
        return _extract_json(content)
    except Exception as exc:
        logger.warning("Recovery invoke failed: %s", exc)
        return None


def _summarize_prior_result(prior: dict) -> str:
    """Build a compact summary of a prior Genie config for refinement context.

    Keeps the description, truncated instructions text, sample questions, and
    first few example SQL questions.  Omits full data_sources, join_specs, and
    sql_snippets (those are force-merged post-generation anyway).
    """
    parts = []
    if prior.get("description"):
        parts.append(f"Description: {prior['description']}")

    ds = prior.get("data_sources", {})
    tables = ds.get("tables", [])
    mvs = ds.get("metric_views", [])
    parts.append(f"Data sources: {len(tables)} tables, {len(mvs)} metric views (managed automatically)")

    inst = prior.get("instructions", {})
    text = inst.get("text") or ""
    if isinstance(text, list):
        text = "\n".join(str(t) for t in text)
    if text:
        preview = text[:500] + ("..." if len(text) > 500 else "")
        parts.append(f"Instructions text (preview): {preview}")

    joins = inst.get("join_specs") or prior.get("join_specs") or []
    parts.append(f"Join specs: {len(joins)} (managed automatically)")

    snips = inst.get("sql_snippets") or prior.get("sql_snippets") or {}
    parts.append(
        f"SQL snippets: {len(snips.get('measures', []))} measures, "
        f"{len(snips.get('filters', []))} filters, "
        f"{len(snips.get('expressions', []))} expressions (managed automatically)"
    )

    examples = inst.get("example_sql") or inst.get("example_question_sqls") or []
    if examples:
        preview_qs = [ex.get("question", [""])[0] if isinstance(ex.get("question"), list) else ex.get("question", "") for ex in examples[:4]]
        parts.append(f"Example SQL ({len(examples)} total), first questions: {preview_qs}")

    sqs = prior.get("sample_questions", [])
    if sqs:
        sq_list = [q if isinstance(q, str) else q.get("question", [""])[0] if isinstance(q.get("question"), list) else q.get("question", "") for q in sqs]
        parts.append(f"Sample questions ({len(sq_list)}): {sq_list}")

    return "Previously generated config summary:\n" + "\n".join(f"- {p}" for p in parts)


@trace(name="genie_generate")
def run_genie_agent(
    ws: WorkspaceClient,
    warehouse_id: str,
    context: Dict[str, Any],
    progress_queue: queue.Queue,
    model_endpoint: str = os.environ.get("LLM_MODEL", "databricks-claude-sonnet-4-6"),
    refinement_feedback: Optional[str] = None,
    prior_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Run the Genie builder agent and emit progress events to the queue.

    Returns the final serialized_space dict or raises on failure.
    """
    start_time = time.time()

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

    llm = ChatDatabricks(
        endpoint=model_endpoint, temperature=0.1, max_tokens=16384,
        max_retries=1, request_timeout=300,
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

    prompt_chars = len(system_prompt)
    prompt_tokens_est = prompt_chars // 4
    logger.info(
        "Genie prompt: %d chars (~%dk tokens), %d tables, %d join_specs, %d snippet items",
        prompt_chars, prompt_tokens_est // 1000,
        len(context.get("data_sources", {}).get("tables", [])),
        len(context.get("join_specs", [])),
        sum(len(context.get("sql_snippets", {}).get(k, [])) for k in ("measures", "filters", "expressions")),
    )
    if prompt_tokens_est > 14000:
        logger.warning(
            "Genie prompt is very large (~%dk tokens). Generation may be slow. "
            "Consider selecting fewer tables.", prompt_tokens_est // 1000,
        )

    # --- Phase 1: single LLM call ---
    messages: list[dict] = [{"role": "system", "content": system_prompt}]
    if prior_result and refinement_feedback:
        summary = _summarize_prior_result(prior_result)
        messages += [
            {"role": "user", "content": "Generate the complete serialized_space JSON now."},
            {"role": "assistant", "content": summary},
            {"role": "user", "content": f"Here is feedback on the config above. Revise and regenerate the complete JSON:\n\n{refinement_feedback}"},
        ]
    else:
        messages.append({"role": "user", "content": "Generate the complete serialized_space JSON now."})

    progress_queue.put({"stage": "generating"})
    logger.info("Genie: starting single LLM call")
    llm_start = time.time()
    serialized = None
    try:
        result = llm.invoke(messages)
        content = getattr(result, "content", "") or ""
        llm_elapsed = round(time.time() - llm_start, 1)
        logger.info("Genie LLM response: %d chars in %.1fs", len(content), llm_elapsed)
        serialized = _extract_json(content)
        if serialized:
            messages.append({"role": "assistant", "content": content})
    except Exception as e:
        llm_elapsed = round(time.time() - llm_start, 1)
        logger.warning("Genie LLM call failed after %.1fs: %s", llm_elapsed, e)

    # --- Recovery if extraction failed ---
    if serialized is None:
        logger.warning("JSON extraction failed after %.1fs -- attempting recovery", _elapsed())
        progress_queue.put({"stage": "recovering"})
        serialized = _recovery_invoke(messages, model_endpoint, context=context)

    if serialized is None:
        detail = f"Did not produce valid JSON after {_elapsed():.0f}s."
        progress_queue.put(_error_event(detail))
        raise ValueError(detail)

    # --- Phase 2: parallel SQL validation ---
    progress_queue.put({"stage": "validating_sql"})
    serialized = _validate_and_strip_sql(serialized, ws, warehouse_id, progress_queue)

    # --- Post-processing (unchanged) ---
    progress_queue.put({"stage": "parsing"})
    serialized = _merge_prebuilt_snippets(serialized, context.get("sql_snippets", {}))
    serialized = _merge_prebuilt_join_specs(serialized, context.get("join_specs", []))
    serialized = _merge_prebuilt_data_sources(serialized, context.get("data_sources", {}))
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
        "rounds_completed": 0,
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


def _merge_prebuilt_join_specs(raw: dict, prebuilt_joins: list) -> dict:
    """Force-merge pre-built join_specs into agent output.

    Pre-built joins win for the same (left, right) pair. Agent can only
    add NEW joins for pairs not already covered. Dedup uses short table
    names in both orientations to handle FQ vs short identifier mismatches.
    """
    if not prebuilt_joins:
        return raw
    inst = raw.get("instructions", {})
    agent_joins = inst.get("join_specs") or raw.get("join_specs") or []

    prebuilt_pairs = set()
    for j in prebuilt_joins:
        l = j.get("left", {}).get("identifier", "").split(".")[-1].lower()
        r = j.get("right", {}).get("identifier", "").split(".")[-1].lower()
        prebuilt_pairs.add(tuple(sorted([l, r])))

    filtered_agent = []
    for j in agent_joins:
        l = j.get("left", {}).get("identifier", "").split(".")[-1].lower()
        r = j.get("right", {}).get("identifier", "").split(".")[-1].lower()
        pair = tuple(sorted([l, r]))
        if pair not in prebuilt_pairs:
            filtered_agent.append(j)

    merged = prebuilt_joins + filtered_agent

    # Always store inside instructions so build_serialized_space finds them
    inst["join_specs"] = merged
    raw["instructions"] = inst
    raw.pop("join_specs", None)  # remove top-level if agent put them there
    return raw


def _merge_prebuilt_data_sources(raw: dict, prebuilt_ds: dict) -> dict:
    """Force-merge pre-built data_sources into agent output.

    Pre-built tables/MVs are the canonical set (user-selected).  If the agent
    included a matching entry, prefer the agent's version (may have enriched
    descriptions from tool calls).  Any pre-built entries the agent omitted are
    added back.  Agent entries not in the pre-built set are discarded.
    """
    if not prebuilt_ds:
        return raw
    agent_ds = raw.get("data_sources", {})

    for section in ("tables", "metric_views"):
        prebuilt_items = prebuilt_ds.get(section, [])
        if not prebuilt_items:
            continue
        prebuilt_by_id = {item["identifier"].lower(): item for item in prebuilt_items}
        agent_by_id = {}
        for item in agent_ds.get(section, []):
            key = item.get("identifier", "").lower()
            if key:
                agent_by_id[key] = item

        merged = []
        for key, pb_item in prebuilt_by_id.items():
            agent_item = agent_by_id.get(key)
            if agent_item and agent_item.get("description"):
                merged.append(agent_item)
            else:
                merged.append(pb_item)
        agent_ds[section] = merged

    raw["data_sources"] = agent_ds
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
    # Join connectivity check (metric views are self-contained, only count raw tables)
    joins = inst.get("join_specs") or raw.get("join_specs") or []
    table_count = len(ds.get("tables", []))
    if table_count > 1 and not joins:
        warnings.append(
            f"{table_count} tables but no join_specs -- "
            "tables may be unreachable by Genie"
        )
    elif table_count > 1 and len(joins) < table_count - 1:
        warnings.append(
            f"Only {len(joins)} join_specs for {table_count} tables "
            f"(need {table_count - 1} for full connectivity)"
        )
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
