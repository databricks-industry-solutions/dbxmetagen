"""Sectional Genie space configuration generator.

Phase 1 (Core): LLM generates description, text_instructions, sample_questions, join_specs.
Phase 2 (SQL):  LLM generates example_sql pairs.
Phase 3 (Snippets): LLM generates sql_snippets (measures, filters, expressions).
Post-processing merges pre-built joins, snippets, and data sources, then validates SQL.
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

# -- Shared context block reused by all phases --
_CONTEXT_BLOCK = """=== METADATA CONTEXT ===
{context_text}

=== DATA SOURCES (FINALIZED) ===
{data_sources}

=== DATE FUNCTIONS (Databricks/Spark SQL) ===
- TIMESTAMPADD(MONTH, 1, col) -- bare keyword, singular, NO quotes
- TIMESTAMPDIFF(MINUTE, start, end) -- bare keyword, no quotes
- DATE_TRUNC('MONTH', col) -- interval IS single-quoted
- EXTRACT(HOUR FROM col) -- use EXTRACT, not DATE_PART
- DATEDIFF(end, start) returns days only. For other units use TIMESTAMPDIFF.
NEVER use plural units or quoted units in TIMESTAMPADD/TIMESTAMPDIFF.

Table references in SQL must be fully qualified (catalog.schema.table).
Column references in snippets MUST use table.column format.
NEVER include PII or PHI in descriptions, instructions, sample questions, or example SQL. Avoid ephemeral stats (row counts, date ranges). Structural patterns (value ranges, enum sets) are fine.
"""

# -- Phase 1: Core Config --
_PHASE1_PROMPT = _CONTEXT_BLOCK + """
=== PRE-BUILT JOIN SPECS (will be merged automatically) ===
{join_specs}

=== USER QUESTIONS ===
{questions}

=== TASK: Generate the CORE config ===
Output a JSON object wrapped in ```json ``` fences with these fields ONLY:

{{
  "description": "<1-2 sentence description of what this Genie space helps users explore>",
  "instructions": {{
    "text": "<1-2 sentence intro summarizing the data domain and analytical focus, followed by concise markdown sections for column disambiguation, business rules, and date handling>"
  }},
  "sample_questions": ["<plain English question for business users>", ...],
  "join_specs": [<ADDITIONAL joins not already pre-built above>]
}}

RULES:
- 5-8 sample_questions: plain English, no SQL jargon, clickable for business users.
- text instructions: CONCISE, under 2000 characters. Structure as follows:
  (0) Open with 1-2 plain sentences summarizing what the data covers, the key entities, and the primary analytical use cases. Do NOT use a heading for this intro.
  (1) ## Column Disambiguation -- columns with the SAME NAME in multiple tables
  (2) ## Business Rules -- non-obvious business rules or calculation conventions
  (3) ## Date & Unit Notes -- critical date/unit handling notes
  Do NOT repeat table descriptions, list all columns, or describe what each table contains -- other sections carry that detail.
- Only generate join_specs for relationships NOT covered by pre-built joins.
  If no pre-built joins exist but FK relationships or shared columns are in context, generate join_specs.
  Format: {{"left": {{"identifier": "catalog.schema.t1"}}, "right": {{"identifier": "catalog.schema.t2"}}, "sql": ["t1.col = t2.col"]}}
- Output ONLY JSON. No explanation.
"""

# -- Phase 2: Example SQL --
_PHASE2_PROMPT = _CONTEXT_BLOCK + """
=== SPACE DESCRIPTION ===
{description}

=== TASK: Generate example_sql pairs ===
Output a JSON object wrapped in ```json ``` fences:

{{
  "example_sql": [
    {{"question": "<natural language question>", "sql": "<complete SQL query>"}},
    {{"question": "<question>", "sql": "<SQL using :param_name syntax>",
      "parameters": [{{"name": "param_name", "type_hint": "STRING", "default_value": {{"values": ["default_val"]}}}}],
      "usage_guidance": ["when to use this query"]
    }},
    ...
  ]
}}

Generate 10 diverse example_sql pairs covering these patterns:
1. Simple aggregation (non-parameterized)
2. Multi-table join using FK relationships from context (non-parameterized)
3. Time-series trend GROUP BY date dimension (non-parameterized)
4. Filtered query with a :parameter for a categorical column like status or strategy (parameterized)
5. Top-N or ratio query with a :parameter for a threshold or date filter (parameterized)
6. Window function -- running total, rank, or lag/lead comparison (non-parameterized)
7. CASE WHEN segmentation or bucketing (non-parameterized)
8. Subquery or CTE for a derived metric (non-parameterized)
9. Multi-join across 3+ tables if available (non-parameterized)
10. Date comparison -- YoY, MoM, or period-over-period (parameterized with :date or :period)

RULES:
- SQL must use fully qualified table names (catalog.schema.table).
- Each question must be different. Cover diverse analytical patterns.
- For parameterized queries (patterns 4, 5, 10):
  - Use :param_name syntax in SQL for user-adjustable values
  - Include "parameters" array with name, type_hint (STRING, INT, or DATE), and default_value.values
  - Include "usage_guidance" with a short note on when to use the query
- Post-processing validates SQL and removes broken entries -- aim for correctness.
- Output ONLY JSON. No explanation.
"""

# -- Phase 3: SQL Snippets --
_PHASE3_PROMPT = _CONTEXT_BLOCK + """
=== PRE-BUILT SQL SNIPPETS (will be merged -- do NOT duplicate) ===
{sql_snippets}

=== TASK: Generate ADDITIONAL sql_snippets ===
Output a JSON object wrapped in ```json ``` fences:

{{
  "sql_snippets": {{
    "measures": [
      {{"alias": "...", "display_name": "...", "sql": ["..."], "synonyms": ["..."], "description": "..."}},
      ...
    ],
    "filters": [
      {{"display_name": "...", "sql": ["..."]}},
      ...
    ],
    "expressions": [
      {{"alias": "...", "display_name": "...", "sql": ["..."], "synonyms": ["..."]}},
      ...
    ]
  }}
}}

Generate 3 NEW measures, 3 NEW filters, and 2 NEW expressions beyond the pre-built ones.
- Every measure/expression needs a "synonyms" array with 1-4 business-friendly names.
- Every measure needs a "description".
- Filters: include time-based filters (last 30 days, YTD) for date columns.
- Column refs use table.column format (e.g. `orders.amount`).
- Do NOT duplicate anything already in pre-built snippets.
- Output ONLY JSON. No explanation.
"""

# -- Feedback classification for targeted refinement --
_PHASE_KEYWORDS = {
    1: {"join", "instruction", "description", "sample", "question", "describe", "title", "text"},
    2: {"sql", "query", "example", "example_sql"},
    3: {"measure", "filter", "expression", "snippet", "metric", "kpi"},
}


def _classify_feedback(feedback: str) -> set[int]:
    """Determine which phases the feedback targets via keyword matching."""
    words = set(feedback.lower().split())
    phases = set()
    for phase, keywords in _PHASE_KEYWORDS.items():
        if words & keywords:
            phases.add(phase)
    return phases or {1}


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


GENIE_WALL_TIMEOUT = 540  # 9 minutes


def _llm_phase(
    llm: ChatDatabricks, system_prompt: str, user_msg: str, label: str,
) -> Optional[dict]:
    """Run one LLM phase and extract JSON from the response."""
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_msg},
    ]
    t0 = time.time()
    try:
        result = llm.invoke(messages)
        content = getattr(result, "content", "") or ""
        logger.info("Phase %s: %d chars in %.1fs", label, len(content), time.time() - t0)
        return _extract_json(content)
    except Exception as e:
        logger.warning("Phase %s failed after %.1fs: %s", label, time.time() - t0, e)
        return None


def _summarize_prior_result(prior: dict) -> str:
    """Build a compact summary of a prior Genie config for refinement context."""
    parts = []
    if prior.get("description"):
        parts.append(f"Description: {prior['description']}")
    ds = prior.get("data_sources", {})
    parts.append(f"Data sources: {len(ds.get('tables', []))} tables, {len(ds.get('metric_views', []))} MVs")
    inst = prior.get("instructions", {})
    text = inst.get("text") or ""
    if isinstance(text, list):
        text = "\n".join(str(t) for t in text)
    if text:
        parts.append(f"Instructions (preview): {text[:400]}...")
    examples = inst.get("example_sql") or inst.get("example_question_sqls") or []
    if examples:
        preview = [ex.get("question", "?") for ex in examples[:4]]
        parts.append(f"Example SQL ({len(examples)} total): {preview}")
    sqs = prior.get("sample_questions", [])
    if sqs:
        parts.append(f"Sample questions ({len(sqs)}): {[q if isinstance(q, str) else q.get('question', '') for q in sqs]}")
    return "Previously generated config:\n" + "\n".join(f"- {p}" for p in parts)


def _build_context_block(context: Dict[str, Any]) -> dict:
    """Pre-format the shared context substitutions."""
    return {
        "context_text": context["context_text"],
        "data_sources": json.dumps(context["data_sources"], indent=2),
    }


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
    """Run the Genie builder agent using sectional generation.

    Fresh generation: 3 focused LLM calls (core, example_sql, snippets).
    Refinement: single LLM call with feedback context.
    """
    start_time = time.time()

    def _elapsed() -> float:
        return round(time.time() - start_time, 1)

    def _error_event(message: str, *, rounds: int = 0) -> dict:
        return {
            "stage": "error", "message": message,
            "elapsed_seconds": _elapsed(), "rounds_completed": rounds,
        }

    progress_queue.put({"stage": "initializing"})

    llm = ChatDatabricks(
        endpoint=model_endpoint, temperature=0.1, max_tokens=16384,
        max_retries=1, request_timeout=300,
    )
    ctx_subs = _build_context_block(context)
    questions_text = "\n".join(f"- {q}" for q in context.get("questions", [])) or "None provided"

    logger.info(
        "Genie: %d tables, %d join_specs, %d snippet items",
        len(context.get("data_sources", {}).get("tables", [])),
        len(context.get("join_specs", [])),
        sum(len(context.get("sql_snippets", {}).get(k, [])) for k in ("measures", "filters", "expressions")),
    )

    # ---- Refinement path: targeted sectional re-generation ----
    if prior_result and refinement_feedback:
        target_phases = _classify_feedback(refinement_feedback)
        logger.info("Refinement targets phases %s from feedback: %s", target_phases, refinement_feedback[:120])

        serialized = dict(prior_result)
        serialized.setdefault("instructions", {})
        feedback_suffix = f"\n\n=== USER FEEDBACK (revise accordingly) ===\n{refinement_feedback}"
        phases_completed = 0

        if 1 in target_phases:
            progress_queue.put({"stage": "generating", "message": "Refining core config..."})
            p1_prompt = (_PHASE1_PROMPT + SAFETY_PROMPT_BLOCK).format(
                **ctx_subs,
                join_specs=json.dumps(context.get("join_specs", []), indent=2),
                questions=questions_text,
            ) + feedback_suffix
            p1 = _llm_phase(llm, p1_prompt, "Revise the core Genie config JSON now.", "refine_core")
            if p1:
                if p1.get("description"):
                    serialized["description"] = p1["description"]
                if p1.get("instructions", {}).get("text"):
                    serialized["instructions"]["text"] = p1["instructions"]["text"]
                if p1.get("sample_questions"):
                    serialized["sample_questions"] = p1["sample_questions"]
                extra_joins = p1.get("join_specs", [])
                if extra_joins:
                    serialized.setdefault("instructions", {})["join_specs"] = extra_joins
                phases_completed += 1

        if 2 in target_phases:
            progress_queue.put({"stage": "generating", "message": "Refining example SQL..."})
            p2_prompt = (_PHASE2_PROMPT + SAFETY_PROMPT_BLOCK).format(
                **ctx_subs,
                description=serialized.get("description", ""),
            ) + feedback_suffix
            p2 = _llm_phase(llm, p2_prompt, "Revise the example_sql JSON now.", "refine_sql")
            if p2:
                examples = p2.get("example_sql") or p2.get("instructions", {}).get("example_sql", [])
                serialized["instructions"]["example_sql"] = examples
                phases_completed += 1

        if 3 in target_phases:
            progress_queue.put({"stage": "generating", "message": "Refining SQL snippets..."})
            p3_prompt = (_PHASE3_PROMPT + SAFETY_PROMPT_BLOCK).format(
                **ctx_subs,
                sql_snippets=json.dumps(context.get("sql_snippets", {}), indent=2),
            ) + feedback_suffix
            p3 = _llm_phase(llm, p3_prompt, "Revise the sql_snippets JSON now.", "refine_snippets")
            if p3:
                snippets = p3.get("sql_snippets", p3)
                if isinstance(snippets, dict) and any(k in snippets for k in ("measures", "filters", "expressions")):
                    serialized["instructions"]["sql_snippets"] = snippets
                phases_completed += 1

        if phases_completed == 0:
            detail = f"Refinement produced no valid JSON for phases {target_phases} after {_elapsed():.0f}s."
            progress_queue.put(_error_event(detail))
            raise ValueError(detail)
    else:
        # ---- Fresh generation: 3 sectional phases ----
        serialized = {}

        # Phase 1: Core Config
        progress_queue.put({"stage": "generating", "message": "Phase 1/3: Generating core config..."})
        p1_prompt = (_PHASE1_PROMPT + SAFETY_PROMPT_BLOCK).format(
            **ctx_subs,
            join_specs=json.dumps(context.get("join_specs", []), indent=2),
            questions=questions_text,
        )
        p1 = _llm_phase(llm, p1_prompt, "Generate the core Genie config JSON now.", "core")
        if p1:
            serialized["description"] = p1.get("description", "")
            serialized["instructions"] = {"text": p1.get("instructions", {}).get("text", "")}
            serialized["sample_questions"] = p1.get("sample_questions", [])
            extra_joins = p1.get("join_specs", [])
            if extra_joins:
                serialized["join_specs"] = extra_joins
        else:
            logger.warning("Phase 1 (core) failed -- continuing with empty core")
            serialized["description"] = ""
            serialized["instructions"] = {"text": ""}
            serialized["sample_questions"] = []

        # Phase 2: Example SQL
        progress_queue.put({"stage": "generating", "message": "Phase 2/3: Generating example SQL..."})
        p2_prompt = (_PHASE2_PROMPT + SAFETY_PROMPT_BLOCK).format(
            **ctx_subs,
            description=serialized.get("description", ""),
        )
        p2 = _llm_phase(llm, p2_prompt, "Generate the example_sql JSON now.", "example_sql")
        if p2:
            examples = p2.get("example_sql") or p2.get("instructions", {}).get("example_sql", [])
            serialized["instructions"]["example_sql"] = examples
        else:
            logger.warning("Phase 2 (example_sql) failed -- continuing without examples")
            serialized["instructions"]["example_sql"] = []

        # Phase 3: SQL Snippets
        progress_queue.put({"stage": "generating", "message": "Phase 3/3: Generating SQL snippets..."})
        p3_prompt = (_PHASE3_PROMPT + SAFETY_PROMPT_BLOCK).format(
            **ctx_subs,
            sql_snippets=json.dumps(context.get("sql_snippets", {}), indent=2),
        )
        p3 = _llm_phase(llm, p3_prompt, "Generate the sql_snippets JSON now.", "snippets")
        if p3:
            snippets = p3.get("sql_snippets", p3)
            if isinstance(snippets, dict) and any(k in snippets for k in ("measures", "filters", "expressions")):
                serialized["instructions"]["sql_snippets"] = snippets
        else:
            logger.warning("Phase 3 (snippets) failed -- continuing without extra snippets")

    # ---- SQL validation ----
    progress_queue.put({"stage": "validating_sql"})
    serialized = _validate_and_strip_sql(serialized, ws, warehouse_id, progress_queue)

    # ---- Post-processing ----
    progress_queue.put({"stage": "parsing"})
    serialized = _merge_prebuilt_snippets(serialized, context.get("sql_snippets", {}))
    serialized = _merge_prebuilt_join_specs(serialized, context.get("join_specs", []))
    serialized = _merge_prebuilt_data_sources(serialized, context.get("data_sources", {}))
    serialized = _dedup_sample_vs_example(serialized)
    serialized = _backfill_synonyms(serialized)
    warnings = _validate_output(serialized, context)
    if warnings:
        logger.warning("Genie output quality warnings: %s", "; ".join(warnings))

    if not (prior_result and refinement_feedback):
        phases_completed = sum(1 for x in [p1, p2, p3] if x)

    progress_queue.put({
        "stage": "done",
        "result": serialized,
        "warnings": warnings,
        "elapsed_seconds": _elapsed(),
        "rounds_completed": phases_completed,
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
        j["_prebuilt"] = True
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
        if section not in prebuilt_ds:
            continue
        prebuilt_items = prebuilt_ds[section]
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


def _validate_output(raw: dict, context: dict | None = None) -> list[str]:
    """Return a list of warnings about missing or thin sections."""
    warnings = []
    if context:
        warnings.extend(context.get("assembler_warnings", []))
        requested = context.get("requested_mv_count")
        actual = len(raw.get("data_sources", {}).get("metric_views", []))
        if requested is not None and actual < requested:
            warnings.append(f"{actual} of {requested} selected metric views included as data sources")
    ds = raw.get("data_sources", {})
    if not ds.get("tables"):
        warnings.append("No tables in data_sources")
    inst = raw.get("instructions", {})
    examples = inst.get("example_sql") or inst.get("example_question_sqls") or []
    if len(examples) < 5:
        warnings.append(f"Only {len(examples)} example_sql pairs (target: 10)")
    snip = inst.get("sql_snippets") or raw.get("sql_snippets") or {}
    if len(snip.get("measures", [])) < 2:
        warnings.append(f"Only {len(snip.get('measures', []))} measures (target: 3+)")
    if len(snip.get("filters", [])) < 2:
        warnings.append(f"Only {len(snip.get('filters', []))} filters (target: 3+)")
    if len(snip.get("expressions", [])) < 1:
        warnings.append(f"Only {len(snip.get('expressions', []))} expressions (target: 2+)")
    sqs = raw.get("sample_questions", [])
    if len(sqs) < 4:
        warnings.append(f"Only {len(sqs)} sample questions (target: 5+)")
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
