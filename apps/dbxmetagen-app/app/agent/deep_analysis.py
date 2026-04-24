"""Two-phase deep analysis for GraphRAG and Baseline modes.

Phase 1: Deterministic data gathering (direct tool calls, no agent loops)
Phase 2: Single LLM analysis call with all gathered context

This replaces the previous multi-agent supervisor/planner/retrieval/analyst
LangGraph architecture which was prone to infinite loops and timeouts.
"""

import contextvars
import json
import logging
import os
import queue
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from contextlib import nullcontext
from typing import Dict, List, Optional

from databricks_langchain import ChatDatabricks
from langchain_core.messages import HumanMessage, SystemMessage

from agent.metadata_tools import (
    CATALOG, SCHEMA,
    search_metadata, execute_metadata_sql, execute_baseline_sql,
    expand_vs_hits, profile_key_columns, traverse_graph, execute_data_sql,
)
from agent.common import ToolResult, measure_phase, vs_cache_get, vs_cache_put
from agent.graph_skill import GRAPH_SCHEMA_CONTEXT
from agent.guardrails import SAFETY_PROMPT_BLOCK, EvidenceBudget, sanitize_output
from agent.intent import classify_and_contextualize
from agent.tracing import trace, ensure_mlflow_context, get_mlflow, maybe_span, tag_trace

logger = logging.getLogger(__name__)

MODEL = os.environ.get("LLM_MODEL", "databricks-claude-sonnet-4-6")
TOOL_TIMEOUT = 30  # hard timeout per tool call (seconds)

_local = threading.local()


def _truncate_part(text: str, limit: int) -> str:
    """Truncate a tool result string to *limit* chars, appending a marker if trimmed."""
    if not text or len(text) <= limit:
        return text
    return text[:limit] + f"\n...[truncated from {len(text):,} to {limit:,} chars]"


def _emit(stage: str, message: str, agent: str = ""):
    tr = getattr(_local, "routing_trace", None)
    if agent and tr is not None:
        tr.append({"agent": agent, "action": stage, "message": message})
    q = getattr(_local, "progress_queue", None)
    if q is not None:
        q.put({"stage": stage, "message": message})


_cache_lock = threading.Lock()
_cached_llm = None


def _llm():
    global _cached_llm
    if _cached_llm is None:
        with _cache_lock:
            if _cached_llm is None:
                _cached_llm = ChatDatabricks(
                    endpoint=MODEL, temperature=0,
                    max_retries=1, request_timeout=120,
                )
    return _cached_llm


# ---------------------------------------------------------------------------
# Analysis prompt (single LLM call replaces the old multi-agent pipeline)
# ---------------------------------------------------------------------------

ANALYSIS_PROMPT = f"""You are a senior data catalog analyst.
{SAFETY_PROMPT_BLOCK}

Given a user's question and evidence gathered from a metadata knowledge graph,
vector search, SQL queries on metadata tables, and structured data retrieval
(LLM-generated SQL against actual data tables), produce a concise, focused analysis.

{GRAPH_SCHEMA_CONTEXT}

## Ontology Entity Model

The ontology uses a per-instance entity model:
- Each discovered entity gets its own UUID (`entity_id`) and row in `ontology_entities`.
- `entity_type` (e.g. Person, Metric, Reference) is the class label -- many entity
  instances can share the same `entity_type`. Multiple UUIDs per type is by design,
  not a deduplication issue.
- `instance_of` edges link table nodes (src=table FQN) to entity nodes (dst=entity UUID)
  with a fixed weight of 1.0. The discovery confidence is on the entity node's
  `quality_score`, not the edge weight.
- Entity types come from the active ontology bundle (e.g. general.yaml, healthcare.yaml).
  Only entities with matching source_tables were actually discovered in user data;
  entity types that appear only in bundle definitions or relationship metadata
  (without source_tables) are not confirmed in user tables.
- Distinguish clearly between "entity types defined in the bundle" and "entity instances
  actually discovered and mapped to tables in this catalog."

## Response Format

### Key Findings
Numbered findings with inline source citations (e.g. "[Source: search_metadata]").

### Data Insights
Summarize key patterns or notable values from retrieved data rows.

### Relationships & Patterns
Structural relationships, join patterns, FK references found in evidence.

### Recommendations
- **Action**: What to do
- **Evidence**: Supporting data (cite source)
- **Priority**: High / Medium / Low

### Suggested Next Analyses
1-2 short, specific follow-up questions the evidence could answer. Frame as
natural language the user could ask next. Omit if the answer is complete and
no natural extension exists.

GROUNDING RULE: Every finding and recommendation MUST cite a specific evidence source
(e.g. "[Source: search_metadata]", "[Source: SQL query]"). Do not include findings that
lack evidence. If the user's question cannot be answered from the gathered evidence,
state this clearly.

Do NOT echo back raw JSON from intent classification or retrieval planning.
Be direct and avoid repetition. Lead with the most impactful finding."""


BASELINE_ANALYSIS_PROMPT = f"""You are a senior data catalog analyst.
{SAFETY_PROMPT_BLOCK}

Given a user's question and evidence gathered from SQL queries on metadata
knowledge base tables, produce a concise, focused analysis.

Available tables queried:
- table_knowledge_base: table_name, comment, domain, subdomain, has_pii, has_phi, row_count
- column_knowledge_base: table_name, column_name, comment, data_type, classification
- schema_knowledge_base: catalog_name, schema_name, comment, tables_count

## Response Format

### Key Findings
Numbered findings referencing the evidence provided.

### Relationships & Patterns
Structural relationships or patterns between tables/columns.

### Recommendations
- **Action**: What to do
- **Evidence**: Supporting data
- **Priority**: High / Medium / Low

### Suggested Next Analyses
1-2 short, specific follow-up questions the evidence could answer. Frame as
natural language the user could ask next. Omit if the answer is complete and
no natural extension exists.

GROUNDING RULE: Every finding MUST reference the evidence provided. Do not include
findings that lack evidence. If the question cannot be answered, state this clearly.

Be direct and avoid repetition."""


DATA_ANALYSIS_PROMPT = f"""You are a data analyst on Databricks.
{SAFETY_PROMPT_BLOCK}

Given a user's data question and the SQL query + results from an executed query,
provide a concise, data-driven answer.

## Response Format

### Answer
Direct answer with key numbers. Include a markdown table showing the top/key
results (first 10-20 rows or the most relevant subset). Keep prose to 2-5
sentences that interpret the data. The SQL query is already shown in the
evidence above -- do NOT repeat it.

### Data Shape and Limitations
- Report distinct value counts for key columns used in the analysis.
- Explicitly separate NULL / unknown from false / negative for flag columns.
- State whether any comparison group is empty.
- If a key variable is single-valued in this cohort, state this before drawing conclusions.
- If profile_key_columns evidence is present, cite it by name as a source
  (e.g. "[Source: profile_key_columns]") in this section. Report the specific
  distinct value counts and distributions it revealed.

### Interpretation
For each notable finding (enrollment gaps between groups, unexpected absence
of an expected category, asymmetric group sizes), state what it likely means
and what would need to be true for it to be significant. Do not just report
numbers -- explain them.

### Suggested Next Analyses
1-2 short, specific follow-up questions the evidence could answer. Frame as
natural language the user could ask next. Omit if the answer is complete and
no natural extension exists.

RULES:
- Lead with the data answer. The user asked a data question, not a metadata question.
- Do NOT echo raw JSON from intermediate steps.
- Do NOT repeat the SQL query -- it is already visible to the user.
- Do NOT generate empty sections or section headers with no content.
- Do NOT repeat the question back to the user.
- For boolean/flag columns, use three-way CASE: TRUE -> 'Yes', FALSE -> 'No', ELSE -> 'Unknown/Not recorded'.
  Never use IS NOT NULL as a proxy for TRUE.
- Be concise. If the data answers the question completely, say so briefly.

ADAPTIVE POLICY: If profiling shows a key comparison variable has only one distinct
non-NULL value in the cohort, you MUST: (1) state this as the topline finding,
(2) propose one alternative query with a relaxed filter or broader cohort.
Do not silently report a trivial result.

BROADENING RULE: If a broadened cohort result is provided alongside the original,
present both in the Answer section with clear labels ("Original Cohort" vs
"Broadened Cohort"). Explain what filter was relaxed and what the broadened
result reveals that the original did not.

CASCADING BROADENING: If a "Diagnostic" distribution table and a
"Filter-Dropped Cohort" are present, the system ran cascading broadening
because the first relaxation was still degenerate. Present all cohorts in order
(Original -> Broadened -> Filter-Dropped) and use the diagnostic distribution
to explain WHY the binding filter was restrictive. The filter-dropped cohort
is the most analytically useful -- lead your interpretation with it. Explain
how each broadening step changed the result."""


# ---------------------------------------------------------------------------
# Deterministic tool calling
# ---------------------------------------------------------------------------

def _safe_tool_call(tool_fn, args: dict, timeout_s: int, label: str,
                    step_num: int, total_steps: int) -> ToolResult:
    """Run a LangChain tool with a hard timeout. Returns a ToolResult.

    Span parenting is handled via contextvars propagation -- callers wrap
    pool.submit() with copy_context().run() so the OTel parent span is visible.
    """
    span_inputs = {"tool": label, "args": str(args)[:200], "timeout_s": timeout_s}
    cm = maybe_span(name=f"tool_{label}", span_type="TOOL")

    with cm as span:
        if span:
            span.set_inputs(span_inputs)
        t0 = time.time()
        if step_num:
            _emit("gathering", f"Step {step_num}/{total_steps}: {label}...")
        logger.info("[deep_analysis] STEP %d/%d: %s -- START (timeout=%ds)",
                    step_num, total_steps, label, timeout_s)
        try:
            ctx = contextvars.copy_context()
            with ThreadPoolExecutor(max_workers=1) as pool:
                result = pool.submit(ctx.run, tool_fn.func, **args).result(timeout=timeout_s)
            elapsed = round(time.time() - t0, 1)
            result_len = len(result) if result else 0
            logger.info("[deep_analysis] STEP %d/%d: %s -- DONE in %.1fs (%d chars)",
                        step_num, total_steps, label, elapsed, result_len)
            if step_num:
                _emit("gathering", f"Step {step_num}/{total_steps}: {label} done ({elapsed}s)")
            outputs = {"status": "ok", "elapsed_s": elapsed, "result_len": result_len}
            if span:
                span.set_outputs(outputs)
            return ToolResult(success=True, data=result, elapsed_s=elapsed, label=label)
        except FuturesTimeout:
            elapsed = round(time.time() - t0, 1)
            logger.warning("[deep_analysis] STEP %d/%d: %s -- TIMEOUT after %.1fs",
                           step_num, total_steps, label, elapsed)
            if step_num:
                _emit("gathering", f"Step {step_num}/{total_steps}: {label} timed out, continuing...")
            outputs = {"status": "timeout", "elapsed_s": elapsed}
            if span:
                span.set_outputs(outputs)
            return ToolResult(success=False, data=None, error_type="timeout",
                              error_hint=f"Timed out after {timeout_s}s", elapsed_s=elapsed, label=label)
        except Exception as e:
            elapsed = round(time.time() - t0, 1)
            err_str = str(e)[:200]
            logger.warning("[deep_analysis] STEP %d/%d: %s -- ERROR in %.1fs: %s",
                           step_num, total_steps, label, elapsed, e)
            if step_num:
                _emit("gathering", f"Step {step_num}/{total_steps}: {label} failed, continuing...")
            outputs = {"status": "error", "error": err_str, "elapsed_s": elapsed}
            if span:
                span.set_outputs(outputs)
            error_type = "sql_error" if "sql" in err_str.lower() else "unknown"
            return ToolResult(success=False, data=None, error_type=error_type,
                              error_hint=err_str, elapsed_s=elapsed, label=label)


def _parse_json(raw: Optional[str]) -> Optional[dict]:
    if not raw:
        return None
    try:
        return json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return None


_TABLE_LEVEL_DOC_TYPES = {"table", "fk_relationship"}


def _extract_node_ids(vs_json: Optional[str]) -> list[str]:
    """Extract node_id values from vector search results.

    For non-table doc types (entity, metric_view), also include the table_name
    so that graph expansion has table-level nodes to work with.
    """
    data = _parse_json(vs_json)
    if not data:
        return []
    ids = []
    for m in data.get("matches", []):
        nid = m.get("node_id")
        if nid and nid not in ids:
            ids.append(nid)
        doc_type = m.get("doc_type", "")
        if doc_type not in _TABLE_LEVEL_DOC_TYPES:
            tn = m.get("table_name")
            if tn and tn not in ids:
                ids.append(tn)
    return ids


def _extract_table_names(vs_json: Optional[str]) -> list[str]:
    """Extract table_name values from vector search results."""
    data = _parse_json(vs_json)
    if not data:
        return []
    names = []
    for m in data.get("matches", []):
        tn = m.get("table_name")
        if tn and tn not in names:
            names.append(tn)
    return names


def _pick_traversal_nodes(node_ids: list[str], table_names: list[str]) -> list[str]:
    """Pick the best nodes for graph traversal (prefer table-level nodes).

    Uses table_names from VS results directly rather than guessing from ID format.
    """
    if table_names:
        return table_names[:3]
    return node_ids[:3]


def _sql_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "''")


def _build_relevance_sql(table_names: list[str]) -> str:
    """Build SQL to fetch FK predictions and column info for discovered tables."""
    tn_list = ", ".join(f"'{_sql_escape(t)}'" for t in table_names[:10])
    return (
        f"SELECT src_table, src_column, dst_table, dst_column, "
        f"final_confidence, join_rate, pk_uniqueness, ri_score, ai_reasoning "
        f"FROM {CATALOG}.{SCHEMA}.fk_predictions "
        f"WHERE src_table IN ({tn_list}) OR dst_table IN ({tn_list}) "
        f"ORDER BY final_confidence DESC LIMIT 10"
    )


def _extract_keywords(query: str) -> list[str]:
    """Extract meaningful keywords from query for SQL LIKE searches."""
    stop = {"find", "all", "tables", "related", "to", "and", "explain", "how",
            "they", "connect", "the", "what", "are", "which", "show", "me",
            "list", "describe", "about", "with", "from", "that", "for", "in"}
    words = re.findall(r'\w+', query.lower())
    return [w for w in words if w not in stop and len(w) > 2][:5]


def _build_baseline_queries(query: str) -> list[str]:
    """Build SQL queries for baseline mode against the 3 KB tables."""
    keywords = _extract_keywords(query)
    queries = []
    if keywords:
        like_clauses = " OR ".join(
            f"(LOWER(table_name) LIKE '%{_sql_escape(k)}%' OR LOWER(comment) LIKE '%{_sql_escape(k)}%')"
            for k in keywords
        )
        queries.append(
            f"SELECT table_name, domain, subdomain, comment, has_pii, has_phi, row_count "
            f"FROM {CATALOG}.{SCHEMA}.table_knowledge_base "
            f"WHERE {like_clauses} LIMIT 20"
        )
        queries.append(
            f"SELECT table_name, column_name, comment, data_type, classification "
            f"FROM {CATALOG}.{SCHEMA}.column_knowledge_base "
            f"WHERE {like_clauses} LIMIT 30"
        )
    else:
        queries.append(
            f"SELECT table_name, domain, comment FROM {CATALOG}.{SCHEMA}.table_knowledge_base LIMIT 20"
        )
    return queries


# ---------------------------------------------------------------------------
# SQL Writer subagent
# ---------------------------------------------------------------------------

SQL_WRITER_PROMPT = f"""You are a SQL writer for Databricks. Given table schemas and a
user question, produce a single read-only SELECT query that answers the question.

Rules:
- Databricks SQL dialect (ANSI SQL with Spark extensions).
- SELECT only. Never use INSERT/UPDATE/DELETE/DROP/CREATE/ALTER/TRUNCATE.
- Use fully-qualified table names (catalog.schema.table).
- If the question cannot be answered from the given schemas, return SKIP.
- Output ONLY the SQL query, nothing else. No markdown fencing, no explanation.

Query quality:
- When a "Relationships (FK Predictions)" section is provided, JOIN dimension/reference
  tables to enrich results with descriptive context (names, categories, phases, etc.).
- When joining multiple tables, use CTEs (WITH clauses) to separate aggregation from
  enrichment. For simple single-table queries, a flat SELECT is preferred.
- NEVER join an aggregated CTE back to the original row-level table unless you need
  row-level attributes that cannot be included in the GROUP BY. Re-joining aggregated
  results to their source table produces fan-out row multiplication. If you need both
  aggregate statistics and detail columns, include them all in the same GROUP BY or use
  window functions on the base table directly.
- Include relevant context columns (identifiers, names, categories) so results are
  self-explanatory without needing to look up codes in other tables.
- For division, use try_divide(numerator, NULLIF(denominator, 0)) -- Databricks-native
  safe division that returns NULL instead of erroring on zero.
- When a column comment identifies a primary key, prefer COUNT(pk_column) over COUNT(*).
- For GROUP BY / aggregate queries, use LIMIT 500 (aggregates produce fewer rows and
  truncation hides comparison groups). For detail/row-level queries, use LIMIT 200.
- For boolean/flag columns, use three-way CASE: TRUE -> 'Yes', FALSE -> 'No', ELSE -> 'Unknown/Not recorded'.
  Never use IS NOT NULL as a proxy for TRUE.
- If a "Column Value Distributions" section shows a column has only one distinct non-NULL value,
  avoid using it as a comparison variable. Note this in a SQL comment.
{SAFETY_PROMPT_BLOCK}"""


def _sql_writer(question: str, schema_context: str, cancel: threading.Event) -> Optional[str]:
    """LLM subagent that generates a SQL query from schema context."""
    if cancel.is_set():
        return None
    cm = maybe_span(name="sql_writer")
    with cm as span:
        if span:
            span.set_inputs({"question": question[:200], "schema_kb": len(schema_context)})
        try:
            resp = _llm().invoke([
                SystemMessage(content=SQL_WRITER_PROMPT),
                HumanMessage(content=f"## Table Schemas\n{schema_context}\n\n## Question\n{question}"),
            ])
            sql = (resp.content or "").strip()
            sql = sql.removeprefix("```sql").removeprefix("```").removesuffix("```").strip()
            if not sql or sql.upper() == "SKIP":
                if span:
                    span.set_outputs({"status": "skipped"})
                return None
            if span:
                span.set_outputs({"sql": sql[:500]})
            return sql
        except Exception as e:
            logger.warning("[deep_analysis] sql_writer error: %s", e)
            if span:
                span.set_outputs({"status": "error", "error": str(e)[:200]})
            return None


# ---------------------------------------------------------------------------
# Structured Retrieval subagent
# ---------------------------------------------------------------------------

def _fetch_table_schemas(table_names: list[str]) -> str:
    """Fetch column schemas, table descriptions, and FK predictions for discovered tables."""
    if not table_names:
        return ""
    tn_list = ", ".join(f"'{_sql_escape(t)}'" for t in table_names[:8])

    col_sql = (
        f"SELECT table_name, column_name, data_type, comment "
        f"FROM {CATALOG}.{SCHEMA}.column_knowledge_base "
        f"WHERE table_name IN ({tn_list}) "
        f"ORDER BY table_name, column_name LIMIT 200"
    )
    tr = _safe_tool_call(execute_metadata_sql, {"query": col_sql}, TOOL_TIMEOUT,
                         "fetch_schemas", 0, 0)
    if not tr.success or not tr.data:
        return ""
    data = _parse_json(tr.data)
    if not data or not data.get("rows"):
        return ""
    lines = []
    current_table = None
    for row in data["rows"]:
        tn = row.get("table_name", "")
        if tn != current_table:
            current_table = tn
            lines.append(f"\n### {tn}")
        col = row.get("column_name", "")
        dt = row.get("data_type", "")
        cmt = row.get("comment", "")
        lines.append(f"  {col} {dt}" + (f"  -- {cmt}" if cmt else ""))

    # Table descriptions -- helps the SQL writer understand dimension vs fact tables
    tbl_sql = (
        f"SELECT table_name, comment "
        f"FROM {CATALOG}.{SCHEMA}.table_knowledge_base "
        f"WHERE table_name IN ({tn_list}) LIMIT 10"
    )
    tbl_tr = _safe_tool_call(execute_metadata_sql, {"query": tbl_sql}, TOOL_TIMEOUT,
                             "fetch_table_descriptions", 0, 0)
    if tbl_tr.success and tbl_tr.data:
        tbl_data = _parse_json(tbl_tr.data)
        if tbl_data and tbl_data.get("rows"):
            lines.append("\n## Table Descriptions")
            for row in tbl_data["rows"]:
                cmt = row.get("comment", "")
                if cmt:
                    lines.append(f"  {row.get('table_name', '')}: {cmt[:300]}")

    # FK predictions -- gives the SQL writer join paths
    fk_sql = (
        f"SELECT src_table, src_column, dst_table, dst_column, final_confidence "
        f"FROM {CATALOG}.{SCHEMA}.fk_predictions "
        f"WHERE (src_table IN ({tn_list}) OR dst_table IN ({tn_list})) "
        f"AND final_confidence >= 0.5 "
        f"ORDER BY final_confidence DESC LIMIT 10"
    )
    fk_tr = _safe_tool_call(execute_metadata_sql, {"query": fk_sql}, TOOL_TIMEOUT,
                            "fetch_fk_predictions", 0, 0)
    if fk_tr.success and fk_tr.data:
        fk_data = _parse_json(fk_tr.data)
        if fk_data and fk_data.get("rows"):
            lines.append("\n## Relationships (FK Predictions)")
            for row in fk_data["rows"]:
                lines.append(
                    f"  {row.get('src_table','')}.{row.get('src_column','')}"
                    f" -> {row.get('dst_table','')}.{row.get('dst_column','')}"
                    f" (confidence: {row.get('final_confidence','')})"
                )

    return "\n".join(lines)


def _format_results_table(json_str: str, max_rows: int = 30) -> str:
    """Convert execute_data_sql JSON output to a markdown table."""
    data = _parse_json(json_str)
    if not data or not data.get("columns") or not data.get("rows"):
        return json_str
    cols = data["columns"]
    rows = data["rows"][:max_rows]
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"
    lines = [header, sep]
    for row in rows:
        vals = [str(row.get(c, "")) for c in cols]
        lines.append("| " + " | ".join(vals) + " |")
    if data.get("row_count", 0) > max_rows:
        lines.append(f"\n*({data['row_count']} total rows, showing first {max_rows})*")
    return "\n".join(lines)


def _pick_profiling_columns(table_names: list[str]) -> list[tuple[str, list[str]]]:
    """Select up to 5 categorical/flag columns per table for live profiling.

    Queries column_knowledge_base for columns likely to be useful for cohort
    detection (booleans, flags, status fields, categoricals).
    """
    if not table_names:
        return []
    B = EvidenceBudget
    tn_list = ", ".join(f"'{_sql_escape(t)}'" for t in table_names[:3])
    sql = (
        f"SELECT table_name, column_name, data_type, comment "
        f"FROM {CATALOG}.{SCHEMA}.column_knowledge_base "
        f"WHERE table_name IN ({tn_list}) "
        f"AND (LOWER(data_type) IN ('boolean', 'string', 'int', 'integer', 'smallint', 'tinyint') "
        f"     OR LOWER(column_name) LIKE '%flag%' OR LOWER(column_name) LIKE '%status%' "
        f"     OR LOWER(column_name) LIKE '%type%' OR LOWER(column_name) LIKE '%category%') "
        f"ORDER BY table_name LIMIT 30"
    )
    tr = _safe_tool_call(execute_metadata_sql, {"query": sql}, TOOL_TIMEOUT,
                         "pick_profiling_cols", 0, 0)
    if not tr.success or not tr.data:
        return []
    data = _parse_json(tr.data)
    if not data or not data.get("rows"):
        return []
    by_table: dict[str, list[str]] = {}
    for row in data["rows"]:
        tn = row.get("table_name", "")
        cn = row.get("column_name", "")
        if tn and cn:
            by_table.setdefault(tn, []).append(cn)
    return [(t, cols[:5]) for t, cols in by_table.items()]


# ---------------------------------------------------------------------------
# Auto-broadening: detect degenerate results and propose relaxation
# ---------------------------------------------------------------------------

_GROUP_BY_RE = re.compile(r'GROUP\s+BY\s+(.+?)(?:\s+HAVING|\s+ORDER|\s+LIMIT|\s+WINDOW|\s*$)', re.IGNORECASE | re.DOTALL)
_CASE_COL_RE = re.compile(r'CASE\s+WHEN\s+(\w+)', re.IGNORECASE)


def _detect_degeneracy(raw_json: str, sql: str) -> Optional[dict]:
    """Check if a key grouping column has only 1 distinct non-NULL value.

    Returns {"degenerate_column": col, "distinct_value": val, "sql": sql} or None.
    """
    data = _parse_json(raw_json)
    if not data or not data.get("rows") or not data.get("columns"):
        return None

    # Extract GROUP BY columns from the SQL
    group_cols = set()
    m = _GROUP_BY_RE.search(sql)
    if m:
        raw_cols = m.group(1)
        for part in raw_cols.split(","):
            col = part.strip().split(".")[-1].strip().strip('"').strip('`')
            if col and not col.startswith("CASE"):
                group_cols.add(col.lower())

    # Also pick up columns used in CASE WHEN (comparison variables)
    for cm in _CASE_COL_RE.finditer(sql):
        group_cols.add(cm.group(1).lower())

    if not group_cols:
        return None

    rows = data["rows"]
    columns = [c.lower() for c in data["columns"]]

    for col_lower in group_cols:
        if col_lower not in columns:
            continue
        orig_col = data["columns"][columns.index(col_lower)]
        distinct = {r.get(orig_col) for r in rows if r.get(orig_col) is not None}
        if len(distinct) == 1:
            return {
                "degenerate_column": orig_col,
                "distinct_value": str(next(iter(distinct))),
                "sql": sql,
            }
    return None


_RELAXATION_PROMPT = """You are a SQL expert. The following query returned a degenerate result:
column `{col}` has only 1 distinct value "{val}".

Original SQL:
{sql}

Schema context:
{schema}

Propose a single relaxed version of this SQL that removes or widens the most restrictive
WHERE clause to broaden the cohort while keeping the same analytical structure.

Rules:
- Keep the same SELECT columns and GROUP BY.
- Only remove or widen WHERE clauses -- do not add new filters or change aggregations.
- The relaxed query must be a strict superset of the original cohort.
- Output ONLY the SQL query. No explanation, no markdown fencing."""


def _propose_relaxation(original_sql: str, degen_info: dict,
                        schema_context: str, cancel: threading.Event) -> Optional[str]:
    """LLM call to generate a relaxed SQL variant."""
    if cancel.is_set():
        return None
    prompt = _RELAXATION_PROMPT.format(
        col=degen_info["degenerate_column"],
        val=degen_info["distinct_value"],
        sql=original_sql,
        schema=schema_context[:3000],
    )
    cm = maybe_span(name="propose_relaxation")
    with cm as span:
        try:
            resp = _llm().invoke([
                SystemMessage(content=prompt),
                HumanMessage(content="Generate the relaxed SQL."),
            ])
            sql = (resp.content or "").strip()
            sql = sql.removeprefix("```sql").removeprefix("```").removesuffix("```").strip()
            if not sql or sql.upper() == "SKIP":
                return None
            from agent.common import check_select_only
            err = check_select_only(sql)
            if err:
                logger.warning("[auto-broaden] Relaxed SQL failed validation: %s", err)
                return None
            if span:
                span.set_outputs({"sql": sql[:300]})
            return sql
        except Exception as e:
            logger.warning("[auto-broaden] Relaxation LLM error: %s", e)
            return None


def _diagnostic_distinct(column: str, table_names: list[str],
                         cancel: threading.Event,
                         step_num: int, total_steps: int) -> Optional[str]:
    """Run SELECT DISTINCT <column>, COUNT(*) on the first matching table.

    Returns a formatted results table or None.
    """
    if cancel.is_set() or not table_names:
        return None
    table = table_names[0]
    diag_sql = (
        f"SELECT `{column}`, COUNT(*) AS cnt "
        f"FROM {table} "
        f"GROUP BY `{column}` ORDER BY cnt DESC LIMIT 30"
    )
    from agent.common import check_select_only
    if check_select_only(diag_sql):
        return None
    _emit("gathering", f"Step {step_num}/{total_steps}: Diagnosing `{column}` value distribution...")
    diag_tr = _safe_tool_call(execute_data_sql, {"query": diag_sql}, TOOL_TIMEOUT,
                              "execute_data_sql (diagnostic)", step_num, total_steps)
    if diag_tr.success and diag_tr.data and "error" not in diag_tr.data.lower():
        return _format_results_table(diag_tr.data)
    return None


def _drop_filter(sql: str, column: str) -> Optional[str]:
    """Deterministic SQL rewrite: remove AND-clauses referencing *column*."""
    col_escaped = re.escape(column)
    # Match "AND ... <column> ... " up to the next AND/GROUP/ORDER/LIMIT/HAVING/)/newline
    pattern = re.compile(
        rf"(?i)\bAND\s+[^\n]*?(?:\w+\.)?`?{col_escaped}`?[^\n]*",
    )
    rewritten = pattern.sub("", sql)
    # Clean up any resulting double whitespace
    rewritten = re.sub(r'\n\s*\n', '\n', rewritten)
    if rewritten.strip() == sql.strip():
        return None
    from agent.common import check_select_only
    if check_select_only(rewritten):
        return None
    return rewritten.strip()


def _cascade_broaden(
    degen: dict, relaxed_sql: str, schema_ctx: str,
    table_names: list[str], cancel: threading.Event,
    step_num: int, total_steps: int, output: str,
) -> tuple[str, str]:
    """Second-pass broadening when the first relaxation is still degenerate.

    1. Runs a diagnostic micro-query to surface the binding column's value distribution.
    2. Drops the binding filter entirely and re-executes.

    Returns (updated_output, final_sql).
    """
    col = degen["degenerate_column"]
    logger.info("[cascade-broaden] First broadening still degenerate on `%s`, cascading...", col)

    # Step 1: diagnostic micro-query
    diag_table = _diagnostic_distinct(col, table_names, cancel, step_num, total_steps)
    if diag_table:
        output += (
            f"\n\n---\n\n"
            f"**Diagnostic: `{col}` value distribution (full table)**:\n{diag_table}"
        )
        logger.info("[cascade-broaden] Diagnostic for `%s` completed", col)

    if cancel.is_set():
        return output, relaxed_sql

    # Step 2: drop the binding filter entirely
    _emit("gathering", f"Step {step_num}/{total_steps}: Dropping `{col}` filter for maximum breadth...")
    nuclear_sql = _drop_filter(relaxed_sql, col)
    if not nuclear_sql:
        nuclear_sql = _drop_filter(relaxed_sql, col.lower())
    if not nuclear_sql:
        logger.info("[cascade-broaden] Could not deterministically drop `%s`, falling back to LLM", col)
        nuclear_sql = _propose_relaxation(relaxed_sql, degen, schema_ctx, cancel)

    if nuclear_sql and not cancel.is_set():
        nuc_tr = _safe_tool_call(execute_data_sql, {"query": nuclear_sql},
                                 TOOL_TIMEOUT, "execute_data_sql (nuclear broaden)",
                                 step_num, total_steps)
        if nuc_tr.success and nuc_tr.data and "error" not in nuc_tr.data.lower():
            nuc_table = _format_results_table(nuc_tr.data)
            nuc_ct = _parse_json(nuc_tr.data).get("row_count", "") if _parse_json(nuc_tr.data) else ""
            output += (
                f"\n\n---\n\n"
                f"**Filter-Dropped Cohort** (removed `{col}` constraint, {nuc_ct} rows):\n"
                f"{nuc_table}"
            )
            logger.info("[cascade-broaden] Nuclear broadening returned %s rows", nuc_ct)
            return output, nuclear_sql

    return output, relaxed_sql


def _structured_retrieval(question: str, table_names: list[str],
                          cancel: threading.Event, step_num: int, total_steps: int,
                          profiling_context: str = "", comparison_intent: bool = False) -> ToolResult:
    """Structured retrieval subagent: schema lookup -> SQL writer -> execute (with 1 retry).

    Returns a ToolResult with the generated SQL and results.
    """
    label = "structured_retrieval"
    if not table_names or cancel.is_set():
        return ToolResult(success=False, data=None, error_type="skipped",
                          error_hint="No tables to query", label=label)

    cm = maybe_span(name="structured_retrieval", span_type="RETRIEVER")

    with cm as span:
        t0 = time.time()
        if span:
            span.set_inputs({"question": question[:200], "tables": table_names[:8]})

        _emit("gathering", f"Step {step_num}/{total_steps}: Fetching schemas for data retrieval...")

        schema_ctx = _fetch_table_schemas(table_names)
        if not schema_ctx or cancel.is_set():
            if span:
                span.set_outputs({"status": "no_schemas"})
            return ToolResult(success=False, data=None, error_type="no_schemas",
                              error_hint="Could not fetch table schemas", label=label)

        if profiling_context:
            schema_ctx += f"\n\n## Column Value Distributions (live profiling)\n{profiling_context}"

        _emit("gathering", f"Step {step_num}/{total_steps}: SQL writer generating query...")

        sql = _sql_writer(question, schema_ctx, cancel)
        if not sql or cancel.is_set():
            if span:
                span.set_outputs({"status": "no_sql"})
            return ToolResult(success=False, data=None, error_type="no_sql",
                              error_hint="SQL writer produced no query", label=label)

        logger.info("[deep_analysis] structured_retrieval: executing generated SQL: %s", sql[:200])
        _emit("gathering", f"Step {step_num}/{total_steps}: Executing data query...")

        tr = _safe_tool_call(execute_data_sql, {"query": sql}, TOOL_TIMEOUT,
                             "execute_data_sql", step_num, total_steps)

        if not tr.success or (tr.data and "error" in tr.data.lower()):
            logger.info("[deep_analysis] structured_retrieval: first SQL attempt failed, retrying...")
            error_msg = tr.error_hint or (tr.data[:300] if tr.data else "Unknown error")
            _emit("gathering", f"Step {step_num}/{total_steps}: SQL retry after error...")
            retry_sql = _sql_writer(
                f"{question}\n\nPrevious SQL failed: {sql}\nError: {error_msg}\nPlease fix the query.",
                schema_ctx, cancel,
            )
            if retry_sql and retry_sql != sql and not cancel.is_set():
                tr = _safe_tool_call(execute_data_sql, {"query": retry_sql}, TOOL_TIMEOUT,
                                     "execute_data_sql (retry)", step_num, total_steps)
                if tr.success:
                    sql = retry_sql

        elapsed = round(time.time() - t0, 1)
        if not tr.success or not tr.data or (tr.data and "error" in tr.data.lower()):
            out = {"status": "exec_failed", "sql": sql[:300]}
            if span:
                span.set_outputs(out)
            return ToolResult(success=False, data=None, error_type=tr.error_type or "exec_failed",
                              error_hint=f"SQL execution failed: {tr.error_hint}", elapsed_s=elapsed, label=label)

        results_table = _format_results_table(tr.data)
        row_ct = _parse_json(tr.data).get("row_count", "") if _parse_json(tr.data) else ""
        output = f"**Original Cohort** ({row_ct} rows):\n{results_table}"

        # Auto-broaden: if comparison query returned a degenerate result, relax and re-run
        if comparison_intent and not cancel.is_set():
            degen = _detect_degeneracy(tr.data, sql)
            if degen:
                logger.info("[auto-broaden] Degenerate column=%s value=%s, proposing relaxation",
                            degen["degenerate_column"], degen["distinct_value"])
                _emit("gathering", f"Step {step_num}/{total_steps}: Broadening degenerate result...")
                relaxed_sql = _propose_relaxation(sql, degen, schema_ctx, cancel)
                if relaxed_sql and not cancel.is_set():
                    relaxed_tr = _safe_tool_call(execute_data_sql, {"query": relaxed_sql},
                                                 TOOL_TIMEOUT, "execute_data_sql (broadened)",
                                                 step_num, total_steps)
                    if relaxed_tr.success and relaxed_tr.data and "error" not in relaxed_tr.data.lower():
                        broad_table = _format_results_table(relaxed_tr.data)
                        broad_ct = _parse_json(relaxed_tr.data).get("row_count", "") if _parse_json(relaxed_tr.data) else ""
                        output += (
                            f"\n\n---\n\n"
                            f"**Broadened Cohort** (relaxed filter on `{degen['degenerate_column']}`, {broad_ct} rows):\n"
                            f"{broad_table}"
                        )
                        logger.info("[auto-broaden] Broadened query returned %s rows", broad_ct)

                        # Cascading broadening: if broadened result is still degenerate,
                        # run a diagnostic query then drop the binding filter entirely
                        broad_degen = _detect_degeneracy(relaxed_tr.data, relaxed_sql)
                        if broad_degen and not cancel.is_set():
                            output, relaxed_sql = _cascade_broaden(
                                broad_degen, relaxed_sql, schema_ctx, table_names,
                                cancel, step_num, total_steps, output,
                            )

        if span:
            span.set_outputs({"status": "ok", "sql": sql[:300], "result_len": len(output)})
        return ToolResult(success=True, data=output, elapsed_s=elapsed, label=label)


# ---------------------------------------------------------------------------
# Query planner -- chooses edge_type filters to prevent graph fan-out
# ---------------------------------------------------------------------------

BUILTIN_EDGE_TYPES = {
    "contains", "references", "instance_of", "has_property", "is_a",
    "same_domain", "same_subdomain", "same_catalog", "same_schema",
    "same_security_level", "similar_embedding",
    "derives_from",
}

_BUILTIN_PROMPT_SECTION = """Available edge types:
  contains        -- schema->table, table->column (structural hierarchy)
  references      -- FK relationship between tables (join paths)
  instance_of     -- table is instance of ontology entity
  has_property    -- ontology entity has a property mapped to a column
  is_a            -- entity type hierarchy
  same_domain     -- tables share business domain
  same_subdomain  -- tables share subdomain
  similar_embedding -- embedding similarity between nodes
  derives_from    -- lineage: one table derives from another"""

_PLANNER_TEMPLATE = """You are a retrieval planner for a metadata knowledge graph.

Given a user question, decide which graph edge types to use so that multi-hop
traversal stays focused and doesn't fan out into unrelated nodes.

{edge_type_section}

Respond with ONLY a JSON object (no markdown fencing):
{{
  "edge_types": ["type1", "type2"],
  "traverse_edge_type": "single_most_important_type",
  "direction": "outgoing",
  "requires_structured_retrieval": true
}}

CRITICAL RULES:
- traverse_edge_type MUST always be set to exactly one type. NEVER null.
  This controls 3-hop graph traversal; unfiltered traversal is too expensive.
- edge_types: 1-3 types for 1-hop expansion from vector search hits.
- direction: "outgoing" (default, safest), "incoming", or "both" (use sparingly).
- requires_structured_retrieval: true if the question needs data from actual tables
  (not just metadata). False for pure metadata/governance questions.

Decision guide:
- Joins / FKs / "how do tables relate": edge_types=["references","contains"], traverse="references", direction="both"
- Ontology / entity / "what type of data": edge_types=["instance_of","has_property"], traverse="instance_of"
- Lineage / "where does data come from": edge_types=["derives_from","contains"], traverse="derives_from"
- Domain / similarity / "similar tables": edge_types=["same_domain","similar_embedding"], traverse="same_domain"
- Table structure / "what columns": edge_types=["contains"], traverse="contains"
- General / unclear: edge_types=["references","contains"], traverse="references"
"""

PLANNER_TIMEOUT = 10  # seconds; fast fail so pipeline isn't blocked

_ontology_edge_cache: dict[str, set[str]] = {}


def _discover_ontology_edge_types() -> set[str]:
    """Query graph_edges for dynamic ontology-sourced edge types not in the builtin set."""
    cache_key = f"{CATALOG}.{SCHEMA}"
    if cache_key in _ontology_edge_cache:
        return _ontology_edge_cache[cache_key]
    try:
        from api_server import graph_query
        rows = graph_query(
            "SELECT DISTINCT edge_type FROM public.graph_edges "
            "WHERE source_system = 'ontology' AND edge_type IS NOT NULL"
        )
        discovered = {r["edge_type"] for r in rows if r.get("edge_type")} - BUILTIN_EDGE_TYPES
        _ontology_edge_cache[cache_key] = discovered
        if discovered:
            logger.info("[deep_analysis] Discovered %d ontology edge types: %s",
                        len(discovered), sorted(discovered)[:10])
        return discovered
    except Exception as e:
        logger.debug("[deep_analysis] Could not discover ontology edge types: %s", e)
        _ontology_edge_cache[cache_key] = set()
        return set()


def _build_planner_prompt(extra_edge_types: set[str]) -> str:
    """Build the planner system prompt, appending any dynamic ontology edge types."""
    if not extra_edge_types:
        return _PLANNER_TEMPLATE.format(edge_type_section=_BUILTIN_PROMPT_SECTION)
    extras = "\n".join(f"  {et:20s}-- ontology relationship" for et in sorted(extra_edge_types)[:15])
    section = _BUILTIN_PROMPT_SECTION + "\n\nOntology-specific edge types (from active bundle):\n" + extras
    return _PLANNER_TEMPLATE.format(edge_type_section=section)


def _plan_retrieval(query: str, cancel: threading.Event,
                    extra_edge_types: set[str] | None = None) -> dict:
    """Fast LLM call to decide edge_type filters for graph traversal.

    Always returns a valid plan dict. On any failure, returns a safe default
    that filters to references+contains (the most common useful pattern).
    """
    valid_types = BUILTIN_EDGE_TYPES | (extra_edge_types or set())
    default = {"edge_types": ["references", "contains"],
               "traverse_edge_type": "references", "direction": "outgoing",
               "requires_structured_retrieval": True}
    if cancel.is_set():
        return default

    prompt = _build_planner_prompt(extra_edge_types or set())
    cm = maybe_span(name="query_planner", span_type="CHAIN")

    with cm as span:
        if span:
            span.set_inputs({"query": query[:200]})
        try:
            ctx = contextvars.copy_context()
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(
                    ctx.run, _llm().invoke,
                    [SystemMessage(content=prompt),
                     HumanMessage(content=query)],
                )
                resp = future.result(timeout=PLANNER_TIMEOUT)
            raw = (resp.content or "").strip()
            raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
            plan = json.loads(raw)

            et = plan.get("edge_types")
            if isinstance(et, list):
                plan["edge_types"] = [e for e in et if e in valid_types] or default["edge_types"]
            else:
                plan["edge_types"] = default["edge_types"]

            tet = plan.get("traverse_edge_type")
            if not tet or tet not in valid_types:
                plan["traverse_edge_type"] = default["traverse_edge_type"]

            if plan.get("direction") not in ("outgoing", "incoming", "both"):
                plan["direction"] = "outgoing"

            plan.setdefault("requires_structured_retrieval", True)

            logger.info("[deep_analysis] query_planner: edges=%s traverse=%s",
                        plan["edge_types"], plan["traverse_edge_type"])
            if span:
                span.set_outputs(plan)
            return plan
        except FuturesTimeout:
            logger.warning("[deep_analysis] query_planner timed out after %ds, using defaults", PLANNER_TIMEOUT)
            if span:
                span.set_outputs({"status": "timeout"})
            return default
        except Exception as e:
            logger.warning("[deep_analysis] query_planner failed, using defaults: %s", e)
            if span:
                span.set_outputs({"status": "error", "error": str(e)[:200]})
            return default


# ---------------------------------------------------------------------------
# Data gathering pipelines
# ---------------------------------------------------------------------------

def _merge_graph_data(existing: dict, new_chunk: dict) -> dict:
    """Merge a traverse_graph result into an accumulated graph_data dict."""
    nodes = dict(existing.get("nodes") or {})
    nodes.update(new_chunk.get("nodes") or {})
    edges = list(existing.get("edges") or [])
    seen = {(e.get("src"), e.get("dst"), e.get("relationship")) for e in edges}
    for e in new_chunk.get("edges") or []:
        key = (e.get("src"), e.get("dst"), e.get("relationship"))
        if key not in seen:
            edges.append(e)
            seen.add(key)
    start = existing.get("start_node") or new_chunk.get("start_node")
    return {"start_node": start, "nodes": nodes, "edges": edges}


def _sparql_ontology_query(table_names: List[str]) -> Optional[str]:
    """Query the formal RDF ontology graph for relationships between discovered tables."""
    try:
        from dbxmetagen.ontology_graph_store import OntologyGraphStore, is_available
        if not is_available():
            logger.debug("pyoxigraph not installed, skipping SPARQL ontology query")
            return None
    except ImportError:
        return None

    ttl_candidates = [
        f"/Volumes/{CATALOG}/{SCHEMA}/generated_metadata/ontology_output.ttl",
        os.path.join(os.path.dirname(__file__), "..", "ontology_output.ttl"),
        f"/tmp/dbxmetagen_ontology_{SCHEMA}.ttl",
    ]
    ttl_path = None
    for p in ttl_candidates:
        if os.path.isfile(p):
            ttl_path = p
            break
    if not ttl_path:
        logger.info("No ontology TTL file found, checked: %s", ttl_candidates)
        return None

    store = OntologyGraphStore(turtle_paths=[ttl_path])
    if store.triple_count == 0:
        return None

    uc_base = f"https://databricks.com/unitycatalog/{CATALOG}/{SCHEMA}/"
    results = []
    for tbl in table_names[:5]:
        related = store.get_related_tables(f"{uc_base}{tbl}")
        if related:
            results.append({"table": tbl, "ontology_relationships": related})

    if not results:
        return None
    return json.dumps(results, indent=2)


def _build_discovery_sql() -> str:
    """SQL to enumerate all discovered ontology entities grouped by type."""
    return (
        f"SELECT entity_type, entity_name, source_tables, confidence, entity_role, "
        f"description, entity_uri, source_ontology "
        f"FROM {CATALOG}.{SCHEMA}.ontology_entities "
        f"ORDER BY entity_type, confidence DESC LIMIT 100"
    )


def _build_graph_entity_sql() -> str:
    """SQL to list entity nodes from the knowledge graph with their relationships."""
    return (
        f"SELECT n.id, n.ontology_type, n.display_name, n.short_description, "
        f"n.quality_score, n.status "
        f"FROM {CATALOG}.{SCHEMA}.graph_nodes n "
        f"WHERE n.node_type = 'entity' AND n.source_system = 'ontology' "
        f"ORDER BY n.ontology_type, n.quality_score DESC LIMIT 100"
    )


def _build_entity_edge_sql() -> str:
    """SQL to list instance_of edges linking tables to entities."""
    return (
        f"SELECT e.src AS table_name, e.dst AS entity_id, e.edge_type, e.weight, "
        f"n.ontology_type AS entity_type, n.display_name AS entity_name "
        f"FROM {CATALOG}.{SCHEMA}.graph_edges e "
        f"JOIN {CATALOG}.{SCHEMA}.graph_nodes n ON e.dst = n.id "
        f"WHERE e.edge_type = 'instance_of' AND e.source_system = 'ontology' "
        f"ORDER BY n.ontology_type, e.src LIMIT 100"
    )


def _gather_discovery(query: str, cancel: threading.Event):
    """Gather evidence for catalog-wide enumeration questions via direct SQL."""
    parts: list[str] = []
    tools_used: list[str] = []
    failed: list[ToolResult] = []
    timing: dict = {}
    B = EvidenceBudget

    queries = [
        ("ontology_entities (all discovered entities)", _build_discovery_sql()),
        ("graph_nodes (entity nodes)", _build_graph_entity_sql()),
        ("graph_edges (table-to-entity mappings)", _build_entity_edge_sql()),
    ]
    with measure_phase("discovery_sql", timing):
        for label, sql in queries:
            if cancel.is_set():
                break
            tr = _safe_tool_call(
                execute_metadata_sql, {"query": sql}, TOOL_TIMEOUT,
                f"execute_metadata_sql ({label})", 1, len(queries),
            )
            if tr.success and tr.data:
                tools_used.append("execute_metadata_sql")
                parts.append(f"### Source: {label}\n{_truncate_part(tr.data, B.VS_RESULTS)}")
            elif not tr.success:
                failed.append(tr)

    return parts, tools_used, failed, timing


_NUM_RESULTS_BY_COMPLEXITY = {"simple": 3, "moderate": 5, "complex": 10}
_DEFAULT_EDGE_TYPES = ["references", "contains"]


def _gather_vs_only(query: str, cancel: threading.Event,
                    session_id: Optional[str] = None, intent_type: str = "new_question",
                    intent_domain: Optional[str] = None, num_results: int = 3):
    """VS + metadata SQL only. No graph calls, no _plan_retrieval."""
    parts = []
    tools_used = []
    failed_sources: list[ToolResult] = []
    timing: dict = {}
    B = EvidenceBudget

    def _collect(tr: ToolResult, source_label: str, limit: int):
        if tr.success and tr.data:
            tools_used.append(tr.label.split("(")[0].strip())
            parts.append(f"### Source: {source_label}\n{_truncate_part(tr.data, limit)}")
        elif not tr.success:
            failed_sources.append(tr)

    with measure_phase("vector_search", timing):
        vs_tr = None
        if session_id:
            cached = vs_cache_get(session_id, intent_type=intent_type)
            if cached:
                vs_tr = ToolResult(success=True, data=cached, label="search_metadata (cached)")
        if vs_tr is None:
            vs_tr = _safe_tool_call(search_metadata, {"query": query, "num_results": num_results},
                                    TOOL_TIMEOUT, "search_metadata", 1, 3)
            if vs_tr.success and vs_tr.data and session_id:
                vs_cache_put(session_id, query, vs_tr.data)
        _collect(vs_tr, "search_metadata (vector search)", B.VS_RESULTS)

    table_names = _extract_table_names(vs_tr.data if vs_tr.success else None)

    if cancel.is_set():
        return "\n\n".join(parts), {}, tools_used, failed_sources, timing, parts

    if table_names:
        with measure_phase("fk_sql", timing):
            fk_tr = _safe_tool_call(execute_metadata_sql,
                                    {"query": _build_relevance_sql(table_names)}, TOOL_TIMEOUT,
                                    "execute_metadata_sql (FK predictions)", 2, 3)
            _collect(fk_tr, "execute_metadata_sql (FK predictions)", B.FK_PREDICTIONS)

    if table_names and intent_domain == "query":
        prof_cols = _pick_profiling_columns(table_names[:1])
        if prof_cols:
            tbl, cols = prof_cols[0]
            prof_tr = _safe_tool_call(profile_key_columns,
                                      {"table_fqn": tbl, "columns": cols}, TOOL_TIMEOUT,
                                      "profile_key_columns", 3, 3)
            if prof_tr.success and prof_tr.data:
                tools_used.append("profile_key_columns")
                parts.append(f"### Source: profile_key_columns (live data distribution)\n"
                             f"{_truncate_part(prof_tr.data, B.PROFILING)}")
            elif not prof_tr.success:
                failed_sources.append(prof_tr)

    context = "\n\n".join(parts) if parts else ""
    return context, {}, tools_used, failed_sources, timing, parts


def _gather_vs_expand(query: str, cancel: threading.Event,
                      session_id: Optional[str] = None, intent_type: str = "new_question",
                      intent_domain: Optional[str] = None, num_results: int = 5,
                      comparison_intent: bool = False):
    """VS + 1-hop expand + FK. No BFS traversal, no _plan_retrieval."""
    parts = []
    tools_used = []
    graph_data = {}
    failed_sources: list[ToolResult] = []
    timing: dict = {}
    B = EvidenceBudget

    def _collect(tr: ToolResult, source_label: str, limit: int):
        if tr.success and tr.data:
            tools_used.append(tr.label.split("(")[0].strip())
            parts.append(f"### Source: {source_label}\n{_truncate_part(tr.data, limit)}")
        elif not tr.success:
            failed_sources.append(tr)

    with measure_phase("vector_search", timing):
        vs_tr = None
        if session_id:
            cached = vs_cache_get(session_id, intent_type=intent_type)
            if cached:
                vs_tr = ToolResult(success=True, data=cached, label="search_metadata (cached)")
        if vs_tr is None:
            vs_tr = _safe_tool_call(search_metadata, {"query": query, "num_results": num_results},
                                    TOOL_TIMEOUT, "search_metadata", 1, 5)
            if vs_tr.success and vs_tr.data and session_id:
                vs_cache_put(session_id, query, vs_tr.data)
        _collect(vs_tr, "search_metadata (vector search)", B.VS_RESULTS)

    node_ids = _extract_node_ids(vs_tr.data if vs_tr.success else None)
    table_names = _extract_table_names(vs_tr.data if vs_tr.success else None)

    if cancel.is_set():
        return "\n\n".join(parts), graph_data, tools_used, failed_sources, timing, parts

    # 1-hop expand
    with measure_phase("graph_expand", timing):
        if node_ids:
            exp_tr = _safe_tool_call(expand_vs_hits,
                                     {"node_ids": node_ids[:5], "edge_types": _DEFAULT_EDGE_TYPES},
                                     TOOL_TIMEOUT, "expand_vs_hits", 2, 5)
            _collect(exp_tr, "expand_vs_hits (graph expansion)", B.GRAPH_EXPANSION)

    if cancel.is_set():
        return "\n\n".join(parts), graph_data, tools_used, failed_sources, timing, parts

    with measure_phase("fk_and_structured", timing):
        if table_names:
            fk_tr = _safe_tool_call(execute_metadata_sql,
                                    {"query": _build_relevance_sql(table_names)}, TOOL_TIMEOUT,
                                    "execute_metadata_sql (FK predictions)", 3, 5)
            _collect(fk_tr, "execute_metadata_sql (FK predictions)", B.FK_PREDICTIONS)

            profiling_summary = ""
            if intent_domain == "query":
                prof_cols = _pick_profiling_columns(table_names[:1])
                if prof_cols:
                    tbl, cols = prof_cols[0]
                    prof_tr = _safe_tool_call(profile_key_columns,
                                              {"table_fqn": tbl, "columns": cols}, TOOL_TIMEOUT,
                                              "profile_key_columns", 4, 5)
                    if prof_tr.success and prof_tr.data:
                        tools_used.append("profile_key_columns")
                        profiling_summary = _truncate_part(prof_tr.data, B.PROFILING)
                        parts.append(f"### Source: profile_key_columns (live data distribution)\n{profiling_summary}")
                    elif not prof_tr.success:
                        failed_sources.append(prof_tr)

                sr_tr = _structured_retrieval(query, table_names, cancel, 5, 5,
                                              profiling_context=profiling_summary,
                                              comparison_intent=comparison_intent)
                if sr_tr.success and sr_tr.data:
                    tools_used.extend(["sql_writer", "execute_data_sql"])
                    parts.append(f"### Source: structured_retrieval (LLM-generated SQL on data)\n"
                                 f"{_truncate_part(sr_tr.data, B.STRUCTURED_RETRIEVAL)}")
                elif not sr_tr.success:
                    failed_sources.append(sr_tr)

    context = "\n\n".join(parts) if parts else ""
    return context, graph_data, tools_used, failed_sources, timing, parts


def _gather_graphrag(query: str, cancel: threading.Event,
                     session_id: Optional[str] = None, intent_type: str = "new_question",
                     intent_domain: Optional[str] = None,
                     max_hops: int = 3, max_traversal_nodes: int = 3,
                     num_results: int = 10, comparison_intent: bool = False):
    """Deterministic GraphRAG data gathering with parallel execution.

    Pipeline: plan -> VS -> [expand + traverse] parallel -> SPARQL ontology -> [FK SQL + structured retrieval] parallel

    For discovery-domain enumeration questions, a direct SQL path against
    ontology_entities and graph_nodes runs first to provide comprehensive coverage
    before the VS -> BFS pipeline adds supplemental context.

    Cross-thread span parenting is handled via contextvars.copy_context() so that
    all ThreadPoolExecutor workers inherit the parent evidence_retrieval span.
    """
    parts = []
    tools_used = []
    graph_data = {}
    failed_sources: list[ToolResult] = []
    timing: dict = {}
    total = 7
    B = EvidenceBudget

    def _collect(tr: ToolResult, source_label: str, limit: int):
        if tr.success and tr.data:
            tools_used.append(tr.label.split("(")[0].strip())
            parts.append(f"### Source: {source_label}\n{_truncate_part(tr.data, limit)}")
        elif not tr.success:
            failed_sources.append(tr)

    # Discovery-domain enumeration: direct SQL against ontology tables first
    if intent_domain == "discovery":
        _emit("gathering", "Querying ontology catalog for entity enumeration...")
        disc_parts, disc_tools, disc_failed, disc_timing = _gather_discovery(query, cancel)
        parts.extend(disc_parts)
        tools_used.extend(disc_tools)
        failed_sources.extend(disc_failed)
        timing.update(disc_timing)
        if cancel.is_set():
            return "\n\n".join(parts), graph_data, tools_used, failed_sources, timing, parts

    ontology_edge_types = _discover_ontology_edge_types()

    with measure_phase("plan_retrieval", timing):
        _emit("gathering", f"Step 1/{total}: Planning retrieval strategy...")
        from agent.common import plan_cache_get, plan_cache_put
        cached_plan = plan_cache_get(session_id or "", intent_domain or "general") if session_id else None
        if cached_plan:
            plan = cached_plan
        else:
            plan = _plan_retrieval(query, cancel, extra_edge_types=ontology_edge_types)
            if session_id:
                plan_cache_put(session_id, intent_domain or "general", plan)
    planned_edge_types = plan["edge_types"]
    planned_traverse_et = plan["traverse_edge_type"]
    planned_direction = plan["direction"]
    logger.info("[deep_analysis] Plan: expand=%s, traverse=%s, direction=%s",
                planned_edge_types, planned_traverse_et, planned_direction)

    if cancel.is_set():
        return "\n\n".join(parts), graph_data, tools_used, failed_sources, timing, parts

    with measure_phase("vector_search", timing):
        vs_tr = None
        if session_id:
            cached = vs_cache_get(session_id, intent_type=intent_type)
            if cached:
                vs_tr = ToolResult(success=True, data=cached, label="search_metadata (cached)")
        if vs_tr is None:
            vs_tr = _safe_tool_call(search_metadata, {"query": query, "num_results": num_results},
                                    TOOL_TIMEOUT, "search_metadata", 2, total)
            if vs_tr.success and vs_tr.data and session_id:
                vs_cache_put(session_id, query, vs_tr.data)
        _collect(vs_tr, "search_metadata (vector search)", B.VS_RESULTS)

    node_ids = _extract_node_ids(vs_tr.data if vs_tr.success else None)
    table_names = _extract_table_names(vs_tr.data if vs_tr.success else None)

    if not node_ids and not table_names:
        logger.info("[deep_analysis] VS returned no matches -- falling back to SQL keyword discovery")
        _emit("gathering", f"Step 2/{total}: VS unavailable, using SQL keyword fallback...")
        keywords = _extract_keywords(query)
        if keywords:
            like_clauses = " OR ".join(
                f"(LOWER(table_name) LIKE '%{_sql_escape(k)}%' OR LOWER(comment) LIKE '%{_sql_escape(k)}%')"
                for k in keywords
            )
            fallback_sql = (
                f"SELECT table_name, domain, subdomain, comment, has_pii, row_count "
                f"FROM {CATALOG}.{SCHEMA}.table_knowledge_base "
                f"WHERE {like_clauses} LIMIT 20"
            )
            fb_tr = _safe_tool_call(
                execute_metadata_sql, {"query": fallback_sql}, TOOL_TIMEOUT,
                "execute_metadata_sql (keyword fallback)", 2, total,
            )
            if fb_tr.success and fb_tr.data:
                _collect(fb_tr, "execute_metadata_sql (keyword fallback)", B.VS_RESULTS)
                fb_data = _parse_json(fb_tr.data)
                if fb_data and fb_data.get("rows"):
                    table_names = [r["table_name"] for r in fb_data["rows"] if r.get("table_name")]
                    node_ids = table_names[:]

    if cancel.is_set():
        return "\n\n".join(parts), graph_data, tools_used, failed_sources, timing, parts

    traverse_et_list = [planned_traverse_et] + [
        et for et in planned_edge_types if et != planned_traverse_et
    ]

    # Group A (parallel): expand_vs_hits + traverse_graph
    with measure_phase("graph_expand_traverse", timing):
        if node_ids:
            trav_nodes = _pick_traversal_nodes(node_ids, table_names)
            with ThreadPoolExecutor(max_workers=4) as pool:
                ctx = contextvars.copy_context()
                expand_future = pool.submit(
                    ctx.run, _safe_tool_call, expand_vs_hits,
                    {"node_ids": node_ids[:5], "edge_types": planned_edge_types},
                    TOOL_TIMEOUT, "expand_vs_hits", 3, total,
                )
                trav_futures = []
                for i, nid in enumerate(trav_nodes[:max_traversal_nodes]):
                    ctx = contextvars.copy_context()
                    trav_futures.append(pool.submit(
                        ctx.run, _safe_tool_call, traverse_graph,
                        {"start_node": nid, "max_hops": max_hops,
                         "edge_types": traverse_et_list, "direction": planned_direction},
                        TOOL_TIMEOUT, f"traverse_graph({nid.split('.')[-1]})", 4 + i, total,
                    ))

                exp_tr = expand_future.result()
                _collect(exp_tr, "expand_vs_hits (graph expansion)", B.GRAPH_EXPANSION)

                for ft in trav_futures:
                    trav_tr = ft.result()
                    if trav_tr.success and trav_tr.data:
                        trav_str = trav_tr.data if isinstance(trav_tr.data, str) else json.dumps(trav_tr.data)
                        tools_used.append("traverse_graph")
                        parts.append(f"### Source: traverse_graph\n{_truncate_part(trav_str, B.GRAPH_TRAVERSAL)}")
                        chunk = _parse_json(trav_str)
                        if chunk and isinstance(chunk, dict):
                            graph_data = _merge_graph_data(graph_data, chunk)
                    elif not trav_tr.success:
                        failed_sources.append(trav_tr)
        else:
            logger.info("[deep_analysis] Group A: SKIP (no node_ids)")
            _emit("gathering", f"Step 3/{total}: No nodes to expand, skipping...")

    if cancel.is_set():
        return "\n\n".join(parts), graph_data, tools_used, failed_sources, timing, parts

    if table_names:
        with measure_phase("sparql_ontology", timing):
            try:
                sparql_result = _sparql_ontology_query(table_names)
                if sparql_result:
                    tools_used.append("sparql_ontology_query")
                    parts.append(f"### Source: sparql_ontology_query (formal RDF ontology)\n{_truncate_part(sparql_result, B.GRAPH_TRAVERSAL)}")
            except Exception as e:
                logger.debug("SPARQL ontology query failed (non-critical): %s", e)

    if cancel.is_set():
        return "\n\n".join(parts), graph_data, tools_used, failed_sources, timing, parts

    # Group B (parallel): FK SQL + profiling + structured retrieval
    with measure_phase("fk_and_structured", timing):
        if table_names:
            with ThreadPoolExecutor(max_workers=3) as pool:
                ctx = contextvars.copy_context()
                fk_future = pool.submit(
                    ctx.run, _safe_tool_call, execute_metadata_sql,
                    {"query": _build_relevance_sql(table_names)}, TOOL_TIMEOUT,
                    "execute_metadata_sql (FK predictions)", 6, total,
                )
                prof_future = None
                if plan.get("requires_structured_retrieval", True):
                    prof_cols = _pick_profiling_columns(table_names[:1])
                    if prof_cols:
                        tbl, cols = prof_cols[0]
                        ctx = contextvars.copy_context()
                        prof_future = pool.submit(
                            ctx.run, _safe_tool_call, profile_key_columns,
                            {"table_fqn": tbl, "columns": cols}, TOOL_TIMEOUT,
                            "profile_key_columns", 7, total,
                        )

                fk_tr = fk_future.result()
                _collect(fk_tr, "execute_metadata_sql (FK predictions)", B.FK_PREDICTIONS)

                profiling_summary = ""
                if prof_future:
                    prof_tr = prof_future.result()
                    if prof_tr.success and prof_tr.data:
                        tools_used.append("profile_key_columns")
                        profiling_summary = _truncate_part(prof_tr.data, B.PROFILING)
                        parts.append(f"### Source: profile_key_columns (live data distribution)\n{profiling_summary}")
                    elif not prof_tr.success:
                        failed_sources.append(prof_tr)

                sr_future = None
                if plan.get("requires_structured_retrieval", True):
                    _emit("gathering", f"Step 8/{total}: Structured retrieval (SQL writer + data query)...")
                    ctx = contextvars.copy_context()
                    sr_future = pool.submit(
                        ctx.run, _structured_retrieval, query, table_names, cancel, 8, total,
                        profiling_summary, comparison_intent,
                    )

                if sr_future:
                    sr_tr = sr_future.result()
                    if sr_tr.success and sr_tr.data:
                        tools_used.extend(["sql_writer", "execute_data_sql"])
                        parts.append(f"### Source: structured_retrieval (LLM-generated SQL on data)\n"
                                     f"{_truncate_part(sr_tr.data, B.STRUCTURED_RETRIEVAL)}")
                    elif not sr_tr.success:
                        failed_sources.append(sr_tr)
        else:
            logger.info("[deep_analysis] Group B: SKIP (no tables)")

    return "\n\n".join(parts), graph_data, tools_used, failed_sources, timing, parts


def _gather_baseline(query: str, cancel: threading.Event):
    """Deterministic baseline data gathering: SQL queries on KB tables."""
    parts = []
    tools_used = []
    failed_sources: list[ToolResult] = []
    timing: dict = {}
    queries = _build_baseline_queries(query)
    total = len(queries)

    with measure_phase("baseline_queries", timing):
        for i, sql_q in enumerate(queries):
            if cancel.is_set():
                break
            tr = _safe_tool_call(
                execute_baseline_sql, {"query": sql_q}, TOOL_TIMEOUT,
                f"execute_baseline_sql (query {i + 1})", i + 1, total,
            )
            if tr.success and tr.data:
                tools_used.append("execute_baseline_sql")
                parts.append(f"### Source: execute_baseline_sql\n{tr.data}")
            elif not tr.success:
                failed_sources.append(tr)

    return "\n\n".join(parts), tools_used, failed_sources, timing


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def _run_pipeline(query: str, mode: str, cancel: threading.Event,
                   session_id: Optional[str] = None,
                   intent_type: str = "new_question",
                   intent_domain: Optional[str] = None,
                   complexity: str = "moderate",
                   comparison_intent: bool = False) -> Optional[Dict]:
    """Two-phase pipeline: gather context deterministically, then single LLM call."""
    t0 = time.time()
    logger.info("[deep_analysis] PIPELINE START mode=%s complexity=%s query=%s", mode, complexity, query[:120])
    B = EvidenceBudget

    num_results = _NUM_RESULTS_BY_COMPLEXITY.get(complexity, 5)

    # Phase 1: Gather context (wrapped in evidence_retrieval span for child parenting)
    _emit("gathering", "Gathering evidence from metadata catalog...")
    mlflow_gather = get_mlflow()
    gather_cm = mlflow_gather.start_span(name="evidence_retrieval", span_type="RETRIEVER") if mlflow_gather else nullcontext()
    with gather_cm as gather_span:
        raw_parts = []
        if mode != "graphrag":
            context, tools_used, failed_sources, timing = _gather_baseline(query, cancel)
            graph_data = {}
        elif complexity == "simple":
            context, graph_data, tools_used, failed_sources, timing, raw_parts = _gather_vs_only(
                query, cancel, session_id, intent_type=intent_type,
                intent_domain=intent_domain, num_results=num_results)
        elif complexity == "moderate":
            context, graph_data, tools_used, failed_sources, timing, raw_parts = _gather_vs_expand(
                query, cancel, session_id, intent_type=intent_type,
                intent_domain=intent_domain, num_results=num_results,
                comparison_intent=comparison_intent)
        else:
            max_hops = {"simple": 2, "moderate": 3, "complex": 5}.get(complexity, 5)
            max_trav = {"simple": 1, "moderate": 3, "complex": 5}.get(complexity, 5)
            context, graph_data, tools_used, failed_sources, timing, raw_parts = _gather_graphrag(
                query, cancel, session_id, intent_type=intent_type,
                intent_domain=intent_domain, max_hops=max_hops,
                max_traversal_nodes=max_trav, num_results=num_results,
                comparison_intent=comparison_intent)
        if gather_span and hasattr(gather_span, "set_attributes"):
            gather_span.set_attributes({
                "mode": mode, "complexity": complexity,
                "tools_used": ",".join(dict.fromkeys(tools_used)) if tools_used else "",
            })

    gather_elapsed = round(time.time() - t0, 1)
    timing["gather_total"] = gather_elapsed
    logger.info("[deep_analysis] GATHER DONE in %.1fs -- %d tool(s): %s | failures=%d",
                gather_elapsed, len(tools_used), ", ".join(dict.fromkeys(tools_used)),
                len(failed_sources))

    if cancel.is_set():
        logger.warning("[deep_analysis] Cancelled after gathering (%.1fs)", gather_elapsed)
        return None

    if not context.strip() and not raw_parts:
        context = "No evidence could be gathered. The metadata catalog may be empty or inaccessible."

    # Re-rank evidence parts by relevance to the query
    from agent.deep_analysis_graph import _rank_and_assemble

    has_sr = any("structured_retrieval" in p for p in raw_parts)
    use_data_prompt = intent_domain == "query" and has_sr and raw_parts
    if use_data_prompt:
        sr_parts = [p for p in raw_parts if "structured_retrieval" in p or "FK predictions" in p
                     or "profile_key_columns" in p]
        context = _rank_and_assemble(sr_parts, query, B.TOTAL)
        logger.info("[deep_analysis] Data-question path: ranked %d SR/FK/profiling parts (%.1fKB)",
                    len(sr_parts), len(context) / 1024)
    elif raw_parts:
        context = _rank_and_assemble(raw_parts, query, B.TOTAL)

    if failed_sources:
        failure_lines = []
        for fs in failed_sources:
            hint = f" -- {fs.error_hint}" if fs.error_hint else ""
            failure_lines.append(f"- {fs.label}: {fs.error_type or 'failed'}{hint}")
        context += "\n\n### Failed Evidence Sources\n" + "\n".join(failure_lines)

    if len(context) > B.TOTAL:
        context = context[:B.TOTAL] + f"\n...[context truncated to {B.TOTAL:,} chars]"

    # Phase 2: Single LLM analysis call
    _emit("analyzing", "Generating analysis (this may take 15-30s)...")
    logger.info("[deep_analysis] STEP FINAL: LLM analysis -- START (context=%.1fKB, model=%s)",
                len(context) / 1024, MODEL)

    mlflow = get_mlflow()
    cm = mlflow.start_span(name="llm_analysis", span_type="LLM") if mlflow else nullcontext()
    with cm as span:
        if span:
            span.set_inputs({"query": query[:200], "context_kb": round(len(context) / 1024, 1)})
        token_usage: dict = {}
        try:
            if use_data_prompt:
                prompt = DATA_ANALYSIS_PROMPT
            elif mode == "graphrag":
                prompt = ANALYSIS_PROMPT
            else:
                prompt = BASELINE_ANALYSIS_PROMPT
            resp = _llm().invoke([
                SystemMessage(content=prompt),
                HumanMessage(content=f"User question: {query}\n\nGathered Evidence:\n{context}"),
            ])
            analysis = resp.content or "Analysis produced no output."
            # Extract token usage from LLM response
            um = getattr(resp, "usage_metadata", None)
            if um:
                token_usage = dict(um) if isinstance(um, dict) else {
                    "input_tokens": getattr(um, "input_tokens", 0) or 0,
                    "output_tokens": getattr(um, "output_tokens", 0) or 0,
                    "total_tokens": getattr(um, "total_tokens", 0) or 0,
                }
            elif hasattr(resp, "response_metadata") and resp.response_metadata:
                rm = resp.response_metadata
                tu = rm.get("usage") or rm.get("token_usage") or {}
                if isinstance(tu, dict):
                    token_usage = {
                        "input_tokens": tu.get("prompt_tokens", 0) or tu.get("input_tokens", 0) or 0,
                        "output_tokens": tu.get("completion_tokens", 0) or tu.get("output_tokens", 0) or 0,
                        "total_tokens": tu.get("total_tokens", 0) or 0,
                    }
        except Exception as e:
            logger.error("[deep_analysis] LLM analysis error: %s", e, exc_info=True)
            analysis = f"Analysis LLM call failed: {e}. Evidence gathered:\n\n{context[:2000]}"
        llm_elapsed = round(time.time() - t0 - gather_elapsed, 1)
        timing["llm_analysis"] = llm_elapsed
        timing["token_usage"] = token_usage
        logger.info("[deep_analysis] STEP FINAL: LLM analysis -- DONE in %.1fs (%d chars) tokens=%s",
                    llm_elapsed, len(analysis), token_usage)
        if span:
            span.set_outputs({"elapsed_s": llm_elapsed, "result_len": len(analysis)})
            if token_usage:
                span.set_attributes({
                    "llm.token_count.prompt": token_usage.get("input_tokens", 0),
                    "llm.token_count.completion": token_usage.get("output_tokens", 0),
                    "llm.token_count.total": token_usage.get("total_tokens", 0),
                })

    total_elapsed = round(time.time() - t0, 1)
    timing["total"] = total_elapsed
    unique_tools = list(dict.fromkeys(tools_used))
    logger.info("[deep_analysis] PIPELINE COMPLETE in %.1fs -- mode=%s tools=%s timing=%s",
                total_elapsed, mode, unique_tools, timing)

    result = {
        "answer": sanitize_output(analysis),
        "tool_calls": unique_tools,
        "mode": mode,
        "steps": len(tools_used),
        "timing": timing,
    }
    if not use_data_prompt and graph_data and (graph_data.get("nodes") or graph_data.get("edges")):
        result["graph_data"] = graph_data
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_deep_analysis(
    message: str,
    mode: str = "graphrag",
    history: Optional[List[Dict]] = None,
    session_id: Optional[str] = None,
) -> Dict:
    """Non-streaming deep analysis. mode is 'graphrag' or 'baseline'."""
    return _run_deep_analysis_traced(
        message=message[:500], mode=mode,
        history_len=len(history) if history else 0,
        session_id=session_id, _history=history,
    )


@trace(name="deep_analysis")
def _run_deep_analysis_traced(
    message: str,
    mode: str = "graphrag",
    history_len: int = 0,
    session_id: Optional[str] = None,
    _history: Optional[List[Dict]] = None,
) -> Dict:
    """Traced inner -- span inputs show message (truncated) and history_len, not raw history."""
    ensure_mlflow_context()
    tag_trace(session_id=session_id, agent="deep_analysis", mode=mode)
    intent_result = classify_and_contextualize(message, _history)
    if intent_result.intent_type == "meta" and intent_result.domain in ("discovery", "query", "governance", "relationship"):
        logger.info("[deep_analysis] reclassifying meta+%s as new_question", intent_result.domain)
        intent_result.intent_type = "new_question"
    logger.info("[deep_analysis] intent=%s domain=%s clear=%s",
                intent_result.intent_type, intent_result.domain, intent_result.question_clear)
    tag_trace(intent=intent_result.intent_type, domain=intent_result.domain)

    if intent_result.intent_type in ("irrelevant", "meta"):
        return {
            "answer": intent_result.meta_answer or "I can help you analyze your data catalog metadata.",
            "tool_calls": [], "mode": mode, "intent": intent_result.intent_type,
        }

    query = intent_result.context_summary
    cancel = threading.Event()
    result = _run_pipeline(query, mode, cancel, session_id=session_id,
                           intent_type=intent_result.intent_type,
                           intent_domain=intent_result.domain,
                           complexity=intent_result.complexity or "moderate",
                           comparison_intent=intent_result.comparison_intent)
    if result:
        result["intent"] = intent_result.intent_type
    return result or {"answer": "Analysis could not be completed.", "tool_calls": [], "mode": mode}


def run_deep_analysis_streaming(
    message: str,
    mode: str = "graphrag",
    history: Optional[List[Dict]] = None,
    session_id: Optional[str] = None,
) -> tuple[queue.Queue, threading.Event]:
    """Streaming deep analysis. Returns (progress_queue, cancel_event).

    Events on the queue:
        {"stage": "gathering"|"analyzing", "message": "..."}
        {"stage": "done", "answer": "...", "tool_calls": [...], "mode": "..."}
        {"stage": "error", "message": "..."}

    Set cancel_event to signal the pipeline to stop at the next step.
    """
    q: queue.Queue = queue.Queue()
    cancel = threading.Event()

    def _run():
        _local.progress_queue = q
        _local.routing_trace = []
        _local.cancel_event = cancel
        ensure_mlflow_context()
        mlflow = get_mlflow()
        try:
            cm = mlflow.start_span(name="deep_analysis_streaming") if mlflow else nullcontext()
            with cm as span:
                try:
                    if span:
                        span.set_inputs({"query": message[:200], "mode": mode})

                    _emit("starting", "Starting analysis...", agent="system")
                    logger.info("[deep_analysis] Starting streaming mode=%s query=%s", mode, message[:120])
                    t0 = time.time()

                    intent_result = classify_and_contextualize(message, history)
                    if intent_result.intent_type == "meta" and intent_result.domain in ("discovery", "query", "governance", "relationship"):
                        logger.info("[deep_analysis:streaming] reclassifying meta+%s as new_question", intent_result.domain)
                        intent_result.intent_type = "new_question"
                    logger.info("[deep_analysis:streaming] intent=%s", intent_result.intent_type)

                    if intent_result.intent_type in ("irrelevant", "meta"):
                        q.put({
                            "stage": "done",
                            "answer": intent_result.meta_answer or "I can help you analyze your data catalog.",
                            "tool_calls": [], "mode": mode,
                        })
                        return

                    query_text = intent_result.context_summary

                    result = _run_pipeline(query_text, mode, cancel, session_id=session_id,
                                           intent_type=intent_result.intent_type,
                                           intent_domain=intent_result.domain,
                                           complexity=intent_result.complexity or "moderate",
                                           comparison_intent=intent_result.comparison_intent)

                    elapsed = round(time.time() - t0, 1)

                    if result and not cancel.is_set():
                        done_event = {
                            "stage": "done",
                            "answer": result["answer"],
                            "tool_calls": result["tool_calls"],
                            "mode": mode,
                            "routing_trace": list(_local.routing_trace),
                        }
                        if "graph_data" in result:
                            done_event["graph_data"] = result["graph_data"]
                        if "timing" in result:
                            done_event["timing"] = result["timing"]
                        q.put(done_event)
                        logger.info("[deep_analysis] Streaming DONE in %.1fs, tools=%s",
                                    elapsed, result.get("tool_calls"))
                    elif cancel.is_set():
                        logger.warning("[deep_analysis] Streaming CANCELLED after %.1fs", elapsed)

                    if span:
                        status = "cancelled" if cancel.is_set() else "done"
                        span.set_outputs({
                            "status": status,
                            "tools": result.get("tool_calls") if result else [],
                            "elapsed_s": elapsed,
                        })
                except Exception as e:
                    logger.error("[deep_analysis] Streaming error: %s", e, exc_info=True)
                    q.put({"stage": "error", "message": str(e)})
                    if span:
                        span.set_outputs({"status": "error", "error": str(e)[:200]})
        finally:
            _local.progress_queue = None
            _local.routing_trace = []
            _local.cancel_event = None

    threading.Thread(target=_run, daemon=True).start()
    return q, cancel
