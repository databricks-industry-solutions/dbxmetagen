"""Two-phase deep analysis for GraphRAG and Baseline modes.

Phase 1: Deterministic data gathering (direct tool calls, no agent loops)
Phase 2: Single LLM analysis call with all gathered context

This replaces the previous multi-agent supervisor/planner/retrieval/analyst
LangGraph architecture which was prone to infinite loops and timeouts.
"""

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
    expand_vs_hits, traverse_graph, execute_data_sql,
)
from agent.graph_skill import GRAPH_SCHEMA_CONTEXT
from agent.guardrails import SAFETY_PROMPT_BLOCK, sanitize_output
from agent.tracing import trace, ensure_mlflow_context, get_mlflow

logger = logging.getLogger(__name__)

MODEL = os.environ.get("LLM_MODEL", "databricks-claude-sonnet-4-6")
TOOL_TIMEOUT = 30  # hard timeout per tool call (seconds)
MAX_CONTEXT_CHARS = 120_000  # ~30-40k tokens; keeps final prompt under 200k limit

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

Be direct and avoid repetition. Lead with the most impactful finding.
If evidence is sparse, say so honestly rather than fabricating details."""


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

Be direct and avoid repetition. If evidence is sparse, say so honestly."""


# ---------------------------------------------------------------------------
# Deterministic tool calling
# ---------------------------------------------------------------------------

def _safe_tool_call(tool_fn, args: dict, timeout_s: int, label: str,
                    step_num: int, total_steps: int) -> Optional[str]:
    """Run a LangChain tool with a hard timeout and structured logging."""
    mlflow = get_mlflow()
    cm = mlflow.start_span(name=f"tool_{label}") if mlflow else nullcontext()
    with cm as span:
        if span:
            span.set_inputs({"tool": label, "args": str(args)[:200], "timeout_s": timeout_s})
        t0 = time.time()
        _emit("gathering", f"Step {step_num}/{total_steps}: {label}...")
        logger.info("[deep_analysis] STEP %d/%d: %s -- START (timeout=%ds)",
                    step_num, total_steps, label, timeout_s)
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                result = pool.submit(tool_fn.func, **args).result(timeout=timeout_s)
            elapsed = round(time.time() - t0, 1)
            result_len = len(result) if result else 0
            logger.info("[deep_analysis] STEP %d/%d: %s -- DONE in %.1fs (%d chars)",
                        step_num, total_steps, label, elapsed, result_len)
            _emit("gathering", f"Step {step_num}/{total_steps}: {label} done ({elapsed}s)")
            if span:
                span.set_outputs({"status": "ok", "elapsed_s": elapsed, "result_len": result_len})
            return result
        except FuturesTimeout:
            elapsed = round(time.time() - t0, 1)
            logger.warning("[deep_analysis] STEP %d/%d: %s -- TIMEOUT after %.1fs",
                           step_num, total_steps, label, elapsed)
            _emit("gathering", f"Step {step_num}/{total_steps}: {label} timed out, continuing...")
            if span:
                span.set_outputs({"status": "timeout", "elapsed_s": elapsed})
            return None
        except Exception as e:
            elapsed = round(time.time() - t0, 1)
            logger.warning("[deep_analysis] STEP %d/%d: %s -- ERROR in %.1fs: %s",
                           step_num, total_steps, label, elapsed, e)
            _emit("gathering", f"Step {step_num}/{total_steps}: {label} failed, continuing...")
            if span:
                span.set_outputs({"status": "error", "error": str(e)[:200], "elapsed_s": elapsed})
            return None


def _parse_json(raw: Optional[str]) -> Optional[dict]:
    if not raw:
        return None
    try:
        return json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return None


def _extract_node_ids(vs_json: Optional[str]) -> list[str]:
    """Extract node_id values from vector search results."""
    data = _parse_json(vs_json)
    if not data:
        return []
    ids = []
    for m in data.get("matches", []):
        nid = m.get("node_id")
        if nid and nid not in ids:
            ids.append(nid)
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
    """Pick the best nodes for graph traversal (prefer table-level nodes)."""
    table_ids = [nid for nid in node_ids if nid.count(".") == 2]
    if table_ids:
        return table_ids[:2]
    if table_names:
        return table_names[:2]
    return node_ids[:2]


def _sql_escape(s: str) -> str:
    return s.replace("'", "''")


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
- Always include LIMIT (max 200 rows).
- Use fully-qualified table names (catalog.schema.table).
- If the question cannot be answered from the given schemas, return SKIP.
- Output ONLY the SQL query, nothing else. No markdown fencing, no explanation.
{SAFETY_PROMPT_BLOCK}"""


def _sql_writer(question: str, schema_context: str, cancel: threading.Event) -> Optional[str]:
    """LLM subagent that generates a SQL query from schema context."""
    if cancel.is_set():
        return None
    mlflow = get_mlflow()
    cm = mlflow.start_span(name="sql_writer") if mlflow else nullcontext()
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
    """Fetch column schemas for discovered tables from column_knowledge_base."""
    if not table_names:
        return ""
    tn_list = ", ".join(f"'{_sql_escape(t)}'" for t in table_names[:8])
    sql = (
        f"SELECT table_name, column_name, data_type, comment "
        f"FROM {CATALOG}.{SCHEMA}.column_knowledge_base "
        f"WHERE table_name IN ({tn_list}) "
        f"ORDER BY table_name, column_name LIMIT 200"
    )
    result = _safe_tool_call(execute_metadata_sql, {"query": sql}, TOOL_TIMEOUT,
                             "fetch_schemas", 0, 0)
    if not result:
        return ""
    data = _parse_json(result)
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
    return "\n".join(lines)


def _structured_retrieval(question: str, table_names: list[str],
                          cancel: threading.Event, step_num: int, total_steps: int) -> Optional[str]:
    """Structured retrieval subagent: schema lookup -> SQL writer -> execute.

    Returns a formatted string with the generated SQL and its results, or None.
    """
    if not table_names or cancel.is_set():
        return None

    mlflow = get_mlflow()
    cm = mlflow.start_span(name="structured_retrieval") if mlflow else nullcontext()
    with cm as span:
        if span:
            span.set_inputs({"question": question[:200], "tables": table_names[:8]})

        _emit("gathering", f"Step {step_num}/{total_steps}: Fetching schemas for data retrieval...")

        schema_ctx = _fetch_table_schemas(table_names)
        if not schema_ctx or cancel.is_set():
            if span:
                span.set_outputs({"status": "no_schemas"})
            return None

        _emit("gathering", f"Step {step_num}/{total_steps}: SQL writer generating query...")

        sql = _sql_writer(question, schema_ctx, cancel)
        if not sql or cancel.is_set():
            if span:
                span.set_outputs({"status": "no_sql"})
            return None

        logger.info("[deep_analysis] structured_retrieval: executing generated SQL: %s", sql[:200])
        _emit("gathering", f"Step {step_num}/{total_steps}: Executing data query...")

        result = _safe_tool_call(execute_data_sql, {"query": sql}, TOOL_TIMEOUT,
                                 "execute_data_sql (structured retrieval)",
                                 step_num, total_steps)
        if not result:
            if span:
                span.set_outputs({"status": "exec_failed", "sql": sql[:300]})
            return None

        if span:
            span.set_outputs({"status": "ok", "sql": sql[:300], "result_len": len(result)})

        return f"**Generated SQL:**\n```sql\n{sql}\n```\n\n**Results:**\n{result}"


# ---------------------------------------------------------------------------
# Query planner -- chooses edge_type filters to prevent graph fan-out
# ---------------------------------------------------------------------------

VALID_EDGE_TYPES = {
    "contains", "references", "instance_of", "has_property", "is_a",
    "same_domain", "same_subdomain", "same_catalog", "same_schema",
    "same_security_level", "same_classification", "similar_embedding",
    "derives_from",
}

PLANNER_PROMPT = f"""You are a retrieval planner for a metadata knowledge graph.

Given a user question, decide which graph edge types to use so that multi-hop
traversal stays focused and doesn't fan out into unrelated nodes.

Available edge types:
  contains        -- schema->table, table->column (structural hierarchy)
  references      -- FK relationship between tables (join paths)
  instance_of     -- table is instance of ontology entity
  has_property    -- ontology entity has a property mapped to a column
  is_a            -- entity type hierarchy
  same_domain     -- tables share business domain
  same_subdomain  -- tables share subdomain
  similar_embedding -- embedding similarity between nodes
  derives_from    -- lineage: one table derives from another

Respond with ONLY a JSON object (no markdown fencing):
{{
  "edge_types": ["type1", "type2"],
  "traverse_edge_type": "single_most_important_type",
  "direction": "outgoing"
}}

CRITICAL RULES:
- traverse_edge_type MUST always be set to exactly one type. NEVER null.
  This controls 2-hop graph traversal; unfiltered traversal is too expensive.
- edge_types: 1-3 types for 1-hop expansion from vector search hits.
- direction: "outgoing" (default, safest), "incoming", or "both" (use sparingly).

Decision guide:
- Joins / FKs / "how do tables relate": edge_types=["references","contains"], traverse="references", direction="both"
- Ontology / entity / "what type of data": edge_types=["instance_of","has_property"], traverse="instance_of"
- Lineage / "where does data come from": edge_types=["derives_from","contains"], traverse="derives_from"
- Domain / similarity / "similar tables": edge_types=["same_domain","similar_embedding"], traverse="same_domain"
- Table structure / "what columns": edge_types=["contains"], traverse="contains"
- General / unclear: edge_types=["references","contains"], traverse="references"
"""

PLANNER_TIMEOUT = 10  # seconds; fast fail so pipeline isn't blocked


def _plan_retrieval(query: str, cancel: threading.Event) -> dict:
    """Fast LLM call to decide edge_type filters for the retrieval pipeline.

    Always returns a valid plan dict. On any failure, returns a safe default
    that filters to references+contains (the most common useful pattern).
    """
    default = {"edge_types": ["references", "contains"],
               "traverse_edge_type": "references", "direction": "outgoing"}
    if cancel.is_set():
        return default

    mlflow = get_mlflow()
    cm = mlflow.start_span(name="query_planner") if mlflow else nullcontext()
    with cm as span:
        if span:
            span.set_inputs({"query": query[:200]})
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(
                    _llm().invoke,
                    [SystemMessage(content=PLANNER_PROMPT),
                     HumanMessage(content=query)],
                )
                resp = future.result(timeout=PLANNER_TIMEOUT)
            raw = (resp.content or "").strip()
            raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
            plan = json.loads(raw)

            # Validate edge_types
            et = plan.get("edge_types")
            if isinstance(et, list):
                plan["edge_types"] = [e for e in et if e in VALID_EDGE_TYPES] or default["edge_types"]
            else:
                plan["edge_types"] = default["edge_types"]

            # Validate traverse_edge_type -- MUST be set
            tet = plan.get("traverse_edge_type")
            if not tet or tet not in VALID_EDGE_TYPES:
                plan["traverse_edge_type"] = default["traverse_edge_type"]

            # Validate direction
            if plan.get("direction") not in ("outgoing", "incoming", "both"):
                plan["direction"] = "outgoing"

            logger.info("[deep_analysis] query_planner: %s", plan)
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


def _gather_graphrag(query: str, cancel: threading.Event):
    """Deterministic GraphRAG data gathering: plan -> VS -> expand -> traverse -> SQL -> structured retrieval."""
    parts = []
    tools_used = []
    graph_data = {}
    total = 7

    LIMIT_VS = 15_000
    LIMIT_EXPAND = 15_000
    LIMIT_TRAVERSE = 20_000
    LIMIT_FK = 15_000
    LIMIT_SR = 15_000

    # Step 1: LLM query planner -- decides edge_type filters to control fan-out
    _emit("gathering", f"Step 1/{total}: Planning retrieval strategy...")
    plan = _plan_retrieval(query, cancel)
    planned_edge_types = plan["edge_types"]
    planned_traverse_et = plan["traverse_edge_type"]
    planned_direction = plan["direction"]
    logger.info("[deep_analysis] Plan: expand=%s, traverse=%s, direction=%s",
                planned_edge_types, planned_traverse_et, planned_direction)

    if cancel.is_set():
        return "\n\n".join(parts), graph_data, tools_used

    # Step 2: Vector search
    vs_result = _safe_tool_call(search_metadata, {"query": query}, TOOL_TIMEOUT,
                                "search_metadata", 2, total)
    if vs_result:
        tools_used.append("search_metadata")
        parts.append(f"### Source: search_metadata (vector search)\n{_truncate_part(vs_result, LIMIT_VS)}")

    node_ids = _extract_node_ids(vs_result)
    table_names = _extract_table_names(vs_result)

    # Fallback: if VS failed or returned no matches, discover tables via SQL keyword search
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
            fb_result = _safe_tool_call(
                execute_metadata_sql, {"query": fallback_sql}, TOOL_TIMEOUT,
                "execute_metadata_sql (keyword fallback)", 2, total,
            )
            if fb_result:
                tools_used.append("execute_metadata_sql")
                parts.append(f"### Source: execute_metadata_sql (keyword fallback for tables)\n{_truncate_part(fb_result, LIMIT_VS)}")
                fb_data = _parse_json(fb_result)
                if fb_data and fb_data.get("rows"):
                    table_names = [r["table_name"] for r in fb_data["rows"] if r.get("table_name")]
                    node_ids = table_names[:]
                    logger.info("[deep_analysis] Keyword fallback found %d tables: %s",
                                len(table_names), table_names[:5])

    if cancel.is_set():
        return "\n\n".join(parts), graph_data, tools_used

    # Step 3: Graph expansion from VS hits (filtered by planned edge_types)
    if node_ids:
        exp_result = _safe_tool_call(
            expand_vs_hits,
            {"node_ids": node_ids[:3], "edge_types": planned_edge_types},
            TOOL_TIMEOUT, "expand_vs_hits", 3, total,
        )
        if exp_result:
            tools_used.append("expand_vs_hits")
            parts.append(f"### Source: expand_vs_hits (graph expansion)\n{_truncate_part(exp_result, LIMIT_EXPAND)}")
    else:
        logger.info("[deep_analysis] STEP 3/%d: expand_vs_hits -- SKIP (no node_ids)", total)
        _emit("gathering", f"Step 3/{total}: No nodes to expand, skipping...")

    if cancel.is_set():
        return "\n\n".join(parts), graph_data, tools_used

    # Steps 4-5: Traverse graph from top nodes (2-hop, filtered by planned edge_type)
    trav_nodes = _pick_traversal_nodes(node_ids, table_names)
    if trav_nodes:
        for i, nid in enumerate(trav_nodes[:2]):
            if cancel.is_set():
                break
            step = 4 + i
            trav_result = _safe_tool_call(
                traverse_graph,
                {"start_node": nid, "max_hops": 2,
                 "edge_type": planned_traverse_et, "direction": planned_direction},
                TOOL_TIMEOUT, f"traverse_graph({nid.split('.')[-1]})", step, total,
            )
            if trav_result:
                trav_str = trav_result if isinstance(trav_result, str) else json.dumps(trav_result)
                tools_used.append("traverse_graph")
                parts.append(f"### Source: traverse_graph (from {nid})\n{_truncate_part(trav_str, LIMIT_TRAVERSE)}")
                chunk = _parse_json(trav_str)
                if chunk and isinstance(chunk, dict):
                    graph_data = _merge_graph_data(graph_data, chunk)
    else:
        logger.info("[deep_analysis] STEP 4/%d: traverse_graph -- SKIP (no nodes)", total)

    if cancel.is_set():
        return "\n\n".join(parts), graph_data, tools_used

    # Step 6: SQL for FK predictions on discovered tables
    if table_names:
        sql_q = _build_relevance_sql(table_names)
        sql_result = _safe_tool_call(
            execute_metadata_sql, {"query": sql_q}, TOOL_TIMEOUT,
            "execute_metadata_sql (FK predictions)", 6, total,
        )
        if sql_result:
            tools_used.append("execute_metadata_sql")
            parts.append(f"### Source: execute_metadata_sql (FK predictions)\n{_truncate_part(sql_result, LIMIT_FK)}")
    else:
        logger.info("[deep_analysis] STEP 6/%d: execute_metadata_sql -- SKIP (no tables)", total)

    if cancel.is_set():
        return "\n\n".join(parts), graph_data, tools_used

    # Step 7: Structured retrieval (SQL writer -> execute on actual data)
    if table_names:
        _emit("gathering", f"Step 7/{total}: Structured retrieval (SQL writer + data query)...")
        sr_result = _structured_retrieval(query, table_names, cancel, 7, total)
        if sr_result:
            tools_used.append("sql_writer")
            tools_used.append("execute_data_sql")
            parts.append(f"### Source: structured_retrieval (LLM-generated SQL on data)\n{_truncate_part(sr_result, LIMIT_SR)}")
    else:
        logger.info("[deep_analysis] STEP 7/%d: structured_retrieval -- SKIP (no tables)", total)

    return "\n\n".join(parts), graph_data, tools_used


def _gather_baseline(query: str, cancel: threading.Event):
    """Deterministic baseline data gathering: SQL queries on KB tables."""
    parts = []
    tools_used = []
    queries = _build_baseline_queries(query)
    total = len(queries)

    for i, sql_q in enumerate(queries):
        if cancel.is_set():
            break
        result = _safe_tool_call(
            execute_baseline_sql, {"query": sql_q}, TOOL_TIMEOUT,
            f"execute_baseline_sql (query {i + 1})", i + 1, total,
        )
        if result:
            tools_used.append("execute_baseline_sql")
            parts.append(f"### Source: execute_baseline_sql\n{result}")

    return "\n\n".join(parts), tools_used


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def _run_pipeline(query: str, mode: str, cancel: threading.Event) -> Optional[Dict]:
    """Two-phase pipeline: gather context deterministically, then single LLM call."""
    t0 = time.time()
    logger.info("[deep_analysis] PIPELINE START mode=%s query=%s", mode, query[:120])

    # Phase 1: Gather context
    _emit("gathering", "Gathering evidence from metadata catalog...")
    if mode == "graphrag":
        context, graph_data, tools_used = _gather_graphrag(query, cancel)
    else:
        context, tools_used = _gather_baseline(query, cancel)
        graph_data = {}

    gather_elapsed = round(time.time() - t0, 1)
    logger.info("[deep_analysis] GATHER DONE in %.1fs -- %d tool(s): %s",
                gather_elapsed, len(tools_used), ", ".join(dict.fromkeys(tools_used)))

    if cancel.is_set():
        logger.warning("[deep_analysis] Cancelled after gathering (%.1fs)", gather_elapsed)
        return None

    if not context.strip():
        context = "No evidence could be gathered. The metadata catalog may be empty or inaccessible."

    # Budget check: if total context still exceeds the cap, hard-truncate
    if len(context) > MAX_CONTEXT_CHARS:
        logger.warning("[deep_analysis] Context too large (%d chars), truncating to %d",
                       len(context), MAX_CONTEXT_CHARS)
        context = context[:MAX_CONTEXT_CHARS] + f"\n...[context truncated from {len(context):,} to {MAX_CONTEXT_CHARS:,} chars]"

    # Phase 2: Single LLM analysis call
    _emit("analyzing", "Generating analysis (this may take 15-30s)...")
    logger.info("[deep_analysis] STEP FINAL: LLM analysis -- START (context=%.1fKB, model=%s)",
                len(context) / 1024, MODEL)

    mlflow = get_mlflow()
    cm = mlflow.start_span(name="llm_analysis") if mlflow else nullcontext()
    with cm as span:
        if span:
            span.set_inputs({"query": query[:200], "context_kb": round(len(context) / 1024, 1)})
        try:
            prompt = ANALYSIS_PROMPT if mode == "graphrag" else BASELINE_ANALYSIS_PROMPT
            resp = _llm().invoke([
                SystemMessage(content=prompt),
                HumanMessage(content=f"User question: {query}\n\nGathered Evidence:\n{context}"),
            ])
            analysis = resp.content or "Analysis produced no output."
        except Exception as e:
            logger.error("[deep_analysis] LLM analysis error: %s", e, exc_info=True)
            analysis = f"Analysis LLM call failed: {e}. Evidence gathered:\n\n{context[:2000]}"
        llm_elapsed = round(time.time() - t0 - gather_elapsed, 1)
        logger.info("[deep_analysis] STEP FINAL: LLM analysis -- DONE in %.1fs (%d chars)",
                    llm_elapsed, len(analysis))
        if span:
            span.set_outputs({"elapsed_s": llm_elapsed, "result_len": len(analysis)})

    total_elapsed = round(time.time() - t0, 1)
    unique_tools = list(dict.fromkeys(tools_used))
    logger.info("[deep_analysis] PIPELINE COMPLETE in %.1fs -- mode=%s tools=%s",
                total_elapsed, mode, unique_tools)

    result = {
        "answer": sanitize_output(analysis),
        "tool_calls": unique_tools,
        "mode": mode,
        "steps": len(tools_used),
    }
    if graph_data and (graph_data.get("nodes") or graph_data.get("edges")):
        result["graph_data"] = graph_data
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_query(message: str, history: Optional[List[Dict]] = None) -> str:
    if history:
        lines = [f"{m.get('role', 'user')}: {m['content']}" for m in history[-6:]]
        lines.append(f"user: {message}")
        return "Conversation so far:\n" + "\n".join(lines) + "\n\nRespond to the latest user message."
    return message


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@trace(name="deep_analysis")
def run_deep_analysis(
    message: str,
    mode: str = "graphrag",
    history: Optional[List[Dict]] = None,
) -> Dict:
    """Non-streaming deep analysis. mode is 'graphrag' or 'baseline'."""
    query = _build_query(message, history)
    cancel = threading.Event()
    result = _run_pipeline(query, mode, cancel)
    return result or {"answer": "Analysis could not be completed.", "tool_calls": [], "mode": mode}


def run_deep_analysis_streaming(
    message: str,
    mode: str = "graphrag",
    history: Optional[List[Dict]] = None,
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
                    query_text = _build_query(message, history)

                    result = _run_pipeline(query_text, mode, cancel)

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
