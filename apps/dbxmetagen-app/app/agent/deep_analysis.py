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
from agent.common import ToolResult, measure_phase, vs_cache_get, vs_cache_put
from agent.graph_skill import GRAPH_SCHEMA_CONTEXT
from agent.guardrails import SAFETY_PROMPT_BLOCK, EvidenceBudget, sanitize_output
from agent.intent import classify_and_contextualize
from agent.tracing import trace, ensure_mlflow_context, get_mlflow

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

GROUNDING RULE: Every finding and recommendation MUST cite a specific evidence source
(e.g. "[Source: search_metadata]", "[Source: SQL query]"). Do not include findings that
lack evidence. If the user's question cannot be answered from the gathered evidence,
state this clearly.

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

GROUNDING RULE: Every finding MUST reference the evidence provided. Do not include
findings that lack evidence. If the question cannot be answered, state this clearly.

Be direct and avoid repetition."""


# ---------------------------------------------------------------------------
# Deterministic tool calling
# ---------------------------------------------------------------------------

def _safe_tool_call(tool_fn, args: dict, timeout_s: int, label: str,
                    step_num: int, total_steps: int) -> ToolResult:
    """Run a LangChain tool with a hard timeout. Returns a ToolResult."""
    mlflow = get_mlflow()
    cm = mlflow.start_span(name=f"tool_{label}") if mlflow else nullcontext()
    with cm as span:
        if span:
            span.set_inputs({"tool": label, "args": str(args)[:200], "timeout_s": timeout_s})
        t0 = time.time()
        if step_num:
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
            if step_num:
                _emit("gathering", f"Step {step_num}/{total_steps}: {label} done ({elapsed}s)")
            if span:
                span.set_outputs({"status": "ok", "elapsed_s": elapsed, "result_len": result_len})
            return ToolResult(success=True, data=result, elapsed_s=elapsed, label=label)
        except FuturesTimeout:
            elapsed = round(time.time() - t0, 1)
            logger.warning("[deep_analysis] STEP %d/%d: %s -- TIMEOUT after %.1fs",
                           step_num, total_steps, label, elapsed)
            if step_num:
                _emit("gathering", f"Step {step_num}/{total_steps}: {label} timed out, continuing...")
            if span:
                span.set_outputs({"status": "timeout", "elapsed_s": elapsed})
            return ToolResult(success=False, data=None, error_type="timeout",
                              error_hint=f"Timed out after {timeout_s}s", elapsed_s=elapsed, label=label)
        except Exception as e:
            elapsed = round(time.time() - t0, 1)
            err_str = str(e)[:200]
            logger.warning("[deep_analysis] STEP %d/%d: %s -- ERROR in %.1fs: %s",
                           step_num, total_steps, label, elapsed, e)
            if step_num:
                _emit("gathering", f"Step {step_num}/{total_steps}: {label} failed, continuing...")
            if span:
                span.set_outputs({"status": "error", "error": err_str, "elapsed_s": elapsed})
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
    tr = _safe_tool_call(execute_metadata_sql, {"query": sql}, TOOL_TIMEOUT,
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
    return "\n".join(lines)


def _structured_retrieval(question: str, table_names: list[str],
                          cancel: threading.Event, step_num: int, total_steps: int) -> ToolResult:
    """Structured retrieval subagent: schema lookup -> SQL writer -> execute (with 1 retry).

    Returns a ToolResult with the generated SQL and results.
    """
    label = "structured_retrieval"
    if not table_names or cancel.is_set():
        return ToolResult(success=False, data=None, error_type="skipped",
                          error_hint="No tables to query", label=label)

    mlflow = get_mlflow()
    cm = mlflow.start_span(name="structured_retrieval") if mlflow else nullcontext()
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

        # One retry on failure: feed the error back to the SQL writer
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
        if not tr.success or not tr.data:
            if span:
                span.set_outputs({"status": "exec_failed", "sql": sql[:300]})
            return ToolResult(success=False, data=None, error_type=tr.error_type or "exec_failed",
                              error_hint=f"SQL execution failed: {tr.error_hint}", elapsed_s=elapsed, label=label)

        output = f"**Generated SQL:**\n```sql\n{sql}\n```\n\n**Results:**\n{tr.data}"
        if span:
            span.set_outputs({"status": "ok", "sql": sql[:300], "result_len": len(output)})
        return ToolResult(success=True, data=output, elapsed_s=elapsed, label=label)


# ---------------------------------------------------------------------------
# Query planner -- chooses edge_type filters to prevent graph fan-out
# ---------------------------------------------------------------------------

BUILTIN_EDGE_TYPES = {
    "contains", "references", "instance_of", "has_property", "is_a",
    "same_domain", "same_subdomain", "same_catalog", "same_schema",
    "same_security_level", "same_classification", "similar_embedding",
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
    mlflow = get_mlflow()
    cm = mlflow.start_span(name="query_planner") if mlflow else nullcontext()
    with cm as span:
        if span:
            span.set_inputs({"query": query[:200]})
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(
                    _llm().invoke,
                    [SystemMessage(content=prompt),
                     HumanMessage(content=query)],
                )
                resp = future.result(timeout=PLANNER_TIMEOUT)
            raw = (resp.content or "").strip()
            raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
            plan = json.loads(raw)

            # Validate edge_types
            et = plan.get("edge_types")
            if isinstance(et, list):
                plan["edge_types"] = [e for e in et if e in valid_types] or default["edge_types"]
            else:
                plan["edge_types"] = default["edge_types"]

            # Validate traverse_edge_type -- MUST be set
            tet = plan.get("traverse_edge_type")
            if not tet or tet not in valid_types:
                plan["traverse_edge_type"] = default["traverse_edge_type"]

            # Validate direction
            if plan.get("direction") not in ("outgoing", "incoming", "both"):
                plan["direction"] = "outgoing"

            # Validate requires_structured_retrieval
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


def _gather_graphrag(query: str, cancel: threading.Event,
                     session_id: Optional[str] = None, intent_type: str = "new_question"):
    """Deterministic GraphRAG data gathering with parallel execution.

    Pipeline: plan -> VS -> [expand + traverse] parallel -> SPARQL ontology -> [FK SQL + structured retrieval] parallel
    """
    parts = []
    tools_used = []
    graph_data = {}
    failed_sources: list[ToolResult] = []
    timing: dict = {}
    total = 7
    B = EvidenceBudget

    def _collect(tr: ToolResult, source_label: str, limit: int):
        """Append a successful tool result to parts, or track as failed."""
        if tr.success and tr.data:
            tools_used.append(tr.label.split("(")[0].strip())
            parts.append(f"### Source: {source_label}\n{_truncate_part(tr.data, limit)}")
        elif not tr.success:
            failed_sources.append(tr)

    # Step 0: Discover dynamic ontology edge types (fast, cached)
    ontology_edge_types = _discover_ontology_edge_types()

    # Step 1: LLM query planner
    with measure_phase("plan_retrieval", timing):
        _emit("gathering", f"Step 1/{total}: Planning retrieval strategy...")
        plan = _plan_retrieval(query, cancel, extra_edge_types=ontology_edge_types)
    planned_edge_types = plan["edge_types"]
    planned_traverse_et = plan["traverse_edge_type"]
    planned_direction = plan["direction"]
    logger.info("[deep_analysis] Plan: expand=%s, traverse=%s, direction=%s",
                planned_edge_types, planned_traverse_et, planned_direction)

    if cancel.is_set():
        return "\n\n".join(parts), graph_data, tools_used, failed_sources, timing

    # Step 2: Vector search (use cache only on refinement/continuation)
    with measure_phase("vector_search", timing):
        vs_tr = None
        if session_id:
            cached = vs_cache_get(session_id, intent_type=intent_type)
            if cached:
                vs_tr = ToolResult(success=True, data=cached, label="search_metadata (cached)")
        if vs_tr is None:
            vs_tr = _safe_tool_call(search_metadata, {"query": query}, TOOL_TIMEOUT,
                                    "search_metadata", 2, total)
            if vs_tr.success and vs_tr.data and session_id:
                vs_cache_put(session_id, query, vs_tr.data)
        _collect(vs_tr, "search_metadata (vector search)", B.VS_RESULTS)

    node_ids = _extract_node_ids(vs_tr.data if vs_tr.success else None)
    table_names = _extract_table_names(vs_tr.data if vs_tr.success else None)

    # Fallback: SQL keyword discovery if VS failed
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
        return "\n\n".join(parts), graph_data, tools_used, failed_sources, timing

    # Build traversal edge type list: primary type + expand types for broader coverage
    traverse_et_list = [planned_traverse_et] + [
        et for et in planned_edge_types if et != planned_traverse_et
    ]

    # Group A (parallel): expand_vs_hits + traverse_graph
    with measure_phase("graph_expand_traverse", timing):
        if node_ids:
            trav_nodes = _pick_traversal_nodes(node_ids, table_names)
            with ThreadPoolExecutor(max_workers=4) as pool:
                # Submit expand
                expand_future = pool.submit(
                    _safe_tool_call, expand_vs_hits,
                    {"node_ids": node_ids[:5], "edge_types": planned_edge_types},
                    TOOL_TIMEOUT, "expand_vs_hits", 3, total,
                )
                # Submit traversals (3 seeds, 3 hops, multi-type filter)
                trav_futures = []
                for i, nid in enumerate(trav_nodes[:3]):
                    trav_futures.append(pool.submit(
                        _safe_tool_call, traverse_graph,
                        {"start_node": nid, "max_hops": 3,
                         "edge_types": traverse_et_list, "direction": planned_direction},
                        TOOL_TIMEOUT, f"traverse_graph({nid.split('.')[-1]})", 4 + i, total,
                    ))

                # Collect expand result
                exp_tr = expand_future.result()
                _collect(exp_tr, "expand_vs_hits (graph expansion)", B.GRAPH_EXPANSION)

                # Collect traversal results
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
        return "\n\n".join(parts), graph_data, tools_used, failed_sources, timing

    # SPARQL ontology query: enrich with formal ontology relationships
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
        return "\n\n".join(parts), graph_data, tools_used, failed_sources, timing

    # Group B (parallel): FK SQL + structured retrieval
    with measure_phase("fk_and_structured", timing):
        if table_names:
            with ThreadPoolExecutor(max_workers=2) as pool:
                # FK predictions
                fk_future = pool.submit(
                    _safe_tool_call, execute_metadata_sql,
                    {"query": _build_relevance_sql(table_names)}, TOOL_TIMEOUT,
                    "execute_metadata_sql (FK predictions)", 6, total,
                )
                # Structured retrieval (only if plan says so)
                sr_future = None
                if plan.get("requires_structured_retrieval", True):
                    _emit("gathering", f"Step 7/{total}: Structured retrieval (SQL writer + data query)...")
                    sr_future = pool.submit(
                        _structured_retrieval, query, table_names, cancel, 7, total,
                    )

                fk_tr = fk_future.result()
                _collect(fk_tr, "execute_metadata_sql (FK predictions)", B.FK_PREDICTIONS)

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

    return "\n\n".join(parts), graph_data, tools_used, failed_sources, timing


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
                   intent_type: str = "new_question") -> Optional[Dict]:
    """Two-phase pipeline: gather context deterministically, then single LLM call."""
    t0 = time.time()
    logger.info("[deep_analysis] PIPELINE START mode=%s query=%s", mode, query[:120])
    B = EvidenceBudget

    # Phase 1: Gather context
    _emit("gathering", "Gathering evidence from metadata catalog...")
    if mode == "graphrag":
        context, graph_data, tools_used, failed_sources, timing = _gather_graphrag(
            query, cancel, session_id, intent_type=intent_type)
    else:
        context, tools_used, failed_sources, timing = _gather_baseline(query, cancel)
        graph_data = {}

    gather_elapsed = round(time.time() - t0, 1)
    timing["gather_total"] = gather_elapsed
    logger.info("[deep_analysis] GATHER DONE in %.1fs -- %d tool(s): %s | failures=%d",
                gather_elapsed, len(tools_used), ", ".join(dict.fromkeys(tools_used)),
                len(failed_sources))

    if cancel.is_set():
        logger.warning("[deep_analysis] Cancelled after gathering (%.1fs)", gather_elapsed)
        return None

    if not context.strip():
        context = "No evidence could be gathered. The metadata catalog may be empty or inaccessible."

    # Append failed sources section so the LLM knows what's missing
    if failed_sources:
        failure_lines = []
        for fs in failed_sources:
            hint = f" -- {fs.error_hint}" if fs.error_hint else ""
            failure_lines.append(f"- {fs.label}: {fs.error_type or 'failed'}{hint}")
        context += "\n\n### Failed Evidence Sources\n" + "\n".join(failure_lines)

    # Budget check: if total context exceeds the cap, hard-truncate
    if len(context) > B.TOTAL:
        logger.warning("[deep_analysis] Context too large (%d chars), truncating to %d",
                       len(context), B.TOTAL)
        context = context[:B.TOTAL] + f"\n...[context truncated from {len(context):,} to {B.TOTAL:,} chars]"

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
        timing["llm_analysis"] = llm_elapsed
        logger.info("[deep_analysis] STEP FINAL: LLM analysis -- DONE in %.1fs (%d chars)",
                    llm_elapsed, len(analysis))
        if span:
            span.set_outputs({"elapsed_s": llm_elapsed, "result_len": len(analysis)})

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
    if graph_data and (graph_data.get("nodes") or graph_data.get("edges")):
        result["graph_data"] = graph_data
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@trace(name="deep_analysis")
def run_deep_analysis(
    message: str,
    mode: str = "graphrag",
    history: Optional[List[Dict]] = None,
    session_id: Optional[str] = None,
) -> Dict:
    """Non-streaming deep analysis. mode is 'graphrag' or 'baseline'."""
    intent_result = classify_and_contextualize(message, history)
    logger.info("[deep_analysis] intent=%s domain=%s clear=%s",
                intent_result.intent_type, intent_result.domain, intent_result.question_clear)

    if intent_result.intent_type in ("irrelevant", "meta"):
        return {
            "answer": intent_result.meta_answer or "I can help you analyze your data catalog metadata.",
            "tool_calls": [], "mode": mode, "intent": intent_result.intent_type,
        }

    query = intent_result.context_summary
    cancel = threading.Event()
    result = _run_pipeline(query, mode, cancel, session_id=session_id,
                           intent_type=intent_result.intent_type)
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
                                           intent_type=intent_result.intent_type)

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
