"""Tools for the metric view agent.

Provides semantic search over metric view documents, listing/describing
metric view definitions, and structured query execution using MEASURE() syntax.

Two API layers:
- @tool functions: LangChain tool-decorated, return JSON strings (for ReAct agents)
- *_direct / fetch_* / execute_* functions: return native dicts (for the purpose-built graph)
"""

import concurrent.futures
import json
import logging
import os
import re
import time
from typing import Optional

from langchain_core.tools import tool

logger = logging.getLogger(__name__)

CATALOG = os.environ.get("CATALOG_NAME", "")
SCHEMA = os.environ.get("SCHEMA_NAME", "")
VS_INDEX_SUFFIX = os.environ.get("VECTOR_SEARCH_INDEX", "metadata_vs_index")

_TRANSIENT_ERRORS = ("TEMPORARILY_UNAVAILABLE", "RESOURCE_EXHAUSTED", "DEADLINE_EXCEEDED")


def _fq(table: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table}"


def _run_sql(query: str) -> dict:
    from agent.metadata_tools import _execute_query
    return _execute_query(query)


def _run_sql_with_retry(query: str, max_retries: int = 2) -> dict:
    """Execute SQL with automatic retry on transient warehouse errors."""
    for attempt in range(max_retries + 1):
        result = _run_sql(query)
        if result.get("success") or attempt == max_retries:
            return result
        err = result.get("error", "")
        if not any(t in err for t in _TRANSIENT_ERRORS):
            return result
        time.sleep(1 * (attempt + 1))
    return result


def _parse_definition(json_str: str) -> dict:
    """Parse a metric view json_definition into a structured dict."""
    try:
        defn = json.loads(json_str) if isinstance(json_str, str) else json_str
    except (json.JSONDecodeError, TypeError):
        return {}
    return {
        "name": defn.get("name", ""),
        "source": defn.get("source", ""),
        "comment": defn.get("comment", ""),
        "filter": defn.get("filter", ""),
        "measures": [
            {"name": m.get("name", ""), "expr": m.get("expr", ""), "comment": m.get("comment", "")}
            for m in defn.get("measures", [])
        ],
        "dimensions": [
            {"name": d.get("name", ""), "expr": d.get("expr", ""), "comment": d.get("comment", "")}
            for d in defn.get("dimensions", [])
        ],
        "joins": [
            {"name": j.get("name", ""), "source": j.get("source", ""), "on": j.get("on", "")}
            for j in defn.get("joins", [])
        ],
    }


def _get_deployed_fqn(row: dict) -> str:
    """Build fully qualified metric view name from a definitions row."""
    cat = row.get("deployed_catalog") or CATALOG
    sch = row.get("deployed_schema") or SCHEMA
    return f"{cat}.{sch}.{row.get('metric_view_name', '')}"


def _extract_mv_name(node_id: str, content: str) -> str:
    """Extract metric_view_name from node_id (legacy 'mv::name::...' format) or content FQN."""
    if "::" in str(node_id):
        return node_id.split("::")[1]
    if content:
        first_line = content.split("\n")[0].strip()
        if "." in first_line:
            return first_line.rsplit(".", 1)[-1]
    return ""


def _quote_order_by(order_by: str, known_names: set[str]) -> str:
    """Backtick-quote known measure/dimension names in an ORDER BY clause."""
    for name in sorted(known_names, key=len, reverse=True):
        if name in order_by and f"`{name}`" not in order_by:
            order_by = order_by.replace(name, f"`{name}`")
    return order_by


def _parse_str_list(val) -> list[str]:
    """Normalize an LLM tool-call argument into a list of strings.

    Handles: list[str], JSON array string, comma-separated string, single value.
    """
    if isinstance(val, list):
        return [str(v).strip() for v in val if str(v).strip()]
    if not isinstance(val, str):
        return [str(val)] if val else []
    val = val.strip()
    if val.startswith("["):
        try:
            parsed = json.loads(val)
            if isinstance(parsed, list):
                return [str(v).strip() for v in parsed if str(v).strip()]
        except (json.JSONDecodeError, TypeError):
            pass
    return [v.strip() for v in val.split(",") if v.strip()]


# ---------------------------------------------------------------------------
# Tool: Semantic search over metric view documents
# ---------------------------------------------------------------------------

def _enrich_candidates_with_definitions(candidates: list[dict]) -> list[dict]:
    """Batch-fetch metric view definitions for VS candidates and attach measure/dimension names."""
    mv_names = list({c.get("metric_view_name") for c in candidates if c.get("metric_view_name")})
    if not mv_names:
        return candidates

    names_sql = ", ".join(f"'{n.replace(chr(39), chr(39)*2)}'" for n in mv_names[:10])
    try:
        result = _run_sql(f"""
            SELECT metric_view_name, json_definition, deployed_catalog, deployed_schema
            FROM {_fq('metric_view_definitions')}
            WHERE metric_view_name IN ({names_sql})
              AND status IN ('applied', 'validated')
        """)
        if not result["success"]:
            return candidates
        defn_map = {}
        for row in result["rows"]:
            name = row.get("metric_view_name")
            parsed = _parse_definition(row.get("json_definition", "{}"))
            parsed["fqn"] = _get_deployed_fqn(row)
            defn_map[name] = parsed
    except Exception:
        return candidates

    for c in candidates:
        mv = c.get("metric_view_name")
        if mv and mv in defn_map:
            d = defn_map[mv]
            c["measures"] = [m["name"] for m in d.get("measures", [])]
            c["dimensions"] = [dim["name"] for dim in d.get("dimensions", [])]
            c["fqn"] = d.get("fqn", "")
    return candidates


@tool
def search_metric_views(question: str) -> str:
    """Search for metric views relevant to a business question using semantic similarity.

    Embeds the question and queries the vector index for metric view documents.
    Returns candidate metric views with names, source tables, content summaries,
    and the measure/dimension names available in each view.

    Args:
        question: The business question to find relevant metric views for.
    """
    try:
        from agent.metadata_tools import _get_vs_index
        index_name = f"{CATALOG}.{SCHEMA}.{VS_INDEX_SUFFIX}"
        index = _get_vs_index(index_name)
    except Exception as e:
        return json.dumps({"error": f"Vector search unavailable: {e}"})

    try:
        kwargs = dict(
            query_text=question,
            columns=["doc_type", "content", "table_name", "node_id"],
            filters={"doc_type": ("metric_view_summary", "metric_view_measures", "metric_view_schema")},
            num_results=10,
            query_type="HYBRID",
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            results = pool.submit(index.similarity_search, **kwargs).result(timeout=30)
        cols = results.get("manifest", {}).get("columns", [])
        col_names = [c.get("name", f"col{i}") for i, c in enumerate(cols)]
        candidates = []
        seen_mv_names: set[str] = set()
        for row in results.get("result", {}).get("data_array", []):
            match = dict(zip(col_names, row)) if col_names else {}
            content = (match.get("content") or "")[:800]
            node_id = match.get("node_id", "")
            mv_name = _extract_mv_name(node_id, content)
            if mv_name:
                seen_mv_names.add(mv_name)
            candidates.append({
                "doc_type": match.get("doc_type", ""),
                "content": content,
                "source_table": match.get("table_name", ""),
                "definition_id": node_id,
                "metric_view_name": mv_name,
            })

        candidates = _enrich_candidates_with_definitions(candidates)
        return json.dumps({"candidates": candidates, "count": len(candidates)})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Tool: List available metric views
# ---------------------------------------------------------------------------

@tool
def list_metric_views(domain: Optional[str] = None) -> str:
    """List available metric views with their source tables, domains, and measure/dimension counts.

    Args:
        domain: Optional domain filter (e.g. 'healthcare', 'finance'). Empty for all.
    """
    domain_join = f"LEFT JOIN {_fq('table_knowledge_base')} kb ON m.source_table = kb.table_name"
    domain_filter = f"AND LOWER(kb.domain) LIKE '%{domain.lower()}%'" if domain else ""
    try:
        result = _run_sql(f"""
            SELECT m.metric_view_name, m.source_table, m.status,
                   m.deployed_catalog, m.deployed_schema,
                   kb.domain, kb.subdomain, m.json_definition
            FROM {_fq('metric_view_definitions')} m
            {domain_join}
            WHERE m.status IN ('applied', 'validated')
              AND m.metric_view_name IS NOT NULL
              {domain_filter}
            ORDER BY m.metric_view_name
        """)
        if not result["success"]:
            return json.dumps({"error": result.get("error", "Query failed")})

        views = []
        for row in result["rows"]:
            defn = {}
            if row.get("json_definition"):
                try:
                    defn = json.loads(row["json_definition"])
                except (json.JSONDecodeError, TypeError):
                    pass
            views.append({
                "metric_view_name": row["metric_view_name"],
                "source_table": row.get("source_table", ""),
                "status": row.get("status", ""),
                "domain": row.get("domain", ""),
                "subdomain": row.get("subdomain", ""),
                "fqn": _get_deployed_fqn(row),
                "measure_count": len(defn.get("measures", [])),
                "dimension_count": len(defn.get("dimensions", [])),
                "join_count": len(defn.get("joins", [])),
            })
        return json.dumps({"metric_views": views, "count": len(views)})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Tool: Describe a metric view (the "query contract")
# ---------------------------------------------------------------------------

@tool
def describe_metric_view(metric_view_name: str) -> str:
    """Fetch the full definition of a metric view -- its measures, dimensions, joins, and filter.

    This is the query contract: it tells you exactly what you can SELECT and GROUP BY.

    Args:
        metric_view_name: Name of the metric view (e.g. 'revenue_analysis').
    """
    name_esc = metric_view_name.replace("'", "''")
    try:
        result = _run_sql(f"""
            SELECT metric_view_name, source_table, json_definition, status,
                   deployed_catalog, deployed_schema
            FROM {_fq('metric_view_definitions')}
            WHERE metric_view_name = '{name_esc}'
              AND status IN ('applied', 'validated')
            ORDER BY created_at DESC
            LIMIT 1
        """)
        if not result["success"]:
            return json.dumps({"error": result.get("error", "Query failed")})
        if not result["rows"]:
            return json.dumps({"error": f"No metric view found with name '{metric_view_name}'"})

        row = result["rows"][0]
        defn = _parse_definition(row.get("json_definition", "{}"))
        defn["fqn"] = _get_deployed_fqn(row)
        defn["status"] = row.get("status", "")
        return json.dumps({"definition": defn})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Tool: Query a metric view using MEASURE() syntax
# ---------------------------------------------------------------------------

_DML_KEYWORDS = {"INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"}


def _validate_sql(query: str) -> Optional[str]:
    q_upper = query.strip().upper()
    if not q_upper.startswith(("SELECT", "WITH")):
        return "Only SELECT queries are allowed."
    for kw in _DML_KEYWORDS:
        if re.search(rf'\b{kw}\b', q_upper):
            return f"Blocked keyword: {kw}"
    return None


@tool
def metric_view_query(
    metric_view_name: str,
    measures: str,
    dimensions: str = "",
    filters: Optional[str] = None,
    order_by: Optional[str] = None,
    limit: int = 100,
) -> str:
    """Construct and execute a SQL query against a metric view using MEASURE() syntax.

    Only measures and dimensions declared in the metric view definition are allowed.
    The query is executed on a SQL warehouse and results are returned.

    Args:
        metric_view_name: Name of the metric view to query.
        measures: Comma-separated measure names to aggregate (e.g. "total_revenue, order_count").
        dimensions: Comma-separated dimension names to group by (e.g. "region, quarter"). Empty string for no grouping.
        filters: Optional SQL WHERE clause (e.g. "region = 'West' AND year >= 2023").
        order_by: Optional ORDER BY clause (e.g. "total_revenue DESC").
        limit: Max rows to return (default 100, max 1000).
    """
    measures = _parse_str_list(measures)
    dimensions = _parse_str_list(dimensions)
    if not measures:
        return json.dumps({"error": "At least one measure is required."})
    limit = min(max(limit, 1), 1000)

    name_esc = metric_view_name.replace("'", "''")
    try:
        result = _run_sql(f"""
            SELECT metric_view_name, json_definition, deployed_catalog, deployed_schema
            FROM {_fq('metric_view_definitions')}
            WHERE metric_view_name = '{name_esc}'
              AND status = 'applied'
            ORDER BY created_at DESC LIMIT 1
        """)
        if not result["success"] or not result["rows"]:
            return json.dumps({"error": f"No applied metric view '{metric_view_name}' found."})

        row = result["rows"][0]
        defn = _parse_definition(row.get("json_definition", "{}"))
        fqn = _get_deployed_fqn(row)

        valid_measures = {m["name"] for m in defn.get("measures", [])}
        valid_dims = {d["name"] for d in defn.get("dimensions", [])}

        bad_measures = [m for m in measures if m not in valid_measures]
        if bad_measures:
            return json.dumps({
                "error": f"Invalid measures: {bad_measures}. Valid: {sorted(valid_measures)}"
            })
        bad_dims = [d for d in dimensions if d not in valid_dims]
        if bad_dims:
            return json.dumps({
                "error": f"Invalid dimensions: {bad_dims}. Valid: {sorted(valid_dims)}"
            })

        dim_clause = ", ".join(f"`{d}`" for d in dimensions) if dimensions else ""
        measure_clause = ", ".join(f"MEASURE(`{m}`) AS `{m}`" for m in measures)
        select_parts = [p for p in [dim_clause, measure_clause] if p]
        sql = f"SELECT {', '.join(select_parts)} FROM {fqn}"
        if filters:
            sql += f" WHERE {filters}"
        if dimensions:
            sql += " GROUP BY ALL"
        if order_by:
            sql += f" ORDER BY {_quote_order_by(order_by, valid_measures | valid_dims)}"
        sql += f" LIMIT {limit}"

        err = _validate_sql(sql)
        if err:
            return json.dumps({"error": err, "sql": sql})

        query_result = _run_sql(sql)
        if query_result["success"]:
            return json.dumps({
                "sql": sql,
                "columns": query_result["columns"],
                "rows": query_result["rows"][:limit],
                "row_count": query_result["row_count"],
            })

        return json.dumps({"error": query_result.get("error"), "sql": sql})

    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Tool: Dimension-only queries (COUNT DISTINCT, unique values, listings)
# ---------------------------------------------------------------------------

def _fetch_definition_and_source(metric_view_name: str) -> tuple[Optional[dict], Optional[str], Optional[str]]:
    """Fetch parsed definition, FQN, and source for a metric view. Returns (defn, fqn, error)."""
    name_esc = metric_view_name.replace("'", "''")
    result = _run_sql(f"""
        SELECT metric_view_name, json_definition, deployed_catalog, deployed_schema
        FROM {_fq('metric_view_definitions')}
        WHERE metric_view_name = '{name_esc}'
          AND status IN ('applied', 'validated')
        ORDER BY created_at DESC LIMIT 1
    """)
    if not result["success"] or not result["rows"]:
        return None, None, f"No applied metric view '{metric_view_name}' found."
    row = result["rows"][0]
    return _parse_definition(row.get("json_definition", "{}")), _get_deployed_fqn(row), None


@tool
def metric_view_dimension_query(
    metric_view_name: str,
    dimensions: str,
    aggregation: str = "list",
    filters: Optional[str] = None,
    limit: int = 100,
) -> str:
    """Query dimension values from a metric view WITHOUT aggregating measures.

    Use this for: counting distinct dimension values, listing unique values,
    or exploring what a dimension contains before writing a full measure query.

    Args:
        metric_view_name: Name of the metric view to query.
        dimensions: Comma-separated dimension names to query (e.g. "region, quarter").
        aggregation: One of 'list' (distinct values), 'count_distinct' (count per dimension),
                     or 'unique_values' (distinct values with counts). Default 'list'.
        filters: Optional SQL WHERE clause.
        limit: Max rows to return (default 100, max 1000).
    """
    dimensions = _parse_str_list(dimensions)
    if not dimensions:
        return json.dumps({"error": "At least one dimension is required."})
    limit = min(max(limit, 1), 1000)

    try:
        defn, fqn, err = _fetch_definition_and_source(metric_view_name)
        if err:
            return json.dumps({"error": err})

        valid_dims = {d["name"] for d in defn.get("dimensions", [])}
        bad_dims = [d for d in dimensions if d not in valid_dims]
        if bad_dims:
            return json.dumps({"error": f"Invalid dimensions: {bad_dims}. Valid: {sorted(valid_dims)}"})

        quoted = [f"`{d}`" for d in dimensions]

        if aggregation == "count_distinct":
            select_parts = [f"COUNT(DISTINCT {q}) AS `{d}_count`" for q, d in zip(quoted, dimensions)]
            sql = f"SELECT {', '.join(select_parts)} FROM {fqn}"
        elif aggregation == "unique_values":
            select_parts = quoted + ["COUNT(*) AS `row_count`"]
            sql = f"SELECT {', '.join(select_parts)} FROM {fqn} GROUP BY ALL ORDER BY `row_count` DESC"
        else:  # "list"
            sql = f"SELECT DISTINCT {', '.join(quoted)} FROM {fqn}"

        if filters:
            sql += f" WHERE {filters}" if "WHERE" not in sql.upper() else f" AND ({filters})"
        sql += f" LIMIT {limit}"

        err = _validate_sql(sql)
        if err:
            return json.dumps({"error": err, "sql": sql})

        query_result = _run_sql(sql)
        if query_result["success"]:
            return json.dumps({
                "sql": sql,
                "columns": query_result["columns"],
                "rows": query_result["rows"][:limit],
                "row_count": query_result["row_count"],
                "aggregation": aggregation,
            })
        return json.dumps({"error": query_result.get("error"), "sql": sql})
    except Exception as e:
        return json.dumps({"error": str(e)})


METRIC_VIEW_TOOLS = [
    search_metric_views,
    list_metric_views,
    describe_metric_view,
    metric_view_query,
    metric_view_dimension_query,
]


# ---------------------------------------------------------------------------
# Direct-call API for the purpose-built graph (returns native dicts, not JSON strings)
# ---------------------------------------------------------------------------

def search_metric_views_direct(question: str) -> list[dict]:
    """Run vector search + definition enrichment, return list of candidate dicts."""
    try:
        from agent.metadata_tools import _get_vs_index
        index_name = f"{CATALOG}.{SCHEMA}.{VS_INDEX_SUFFIX}"
        index = _get_vs_index(index_name)
    except Exception as e:
        logger.warning("Vector search unavailable: %s", e)
        return []

    try:
        kwargs = dict(
            query_text=question,
            columns=["doc_type", "content", "table_name", "node_id"],
            filters={"doc_type": ("metric_view_summary", "metric_view_measures", "metric_view_schema")},
            num_results=10,
            query_type="HYBRID",
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            results = pool.submit(index.similarity_search, **kwargs).result(timeout=30)
        cols = results.get("manifest", {}).get("columns", [])
        col_names = [c.get("name", f"col{i}") for i, c in enumerate(cols)]
        candidates = []
        for row in results.get("result", {}).get("data_array", []):
            match = dict(zip(col_names, row)) if col_names else {}
            content = (match.get("content") or "")[:800]
            node_id = match.get("node_id", "")
            mv_name = _extract_mv_name(node_id, content)
            candidates.append({
                "doc_type": match.get("doc_type", ""),
                "content": content,
                "source_table": match.get("table_name", ""),
                "definition_id": node_id,
                "metric_view_name": mv_name,
            })
        return _enrich_candidates_with_definitions(candidates)
    except Exception as e:
        logger.warning("search_metric_views_direct failed: %s", e)
        return []


def list_metric_views_direct(domain: Optional[str] = None) -> list[dict]:
    """List all deployed metric views, return list of dicts."""
    domain_join = f"LEFT JOIN {_fq('table_knowledge_base')} kb ON m.source_table = kb.table_name"
    domain_filter = f"AND LOWER(kb.domain) LIKE '%{domain.lower()}%'" if domain else ""
    try:
        result = _run_sql(f"""
            SELECT m.metric_view_name, m.source_table, m.status,
                   m.deployed_catalog, m.deployed_schema,
                   kb.domain, kb.subdomain, m.json_definition
            FROM {_fq('metric_view_definitions')} m
            {domain_join}
            WHERE m.status IN ('applied', 'validated')
              AND m.metric_view_name IS NOT NULL
              {domain_filter}
            ORDER BY m.metric_view_name
        """)
        if not result["success"]:
            return []
        views = []
        for row in result["rows"]:
            defn = {}
            if row.get("json_definition"):
                try:
                    defn = json.loads(row["json_definition"])
                except (json.JSONDecodeError, TypeError):
                    pass
            views.append({
                "metric_view_name": row["metric_view_name"],
                "source_table": row.get("source_table", ""),
                "domain": row.get("domain", ""),
                "fqn": _get_deployed_fqn(row),
                "measures": [m.get("name", "") for m in defn.get("measures", [])],
                "dimensions": [d.get("name", "") for d in defn.get("dimensions", [])],
            })
        return views
    except Exception as e:
        logger.warning("list_metric_views_direct failed: %s", e)
        return []


def fetch_definition(metric_view_name: str) -> tuple[Optional[dict], Optional[str], Optional[str]]:
    """Fetch parsed definition and FQN for a metric view. Returns (defn, fqn, error)."""
    return _fetch_definition_and_source(metric_view_name)


def execute_measure_query(
    metric_view_name: str,
    measures: list[str],
    dimensions: list[str] = None,
    filters: Optional[str] = None,
    order_by: Optional[str] = None,
    limit: int = 100,
) -> dict:
    """Build and run a MEASURE() query, returning result dict with 'sql', 'rows', 'error' keys."""
    dimensions = dimensions or []
    if not measures:
        return {"error": "At least one measure is required."}
    limit = min(max(limit, 1), 1000)

    defn, fqn, err = _fetch_definition_and_source(metric_view_name)
    if err:
        return {"error": err}

    valid_measures = {m["name"] for m in defn.get("measures", [])}
    valid_dims = {d["name"] for d in defn.get("dimensions", [])}

    bad_measures = [m for m in measures if m not in valid_measures]
    if bad_measures:
        return {"error": f"Invalid measures: {bad_measures}. Valid: {sorted(valid_measures)}"}
    bad_dims = [d for d in dimensions if d not in valid_dims]
    if bad_dims:
        return {"error": f"Invalid dimensions: {bad_dims}. Valid: {sorted(valid_dims)}"}

    dim_clause = ", ".join(f"`{d}`" for d in dimensions) if dimensions else ""
    measure_clause = ", ".join(f"MEASURE(`{m}`) AS `{m}`" for m in measures)
    select_parts = [p for p in [dim_clause, measure_clause] if p]
    sql = f"SELECT {', '.join(select_parts)} FROM {fqn}"
    if filters:
        sql += f" WHERE {filters}"
    if dimensions:
        sql += " GROUP BY ALL"
    if order_by:
        sql += f" ORDER BY {_quote_order_by(order_by, valid_measures | valid_dims)}"
    sql += f" LIMIT {limit}"

    err = _validate_sql(sql)
    if err:
        return {"error": err, "sql": sql}

    result = _run_sql_with_retry(sql)
    if result["success"]:
        return {
            "sql": sql,
            "columns": result["columns"],
            "rows": result["rows"][:limit],
            "row_count": result["row_count"],
        }
    return {"error": result.get("error"), "sql": sql}


def execute_dimension_query(
    metric_view_name: str,
    dimensions: list[str],
    aggregation: str = "list",
    filters: Optional[str] = None,
    limit: int = 100,
) -> dict:
    """Build and run a dimension-only query, returning result dict."""
    if not dimensions:
        return {"error": "At least one dimension is required."}
    limit = min(max(limit, 1), 1000)

    defn, fqn, err = _fetch_definition_and_source(metric_view_name)
    if err:
        return {"error": err}

    valid_dims = {d["name"] for d in defn.get("dimensions", [])}
    bad_dims = [d for d in dimensions if d not in valid_dims]
    if bad_dims:
        return {"error": f"Invalid dimensions: {bad_dims}. Valid: {sorted(valid_dims)}"}

    quoted = [f"`{d}`" for d in dimensions]

    if aggregation == "count_distinct":
        select_parts = [f"COUNT(DISTINCT {q}) AS `{d}_count`" for q, d in zip(quoted, dimensions)]
        sql = f"SELECT {', '.join(select_parts)} FROM {fqn}"
    elif aggregation == "unique_values":
        select_parts = quoted + ["COUNT(*) AS `row_count`"]
        sql = f"SELECT {', '.join(select_parts)} FROM {fqn} GROUP BY ALL ORDER BY `row_count` DESC"
    else:
        sql = f"SELECT DISTINCT {', '.join(quoted)} FROM {fqn}"

    if filters:
        sql += f" WHERE {filters}" if "WHERE" not in sql.upper() else f" AND ({filters})"
    sql += f" LIMIT {limit}"

    err = _validate_sql(sql)
    if err:
        return {"error": err, "sql": sql}

    result = _run_sql_with_retry(sql)
    if result["success"]:
        return {
            "sql": sql,
            "columns": result["columns"],
            "rows": result["rows"][:limit],
            "row_count": result["row_count"],
            "aggregation": aggregation,
        }
    return {"error": result.get("error"), "sql": sql}
