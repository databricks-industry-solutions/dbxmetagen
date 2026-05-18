"""Tools for the metric view agent.

Provides semantic search over metric view documents, listing/describing
metric view definitions, and structured query execution using MEASURE() syntax.
"""

import concurrent.futures
import json
import logging
import os
import re
from typing import Optional

from langchain_core.tools import tool

logger = logging.getLogger(__name__)

CATALOG = os.environ.get("CATALOG_NAME", "")
SCHEMA = os.environ.get("SCHEMA_NAME", "")
VS_INDEX_SUFFIX = os.environ.get("VECTOR_SEARCH_INDEX", "metadata_vs_index")


def _fq(table: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table}"


def _run_sql(query: str) -> dict:
    from agent.metadata_tools import _execute_query
    return _execute_query(query)


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
            # Extract metric view name from node_id (format: "mv::<name>::...")
            node_id = match.get("node_id", "")
            mv_name = node_id.split("::")[1] if "::" in str(node_id) else ""
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
    measures: list[str],
    dimensions: list[str],
    filters: Optional[str] = None,
    order_by: Optional[str] = None,
    limit: int = 100,
) -> str:
    """Construct and execute a SQL query against a metric view using MEASURE() syntax.

    Only measures and dimensions declared in the metric view definition are allowed.
    The query is executed on a SQL warehouse and results are returned.

    Args:
        metric_view_name: Name of the metric view to query.
        measures: List of measure names to aggregate (must exist in the definition).
        dimensions: List of dimension names to group by (must exist in the definition).
        filters: Optional SQL WHERE clause (e.g. "region = 'West' AND year >= 2023").
        order_by: Optional ORDER BY clause (e.g. "total_revenue DESC").
        limit: Max rows to return (default 100, max 1000).
    """
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

        dim_clause = ", ".join(dimensions) if dimensions else ""
        measure_clause = ", ".join(f"MEASURE({m}) AS `{m}`" for m in measures)
        select_parts = [p for p in [dim_clause, measure_clause] if p]
        sql = f"SELECT {', '.join(select_parts)} FROM {fqn}"
        if filters:
            sql += f" WHERE {filters}"
        if dimensions:
            sql += " GROUP BY ALL"
        if order_by:
            sql += f" ORDER BY {order_by}"
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

        # Fallback: try raw-expression SQL if MEASURE() isn't supported
        logger.warning("MEASURE() query failed, attempting expression fallback: %s", query_result.get("error"))
        return _fallback_raw_query(defn, fqn, measures, dimensions, filters, order_by, limit)

    except Exception as e:
        return json.dumps({"error": str(e)})


def _fallback_raw_query(
    defn: dict, fqn: str,
    measures: list[str], dimensions: list[str],
    filters: Optional[str], order_by: Optional[str], limit: int,
) -> str:
    """Build conventional SQL from the metric view's raw expressions."""
    measure_map = {m["name"]: m["expr"] for m in defn.get("measures", [])}
    dim_map = {d["name"]: d["expr"] for d in defn.get("dimensions", [])}
    source = defn.get("source", fqn)

    dim_exprs = [f"{dim_map.get(d, d)} AS `{d}`" for d in dimensions]
    measure_exprs = [f"{measure_map.get(m, m)} AS `{m}`" for m in measures]
    select_parts = dim_exprs + measure_exprs

    join_clause = ""
    for j in defn.get("joins", []):
        join_clause += f" LEFT JOIN {j['source']} AS {j['name']} ON {j['on']}"

    sql = f"SELECT {', '.join(select_parts)} FROM {source} AS source{join_clause}"
    if defn.get("filter"):
        sql += f" WHERE {defn['filter']}"
        if filters:
            sql += f" AND ({filters})"
    elif filters:
        sql += f" WHERE {filters}"
    if dimensions:
        dim_refs = [dim_map.get(d, d) for d in dimensions]
        sql += f" GROUP BY {', '.join(dim_refs)}"
    if order_by:
        sql += f" ORDER BY {order_by}"
    sql += f" LIMIT {limit}"

    err = _validate_sql(sql)
    if err:
        return json.dumps({"error": err, "sql": sql})

    try:
        result = _run_sql(sql)
        if result["success"]:
            return json.dumps({
                "sql": sql,
                "columns": result["columns"],
                "rows": result["rows"][:limit],
                "row_count": result["row_count"],
                "note": "Executed using raw expressions (MEASURE() fallback).",
            })
        return json.dumps({"error": result.get("error"), "sql": sql})
    except Exception as e:
        return json.dumps({"error": str(e), "sql": sql})


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
    dimensions: list[str],
    aggregation: str = "list",
    filters: Optional[str] = None,
    limit: int = 100,
) -> str:
    """Query dimension values from a metric view WITHOUT aggregating measures.

    Use this for: counting distinct dimension values, listing unique values,
    or exploring what a dimension contains before writing a full measure query.

    Args:
        metric_view_name: Name of the metric view to query.
        dimensions: Dimension names to query (must exist in the definition).
        aggregation: One of 'list' (distinct values), 'count_distinct' (count per dimension),
                     or 'unique_values' (distinct values with counts). Default 'list'.
        filters: Optional SQL WHERE clause.
        limit: Max rows to return (default 100, max 1000).
    """
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

        dim_map = {d["name"]: d["expr"] for d in defn.get("dimensions", [])}
        source = defn.get("source", fqn)
        join_clause = ""
        for j in defn.get("joins", []):
            join_clause += f" LEFT JOIN {j['source']} AS {j['name']} ON {j['on']}"

        where = ""
        if defn.get("filter"):
            where = f" WHERE {defn['filter']}"
            if filters:
                where += f" AND ({filters})"
        elif filters:
            where = f" WHERE {filters}"

        base_from = f"{source} AS source{join_clause}{where}"

        if aggregation == "count_distinct":
            select_parts = [f"COUNT(DISTINCT {dim_map.get(d, d)}) AS `{d}_count`" for d in dimensions]
            sql = f"SELECT {', '.join(select_parts)} FROM {base_from}"
        elif aggregation == "unique_values":
            dim_exprs = [f"{dim_map.get(d, d)} AS `{d}`" for d in dimensions]
            select_parts = dim_exprs + ["COUNT(*) AS `row_count`"]
            group_refs = [dim_map.get(d, d) for d in dimensions]
            sql = f"SELECT {', '.join(select_parts)} FROM {base_from} GROUP BY {', '.join(group_refs)} ORDER BY `row_count` DESC LIMIT {limit}"
        else:  # "list"
            dim_exprs = [f"DISTINCT {dim_map.get(d, d)} AS `{d}`" for d in dimensions]
            sql = f"SELECT {', '.join(dim_exprs)} FROM {base_from} LIMIT {limit}"

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
