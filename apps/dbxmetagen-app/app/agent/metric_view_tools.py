"""Tools for the metric view agent.

Provides semantic search over metric view documents, listing/describing
metric view definitions, and structured query execution using MEASURE() syntax.
"""

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

@tool
def search_metric_views(question: str) -> str:
    """Search for metric views relevant to a business question using semantic similarity.

    Embeds the question and queries the vector index for metric view documents.
    Returns candidate metric views with names, source tables, and content summaries.

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
        results = index.similarity_search(
            query_text=question,
            columns=["doc_type", "content", "table_name", "node_id"],
            filters={"doc_type": ("metric_view_summary", "metric_view_measures", "metric_view_schema")},
            num_results=10,
            query_type="HYBRID",
        )
        cols = results.get("manifest", {}).get("columns", [])
        col_names = [c.get("name", f"col{i}") for i, c in enumerate(cols)]
        candidates = []
        for row in results.get("result", {}).get("data_array", []):
            match = dict(zip(col_names, row)) if col_names else {}
            candidates.append({
                "doc_type": match.get("doc_type", ""),
                "content": (match.get("content") or "")[:500],
                "source_table": match.get("table_name", ""),
                "definition_id": match.get("node_id", ""),
            })
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


METRIC_VIEW_TOOLS = [
    search_metric_views,
    list_metric_views,
    describe_metric_view,
    metric_view_query,
]
