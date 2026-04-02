"""Tools for the metadata intelligence agent.

Provides vector search over metadata_documents, read-only SQL against
knowledge-base tables, and convenience functions for table summaries
and data quality lookups.
"""

import json
import logging
import os
import re
import time
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Format, Disposition
from langchain_core.tools import tool

logger = logging.getLogger(__name__)


def _log_tool(name):
    """Return a context-manager-like pair (start, finish) for tool timing."""
    t0 = time.time()
    logger.info("[TOOL] %s -- start", name)
    return t0


def _log_tool_end(name, t0, error=None):
    elapsed = round(time.time() - t0, 2)
    if error:
        logger.warning("[TOOL] %s -- FAILED in %.2fs: %s", name, elapsed, error)
    else:
        logger.info("[TOOL] %s -- done in %.2fs", name, elapsed)

CATALOG = os.environ.get("CATALOG_NAME", "")
SCHEMA = os.environ.get("SCHEMA_NAME", "")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "")
VS_ENDPOINT = os.environ.get("VECTOR_SEARCH_ENDPOINT", "dbxmetagen-vs")
VS_INDEX_SUFFIX = os.environ.get("VECTOR_SEARCH_INDEX", "metadata_vs_index")

ALLOWED_TABLES = {
    "table_knowledge_base", "column_knowledge_base", "ontology_entities",
    "fk_predictions", "metric_view_definitions", "profiling_results",
    "metadata_documents", "metadata_generation_log",
}

_ws = None
_vsc = None
_vs_indexes: dict = {}


def _get_ws() -> WorkspaceClient:
    global _ws
    if _ws is None:
        _ws = WorkspaceClient()
    return _ws


def _build_vs_client():
    """Create a fresh VectorSearchClient with auto-refreshing credentials."""
    from databricks.vector_search.client import VectorSearchClient
    ws = _get_ws()
    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    if client_id and client_secret:
        return VectorSearchClient(
            workspace_url=ws.config.host,
            service_principal_client_id=client_id,
            service_principal_client_secret=client_secret,
        )
    _token = os.environ.get("DATABRICKS_TOKEN")
    if not _token:
        headers = ws.config.authenticate()
        _token = headers.get("Authorization", "").removeprefix("Bearer ")
    return VectorSearchClient(workspace_url=ws.config.host, personal_access_token=_token)


def _get_vs_index(index_name: str, _retry: bool = True):
    """Return a cached VectorSearchIndex, recreating the client on auth errors."""
    global _vsc
    if index_name in _vs_indexes:
        return _vs_indexes[index_name]
    if _vsc is None:
        _vsc = _build_vs_client()
    import concurrent.futures
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            idx = pool.submit(_vsc.get_index, endpoint_name=VS_ENDPOINT, index_name=index_name).result(timeout=15)
    except concurrent.futures.TimeoutError:
        raise RuntimeError(f"get_index timed out for {index_name} on {VS_ENDPOINT}")
    except Exception as e:
        err_str = str(e).lower()
        if _retry and ("invalid token" in err_str or "unauthorized" in err_str or "401" in err_str):
            logger.info("VS auth error, refreshing token and retrying: %s", e)
            _vsc = _build_vs_client()
            _vs_indexes.pop(index_name, None)
            return _get_vs_index(index_name, _retry=False)
        logger.warning("Failed to get VS index %s on endpoint %s: %s", index_name, VS_ENDPOINT, e)
        raise
    _vs_indexes[index_name] = idx
    return idx


def _execute_query(query: str) -> dict:
    w = _get_ws()
    result = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=query,
        wait_timeout="30s", format=Format.JSON_ARRAY, disposition=Disposition.INLINE,
    )
    state = result.status.state.value if result.status and result.status.state else "UNKNOWN"
    if state in ("SUCCEEDED", "CLOSED"):
        if result.result and result.result.data_array:
            columns = [col.name for col in result.manifest.schema.columns]
            rows = [dict(zip(columns, row)) for row in result.result.data_array]
            return {"success": True, "columns": columns, "rows": rows, "row_count": len(rows)}
        return {"success": True, "columns": [], "rows": [], "row_count": 0}
    return {"success": False, "error": str(result.status.error)}


def _check_table_allowlist(query: str) -> Optional[str]:
    from agent.common import check_table_allowlist
    return check_table_allowlist(query, ALLOWED_TABLES)


def _auto_qualify(query: str, allowed: set) -> str:
    """Replace bare allowed table names with fully-qualified catalog.schema.table."""
    prefix = f"{CATALOG}.{SCHEMA}."
    result = query
    for t in allowed:
        result = re.sub(rf'\b(?<!\.)({t})\b', f'{prefix}{t}', result)
    return result


# ---------------------------------------------------------------------------
# Tool: Vector search
# ---------------------------------------------------------------------------

@tool
def search_metadata(query: str, doc_type_filter: Optional[str] = None, num_results: int = 5) -> str:
    """Semantic search over all indexed metadata (tables, columns, entities, metric views, FK relationships).

    Args:
        query: Natural language search query.
        doc_type_filter: Optional filter -- one of 'table', 'column', 'entity', 'metric_view', 'fk_relationship'.
        num_results: Number of results (1-20).
    """
    t0 = _log_tool("search_metadata")
    num_results = min(max(num_results, 1), 20)
    vs_index = f"{CATALOG}.{SCHEMA}.{VS_INDEX_SUFFIX}"
    try:
        index = _get_vs_index(vs_index)
        kwargs = dict(
            query_text=query,
            columns=["doc_id", "doc_type", "content", "node_id", "table_name", "domain", "entity_type", "confidence_score"],
            num_results=num_results,
        )
        if doc_type_filter:
            kwargs["filters"] = {"doc_type": doc_type_filter}
        kwargs["query_type"] = "HYBRID"
        # Run VS call with a timeout to prevent indefinite hangs
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(index.similarity_search, **kwargs)
            results = future.result(timeout=30)
        matches = []
        cols = results.get("manifest", {}).get("columns", [])
        col_names = [c.get("name", f"col{i}") for i, c in enumerate(cols)] if cols else []
        for row in results.get("result", {}).get("data_array", []):
            if col_names:
                matches.append(dict(zip(col_names, row)))
            else:
                matches.append({"data": row})
        _log_tool_end("search_metadata", t0)
        return json.dumps({"matches": matches, "count": len(matches)})
    except concurrent.futures.TimeoutError:
        _log_tool_end("search_metadata", t0, error="Vector search timed out after 30s")
        return json.dumps({"error": "Vector search timed out after 30s. The index may not be ready."})
    except Exception as e:
        err_str = str(e).lower()
        if "invalid token" in err_str or "unauthorized" in err_str or "401" in err_str:
            logger.info("VS query auth error, refreshing client and retrying: %s", e)
            global _vsc
            _vsc = None
            _vs_indexes.pop(vs_index, None)
            try:
                index = _get_vs_index(vs_index)
                import concurrent.futures as _cf2
                with _cf2.ThreadPoolExecutor(max_workers=1) as pool2:
                    results = pool2.submit(index.similarity_search, **kwargs).result(timeout=30)
                matches = []
                cols = results.get("manifest", {}).get("columns", [])
                col_names = [c.get("name", f"col{i}") for i, c in enumerate(cols)] if cols else []
                for row in results.get("result", {}).get("data_array", []):
                    matches.append(dict(zip(col_names, row)) if col_names else {"data": row})
                _log_tool_end("search_metadata", t0)
                return json.dumps({"matches": matches, "count": len(matches)})
            except Exception as retry_err:
                _log_tool_end("search_metadata", t0, error=retry_err)
                return json.dumps({"error": f"Auth retry failed: {retry_err}"})
        _log_tool_end("search_metadata", t0, error=e)
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Tool: SQL queries on knowledge base
# ---------------------------------------------------------------------------

@tool
def execute_metadata_sql(query: str) -> str:
    """Execute a read-only SQL query against the metadata knowledge base tables.

    Allowed tables (use fully-qualified names with {catalog}.{schema}.table or just the table name):
    - table_knowledge_base: table_name, comment, domain, subdomain, has_pii, has_phi, row_count
    - column_knowledge_base: table_name, column_name, comment, data_type, classification, classification_type
    - ontology_entities: entity_id, entity_name, entity_type, description, source_tables, confidence, entity_uri, source_ontology
    - fk_predictions: src_table, src_column, dst_table, dst_column, final_confidence, join_rate, pk_uniqueness, ri_score, ai_reasoning
    - metric_view_definitions: definition_id, metric_view_name, source_table, source_questions, json_definition, status
    - profiling_results: table_name, column_name, distinct_count, null_count, min_value, max_value, avg_value
    - metadata_generation_log: table_name, mode, status, comment
    """
    t0 = _log_tool("execute_metadata_sql")
    from agent.common import check_select_only
    err = check_select_only(query)
    if err:
        _log_tool_end("execute_metadata_sql", t0, error=err)
        return json.dumps({"error": err})
    err = _check_table_allowlist(query)
    if err:
        _log_tool_end("execute_metadata_sql", t0, error=err)
        return json.dumps({"error": err})
    query = _auto_qualify(query, ALLOWED_TABLES)
    try:
        result = _execute_query(query)
        if result["success"]:
            _log_tool_end("execute_metadata_sql", t0)
            return json.dumps({"columns": result["columns"], "rows": result["rows"][:100], "row_count": result["row_count"]})
        _log_tool_end("execute_metadata_sql", t0, error=result["error"])
        return json.dumps({"error": result["error"]})
    except Exception as e:
        _log_tool_end("execute_metadata_sql", t0, error=e)
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Tool: Table summary
# ---------------------------------------------------------------------------

@tool
def get_table_summary(table_name: str) -> str:
    """Get a comprehensive summary of a table: metadata, top columns, FK relationships, and entity types in a single call.

    This tool is expensive -- only use it when you need a full picture of ONE table.
    For simple lookups, prefer execute_metadata_sql.

    Args:
        table_name: Fully qualified table name (catalog.schema.table) or short name.
    """
    t0 = _log_tool("get_table_summary")
    short = table_name.split(".")[-1]
    fq = f"{CATALOG}.{SCHEMA}."
    query = f"""
        WITH tbl AS (
            SELECT table_name, comment, domain, subdomain, has_pii, has_phi, row_count
            FROM {fq}table_knowledge_base WHERE table_name LIKE '%{short}%' LIMIT 1
        ),
        cols AS (
            SELECT column_name, data_type, comment, classification
            FROM {fq}column_knowledge_base WHERE table_name LIKE '%{short}%' LIMIT 30
        ),
        fks AS (
            SELECT src_table, src_column, dst_table, dst_column, ROUND(final_confidence, 2) AS confidence
            FROM {fq}fk_predictions
            WHERE (src_table LIKE '%{short}%' OR dst_table LIKE '%{short}%') AND final_confidence >= 0.5
        ),
        ents AS (
            SELECT entity_type, entity_name, confidence, entity_uri, source_ontology
            FROM {fq}ontology_entities
            WHERE EXISTS(source_tables, t -> t LIKE '%{short}%')
        )
        SELECT 'table' AS _section, TO_JSON(STRUCT(*)) AS _data FROM tbl
        UNION ALL
        SELECT 'column', TO_JSON(STRUCT(*)) FROM cols
        UNION ALL
        SELECT 'fk', TO_JSON(STRUCT(*)) FROM fks
        UNION ALL
        SELECT 'ontology', TO_JSON(STRUCT(*)) FROM ents
    """
    try:
        result = _execute_query(query)
        sections: dict = {"table": [], "columns": [], "foreign_keys": [], "ontology_entities": []}
        for row in result.get("rows", []):
            sec = row.get("_section", "")
            data = json.loads(row.get("_data", "{}"))
            if sec == "table":
                sections["table"].append(data)
            elif sec == "column":
                sections["columns"].append(data)
            elif sec == "fk":
                sections["foreign_keys"].append(data)
            elif sec == "ontology":
                sections["ontology_entities"].append(data)
        _log_tool_end("get_table_summary", t0)
        return json.dumps(sections)
    except Exception as e:
        _log_tool_end("get_table_summary", t0, error=e)
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Tool: Data quality
# ---------------------------------------------------------------------------

@tool
def get_data_quality(table_name_or_domain: str) -> str:
    """Get profiling stats and data quality scores for a table or domain.

    Args:
        table_name_or_domain: A table name pattern or domain name.
    """
    t0 = _log_tool("get_data_quality")
    fq_prefix = f"{CATALOG}.{SCHEMA}."
    try:
        result = _execute_query(f"""
            SELECT table_name, column_name, distinct_count, null_count, min_value, max_value
            FROM {fq_prefix}profiling_results
            WHERE table_name LIKE '%{table_name_or_domain}%'
               OR table_name IN (
                   SELECT table_name FROM {fq_prefix}table_knowledge_base
                   WHERE domain LIKE '%{table_name_or_domain}%'
               )
            LIMIT 50
        """)
        if result["success"]:
            _log_tool_end("get_data_quality", t0)
            return json.dumps({"rows": result["rows"], "row_count": result["row_count"]})
        _log_tool_end("get_data_quality", t0, error=result["error"])
        return json.dumps({"error": result["error"]})
    except Exception as e:
        _log_tool_end("get_data_quality", t0, error=e)
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Tool: VS-to-graph bridge (1-hop expansion from VS hits)
# ---------------------------------------------------------------------------

@tool
def expand_vs_hits(
    node_ids: list[str],
    edge_types: Optional[list[str]] = None,
    max_per_node: int = 5,
) -> str:
    """Given node_ids from vector search results, do 1-hop graph expansion.

    Returns connected nodes with edge types, join expressions, and display names.
    This bridges semantic search results into the structural knowledge graph.

    Args:
        node_ids: List of node IDs (from VS search_metadata results' node_id field).
        edge_types: Optional filter for edge types (e.g. ['references', 'contains']).
        max_per_node: Max neighbors per starting node.
    """
    t0 = _log_tool("expand_vs_hits")
    from api_server import graph_query
    all_neighbors = []
    for nid in node_ids[:20]:
        et_filter = ""
        if edge_types:
            et_list = ", ".join(f"'{t}'" for t in edge_types)
            et_filter = f" AND e.edge_type IN ({et_list})"
        q = f"""
            SELECT e.src, e.dst, e.relationship, e.edge_type, e.weight,
                   e.join_expression, e.join_confidence, e.source_system,
                   n.node_type, n.domain, n.display_name, n.short_description
            FROM public.graph_edges e
            JOIN public.graph_nodes n ON n.id = CASE WHEN e.src = '{nid}' THEN e.dst ELSE e.src END
            WHERE (e.src = '{nid}' OR e.dst = '{nid}'){et_filter}
            LIMIT {max_per_node}
        """
        rows = graph_query(q)
        for r in rows:
            r["origin_node"] = nid
        all_neighbors.extend(rows)
    _log_tool_end("expand_vs_hits", t0)
    return json.dumps({"neighbors": all_neighbors, "count": len(all_neighbors)})


# ---------------------------------------------------------------------------
# Tool: Baseline-only SQL (restricted to 3 KB tables)
# ---------------------------------------------------------------------------

BASELINE_TABLES = {"table_knowledge_base", "column_knowledge_base", "schema_knowledge_base"}


def _check_baseline_allowlist(query: str) -> Optional[str]:
    from agent.common import check_table_allowlist
    return check_table_allowlist(query, BASELINE_TABLES)


@tool
def execute_baseline_sql(query: str) -> str:
    """Execute read-only SQL against ONLY the three core knowledge base tables.

    Allowed tables (use fully-qualified {catalog}.{schema}.table or just the table name):
    - table_knowledge_base: table_name, comment, domain, subdomain, has_pii, has_phi, row_count
    - column_knowledge_base: table_name, column_name, comment, data_type, classification, classification_type
    - schema_knowledge_base: catalog_name, schema_name, comment, tables_count
    """
    t0 = _log_tool("execute_baseline_sql")
    from agent.common import check_select_only
    err = check_select_only(query)
    if err:
        _log_tool_end("execute_baseline_sql", t0, error=err)
        return json.dumps({"error": err})
    err = _check_baseline_allowlist(query)
    if err:
        _log_tool_end("execute_baseline_sql", t0, error=err)
        return json.dumps({"error": err})
    query = _auto_qualify(query, BASELINE_TABLES)
    try:
        result = _execute_query(query)
        if result["success"]:
            _log_tool_end("execute_baseline_sql", t0)
            return json.dumps({"columns": result["columns"], "rows": result["rows"][:100], "row_count": result["row_count"]})
        _log_tool_end("execute_baseline_sql", t0, error=result["error"])
        return json.dumps({"error": result["error"]})
    except Exception as e:
        _log_tool_end("execute_baseline_sql", t0, error=e)
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Tool: Arbitrary read-only SQL (for structured retrieval on discovered tables)
# ---------------------------------------------------------------------------

@tool
def execute_data_sql(query: str) -> str:
    """Execute a read-only SQL query against ANY table the user has access to.

    Use this for querying actual data tables discovered during analysis -- not
    limited to the metadata knowledge base tables. SELECT only; LIMIT enforced.

    Args:
        query: A SELECT query. A LIMIT 200 clause is appended if missing.
    """
    t0 = _log_tool("execute_data_sql")
    from agent.common import check_select_only
    err = check_select_only(query)
    if err:
        _log_tool_end("execute_data_sql", t0, error=err)
        return json.dumps({"error": err})
    q = query.rstrip().rstrip(";")
    if not re.search(r'\bLIMIT\b', q, re.IGNORECASE):
        q += " LIMIT 200"
    try:
        result = _execute_query(q)
        if result["success"]:
            _log_tool_end("execute_data_sql", t0)
            return json.dumps({"columns": result["columns"], "rows": result["rows"][:200], "row_count": result["row_count"]})
        _log_tool_end("execute_data_sql", t0, error=result["error"])
        return json.dumps({"error": result["error"]})
    except Exception as e:
        _log_tool_end("execute_data_sql", t0, error=e)
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Tool: Graph SQL (Lakebase PG -> UC Delta fallback)
# ---------------------------------------------------------------------------

GRAPH_TABLES = {"graph_nodes", "graph_edges"}


def _auto_qualify_graph(query: str) -> str:
    """Replace bare graph table names with public.graph_nodes / public.graph_edges."""
    result = query
    for t in GRAPH_TABLES:
        result = re.sub(rf'\b(?<!\.){t}\b', f'public.{t}', result)
    return result


@tool
def execute_graph_sql(query: str) -> str:
    """Execute a read-only SQL query against the knowledge graph tables.

    Uses Lakebase (PostgreSQL) for low-latency queries when configured,
    otherwise falls back to UC Delta tables via SQL warehouse.

    Allowed tables (use public.graph_nodes / public.graph_edges or bare names):
    - graph_nodes: id, node_type, table_name, catalog, schema, domain, subdomain,
      display_name, short_description, comment, has_pii, has_phi, security_level,
      sensitivity, status, quality_score, ontology_id, ontology_type, data_type
    - graph_edges: src, dst, relationship, edge_type, weight, join_expression,
      join_confidence, ontology_rel, source_system, direction, status

    Args:
        query: A SELECT query against graph_nodes and/or graph_edges.
    """
    t0 = _log_tool("execute_graph_sql")
    from agent.common import check_select_only
    err = check_select_only(query)
    if err:
        _log_tool_end("execute_graph_sql", t0, error=err)
        return json.dumps({"error": err})
    from api_server import graph_query
    q = _auto_qualify_graph(query.rstrip().rstrip(";"))
    if not re.search(r'\bLIMIT\b', q, re.IGNORECASE):
        q += " LIMIT 100"
    try:
        rows = graph_query(q)
        _log_tool_end("execute_graph_sql", t0)
        return json.dumps({"rows": rows[:100], "row_count": len(rows)})
    except Exception as e:
        _log_tool_end("execute_graph_sql", t0, error=e)
        return json.dumps({"error": str(e)})


# Re-export existing graph tools
from agent.tools import query_graph_nodes, get_node_details, find_similar_nodes, traverse_graph  # noqa: E402, F401

ALL_METADATA_TOOLS = [
    search_metadata, execute_metadata_sql, execute_graph_sql, get_table_summary,
    get_data_quality, query_graph_nodes, get_node_details, find_similar_nodes,
    traverse_graph,
]

GRAPHRAG_TOOLS = ALL_METADATA_TOOLS + [expand_vs_hits, execute_data_sql]
BASELINE_TOOLS = [execute_baseline_sql]
