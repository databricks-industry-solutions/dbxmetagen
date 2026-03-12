"""Tools for the metadata intelligence agent.

Provides vector search over metadata_documents, read-only SQL against
knowledge-base tables, and convenience functions for table summaries
and data quality lookups.
"""

import json
import logging
import os
import re
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Format, Disposition
from langchain_core.tools import tool

logger = logging.getLogger(__name__)

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


def _get_ws() -> WorkspaceClient:
    global _ws
    if _ws is None:
        _ws = WorkspaceClient()
    return _ws


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
    q = query.lower()
    prefix = f"{CATALOG.lower()}.{SCHEMA.lower()}."
    for t in ALLOWED_TABLES:
        q = q.replace(f"{prefix}{t}", t)
    refs = re.findall(r'\bfrom\s+(\w+)|\bjoin\s+(\w+)', q)
    for match in refs:
        name = match[0] or match[1]
        if name and name not in ALLOWED_TABLES:
            return f"Table '{name}' is not in the allowed list: {', '.join(sorted(ALLOWED_TABLES))}"
    return None


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
    num_results = min(max(num_results, 1), 20)
    vs_index = f"{CATALOG}.{SCHEMA}.{VS_INDEX_SUFFIX}"
    try:
        from databricks.vector_search.client import VectorSearchClient
        vsc = VectorSearchClient()
        index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=vs_index)
        kwargs = dict(
            query_text=query,
            columns=["doc_id", "doc_type", "content", "table_name", "domain", "entity_type", "confidence_score"],
            num_results=num_results,
        )
        if doc_type_filter:
            kwargs["filters"] = {"doc_type": doc_type_filter}
        results = index.similarity_search(**kwargs)
        matches = []
        cols = results.get("manifest", {}).get("columns", [])
        col_names = [c.get("name", f"col{i}") for i, c in enumerate(cols)] if cols else []
        for row in results.get("result", {}).get("data_array", []):
            if col_names:
                matches.append(dict(zip(col_names, row)))
            else:
                matches.append({"data": row})
        return json.dumps({"matches": matches, "count": len(matches)})
    except Exception as e:
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
    - ontology_entities: entity_id, entity_name, entity_type, description, source_tables, confidence
    - fk_predictions: src_table, src_column, dst_table, dst_column, final_confidence, cardinality, ai_reasoning
    - metric_view_definitions: definition_id, metric_view_name, source_table, source_questions, json_definition, status
    - profiling_results: table_name, column_name, distinct_count, null_count, min_value, max_value, avg_value
    - metadata_generation_log: table_name, mode, status, comment
    """
    q_upper = query.strip().upper()
    if not q_upper.startswith("SELECT"):
        return json.dumps({"error": "Only SELECT queries are allowed"})
    for kw in ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"]:
        if kw in q_upper:
            return json.dumps({"error": f"Blocked keyword: {kw}"})
    err = _check_table_allowlist(query)
    if err:
        return json.dumps({"error": err})
    try:
        result = _execute_query(query)
        if result["success"]:
            return json.dumps({"columns": result["columns"], "rows": result["rows"][:100], "row_count": result["row_count"]})
        return json.dumps({"error": result["error"]})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Tool: Table summary
# ---------------------------------------------------------------------------

@tool
def get_table_summary(table_name: str) -> str:
    """Get a comprehensive summary of a specific table including columns, domain, entity types, FK relationships, and quality.

    Args:
        table_name: Fully qualified table name (catalog.schema.table) or short name.
    """
    fq_prefix = f"{CATALOG}.{SCHEMA}."
    try:
        tbl = _execute_query(f"""
            SELECT table_name, comment, domain, subdomain, has_pii, has_phi, row_count
            FROM {fq_prefix}table_knowledge_base
            WHERE table_name LIKE '%{table_name.split('.')[-1]}%' LIMIT 1
        """)
        cols = _execute_query(f"""
            SELECT column_name, data_type, comment, classification
            FROM {fq_prefix}column_knowledge_base
            WHERE table_name LIKE '%{table_name.split('.')[-1]}%'
        """)
        fks = _execute_query(f"""
            SELECT src_table, src_column, dst_table, dst_column, ROUND(final_confidence, 2) AS confidence
            FROM {fq_prefix}fk_predictions
            WHERE (src_table LIKE '%{table_name.split('.')[-1]}%' OR dst_table LIKE '%{table_name.split('.')[-1]}%')
              AND final_confidence >= 0.5
        """)
        entities = _execute_query(f"""
            SELECT entity_name, entity_type, confidence
            FROM {fq_prefix}ontology_entities
            WHERE ARRAY_CONTAINS(source_tables, (SELECT table_name FROM {fq_prefix}table_knowledge_base WHERE table_name LIKE '%{table_name.split('.')[-1]}%' LIMIT 1))
        """)
        return json.dumps({
            "table": tbl.get("rows", []),
            "columns": cols.get("rows", []),
            "foreign_keys": fks.get("rows", []),
            "entities": entities.get("rows", []),
        })
    except Exception as e:
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
            return json.dumps({"rows": result["rows"], "row_count": result["row_count"]})
        return json.dumps({"error": result["error"]})
    except Exception as e:
        return json.dumps({"error": str(e)})


# Re-export existing graph tools
from agent.tools import query_graph_nodes, get_node_details, find_similar_nodes, traverse_graph  # noqa: E402, F401

ALL_METADATA_TOOLS = [
    search_metadata, execute_metadata_sql, get_table_summary, get_data_quality,
    query_graph_nodes, get_node_details, find_similar_nodes, traverse_graph,
]
