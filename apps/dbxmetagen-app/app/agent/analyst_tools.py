"""Tools for the dual-mode SQL analyst agent.

Blind-mode tools: schema introspection only (DESCRIBE, SHOW TABLES, sample rows).
Enriched-mode tools: full semantic layer (vector search, KB comments, FK join paths,
ontology roles, profiling stats, metric view definitions).
Shared tools: test_sql, execute_query (both modes).
"""

import json
import logging
import os
import re
import time
from typing import Optional

from databricks.sdk.service.sql import Format, Disposition
from langchain_core.tools import tool

from agent.common import (
    CATALOG, SCHEMA, WAREHOUSE_ID,
    fq as _fq, check_select_only as _check_select_only, latest_profiling_join,
)

logger = logging.getLogger(__name__)

VS_ENDPOINT = os.environ.get("VECTOR_SEARCH_ENDPOINT", "dbxmetagen-vs")
VS_INDEX_SUFFIX = os.environ.get("VECTOR_SEARCH_INDEX", "metadata_vs_index")

MAX_RESULT_ROWS = 500
ANALYST_TIMEOUT = "60s"


def _get_ws():
    from agent.metadata_tools import _get_ws as _mget
    return _mget()


def _run_sql(query: str, timeout: str = ANALYST_TIMEOUT, max_rows: int = MAX_RESULT_ROWS) -> dict:
    if not WAREHOUSE_ID:
        return {"success": False, "error": "WAREHOUSE_ID not configured"}
    w = _get_ws()
    wait_s = int(timeout.rstrip("s"))
    result = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=query,
        wait_timeout=f"{min(wait_s, 50)}s",
        format=Format.JSON_ARRAY, disposition=Disposition.INLINE,
    )
    deadline = time.time() + wait_s
    while (
        result.status and result.status.state
        and result.status.state.value in ("PENDING", "RUNNING")
    ):
        if time.time() > deadline:
            return {"success": False, "error": f"Query timed out after {wait_s}s"}
        time.sleep(2)
        result = w.statement_execution.get_statement(result.statement_id)

    state = result.status.state.value if result.status and result.status.state else "UNKNOWN"
    if state in ("SUCCEEDED", "CLOSED"):
        if result.result and result.result.data_array:
            columns = [col.name for col in result.manifest.schema.columns]
            rows = [dict(zip(columns, row)) for row in result.result.data_array[:max_rows]]
            return {"success": True, "columns": columns, "rows": rows, "row_count": len(rows)}
        return {"success": True, "columns": [], "rows": [], "row_count": 0}
    err_msg = result.status.error.message if result.status and result.status.error else f"Query failed with state: {state}"
    return {"success": False, "error": err_msg}


# ===================================================================
# Shared tools (both modes)
# ===================================================================

@tool
def test_sql(sql: str) -> str:
    """Validate a SQL query by running it with LIMIT 0. Returns 'valid' or an error message.

    Args:
        sql: The SQL query to validate.
    """
    err = _check_select_only(sql)
    if err:
        return json.dumps({"valid": False, "error": err})
    test_q = re.sub(r'(?i)\bLIMIT\s+\d+\s*$', '', sql.rstrip().rstrip(';'))
    test_q = f"SELECT * FROM ({test_q}) _t LIMIT 0"
    try:
        result = _run_sql(test_q, timeout="15s", max_rows=0)
        if result["success"]:
            return json.dumps({"valid": True})
        return json.dumps({"valid": False, "error": result["error"]})
    except Exception as e:
        return json.dumps({"valid": False, "error": str(e)})


@tool
def execute_query(sql: str) -> str:
    """Execute a read-only SQL query and return up to 500 rows as JSON.

    Only SELECT queries are allowed. No INSERT/UPDATE/DELETE/DROP.

    Args:
        sql: The SQL query to execute.
    """
    err = _check_select_only(sql)
    if err:
        return json.dumps({"error": err})
    try:
        result = _run_sql(sql)
        if result["success"]:
            return json.dumps({
                "columns": result["columns"],
                "rows": result["rows"],
                "row_count": result["row_count"],
            })
        return json.dumps({"error": result["error"]})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ===================================================================
# KB-blocking helpers for blind mode
# ===================================================================

_KB_TABLES = frozenset({
    "table_knowledge_base", "column_knowledge_base", "column_profiling_stats",
    "profiling_snapshots", "fk_predictions", "ontology_entities",
    "ontology_relationships", "ontology_column_properties", "ontology_metrics",
    "metric_view_definitions", "metadata_documents", "graph_edges",
})


def _check_kb_tables(sql: str) -> Optional[str]:
    """Return an error string if the SQL references any KB table, else None."""
    sql_lower = sql.lower()
    for t in _KB_TABLES:
        if t in sql_lower:
            return (
                f"Table '{t}' is internal metadata infrastructure. "
                "Only query the user's actual data tables."
            )
    return None


def _is_kb_table(table_name: str) -> bool:
    """Check if a (possibly fully-qualified) table name belongs to the KB schema."""
    short = table_name.split(".")[-1].lower()
    return short in _KB_TABLES


def _make_list_tables(block_kb: bool):
    @tool
    def list_tables(catalog: str, schema_name: str) -> str:
        """List all tables in a catalog.schema. Returns table names and types.

        Args:
            catalog: The catalog name.
            schema_name: The schema name.
        """
        if block_kb and catalog.lower() == CATALOG.lower() and schema_name.lower() == SCHEMA.lower():
            return json.dumps({
                "error": f"{catalog}.{schema_name} contains internal metadata tables. "
                "Only list tables in the user's data schemas."
            })
        try:
            result = _run_sql(f"SHOW TABLES IN {catalog}.{schema_name}", timeout="15s")
            if result["success"]:
                return json.dumps({"tables": result["rows"], "count": result["row_count"]})
            return json.dumps({"error": result["error"]})
        except Exception as e:
            return json.dumps({"error": str(e)})
    return list_tables


def _make_execute_query(block_kb: bool):
    @tool
    def execute_query(sql: str) -> str:
        """Execute a read-only SQL query and return up to 500 rows as JSON.

        Only SELECT queries are allowed. No INSERT/UPDATE/DELETE/DROP.

        Args:
            sql: The SQL query to execute.
        """
        err = _check_select_only(sql)
        if err:
            return json.dumps({"error": err})
        if block_kb:
            kb_err = _check_kb_tables(sql)
            if kb_err:
                return json.dumps({"error": kb_err})
        try:
            result = _run_sql(sql)
            if result["success"]:
                return json.dumps({
                    "columns": result["columns"],
                    "rows": result["rows"],
                    "row_count": result["row_count"],
                })
            return json.dumps({"error": result["error"]})
        except Exception as e:
            return json.dumps({"error": str(e)})
    return execute_query


# Unrestricted originals (used by enriched mode and other callers)
list_tables = _make_list_tables(block_kb=False)

_KB_BLOCK_MSG = "That table is internal metadata infrastructure. Only query the user's actual data tables."


def _make_describe_table(block_kb: bool):
    @tool
    def describe_table(table_name: str) -> str:
        """Get column names, data types, and any existing comments for a table via DESCRIBE TABLE.

        Args:
            table_name: Fully qualified table name (catalog.schema.table).
        """
        if block_kb and _is_kb_table(table_name):
            return json.dumps({"error": _KB_BLOCK_MSG})
        try:
            result = _run_sql(f"DESCRIBE TABLE {table_name}", timeout="15s")
            if result["success"]:
                return json.dumps({"columns": result["rows"], "count": result["row_count"]})
            return json.dumps({"error": result["error"]})
        except Exception as e:
            return json.dumps({"error": str(e)})
    return describe_table


def _make_sample_rows(block_kb: bool):
    @tool
    def sample_rows(table_name: str, n: int = 5) -> str:
        """Get a small sample of rows from a table to understand the data.

        Args:
            table_name: Fully qualified table name.
            n: Number of rows to sample (1-20).
        """
        if block_kb and _is_kb_table(table_name):
            return json.dumps({"error": _KB_BLOCK_MSG})
        n = min(max(n, 1), 20)
        try:
            result = _run_sql(f"SELECT * FROM {table_name} LIMIT {n}", timeout="30s", max_rows=n)
            if result["success"]:
                return json.dumps({"columns": result["columns"], "rows": result["rows"]})
            return json.dumps({"error": result["error"]})
        except Exception as e:
            return json.dumps({"error": str(e)})
    return sample_rows


describe_table = _make_describe_table(block_kb=False)
sample_rows = _make_sample_rows(block_kb=False)


# ===================================================================
# Blind-mode tools (schema introspection only)
# ===================================================================


@tool
def query_information_schema(catalog: str, schema_name: str, query_type: str = "columns", table_name: str = "") -> str:
    """Query INFORMATION_SCHEMA for detailed table/column metadata.

    This is the standard way to discover schema information without a semantic layer.
    Provide table_name to scope results to a single table (strongly recommended).
    Without table_name, returns at most 100 rows across all tables.

    Args:
        catalog: The catalog name.
        schema_name: The schema name.
        query_type: One of 'columns', 'tables', or 'constraints'.
        table_name: Optional specific table to query (recommended to avoid large result sets).
    """
    table_filter = f" AND table_name = '{table_name}'" if table_name else ""
    row_limit = 200 if table_name else 100
    valid_types = {
        "columns": (
            f"SELECT table_name, column_name, data_type, is_nullable, column_default, comment "
            f"FROM {catalog}.information_schema.columns "
            f"WHERE table_schema = '{schema_name}'{table_filter} ORDER BY table_name, ordinal_position"
        ),
        "tables": (
            f"SELECT table_name, table_type, comment "
            f"FROM {catalog}.information_schema.tables "
            f"WHERE table_schema = '{schema_name}'{table_filter} ORDER BY table_name"
        ),
        "constraints": (
            f"SELECT constraint_name, table_name, constraint_type "
            f"FROM {catalog}.information_schema.table_constraints "
            f"WHERE table_schema = '{schema_name}'{table_filter} ORDER BY table_name"
        ),
    }
    if query_type not in valid_types:
        return json.dumps({"error": f"query_type must be one of: {list(valid_types.keys())}"})
    try:
        result = _run_sql(valid_types[query_type], timeout="30s", max_rows=row_limit)
        if result["success"]:
            return json.dumps({"rows": result["rows"], "count": result["row_count"]})
        return json.dumps({"error": result["error"]})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ===================================================================
# Enriched-mode tools (semantic layer access)
# ===================================================================

@tool
def find_relevant_tables(
    question: str,
    include_columns: bool = False,
    domain: str = "",
    schema_name: str = "",
) -> str:
    """Find tables relevant to a natural language question using semantic search and ontology entities.

    Returns table names with their comments, domains, entity types, and relevance scores.
    For targeted questions about specific data, this is the best starting point.
    For broad catalog-wide questions, consider querying table_knowledge_base or
    column_knowledge_base directly with execute_query instead.

    Args:
        question: The user's question or topic to search for.
        include_columns: If true, also search column-level documents (default false).
        domain: Optional domain filter to narrow results (e.g. 'healthcare', 'finance').
        schema_name: Optional schema name filter to narrow results.
    """
    from agent.metadata_tools import _get_vs_index
    vs_index = f"{CATALOG}.{SCHEMA}.{VS_INDEX_SUFFIX}"
    try:
        index = _get_vs_index(vs_index)
        filters = {}
        if not include_columns:
            filters["doc_type"] = "table"
        if domain:
            filters["domain"] = domain
        if schema_name:
            filters["schema_name"] = schema_name
        vs_results = index.similarity_search(
            query_text=question, num_results=15,
            columns=["doc_id", "doc_type", "content", "table_name", "domain", "entity_type", "confidence_score"],
            filters=filters if filters else None,
            query_type="HYBRID",
        )
        tables = []
        cols = vs_results.get("manifest", {}).get("columns", [])
        col_names = [c.get("name", f"col{i}") for i, c in enumerate(cols)] if cols else []
        for row in vs_results.get("result", {}).get("data_array", []):
            tables.append(dict(zip(col_names, row)) if col_names else {"data": row})

        entity_rows = _run_sql(f"""
            SELECT entity_name, entity_type, description,
                   CONCAT_WS(', ', source_tables) AS source_tables
            FROM {_fq('ontology_entities')}
            WHERE description LIKE '%{question.split()[0] if question.split() else ""}%'
               OR entity_name LIKE '%{question.split()[0] if question.split() else ""}%'
            LIMIT 10
        """, timeout="15s")

        return json.dumps({
            "tables_from_search": tables,
            "related_entities": entity_rows.get("rows", []) if entity_rows.get("success") else [],
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_join_path(table_a: str, table_b: str) -> str:
    """Find the best join path between two tables using FK predictions and graph edges.

    Returns join columns, expressions, and confidence scores.

    Args:
        table_a: First table (fully qualified or short name).
        table_b: Second table (fully qualified or short name).
    """
    short_a = table_a.split(".")[-1]
    short_b = table_b.split(".")[-1]
    try:
        direct = _run_sql(f"""
            SELECT src_table, src_column, dst_table, dst_column,
                   ROUND(final_confidence, 3) AS confidence,
                   ai_reasoning
            FROM {_fq('fk_predictions')}
            WHERE (src_table LIKE '%{short_a}' AND dst_table LIKE '%{short_b}')
               OR (src_table LIKE '%{short_b}' AND dst_table LIKE '%{short_a}')
            ORDER BY final_confidence DESC LIMIT 5
        """, timeout="15s")

        from api_server import graph_query
        graph_paths = graph_query(f"""
            SELECT e.src, e.dst, e.relationship, e.join_expression,
                   ROUND(e.join_confidence, 3) AS join_confidence
            FROM public.graph_edges e
            WHERE e.relationship IN ('predicted_fk', 'references')
              AND ((e.src LIKE '%{short_a}%' AND e.dst LIKE '%{short_b}%')
                OR (e.src LIKE '%{short_b}%' AND e.dst LIKE '%{short_a}%'))
            LIMIT 5
        """)

        return json.dumps({
            "fk_predictions": direct.get("rows", []) if direct.get("success") else [],
            "graph_edges": graph_paths if isinstance(graph_paths, list) else [],
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_column_context(table_name: str) -> str:
    """Get rich context for all columns in a table: comments, ontology roles, profiling stats, and sensitivity.

    Args:
        table_name: Fully qualified or short table name.
    """
    short = table_name.split(".")[-1]
    try:
        cols = _run_sql(f"""
            SELECT c.column_name, c.data_type, c.comment, c.classification, c.classification_type,
                   p.property_role, p.owning_entity_type, p.linked_entity_type
            FROM {_fq('column_knowledge_base')} c
            LEFT JOIN {_fq('ontology_column_properties')} p
              ON c.table_name = p.table_name AND c.column_name = p.column_name
            WHERE c.table_name LIKE '%{short}'
        """, timeout="15s")

        profiling = _run_sql(f"""
            SELECT column_name, distinct_count, null_rate,
                   min_value, max_value, pattern_detected, sample_values
            FROM {_fq('column_profiling_stats')} cs
            {latest_profiling_join()}
            WHERE cs.table_name LIKE '%{short}'
        """, timeout="15s")

        table_info = _run_sql(f"""
            SELECT table_name, comment, domain, subdomain, has_pii, has_phi
            FROM {_fq('table_knowledge_base')}
            WHERE table_name LIKE '%{short}' LIMIT 1
        """, timeout="15s")

        return json.dumps({
            "table": table_info.get("rows", [{}])[0] if table_info.get("success") and table_info.get("rows") else {},
            "columns": cols.get("rows", []) if cols.get("success") else [],
            "profiling": profiling.get("rows", []) if profiling.get("success") else [],
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_metric_definitions(keywords: str) -> str:
    """Search metric view definitions for reusable aggregation SQL, measures, and dimensions.

    Args:
        keywords: Keywords to search metric views for (e.g. 'revenue', 'length of stay').
    """
    try:
        result = _run_sql(f"""
            SELECT metric_view_name, source_table, source_questions, json_definition, status
            FROM {_fq('metric_view_definitions')}
            WHERE (metric_view_name LIKE '%{keywords.replace("'", "")}%'
                OR source_questions LIKE '%{keywords.replace("'", "")}%'
                OR json_definition LIKE '%{keywords.replace("'", "")}%')
              AND status IN ('validated', 'applied')
            LIMIT 5
        """, timeout="15s")
        if result["success"]:
            return json.dumps({"metric_views": result["rows"], "count": result["row_count"]})
        return json.dumps({"error": result["error"]})
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_value_distribution(table_name: str, column_name: str) -> str:
    """Get profiling-based value distribution info for a column: distinct count, null rate, pattern, top values.

    Args:
        table_name: Fully qualified or short table name.
        column_name: Column name.
    """
    short = table_name.split(".")[-1]
    try:
        result = _run_sql(f"""
            SELECT cs.distinct_count, cs.null_rate, cs.min_value, cs.max_value,
                   cs.pattern_detected, cs.sample_values, cs.value_distribution,
                   cs.mean_value, cs.mode_value
            FROM {_fq('column_profiling_stats')} cs
            {latest_profiling_join()}
            WHERE cs.table_name LIKE '%{short}' AND cs.column_name = '{column_name}'
            LIMIT 1
        """, timeout="15s")
        if result["success"] and result["rows"]:
            return json.dumps(result["rows"][0])
        return json.dumps({"message": "No profiling data found for this column."})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ===================================================================
# Tool collections
# ===================================================================

SHARED_TOOLS = [test_sql]

BLIND_TOOLS = [
    _make_list_tables(block_kb=True),
    _make_describe_table(block_kb=True),
    _make_sample_rows(block_kb=True),
    query_information_schema, test_sql,
    _make_execute_query(block_kb=True),
]

ENRICHED_TOOLS = [
    list_tables, describe_table, sample_rows, query_information_schema,
    test_sql, execute_query,
    find_relevant_tables, get_join_path, get_column_context,
    get_metric_definitions, get_value_distribution,
]
