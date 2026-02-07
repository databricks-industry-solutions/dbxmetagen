"""Graph traversal tools for the GraphRAG agent.

Provides both a GraphQL client (for Lakebase) and a SQL fallback
that queries graph_nodes / graph_edges via the Statement Execution API.
"""

import os
import logging
from typing import Optional

import requests
from langchain_core.tools import tool

logger = logging.getLogger(__name__)

CATALOG = os.environ.get("CATALOG_NAME", "")
SCHEMA = os.environ.get("SCHEMA_NAME", "metadata_results")
LAKEBASE_URL = os.environ.get("LAKEBASE_GRAPHQL_URL", "")  # e.g. https://<host>/api/graphql


def _fq(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


# ---------------------------------------------------------------------------
# SQL fallback helpers (used when Lakebase is not configured)
# ---------------------------------------------------------------------------

def _sql_query(query: str) -> list[dict]:
    """Execute SQL via the Statement Execution API."""
    from api_server import execute_sql
    return execute_sql(query)


# ---------------------------------------------------------------------------
# GraphQL client (for Lakebase)
# ---------------------------------------------------------------------------

def _graphql_query(query: str, variables: Optional[dict] = None) -> dict:
    """Execute a GraphQL query against Lakebase Postgres GraphQL endpoint."""
    if not LAKEBASE_URL:
        raise RuntimeError("LAKEBASE_GRAPHQL_URL not configured")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.post(LAKEBASE_URL, json={"query": query, "variables": variables or {}}, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"GraphQL error: {data['errors']}")
    return data.get("data", {})


def _use_lakebase() -> bool:
    return bool(LAKEBASE_URL)


# ---------------------------------------------------------------------------
# Agent tools
# ---------------------------------------------------------------------------

@tool
def query_graph_nodes(node_type: Optional[str] = None, domain: Optional[str] = None, search_term: Optional[str] = None, limit: int = 20) -> list[dict]:
    """Search graph nodes by type, domain, or keyword in the comment.

    Args:
        node_type: Filter by node type (table, column, schema, entity).
        domain: Filter by domain name.
        search_term: Keyword to search in node id or comment.
        limit: Max results.
    """
    if _use_lakebase():
        # GraphQL path
        filters = []
        if node_type:
            filters.append(f'node_type: {{eq: "{node_type}"}}')
        if domain:
            filters.append(f'domain: {{eq: "{domain}"}}')
        if search_term:
            filters.append(f'comment: {{contains: "{search_term}"}}')
        filter_clause = ", ".join(filters)
        gql = f"""
        query {{
            graph_nodes(filter: {{{filter_clause}}}, first: {limit}) {{
                edges {{ node {{ id, node_type, domain, security_level, comment }} }}
            }}
        }}
        """
        data = _graphql_query(gql)
        return [e["node"] for e in data.get("graph_nodes", {}).get("edges", [])]
    else:
        # SQL fallback
        conditions = []
        if node_type:
            conditions.append(f"node_type = '{node_type}'")
        if domain:
            conditions.append(f"domain = '{domain}'")
        if search_term:
            conditions.append(f"(id LIKE '%{search_term}%' OR comment LIKE '%{search_term}%')")
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        q = f"SELECT id, node_type, domain, security_level, comment FROM {_fq('graph_nodes')} {where} LIMIT {limit}"
        return _sql_query(q)


@tool
def traverse_edges(node_id: str, relationship: Optional[str] = None, direction: str = "outgoing", limit: int = 20) -> list[dict]:
    """Traverse edges from a node. Returns connected nodes with relationship info.

    Args:
        node_id: The source node id to traverse from.
        relationship: Optional filter on edge relationship type.
        direction: 'outgoing' (src=node_id) or 'incoming' (dst=node_id).
        limit: Max results.
    """
    if _use_lakebase():
        dir_filter = f'src: {{eq: "{node_id}"}}' if direction == "outgoing" else f'dst: {{eq: "{node_id}"}}'
        rel_filter = f', relationship: {{eq: "{relationship}"}}' if relationship else ""
        gql = f"""
        query {{
            graph_edges(filter: {{{dir_filter}{rel_filter}}}, first: {limit}) {{
                edges {{ node {{ src, dst, relationship, weight }} }}
            }}
        }}
        """
        data = _graphql_query(gql)
        return [e["node"] for e in data.get("graph_edges", {}).get("edges", [])]
    else:
        col = "src" if direction == "outgoing" else "dst"
        conditions = [f"{col} = '{node_id}'"]
        if relationship:
            conditions.append(f"relationship = '{relationship}'")
        where = "WHERE " + " AND ".join(conditions)
        q = f"SELECT src, dst, relationship, weight FROM {_fq('graph_edges')} {where} ORDER BY weight DESC LIMIT {limit}"
        return _sql_query(q)


@tool
def get_node_details(node_id: str) -> dict:
    """Get full details for a specific node including all properties.

    Args:
        node_id: The node id to look up.
    """
    if _use_lakebase():
        gql = f"""
        query {{
            graph_nodes(filter: {{id: {{eq: "{node_id}"}}}}, first: 1) {{
                edges {{ node {{ id, node_type, domain, security_level, comment, quality_score, completeness_score, data_quality_score }} }}
            }}
        }}
        """
        data = _graphql_query(gql)
        edges = data.get("graph_nodes", {}).get("edges", [])
        return edges[0]["node"] if edges else {}
    else:
        q = f"SELECT * FROM {_fq('graph_nodes')} WHERE id = '{node_id}'"
        rows = _sql_query(q)
        return rows[0] if rows else {}


@tool
def find_similar_nodes(node_id: str, min_similarity: float = 0.8, limit: int = 10) -> list[dict]:
    """Find nodes similar to the given node based on embedding similarity.

    Args:
        node_id: The reference node id.
        min_similarity: Minimum similarity score (0-1).
        limit: Max results.
    """
    q = f"""
        SELECT e.dst as similar_node, e.weight as similarity, n.node_type, n.domain, n.comment
        FROM {_fq('graph_edges')} e
        JOIN {_fq('graph_nodes')} n ON e.dst = n.id
        WHERE e.src = '{node_id}' AND e.relationship = 'similar_embedding' AND e.weight >= {min_similarity}
        ORDER BY e.weight DESC LIMIT {limit}
    """
    return _sql_query(q)


ALL_TOOLS = [query_graph_nodes, traverse_edges, get_node_details, find_similar_nodes]
