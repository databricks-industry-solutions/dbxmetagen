"""Graph traversal tools for the GraphRAG agent.

Queries graph_nodes / graph_edges in Lakebase via direct PostgreSQL connection
(SQLAlchemy engine configured in db.py).
"""

import logging
from typing import Optional

from langchain_core.tools import tool

logger = logging.getLogger(__name__)


def _pg(query: str) -> list[dict]:
    from db import pg_execute
    return pg_execute(query)


# ---------------------------------------------------------------------------
# Agent tools
# ---------------------------------------------------------------------------

@tool
def query_graph_nodes(
    node_type: Optional[str] = None,
    domain: Optional[str] = None,
    search_term: Optional[str] = None,
    limit: int = 20,
) -> list[dict]:
    """Search graph nodes by type, domain, or keyword in the comment.

    Args:
        node_type: Filter by node type (table, column, schema, entity).
        domain: Filter by domain name.
        search_term: Keyword to search in node id or comment.
        limit: Max results.
    """
    conditions = []
    if node_type:
        conditions.append(f"node_type = '{node_type}'")
    if domain:
        conditions.append(f"domain = '{domain}'")
    if search_term:
        conditions.append(f"(id LIKE '%{search_term}%' OR comment LIKE '%{search_term}%')")
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    q = f"SELECT id, node_type, domain, security_level, comment FROM public.graph_nodes {where} LIMIT {limit}"
    return _pg(q)


@tool
def get_node_details(node_id: str) -> dict:
    """Get full details for a specific node including all properties.

    Args:
        node_id: The node id to look up.
    """
    q = f"SELECT * FROM public.graph_nodes WHERE id = '{node_id}'"
    rows = _pg(q)
    return rows[0] if rows else {}


@tool
def find_similar_nodes(node_id: str, min_similarity: float = 0.8, limit: int = 10) -> list[dict]:
    """Find nodes similar to the given node based on embedding similarity edges.

    Args:
        node_id: The reference node id.
        min_similarity: Minimum similarity score (0-1).
        limit: Max results.
    """
    q = f"""
        SELECT e.dst as similar_node, e.weight as similarity, n.node_type, n.domain, n.comment
        FROM public.graph_edges e
        JOIN public.graph_nodes n ON e.dst = n.id
        WHERE e.src = '{node_id}' AND e.relationship = 'similar_embedding' AND e.weight >= {min_similarity}
        ORDER BY e.weight DESC LIMIT {limit}
    """
    return _pg(q)


@tool
def traverse_graph(
    start_node: str,
    max_hops: int = 3,
    relationship: Optional[str] = None,
    direction: str = "outgoing",
) -> dict:
    """Multi-hop graph traversal from a starting node.

    Walks the graph iteratively, returning all discovered nodes, edges, and
    the paths taken. Use this after identifying a starting node with
    query_graph_nodes to explore the surrounding graph context.

    Args:
        start_node: Node id to start traversal from.
        max_hops: Maximum number of hops (1-5).
        relationship: Optional edge relationship filter (e.g. 'has_column', 'similar_embedding').
        direction: 'outgoing' (follow src->dst), 'incoming' (follow dst->src), or 'both'.
    """
    from api_server import multi_hop_traverse
    return multi_hop_traverse(
        start_node=start_node,
        max_hops=min(max_hops, 5),
        relationship=relationship,
        direction=direction,
    )


ALL_TOOLS = [query_graph_nodes, get_node_details, find_similar_nodes, traverse_graph]
