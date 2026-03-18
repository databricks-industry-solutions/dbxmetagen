"""Graph traversal tools for the metadata intelligence agents.

Queries graph_nodes / graph_edges via graph_query() which tries Lakebase PG
first, then falls back to UC Delta tables.
"""

import logging
from typing import Optional

from langchain_core.tools import tool

logger = logging.getLogger(__name__)


def _gq(query: str) -> list[dict]:
    """Execute a graph query with automatic Lakebase PG -> UC Delta fallback."""
    from api_server import graph_query
    return graph_query(query)


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
    q = (
        f"SELECT id, node_type, domain, security_level, display_name, "
        f"short_description, sensitivity, status, ontology_id, ontology_type "
        f"FROM public.graph_nodes {where} LIMIT {limit}"
    )
    return _gq(q)


@tool
def get_node_details(node_id: str) -> dict:
    """Get full details for a specific node including all properties.

    Args:
        node_id: The node id to look up.
    """
    q = f"SELECT * FROM public.graph_nodes WHERE id = '{node_id}'"
    rows = _gq(q)
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
        SELECT e.dst as similar_node, e.weight as similarity,
               e.edge_type, e.source_system,
               n.node_type, n.domain, n.display_name, n.short_description
        FROM public.graph_edges e
        JOIN public.graph_nodes n ON e.dst = n.id
        WHERE e.src = '{node_id}'
          AND (e.edge_type = 'similar_to' OR e.relationship = 'similar_embedding')
          AND e.weight >= {min_similarity}
        ORDER BY e.weight DESC LIMIT {limit}
    """
    return _gq(q)


@tool
def traverse_graph(
    start_node: str,
    max_hops: int = 3,
    relationship: Optional[str] = None,
    edge_type: Optional[str] = None,
    direction: str = "outgoing",
) -> dict:
    """Multi-hop graph traversal from a starting node.

    Walks the graph iteratively, returning all discovered nodes, edges, and
    the paths taken. Use this after identifying a starting node with
    query_graph_nodes to explore the surrounding graph context.

    Args:
        start_node: Node id to start traversal from.
        max_hops: Maximum number of hops (1-5).
        relationship: Optional edge relationship filter (e.g. 'contains', 'similar_embedding').
        edge_type: Optional edge_type filter (contains, references, instance_of, similar_to, etc.).
        direction: 'outgoing' (follow src->dst), 'incoming' (follow dst->src), or 'both'.
    """
    from api_server import multi_hop_traverse
    return multi_hop_traverse(
        start_node=start_node,
        max_hops=min(max_hops, 5),
        relationship=relationship,
        edge_type=edge_type,
        direction=direction,
    )


ALL_TOOLS = [query_graph_nodes, get_node_details, find_similar_nodes, traverse_graph]
