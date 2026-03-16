"""Tools for the impact analysis agent.

Provides dependency tracing, metric view impact assessment, entity-level impact,
column importance scoring, and impact-specific SQL on metadata tables.
"""

import json
import logging

from langchain_core.tools import tool

from agent.common import (
    CATALOG, SCHEMA,
    fq as _fq, check_select_only, check_table_allowlist, latest_profiling_join,
)

logger = logging.getLogger(__name__)


def _run_sql(query: str) -> dict:
    from agent.metadata_tools import _execute_query
    return _execute_query(query)


def _graph_query(query: str):
    from api_server import graph_query
    return graph_query(query)


@tool
def get_direct_dependencies(target: str) -> str:
    """Get direct FK, lineage, and graph dependencies for a table or column.

    Returns immediate upstream/downstream connections via FK predictions,
    extended metadata lineage, and graph edges.

    Args:
        target: Table name (fully qualified or short) or column ID (table.column).
    """
    short = target.split(".")[-1]
    is_column = "." in target and len(target.split(".")) >= 4
    try:
        fk_result = _run_sql(f"""
            SELECT src_table, src_column, dst_table, dst_column,
                   ROUND(final_confidence, 3) AS confidence
            FROM {_fq('fk_predictions')}
            WHERE (src_table LIKE '%{short.split('.')[0] if is_column else short}%'
                OR dst_table LIKE '%{short.split('.')[0] if is_column else short}%')
              AND final_confidence >= 0.5
            ORDER BY final_confidence DESC LIMIT 20
        """)

        lineage_result = _run_sql(f"""
            SELECT table_name, upstream_tables, downstream_tables
            FROM {_fq('extended_table_metadata')}
            WHERE table_name LIKE '%{short.split('.')[0] if is_column else short}%'
            LIMIT 5
        """)

        graph_edges = _graph_query(f"""
            SELECT e.src, e.dst, e.relationship, e.edge_type,
                   e.join_expression, ROUND(e.weight, 3) AS weight
            FROM public.graph_edges e
            WHERE (e.src LIKE '%{short}%' OR e.dst LIKE '%{short}%')
              AND e.relationship IN ('predicted_fk', 'references', 'derives_from', 'contains')
            LIMIT 20
        """)

        return json.dumps({
            "fk_predictions": fk_result.get("rows", []) if fk_result.get("success") else [],
            "lineage": lineage_result.get("rows", []) if lineage_result.get("success") else [],
            "graph_edges": graph_edges if isinstance(graph_edges, list) else [],
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def find_affected_metric_views(table_names: str) -> str:
    """Find metric view definitions that reference any of the given tables.

    Args:
        table_names: Comma-separated table names (short names ok).
    """
    tables = [t.strip().split(".")[-1] for t in table_names.split(",")]
    conditions = " OR ".join(f"json_definition LIKE '%{t}%'" for t in tables if t)
    if not conditions:
        return json.dumps({"metric_views": [], "count": 0})
    try:
        result = _run_sql(f"""
            SELECT metric_view_name, source_table, source_questions, status, json_definition
            FROM {_fq('metric_view_definitions')}
            WHERE ({conditions})
            LIMIT 20
        """)
        if result["success"]:
            return json.dumps({"metric_views": result["rows"], "count": result["row_count"]})
        return json.dumps({"error": result["error"]})
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_entity_impact(target: str) -> str:
    """Assess ontology-level impact: find which entities and relationships are affected.

    Args:
        target: Table name, column name, or entity type.
    """
    short = target.split(".")[-1]
    try:
        props = _run_sql(f"""
            SELECT p.table_name, p.column_name, p.property_role,
                   p.owning_entity_type, p.linked_entity_type, p.confidence
            FROM {_fq('ontology_column_properties')} p
            WHERE p.table_name LIKE '%{short}%'
               OR p.column_name LIKE '%{short}%'
               OR p.owning_entity_type LIKE '%{short}%'
            LIMIT 30
        """)

        entity_types = set()
        for r in props.get("rows", []) if props.get("success") else []:
            if r.get("owning_entity_type"):
                entity_types.add(r["owning_entity_type"])
            if r.get("linked_entity_type"):
                entity_types.add(r["linked_entity_type"])

        rels = {"rows": [], "success": True}
        if entity_types:
            et_list = ", ".join(f"'{et}'" for et in entity_types)
            rels = _run_sql(f"""
                SELECT relationship_id, src_entity_type, relationship_name, dst_entity_type,
                       cardinality, confidence
                FROM {_fq('ontology_relationships')}
                WHERE src_entity_type IN ({et_list}) OR dst_entity_type IN ({et_list})
                LIMIT 30
            """)

        return json.dumps({
            "column_properties": props.get("rows", []) if props.get("success") else [],
            "affected_entity_types": list(entity_types),
            "relationships": rels.get("rows", []) if rels.get("success") else [],
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def assess_column_importance(column_id: str) -> str:
    """Assess the importance/severity of changing a column using profiling, sensitivity, and ontology.

    Args:
        column_id: Column identifier (table_name.column_name or just column_name).
    """
    parts = column_id.rsplit(".", 1)
    col_name = parts[-1]
    table_pattern = parts[0].split(".")[-1] if len(parts) > 1 else "%"
    try:
        kb = _run_sql(f"""
            SELECT column_name, comment, classification, classification_type, data_type
            FROM {_fq('column_knowledge_base')}
            WHERE table_name LIKE '%{table_pattern}%' AND column_name = '{col_name}'
            LIMIT 1
        """)

        profiling = _run_sql(f"""
            SELECT cs.distinct_count, cs.null_rate, cs.is_unique_candidate,
                   cs.pattern_detected, cs.cardinality_ratio
            FROM {_fq('column_profiling_stats')} cs
            {latest_profiling_join()}
            WHERE cs.table_name LIKE '%{table_pattern}%' AND cs.column_name = '{col_name}'
            LIMIT 1
        """)

        ontology = _run_sql(f"""
            SELECT property_role, owning_entity_type, linked_entity_type
            FROM {_fq('ontology_column_properties')}
            WHERE table_name LIKE '%{table_pattern}%' AND column_name = '{col_name}'
            LIMIT 1
        """)

        severity = "LOW"
        reasons = []
        kb_row = (kb.get("rows", []) or [{}])[0] if kb.get("success") else {}
        prof_row = (profiling.get("rows", []) or [{}])[0] if profiling.get("success") else {}
        ont_row = (ontology.get("rows", []) or [{}])[0] if ontology.get("success") else {}

        if kb_row.get("classification_type") in ("pii", "phi", "pci"):
            severity = "HIGH"
            reasons.append(f"Classified as {kb_row['classification_type'].upper()}")
        if ont_row.get("property_role") == "identifier":
            severity = "HIGH"
            reasons.append(f"Ontology role: identifier for {ont_row.get('owning_entity_type', '?')}")
        if prof_row.get("is_unique_candidate") == "true":
            severity = max(severity, "MEDIUM")
            reasons.append("Unique candidate (likely primary/foreign key)")
        if ont_row.get("linked_entity_type"):
            severity = max(severity, "MEDIUM")
            reasons.append(f"Links to entity: {ont_row['linked_entity_type']}")

        return json.dumps({
            "column": col_name, "severity": severity, "reasons": reasons,
            "knowledge_base": kb_row, "profiling": prof_row, "ontology": ont_row,
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def find_similar_columns(column_id: str) -> str:
    """Find columns similar to the target that might need the same change.

    Uses graph similarity edges to identify related columns.

    Args:
        column_id: Node ID or column identifier.
    """
    try:
        results = _graph_query(f"""
            SELECT e.src, e.dst, ROUND(e.weight, 3) AS similarity,
                   n.display_name, n.node_type, n.domain
            FROM public.graph_edges e
            JOIN public.graph_nodes n ON n.id = CASE WHEN e.src LIKE '%{column_id.split(".")[-1]}%' THEN e.dst ELSE e.src END
            WHERE e.relationship = 'similar_embedding'
              AND (e.src LIKE '%{column_id.split(".")[-1]}%' OR e.dst LIKE '%{column_id.split(".")[-1]}%')
            ORDER BY e.weight DESC LIMIT 10
        """)
        return json.dumps({"similar": results if isinstance(results, list) else [], "count": len(results) if isinstance(results, list) else 0})
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def execute_impact_sql(query: str) -> str:
    """Execute read-only SQL against impact-relevant metadata tables.

    Allowed: graph_nodes, graph_edges, fk_predictions, extended_table_metadata,
    metric_view_definitions, ontology_entities, ontology_relationships,
    ontology_column_properties, column_knowledge_base, table_knowledge_base,
    column_profiling_stats, profiling_snapshots.

    Args:
        query: SELECT query only.
    """
    allowed = {
        "fk_predictions", "extended_table_metadata", "metric_view_definitions",
        "ontology_entities", "ontology_relationships", "ontology_column_properties",
        "column_knowledge_base", "table_knowledge_base",
        "column_profiling_stats", "profiling_snapshots",
    }
    err = check_select_only(query)
    if err:
        return json.dumps({"error": err})
    err = check_table_allowlist(query, allowed)
    if err:
        return json.dumps({"error": err})
    from agent.metadata_tools import _auto_qualify
    query = _auto_qualify(query, allowed)
    try:
        result = _run_sql(query)
        if result["success"]:
            return json.dumps({"columns": result["columns"], "rows": result["rows"][:200], "row_count": result["row_count"]})
        return json.dumps({"error": result["error"]})
    except Exception as e:
        return json.dumps({"error": str(e)})


from agent.tools import query_graph_nodes, traverse_graph, get_node_details, find_similar_nodes  # noqa: E402

IMPACT_TOOLS = [
    get_direct_dependencies, find_affected_metric_views,
    get_entity_impact, assess_column_importance,
    find_similar_columns, execute_impact_sql,
    query_graph_nodes, traverse_graph, get_node_details, find_similar_nodes,
]
