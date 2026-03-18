"""Tools for the governance & compliance agent.

Provides sensitivity summary, classification gap detection, lineage tracing
for sensitive data, masking coverage audit, and re-identification risk paths.
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


GOVERNANCE_TABLES = {
    "column_knowledge_base", "table_knowledge_base", "extended_table_metadata",
    "column_profiling_stats", "profiling_snapshots", "ontology_column_properties",
    "ontology_entities", "fk_predictions",
}


@tool
def get_sensitivity_summary(scope: str = "") -> str:
    """Get aggregated PII/PHI/PCI column counts grouped by schema and classification type.

    Args:
        scope: Optional filter -- catalog, schema, or table name pattern. Empty for all.
    """
    where = f"AND (c.catalog LIKE '%{scope}%' OR c.schema LIKE '%{scope}%' OR c.table_name LIKE '%{scope}%')" if scope else ""
    try:
        result = _run_sql(f"""
            SELECT c.catalog, c.schema, c.classification_type,
                   COUNT(*) AS classified_columns,
                   COUNT(DISTINCT c.table_name) AS affected_tables
            FROM {_fq('column_knowledge_base')} c
            WHERE c.classification_type IS NOT NULL AND c.classification_type != 'none' {where}
            GROUP BY c.catalog, c.schema, c.classification_type
            ORDER BY c.catalog, c.schema, classified_columns DESC
        """)
        if result["success"]:
            return json.dumps({"summary": result["rows"], "count": result["row_count"]})
        return json.dumps({"error": result["error"]})
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def find_classification_gaps() -> str:
    """Find columns where profiling patterns suggest sensitive data but no classification exists.

    Detects potential PII/PHI gaps by cross-referencing column_profiling_stats.pattern_detected
    with column_knowledge_base.classification.
    """
    try:
        result = _run_sql(f"""
            SELECT cs.table_name, cs.column_name, cs.pattern_detected,
                   cs.distinct_count, cs.null_rate, cs.sample_values,
                   ck.classification, ck.classification_type
            FROM {_fq('column_profiling_stats')} cs
            {latest_profiling_join()}
            LEFT JOIN {_fq('column_knowledge_base')} ck
              ON cs.table_name = ck.table_name AND cs.column_name = ck.column_name
            WHERE cs.pattern_detected IN ('email', 'phone', 'ssn', 'uuid', 'ip_address', 'credit_card', 'date', 'numeric_id')
              AND (ck.classification IS NULL OR ck.classification = 'none' OR ck.classification = '')
            ORDER BY cs.pattern_detected, cs.table_name
            LIMIT 100
        """)
        if result["success"]:
            return json.dumps({"gaps": result["rows"], "count": result["row_count"]})
        return json.dumps({"error": result["error"]})
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def trace_sensitive_lineage(table_name: str) -> str:
    """Trace upstream and downstream lineage for a table that contains classified columns.

    Shows where sensitive data comes from and where it flows to.

    Args:
        table_name: Fully qualified or short table name.
    """
    short = table_name.split(".")[-1]
    try:
        result = _run_sql(f"""
            SELECT ck.table_name, ck.column_name, ck.classification_type,
                   em.upstream_tables, em.downstream_tables
            FROM {_fq('column_knowledge_base')} ck
            INNER JOIN {_fq('extended_table_metadata')} em ON ck.table_name = em.table_name
            WHERE ck.classification_type IN ('pii', 'phi', 'pci')
              AND ck.table_name LIKE '%{short}%'
        """)
        if result["success"]:
            return json.dumps({"lineage": result["rows"], "count": result["row_count"]})
        return json.dumps({"error": result["error"]})
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def check_masking_coverage() -> str:
    """Find classified PII/PHI/PCI columns that lack column mask policies.

    Cross-references column_knowledge_base classifications with
    extended_table_metadata.column_mask_policies.
    """
    try:
        result = _run_sql(f"""
            SELECT ck.table_name, ck.column_name, ck.classification, ck.classification_type,
                   em.column_mask_policies
            FROM {_fq('column_knowledge_base')} ck
            LEFT JOIN {_fq('extended_table_metadata')} em ON ck.table_name = em.table_name
            WHERE ck.classification_type IN ('pii', 'phi', 'pci')
              AND (em.column_mask_policies IS NULL
                   OR NOT ARRAY_CONTAINS(MAP_KEYS(em.column_mask_policies), ck.column_name))
            ORDER BY ck.classification_type DESC, ck.table_name
            LIMIT 100
        """)
        if result["success"]:
            return json.dumps({"unmasked": result["rows"], "count": result["row_count"]})
        return json.dumps({"error": result["error"]})
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def find_reidentification_paths(entity_type: str = "Patient") -> str:
    """Find FK join paths that could enable re-identification of sensitive entities.

    Identifies tables with PII identifier columns that FK-join to other tables,
    potentially enabling cross-table re-identification.

    Args:
        entity_type: Ontology entity type to analyze (e.g. 'Patient', 'Person').
    """
    try:
        result = _run_sql(f"""
            SELECT fk.src_table, fk.src_column, fk.dst_table, fk.dst_column,
                   ROUND(fk.final_confidence, 3) AS fk_confidence,
                   ck_src.classification_type AS src_sensitivity,
                   ck_dst.classification_type AS dst_sensitivity,
                   ocp.property_role AS src_role
            FROM {_fq('fk_predictions')} fk
            LEFT JOIN {_fq('column_knowledge_base')} ck_src
              ON fk.src_table = ck_src.table_name AND fk.src_column = ck_src.column_name
            LEFT JOIN {_fq('column_knowledge_base')} ck_dst
              ON fk.dst_table = ck_dst.table_name AND fk.dst_column = ck_dst.column_name
            LEFT JOIN {_fq('ontology_column_properties')} ocp
              ON fk.src_table = ocp.table_name AND fk.src_column = ocp.column_name
            WHERE fk.final_confidence >= 0.5
              AND (ck_src.classification_type IN ('pii', 'phi') OR ck_dst.classification_type IN ('pii', 'phi'))
              AND (ocp.owning_entity_type = '{entity_type}' OR ocp.owning_entity_type IS NULL)
            ORDER BY fk.final_confidence DESC
            LIMIT 50
        """)
        if result["success"]:
            return json.dumps({"risk_paths": result["rows"], "count": result["row_count"]})
        return json.dumps({"error": result["error"]})
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def execute_governance_sql(query: str) -> str:
    """Execute read-only SQL against governance-relevant metadata tables.

    Allowed tables: column_knowledge_base, table_knowledge_base, extended_table_metadata,
    column_profiling_stats, profiling_snapshots, ontology_column_properties,
    ontology_entities, fk_predictions.

    Args:
        query: SELECT query only.
    """
    err = check_select_only(query)
    if err:
        return json.dumps({"error": err})
    err = check_table_allowlist(query, GOVERNANCE_TABLES)
    if err:
        return json.dumps({"error": err})
    from agent.metadata_tools import _auto_qualify
    query = _auto_qualify(query, GOVERNANCE_TABLES)
    try:
        result = _run_sql(query)
        if result["success"]:
            return json.dumps({"columns": result["columns"], "rows": result["rows"][:200], "row_count": result["row_count"]})
        return json.dumps({"error": result["error"]})
    except Exception as e:
        return json.dumps({"error": str(e)})


GOVERNANCE_TOOLS = [
    get_sensitivity_summary, find_classification_gaps,
    trace_sensitive_lineage, check_masking_coverage,
    find_reidentification_paths, execute_governance_sql,
]
