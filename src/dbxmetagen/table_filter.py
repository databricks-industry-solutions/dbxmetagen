"""Shared utilities for table-name filtering across analytics pipeline modules."""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Infrastructure table exclusion
# ---------------------------------------------------------------------------

INFRASTRUCTURE_TABLE_NAMES: frozenset[str] = frozenset({
    "metadata_generation_log",
    "table_knowledge_base",
    "column_knowledge_base",
    "schema_knowledge_base",
    "extended_table_metadata",
    "graph_nodes",
    "graph_edges",
    "ontology_entities",
    "ontology_metrics",
    "ontology_column_properties",
    "ontology_relationships",
    "discovery_diff_report",
    "entity_tag_audit_log",
    "fk_predictions",
    "profiling_snapshots",
    "column_profiling_stats",
    "data_quality_scores",
    "semantic_layer_questions",
    "metric_view_definitions",
    "llm_payloads",
    "metadata_documents",
    "dbxmetagen_token_usage",
})

_INFRASTRUCTURE_DYNAMIC_RE = re.compile(
    r"^(metadata_control_|temp_metadata_generation_log_)"
)


def is_infrastructure_table(table_name: str) -> bool:
    """Return True if *table_name* is a dbxmetagen infrastructure table.

    Works with both bare names (``graph_nodes``) and fully-qualified names
    (``catalog.schema.graph_nodes``).
    """
    bare = table_name.rsplit(".", 1)[-1]
    return bare in INFRASTRUCTURE_TABLE_NAMES or bool(
        _INFRASTRUCTURE_DYNAMIC_RE.match(bare)
    )


def infrastructure_exclude_sql(column: str = "table_name") -> str:
    """Return a SQL ``AND NOT ...`` clause that excludes infrastructure tables.

    The clause filters on the **bare** (unqualified) table name extracted from
    *column* via ``substring_index(..., '.', -1)``.
    """
    quoted = ", ".join(f"'{n}'" for n in sorted(INFRASTRUCTURE_TABLE_NAMES))
    bare_expr = f"substring_index({column}, '.', -1)"
    return (
        f"AND {bare_expr} NOT IN ({quoted}) "
        f"AND NOT {bare_expr} RLIKE '^(metadata_control_|temp_metadata_generation_log_)'"
    )


def parse_table_names(raw: str) -> list[str]:
    """Parse comma-separated table names, strip whitespace, drop empties."""
    if not raw or not raw.strip():
        return []
    return [t.strip() for t in raw.split(",") if t.strip()]


def table_names_col_filter(col, table_names: list[str]):
    """PySpark Column filter matching table_names (exact or ``catalog.schema.*`` wildcard).

    Returns a Column boolean expression that is True when *col* matches any
    entry in *table_names*.  Useful when SQL string manipulation is awkward
    (e.g. OR-ing across two different columns).
    """
    from functools import reduce

    from pyspark.sql import functions as F

    exact = [t for t in table_names if not t.endswith(".*")]
    wildcards = [t[:-2] + "." for t in table_names if t.endswith(".*")]
    parts = []
    if exact:
        parts.append(col.isin(exact))
    for pfx in wildcards:
        parts.append(col.startswith(pfx))
    return reduce(lambda a, b: a | b, parts) if parts else F.lit(False)


def table_filter_sql(table_names: list[str], column: str = "table_name") -> str:
    """Return a SQL ``AND ...`` clause filtering *column* to the given names.

    Supports schema wildcards: entries ending in ``.*`` produce ``LIKE``
    conditions so that ``catalog.schema.*`` matches all tables in that schema.
    Literal names use an ``IN (...)`` clause.  Returns ``""`` when the list is
    empty.
    """
    if not table_names:
        return ""
    literals: list[str] = []
    like_patterns: list[str] = []
    for t in table_names:
        escaped = t.replace("\\", "\\\\").replace("'", "''")
        if escaped.endswith(".*"):
            prefix = escaped[:-2]
            like_patterns.append(f"{column} LIKE '{prefix}.%'")
        else:
            literals.append(f"'{escaped}'")
    parts: list[str] = []
    if literals:
        parts.append(f"{column} IN ({', '.join(literals)})")
    parts.extend(like_patterns)
    if not parts:
        return ""
    return f"AND ({' OR '.join(parts)})"
