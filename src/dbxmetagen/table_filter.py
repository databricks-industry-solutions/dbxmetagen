"""Shared utilities for table-name filtering across analytics pipeline modules."""

from __future__ import annotations


def parse_table_names(raw: str) -> list[str]:
    """Parse comma-separated table names, strip whitespace, drop empties."""
    if not raw or not raw.strip():
        return []
    return [t.strip() for t in raw.split(",") if t.strip()]


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
