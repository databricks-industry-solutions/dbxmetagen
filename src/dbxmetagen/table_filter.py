"""Shared utilities for table-name filtering across analytics pipeline modules."""

from __future__ import annotations


def parse_table_names(raw: str) -> list[str]:
    """Parse comma-separated table names, strip whitespace, drop empties."""
    if not raw or not raw.strip():
        return []
    return [t.strip() for t in raw.split(",") if t.strip()]


def table_filter_sql(table_names: list[str], column: str = "table_name") -> str:
    """Return a SQL ``AND column IN (...)`` clause, or ``""`` if *table_names* is empty."""
    if not table_names:
        return ""
    escaped = [t.replace("\\", "\\\\").replace("'", "''") for t in table_names]
    in_list = ", ".join(f"'{t}'" for t in escaped)
    return f"AND {column} IN ({in_list})"
