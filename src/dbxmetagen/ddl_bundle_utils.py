"""Pure-logic utilities for DDL bundle generation.

These are extracted from api_server.py so they can be unit-tested
without loading the full FastAPI application.
"""


def rewrite_ddl_catalog_schema(
    stmts: list[str],
    source_catalog: str,
    source_schema: str,
    target_catalog: str,
    target_schema: str,
) -> list[str]:
    """Rewrite catalog.schema references in DDL, skipping single-quoted string literals."""
    if source_catalog == target_catalog and source_schema == target_schema:
        return stmts
    old_prefix = f"`{source_catalog}`.`{source_schema}`"
    new_prefix = f"`{target_catalog}`.`{target_schema}`"
    old_bare = f"{source_catalog}.{source_schema}"
    new_bare = f"{target_catalog}.{target_schema}"
    result = []
    for s in stmts:
        parts = s.split("'")
        for i in range(0, len(parts), 2):  # even indices are outside quotes
            parts[i] = parts[i].replace(old_prefix, new_prefix).replace(old_bare, new_bare)
        result.append("'".join(parts))
    return result


def dq_grade(score: float) -> str:
    """Map a data-quality score (0-100) to a letter grade."""
    if score >= 90:
        return "A"
    if score >= 80:
        return "B"
    if score >= 70:
        return "C"
    if score >= 60:
        return "D"
    return "F"
