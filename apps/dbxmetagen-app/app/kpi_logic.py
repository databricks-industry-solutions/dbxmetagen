"""Pure (SQL-free) KPI validation and source-table resolution helpers.

Extracted from api_server.py so they can be unit-tested without importing the
full FastAPI app (which pulls in heavy runtime deps). api_server imports these
and layers the actual warehouse SQL execution on top.
"""

from typing import List, Tuple


def reduce_kpi_validation(results: List[tuple]) -> Tuple[str, str, str]:
    """Reduce per-table dry-run outcomes into a single (status, error, resolved_table).

    A KPI formula belongs to ONE source table; it is valid if it resolves against
    AT LEAST ONE of its target tables -- not all of them.

    Each result is a tuple ("ok"|"empty"|"error", table, detail):
      - "ok":    formula ran and returned rows against `table`
      - "empty": formula ran (schema resolved) but returned no rows
      - "error": formula raised (missing column / syntax) with `detail` message

    Precedence: any table with rows -> valid (report the first such table).
    Else if any table resolved-but-empty -> empty (schema OK, no data; non-blocking
    for generation). Else -> invalid, joining every per-table error so the user sees
    why it failed against each candidate, not just the first.
    """
    if not results:
        return "skipped", "", ""
    empty_table = ""
    errors = []
    for outcome, table, detail in results:
        if outcome == "ok":
            return "valid", "", table
        if outcome == "empty" and not empty_table:
            empty_table = table
        elif outcome == "error":
            errors.append(f"Against {table}: {detail}")
    if empty_table:
        return "empty", f"Resolved against {empty_table} but no rows returned", empty_table
    return "invalid", "; ".join(errors)[:1000], ""


def resolve_kpi_target(source_table: str, formula: str, table_identifiers: List[str],
                       col_by_table: dict) -> List[str]:
    """Resolve a suggested KPI's source_table to a single fully-qualified table.

    A KPI formula belongs to ONE table, so this returns a single-element list.
    Resolution order: exact short-name match -> exact fq match -> short name of a
    dotted value -> fallback to the table whose columns most overlap the formula.
    col_by_table maps fq table -> list of {"column_name": ...} dicts.
    """
    short_to_fq = {t.split(".")[-1].lower(): t for t in table_identifiers}
    fq_to_fq = {t.lower(): t for t in table_identifiers}
    raw = (source_table or "").lower().strip()
    if raw in short_to_fq:
        return [short_to_fq[raw]]
    if raw in fq_to_fq:
        return [fq_to_fq[raw]]
    if raw.split(".")[-1] in short_to_fq:
        return [short_to_fq[raw.split(".")[-1]]]
    # fallback: pick table with most column overlap in the formula
    formula_lower = (formula or "").lower()
    best, best_score = table_identifiers[:1], 0
    for tbl_fq, cols in col_by_table.items():
        score = sum(1 for c in cols if c["column_name"].lower() in formula_lower)
        if score > best_score:
            best, best_score = [tbl_fq], score
    return best
