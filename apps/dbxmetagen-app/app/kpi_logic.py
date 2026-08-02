"""Pure (SQL-free) KPI validation and source-table resolution helpers.

Extracted from api_server.py so they can be unit-tested without importing the
full FastAPI app (which pulls in heavy runtime deps). api_server imports these
and layers the actual warehouse SQL execution on top.
"""

import re
from difflib import SequenceMatcher
from typing import List, Optional, Tuple


def _normalize_name(name: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace for name comparison."""
    return " ".join(re.sub(r"[^\w\s]", " ", (name or "").lower()).split())


def _formula_tokens(formula: str) -> set:
    """Identifier-ish tokens in a formula, minus common SQL keywords, for overlap scoring."""
    toks = set(re.findall(r"[a-z_][a-z0-9_]*", (formula or "").lower()))
    return toks - {
        "select", "from", "where", "group", "by", "order", "and", "or", "as",
        "sum", "count", "avg", "min", "max", "case", "when", "then", "else", "end",
        "on", "join", "left", "right", "inner", "outer", "distinct", "null", "is",
    }


def find_similar_kpi(
    new_name: str,
    new_formula: str,
    existing_kpis: List[dict],
    name_threshold: float = 0.85,
    formula_threshold: float = 0.8,
) -> Optional[dict]:
    """Return the closest existing KPI that looks like a duplicate, or None.

    WARN-not-block: this only surfaces a likely duplicate so the caller can ask
    the user to confirm; it never mutates or drops anything. A match is reported
    when the normalized name is (near-)identical OR the formula token-set overlap
    (Jaccard) is high with a matching aggregate. Returns a dict:
    ``{"kpi": <existing>, "reason": "name"|"formula", "score": float}``.
    """
    new_name_n = _normalize_name(new_name)
    new_toks = _formula_tokens(new_formula)
    best = None
    for ex in existing_kpis or []:
        ex_name_n = _normalize_name(ex.get("name", ""))
        if new_name_n and ex_name_n:
            if new_name_n == ex_name_n:
                return {"kpi": ex, "reason": "name", "score": 1.0}
            ratio = SequenceMatcher(None, new_name_n, ex_name_n).ratio()
            if ratio >= name_threshold and (best is None or ratio > best["score"]):
                best = {"kpi": ex, "reason": "name", "score": round(ratio, 3)}
        ex_toks = _formula_tokens(ex.get("formula", ""))
        if new_toks and ex_toks:
            jaccard = len(new_toks & ex_toks) / len(new_toks | ex_toks)
            if jaccard >= formula_threshold and (best is None or jaccard > best["score"]):
                best = {"kpi": ex, "reason": "formula", "score": round(jaccard, 3)}
    return best


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

    A KPI formula belongs to ONE table. When we can confidently identify it (name
    match, or a clear column-overlap winner) this returns that single table. When
    there is NO signal at all (source_table matches nothing AND no column overlaps
    the formula -- e.g. a literal-only formula), it returns ALL candidate tables
    rather than an arbitrary first one: downstream any-table-valid validation
    (_validate_kpi_formula / reduce_kpi_validation) then resolves the KPI to
    whichever table its formula actually runs against. col_by_table maps fq table
    -> list of {"column_name": ...} dicts.
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
    # fallback: pick table with most column overlap in the formula. Match on
    # whole-word tokens (not naive substring) so short/common column names like
    # "id" or "a" don't score just because those letters appear inside another
    # identifier or function name.
    formula_lower = (formula or "").lower()
    formula_tokens = set(re.findall(r"[a-z_][a-z0-9_]*", formula_lower))
    best, best_score = None, 0
    for tbl_fq, cols in col_by_table.items():
        score = sum(1 for c in cols if c["column_name"].lower() in formula_tokens)
        if score > best_score:
            best, best_score = [tbl_fq], score
    # No confident single table -> hand ALL candidates to validation instead of an
    # arbitrary table[0], so the KPI binds to the table its formula truly resolves on.
    return best if best is not None else list(table_identifiers)
