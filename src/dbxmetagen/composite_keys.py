"""Composite (multi-column) join keys stored on ``fk_predictions`` rows.

A composite relationship is stored as ONE ``fk_predictions`` row: the anchor
column pair sits in ``src_column`` / ``dst_column``, ``is_composite`` is TRUE and
``join_condition`` holds the full condition. The ERD designer writes the
canonical form ``source.a = <dst>.a AND source.b = <dst>.b`` (child side
qualified ``source``). Older rows may hold only the key names
(``"guid AND base_dt"``), meaning same-named columns on both sides.

Pure helpers (no Spark / SDK imports) shared by the FK pipeline, the ERD
recommender and the app.
"""

from __future__ import annotations

import re
from typing import Optional

# Both sides must be column references (start with a letter, underscore or
# backtick), so a filter term such as `dst.active = 1` is not read as a key pair.
_EQ_TERM_RE = re.compile(r"^\s*([A-Za-z_`][\w.`]*)\s*=\s*([A-Za-z_`][\w.`]*)\s*$")
_AND_SPLIT_RE = re.compile(r"\s+AND\s+|\s*,\s*", re.IGNORECASE)
_BARE_NAME_RE = re.compile(r"^`?[A-Za-z_]\w*`?$")


def _leaf(ref: str) -> str:
    """Last dotted segment of a column reference, without backticks."""
    return ref.strip().split(".")[-1].strip("`").strip()


def _qualifier(ref: str) -> str:
    parts = ref.strip().split(".")
    return parts[-2].strip("`").strip().lower() if len(parts) > 1 else ""


def composite_key_pairs(
    join_condition: Optional[str],
    src_column: Optional[str] = None,
    dst_column: Optional[str] = None,
    child_qualifier: str = "source",
) -> list[tuple[str, str]]:
    """Return the key column pairs ``[(src_col, dst_col), ...]`` of a composite key.

    Accepts the canonical ERD form (``source.a = dst.a AND source.b = dst.b``),
    generic qualified equalities (the left side is taken as the source side
    unless only the right side carries ``child_qualifier``), and the legacy
    key-name list (``"a AND b"``), which pairs same-named columns. The row's own
    anchor pair (``src_column``, ``dst_column``) is always included, first.

    Returns ``[]`` when the condition has fewer than two distinct pairs or any
    term cannot be parsed, so callers can fall back to single-column handling.
    """
    if not join_condition or not join_condition.strip():
        return []
    pairs: list[tuple[str, str]] = []
    for term in _AND_SPLIT_RE.split(join_condition.strip()):
        term = term.strip().strip("()").strip()
        if not term:
            continue
        m = _EQ_TERM_RE.match(term)
        if m:
            left, right = m.group(1), m.group(2)
            if _qualifier(right) == child_qualifier and _qualifier(left) != child_qualifier:
                left, right = right, left
            pairs.append((_leaf(left), _leaf(right)))
        elif _BARE_NAME_RE.match(term):
            name = _leaf(term)
            pairs.append((name, name))
        else:
            return []

    anchor = (_leaf(src_column), _leaf(dst_column)) if src_column and dst_column else None
    ordered: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for p in ([anchor] if anchor else []) + pairs:
        key = (p[0].lower(), p[1].lower())
        if p[0] and p[1] and key not in seen:
            seen.add(key)
            ordered.append(p)
    return ordered if len(ordered) >= 2 else []


def render_composite_condition(
    pairs: list[tuple[str, str]], src_alias: str, dst_alias: str
) -> str:
    """Render key pairs as ``<src_alias>.a = <dst_alias>.a AND ...``."""
    return " AND ".join(f"{src_alias}.{s} = {dst_alias}.{d}" for s, d in pairs)


def _quote_col(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def quote_table_name(fqn: str) -> str:
    """Backtick-quote each segment of a dotted table name (``a.b.c`` ->
    ```` `a`.`b`.`c` ````). Spark-free counterpart of
    ``databricks_utils.quote_fqn`` for the app; already-quoted segments are
    unwrapped first so quoting is idempotent."""
    return ".".join(_quote_col(seg.strip().strip("`")) for seg in fqn.split("."))


def composite_uniqueness_sql(
    table_sql: str, columns: list[str], limit_rows: Optional[int] = None
) -> str:
    """SQL returning ``total`` rows and ``distinct_keys`` for a column combination.

    ``table_sql`` must already be quoted by the caller. The count is exact
    unless ``limit_rows`` is given (used for federated sources): a sample
    overstates uniqueness, because duplicate keys rarely land in the same
    sample, which would hide the very fan-out this measures.
    Rows with a NULL in any key column are not counted as distinct keys
    (they cannot match a join).
    """
    if not columns:
        raise ValueError("composite_uniqueness_sql needs at least one column")
    cols = ", ".join(_quote_col(c) for c in columns)
    source = (
        f"(SELECT {cols} FROM {table_sql} LIMIT {int(limit_rows)})"
        if limit_rows
        else table_sql
    )
    return f"SELECT COUNT(*) AS total, COUNT(DISTINCT {cols}) AS distinct_keys FROM {source}"


def uniqueness_ratio(total, distinct_keys) -> Optional[float]:
    """``distinct_keys / total`` as a float in [0, 1], or None when unknown."""
    try:
        t = float(total)
        d = float(distinct_keys)
    except (TypeError, ValueError):
        return None
    if t <= 0:
        return None
    return max(0.0, min(1.0, d / t))
