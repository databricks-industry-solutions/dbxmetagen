"""Shared, substrate-agnostic metric-view helpers.

This module holds the *pure* (no Spark, no SQL-warehouse, no I/O beyond logging)
metric-view definition transforms that were previously duplicated between the
library generator (``dbxmetagen.semantic_layer.SemanticLayerGenerator``) and the
app backend (``apps/dbxmetagen-app/app/api_server.py``). Both import from here so
there is a single source of truth and the two copies can no longer drift.

IMPORTANT: keep this module free of ``pyspark`` / ``databricks`` imports so it is
importable and unit-testable anywhere (including the FastAPI app process, which
has no SparkSession). Only stdlib + ``re`` are used here.

See docs/SEMANTIC_LAYER_LIBRARY_VS_APP.md for the consolidation plan. The full
metric-view transform surface now lives here: the ``_autofix_expr`` + ``_fix_*``
family, ``_PERCENTAGE_*`` / ``_infer_format_specs`` / ``_fix_percentage_scaling``,
``_KPI_REF_RE`` / ``_strip_kpi_references``, and the join/serialization pipeline
(``_normalize_joins``, ``_restructure_chained_to_nested``, ``_qualify_nested_refs``,
``_IndentYamlDumper``, ``_definition_to_yaml``). Phase 0 of the consolidation is
complete; both the library generator and the app backend import from here.
"""

import json
import logging
import re

import yaml

logger = logging.getLogger(__name__)


# ── Display-name / synonym inference ──────────────────────────────────

def _infer_display_name(name: str) -> str:
    """Convert snake_case/kebab-case measure/dimension names to Title Case."""
    return name.replace("_", " ").replace("-", " ").title()


def _infer_synonyms(name: str, comment: str | None) -> list[str]:
    """Extract 2-3 keyword synonyms from the name and comment."""
    synonyms = set()
    clean = name.replace("_", " ").lower()
    words = clean.split()
    _STOP = {"the", "a", "an", "of", "for", "by", "in", "to", "and", "or", "is", "as", "per", "with", "from"}
    # Abbreviation from initials
    if len(words) >= 2:
        abbr = "".join(w[0] for w in words if w not in _STOP).upper()
        if len(abbr) >= 2:
            synonyms.add(abbr)
    # Keywords from comment
    if comment:
        for w in comment.lower().split():
            w = w.strip(".,;:()")
            if len(w) > 3 and w not in _STOP and w not in clean:
                synonyms.add(w)
                if len(synonyms) >= 3:
                    break
    return list(synonyms)[:5]


def _backfill_agent_metadata(defn: dict) -> None:
    """Backfill display_name and synonyms on measures/dimensions that lack them."""
    for item in defn.get("measures", []) + defn.get("dimensions", []):
        if not item.get("display_name"):
            item["display_name"] = _infer_display_name(item.get("name", ""))
        if not item.get("synonyms"):
            item["synonyms"] = _infer_synonyms(item.get("name", ""), item.get("comment"))


# ── Measure / dimension cleanup ───────────────────────────────────────

_SELF_DIV_RE = re.compile(
    r"^(SUM|COUNT|AVG|MIN|MAX)\s*\(([^)]+)\)\s*/\s*NULLIF\s*\(\s*\1\s*\(\2\)",
    re.IGNORECASE,
)


def _drop_broken_measures(defn: dict) -> None:
    """Remove self-dividing share measures (always=1.0) and deduplicate identical exprs."""
    measures = defn.get("measures", [])
    if not measures:
        return

    cleaned: list[dict] = []
    seen_exprs: set[str] = set()
    for m in measures:
        expr = re.sub(r"\s+", " ", m.get("expr", "").strip())
        if _SELF_DIV_RE.search(expr):
            continue
        norm = expr.upper()
        if norm in seen_exprs:
            continue
        seen_exprs.add(norm)
        cleaned.append(m)
    defn["measures"] = cleaned


_ALIAS_DOT_RE = re.compile(r"\b([A-Za-z_]\w*)\.\w+")


def _drop_placeholder_dimensions(defn: dict) -> None:
    """Drop dimensions whose name implies a join alias but whose expr uses a different alias.

    E.g. a dim named "Territory Code" whose expr is "source.prescription_id" is a placeholder.
    """
    joins = defn.get("joins", [])
    if not joins:
        return

    def _collect_aliases(jlist: list[dict]) -> set[str]:
        out: set[str] = set()
        for j in jlist:
            alias = j.get("name", "").lower()
            if alias:
                out.add(alias)
            if j.get("joins"):
                out |= _collect_aliases(j["joins"])
        return out

    all_aliases = _collect_aliases(joins)
    if not all_aliases:
        return

    cleaned: list[dict] = []
    for d in defn.get("dimensions", []):
        name_lower = d.get("name", "").lower().replace("_", " ")
        expr = d.get("expr", "")
        refs = {m.group(1).lower() for m in _ALIAS_DOT_RE.finditer(expr)}

        implied_alias = None
        for alias in all_aliases:
            if alias in name_lower:
                implied_alias = alias
                break

        if implied_alias and implied_alias not in refs:
            continue
        cleaned.append(d)
    defn["dimensions"] = cleaned


# ── Format inference + percentage scaling ─────────────────────────────
#
# Reconciled from the library + app copies (Phase 0b), converged on the
# CORRECT behavior verified against official Databricks docs, NOT on either
# prior copy:
#
#   A metric-view ``format: {type: percentage}`` expects the measure expression
#   to produce a 0-1 FRACTION; the rendering layer multiplies by 100 for
#   display. There is no scale knob. So ``SUM(won)/NULLIF(COUNT(*),0)`` (returns
#   0.167) renders as "16.7%". A pre-scaled ``100.0 * ratio`` renders as "1667%".
#
# The library encoded this correctly (prompt + strip guardrail); the app did
# not (its prompt told the LLM to pre-scale by 100 and it had no strip), so app
# percentage measures could render 100x too large. This shared implementation
# gives BOTH paths the guardrail and detects/strips a stray ``100 *`` in either
# the leading (``100 * ratio / NULLIF``) or trailing (``ratio * 100.0 / NULLIF``)
# ordering -- the two are commutative ways of writing the same mistake, and the
# library's original strip only handled the leading form.

_CURRENCY_PATTERNS = re.compile(
    r"SUM\s*\(\s*(total_amount|amount|revenue|cost|price|charge|fee|salary|budget|payment|balance)",
    re.IGNORECASE,
)
_PERCENTAGE_PATTERNS = re.compile(
    r"(\*\s*1\.0\s*/\s*NULLIF"
    r"|100(?:\.0)?\s*\*\s*.*?/\s*NULLIF"       # leading:  100.0 * ratio / NULLIF(...)
    r"|\*\s*100(?:\.0)?\s*/\s*NULLIF"          # trailing: ratio * 100.0 / NULLIF(...)
    r"|THEN\s+1\s+ELSE\s+0\s+END\)\s*\*\s*1\.0)",
    re.IGNORECASE,
)
_PERCENTAGE_NAME_PATTERNS = re.compile(r"\brate\b|\bpct\b|\bpercentage\b|\bratio\b", re.IGNORECASE)


def _infer_format_specs(defn: dict) -> None:
    """Infer and backfill format specs on measures that lack them.

    Also backfills a missing ``currency_code`` (USD) on measures that already
    carry a currency format, so downstream YAML always has a concrete code.
    """
    for m in defn.get("measures", []):
        fmt = m.get("format")
        if fmt:
            if isinstance(fmt, dict) and fmt.get("type") == "currency" and not fmt.get("currency_code"):
                fmt["currency_code"] = "USD"
            continue
        expr = m.get("expr", "")
        name = m.get("name", "")
        if _PERCENTAGE_PATTERNS.search(expr) or _PERCENTAGE_NAME_PATTERNS.search(name):
            m["format"] = {"type": "percentage"}
        elif _CURRENCY_PATTERNS.search(expr):
            m["format"] = {"type": "currency", "currency_code": "USD"}
        else:
            m["format"] = {"type": "number"}


# Matches a stray ``100 *`` premultiply in either ordering, optionally wrapped in
# ROUND(...). Group 1, when present, is the trailing ``* 100`` form.
_PERCENTAGE_PREMULTIPLY_LEADING = re.compile(
    r"(?:ROUND\s*\(\s*)?100(?:\.0)?\s*\*\s*", re.IGNORECASE
)
_PERCENTAGE_PREMULTIPLY_TRAILING = re.compile(
    r"\s*\*\s*100(?:\.0)?\b", re.IGNORECASE
)


def _fix_percentage_scaling(defn: dict) -> None:
    """Strip a stray ``100 *`` from percentage-formatted measures to avoid double-multiply.

    Metric-view percentage format expects a 0-1 fraction; the rendering layer
    multiplies by 100. If the expression already scales by 100 (``100.0 * ratio``
    or ``ratio * 100.0``), the displayed value is 100x too large (e.g. 1667%
    instead of 16.7%). Handles both the leading and trailing orderings.
    """
    for m in defn.get("measures", []):
        fmt = m.get("format", {})
        if not isinstance(fmt, dict) or fmt.get("type") != "percentage":
            continue
        expr = m.get("expr", "")
        new_expr = expr
        # Leading form: 100 * ratio ...  (also unwraps a ROUND( wrapper)
        lead = _PERCENTAGE_PREMULTIPLY_LEADING.search(new_expr)
        if lead:
            candidate = new_expr[:lead.start()] + new_expr[lead.end():]
            if candidate.rstrip().endswith(")") and "ROUND" in lead.group(0).upper():
                candidate = re.sub(r",\s*\d+\s*\)\s*$", "", candidate)
            new_expr = candidate
        else:
            # Trailing form: ratio * 100 ...
            trail = _PERCENTAGE_PREMULTIPLY_TRAILING.search(new_expr)
            if trail:
                new_expr = new_expr[:trail.start()] + new_expr[trail.end():]
        new_expr = new_expr.strip()
        if new_expr != expr:
            m["expr"] = new_expr
            logger.info("Stripped 100x multiplier from percentage measure '%s'", m.get("name", "?"))


# ── KPI reference stripping ───────────────────────────────────────────
#
# Reconciled from the library + app copies (Phase 0b). The app version is the
# canonical superset: it matches more reference forms (``KPI #3``, ``(#3)``, the
# ``KPI: ...`` colon form) and collapses double spaces left behind after removal.
# Adopting it on both sides only strips *more* boilerplate; it never leaves a
# reference the old library regex would have caught.

_KPI_REF_RE = re.compile(
    r"\.?\s*(?:Implements|Supports|Addresses|Answers|Covers|Partially implements)"
    r"\s+(?:KPI|question|Q)s?\s*(?::\s*[^.]*(?:\.\s*)?|[\d,\s\-and#()]+\.?)"
    r"|\s*\(KPI:\s*[^)]+\)"
    r"|\s*\bKPI\s*#\d+\b(?:\s*\([^)]*\))?\.?"
    r"|\s*\(#\d+\)",
    re.IGNORECASE,
)


def _strip_kpi_references(defn: dict) -> None:
    """Remove KPI/question number references from comments."""
    def _clean(text: str) -> str:
        text = _KPI_REF_RE.sub("", text)
        text = re.sub(r"  +", " ", text).strip().rstrip(".")
        return (text + ".") if text else ""
    for field in ("comment",):
        if defn.get(field):
            defn[field] = _clean(defn[field])
    for item in defn.get("measures", []) + defn.get("dimensions", []):
        if item.get("comment"):
            item["comment"] = _clean(item["comment"])


# ── Window spec normalization ─────────────────────────────────────────

def _normalize_window_specs(w) -> list[dict]:
    """Normalize window field to YAML 1.1 spec: array of {order, range/rows, semiadditive}."""
    if w is None:
        return []
    if isinstance(w, dict):
        w = [w]
    if not isinstance(w, list):
        return []
    result = []
    for spec in w:
        if not isinstance(spec, dict):
            continue
        order = spec.get("order") or spec.get("order_by")
        if not order:
            continue
        rng = spec.get("range", "")
        if isinstance(rng, str) and "INTERVAL" in rng.upper():
            m = re.search(r"INTERVAL\s+(\d+)\s+(\w+)", rng, re.IGNORECASE)
            if m:
                rng = f"trailing {m.group(1)} {m.group(2).lower().rstrip('s')}"
        rows = spec.get("rows", "")
        if isinstance(rows, str) and "UNBOUNDED" in rows.upper():
            rng = "unbounded"
            rows = ""
        entry = {"order": order}
        if rng:
            entry["range"] = rng
        if rows:
            entry["rows"] = rows
        entry["semiadditive"] = spec.get("semiadditive", "last")
        result.append(entry)
    return result


# ── SQL expression autofix (``_autofix_expr`` + ``_fix_*`` family) ─────
#
# Reconciled from the library + app copies (Phase 0b), verified empirically:
# both copies were run over a corpus and the unified pipeline below preserves
# every "good expression" the library's do-no-harm suite pins (35/35) while
# fixing each real Spark-SQL problem either side caught. Composition:
#   * library ordering + library-only fixers (_fix_date_part, _fix_datediff,
#     _fix_null_comparison, _fix_case_quoting, _fix_quoted_computation), which
#     the app lacked;
#   * the app's genuinely-correct additions: _fix_percentile_cont (2-arg ->
#     WITHIN GROUP) and the empty-``OVER()`` strip;
#   * the app's bare-interval DATE_TRUNC loop is DELIBERATELY DROPPED -- it
#     corrupted valid ``MONTH(col)``/``YEAR(col)`` extract functions into
#     ``DATE_TRUNC('MONTH', col)`` (a semantics change, not a fix).

_DATE_TRUNC_INTERVALS = {
    "YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND",
}
_SQL_RESERVED = {
    "THEN", "ELSE", "END", "AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "CASE",
    "WHEN", "IN", "IS", "LIKE", "BETWEEN", "SELECT", "FROM", "WHERE", "FILTER",
    "DISTINCT", "SUM", "AVG", "COUNT", "MIN", "MAX", "DATE_TRUNC", "IF",
    "COALESCE", "NULLIF", "OVER", "PARTITION", "BY", "ORDER", "ASC", "DESC",
    "CURRENT_DATE", "CURRENT_TIMESTAMP", "CURRENT_TIME",
}
_DATEDIFF_UNITS = {
    "YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND",
    "MILLISECOND", "MICROSECOND",
}
_PLURAL_UNITS = {
    "YEARS": "YEAR", "QUARTERS": "QUARTER", "MONTHS": "MONTH", "WEEKS": "WEEK",
    "DAYS": "DAY", "HOURS": "HOUR", "MINUTES": "MINUTE", "SECONDS": "SECOND",
}
_COMPUTATION_FUNC_RE = re.compile(
    r'(?:UNIX_TIMESTAMP|DATEDIFF|TIMESTAMPDIFF|CAST|COALESCE|NULLIF|ROUND|ABS|CEIL|FLOOR|DATE_ADD|DATE_SUB|MONTHS_BETWEEN)\s*\(',
    re.IGNORECASE,
)


def _fix_unquoted_literals(expr: str) -> str:
    """Quote bare words/phrases used as string literals in comparisons."""
    def _replacer(m):
        op = m.group(1)
        value = m.group(2).strip()
        trail = m.group(3)
        if not value:
            return m.group(0)
        if value.startswith("'") or value.startswith('"'):
            return m.group(0)
        if re.match(r"^-?\d+(\.\d+)?$", value):
            return m.group(0)
        if "." in value and " " not in value:
            return m.group(0)
        if "(" in value and re.match(r"^[A-Za-z_]\w*\(", value):
            return m.group(0)
        if value.upper() in _SQL_RESERVED or value.upper() in _DATE_TRUNC_INTERVALS:
            return m.group(0)
        return f"{op}'{value}'{trail}"
    return re.sub(
        r"([=!<>]+\s*)(.*?)(\s+(?:THEN|ELSE|END|AND|OR|WHEN)\b|\s*[,)]|$)",
        _replacer, expr, flags=re.IGNORECASE,
    )


def _fix_case_quoting(expr: str) -> str:
    """Fix ELSE/END keywords trapped inside single-quoted string literals."""
    _KW = re.compile(r"\b(ELSE|END|WHEN)\b", re.IGNORECASE)

    def _split_lit(m):
        full = m.group(0)
        inner = m.group(1)
        if not _KW.search(inner):
            return full
        if "(" in inner or ")" in inner or "." in inner:
            return full
        parts = _KW.split(inner)
        text_parts = [p.strip() for p in parts if not _KW.fullmatch(p.strip())]
        if not any(text_parts):
            return full
        result = ""
        for i, part in enumerate(parts):
            text = part.strip()
            if _KW.fullmatch(text):
                result += f" {text.upper()} "
            elif text:
                result += f"'{text}'"
        return result.strip()
    return re.sub(r"'([^']*\b(?:ELSE|END|WHEN)\b[^']*)'", _split_lit, expr, flags=re.IGNORECASE)


def _fix_then_else_literals(expr: str) -> str:
    """Quote bare text after THEN/ELSE that isn't already quoted or a number/column/keyword."""
    def _replacer(m):
        kw = m.group(1)
        body = m.group(2).strip()
        if not body:
            return m.group(0)
        if body.startswith("'") or body.startswith('"'):
            return m.group(0)
        if re.match(r"^-?\d+(\.\d+)?$", body):
            return m.group(0)
        if "." in body and " " not in body:
            return m.group(0)
        if "(" in body and re.match(r"^[A-Za-z_]\w*\(", body):
            return m.group(0)
        tokens = body.split()
        if len(tokens) == 1 and tokens[0].upper() in _SQL_RESERVED:
            return m.group(0)
        if len(tokens) == 1 and re.match(r"^[A-Za-z_]\w*$", tokens[0]):
            if tokens[0].upper() not in _SQL_RESERVED:
                return f"{kw} '{body}'"
            return m.group(0)
        if len(tokens) > 1:
            return f"{kw} '{body}'"
        return m.group(0)
    return re.sub(
        r"\b(THEN|ELSE)\s+(.*?)(?=\s+(?:WHEN|ELSE|END)\b)",
        _replacer, expr, flags=re.IGNORECASE,
    )


def _fix_in_clause_literals(expr: str) -> str:
    """Quote bare words inside IN (...) clauses."""
    def _fix_in_body(m):
        prefix, body = m.group(1), m.group(2)
        tokens = [t.strip() for t in body.split(",")]
        fixed = []
        for tok in tokens:
            if not tok or tok.startswith("'") or tok.startswith('"'):
                fixed.append(tok)
            elif re.match(r"^-?\d+(\.\d+)?$", tok):
                fixed.append(tok)
            elif tok.upper() in _SQL_RESERVED:
                fixed.append(tok)
            else:
                fixed.append(f"'{tok}'")
        return f"{prefix}{', '.join(fixed)})"
    return re.sub(r"(\bIN\s*\()([^)]+)\)", _fix_in_body, expr, flags=re.IGNORECASE)


def _fix_concat_separators(expr: str) -> str:
    """Quote bare separator tokens between commas (e.g. -Q, /, : in CONCAT)."""
    def _repl(m):
        tok = m.group(1).strip()
        if re.match(r"^-?\d+(\.\d+)?$", tok):
            return m.group(0)
        return f", '{tok}',"
    return re.sub(r",\s*([^\w\s'\"`(][^'\"`(,)]{0,4})\s*,", _repl, expr)


def _fix_date_part(expr: str) -> str:
    """Rewrite DATE_PART(UNIT, col) to EXTRACT(UNIT FROM col)."""
    return re.sub(
        r"DATE_PART\(\s*(['\"]?)(\w+)\1\s*,\s*(.+?)\)",
        lambda m: (
            f"EXTRACT({m.group(2).upper()} FROM {m.group(3).strip()})"
            if m.group(2).upper() in _DATE_TRUNC_INTERVALS
            else m.group(0)
        ),
        expr, flags=re.IGNORECASE,
    )


def _fix_datediff(expr: str) -> str:
    """Rewrite DATEDIFF(UNIT, start, end) to TIMESTAMPDIFF(UNIT, start, end)."""
    return re.sub(
        r"DATEDIFF\(\s*(['\"]?)(\w+)\1\s*(,\s*\w+.*?,\s*\w+.*?\))",
        lambda m: (
            f"TIMESTAMPDIFF({_PLURAL_UNITS.get(m.group(2).upper(), m.group(2).upper())}{m.group(3)}"
            if _PLURAL_UNITS.get(m.group(2).upper(), m.group(2).upper()) in _DATEDIFF_UNITS
            else m.group(0)
        ),
        expr, flags=re.IGNORECASE,
    )


def _fix_like_patterns(expr: str) -> str:
    """Quote bare LIKE/NOT LIKE patterns: ``col LIKE HW%`` -> ``col LIKE 'HW%'``."""
    def _repl(m):
        prefix = m.group(1)
        pat = m.group(2).strip()
        if pat.startswith("'") or pat.startswith('"'):
            return m.group(0)
        return f"{prefix}'{pat}'"
    return re.sub(r"(LIKE\s+)([^'\"\s(]+)", _repl, expr, flags=re.IGNORECASE)


def _fix_dquote_identifier(expr: str) -> str:
    """Convert dotted double-quoted identifiers to backtick-quoted.

    ``source."assay name"`` -> ``source.`assay name```. Only matches
    ``word."..."`` (dotted identifier), never bare ``"..."`` (string literals).
    """
    return re.sub(r'(\b\w+)\."([^"]+)"', r"\1.`\2`", expr)


def _fix_instr_bare_arg(expr: str) -> str:
    """Quote bare non-alnum arg in INSTR (2nd arg) and LOCATE (1st arg)."""
    def _repl_second(m):
        ch = m.group(2).strip()
        if ch.startswith("'") or ch.startswith('"'):
            return m.group(0)
        return f"{m.group(1)}'{ch}')"
    expr = re.sub(r"(INSTR\([^,]+,\s*)([^\w\s'\"]+)\)", _repl_second, expr, flags=re.IGNORECASE)
    def _repl_first(m):
        ch = m.group(1).strip()
        if ch.startswith("'") or ch.startswith('"'):
            return m.group(0)
        return f"LOCATE('{ch}'{m.group(2)}"
    expr = re.sub(r"LOCATE\(\s*([^\w\s'\"]+)(,)", _repl_first, expr, flags=re.IGNORECASE)
    return expr


def _fix_position_bare_char(expr: str) -> str:
    """Quote bare non-alnum char in POSITION(X IN ...)."""
    def _repl(m):
        ch = m.group(1).strip()
        if ch.startswith("'") or ch.startswith('"'):
            return m.group(0)
        return f"POSITION('{ch}' IN{m.group(2)}"
    return re.sub(r"POSITION\(\s*([^\w\s'\"]+)\s+(IN\b)", _repl, expr, flags=re.IGNORECASE)


def _fix_double_commas(expr: str) -> str:
    """Collapse empty arguments: CONCAT(a, , b) -> CONCAT(a, b)."""
    while ", ," in expr:
        expr = expr.replace(", ,", ",")
    while ",," in expr:
        expr = expr.replace(",,", ",")
    return expr


def _fix_bare_whitespace_separator(expr: str) -> str:
    """Quote bare whitespace between commas: f(a,  , b) -> f(a, ' ', b)."""
    return re.sub(r",(\s+),", ", ' ',", expr)


def _fix_quoted_computation(expr: str) -> str:
    """Strip quotes from THEN/ELSE values that are SQL computations, not string literals."""
    def _unquote(m):
        inner = m.group(2)
        if _COMPUTATION_FUNC_RE.search(inner):
            return f"{m.group(1)}{inner}{m.group(3)}"
        return m.group(0)
    return re.sub(
        r"((?:THEN|ELSE)\s+)'([^']{20,})'(\s*(?:ELSE|END|WHEN|$))",
        _unquote, expr, flags=re.IGNORECASE,
    )


def _fix_bare_comparison(expr: str) -> str:
    """Insert '' when a comparison operator has no RHS value (LLM omitted empty string literal)."""
    return re.sub(
        r"([!=<>]+)\s*(?=\s*[,)]|\s+(?:AND|OR|THEN|ELSE|END|WHEN)\b)",
        r"\1 ''", expr, flags=re.IGNORECASE,
    )


def _fix_null_comparison(expr: str) -> str:
    """Rewrite <> NULL / != NULL to IS NOT NULL, = NULL to IS NULL."""
    expr = re.sub(r'\s*<>\s*NULL\b', ' IS NOT NULL', expr, flags=re.IGNORECASE)
    expr = re.sub(r'\s*!=\s*NULL\b', ' IS NOT NULL', expr, flags=re.IGNORECASE)
    expr = re.sub(r'(?<![!<>])\s*=\s*NULL\b', ' IS NULL', expr, flags=re.IGNORECASE)
    return expr


def _fix_none_literal(expr: str) -> str:
    """Replace Python None leaked into SQL with NULL."""
    return re.sub(r"\bNone\b", "NULL", expr)


def _fix_concat_bare_first_arg(expr: str) -> str:
    """Quote bare short non-column first arg in CONCAT."""
    def _repl(m):
        fn = m.group(1)
        arg = m.group(2).strip()
        if arg.startswith("'") or arg.startswith('"'):
            return m.group(0)
        if "." in arg or arg.upper() in _SQL_RESERVED or re.match(r"^-?\d", arg):
            return m.group(0)
        if len(arg) <= 3 and arg.isalpha():
            return f"{fn}'{arg}',"
        return m.group(0)
    return re.sub(r"(CONCAT\(\s*)([^',\s]+)\s*,", _repl, expr, flags=re.IGNORECASE)


def _fix_percentile_cont(expr: str) -> str:
    """Rewrite 2-arg percentile_cont/disc to ANSI WITHIN GROUP syntax."""
    def _repl(m):
        func = m.group(1)
        pct = m.group(2).strip()
        col = m.group(3).strip()
        return f"{func}({pct}) WITHIN GROUP (ORDER BY {col})"
    return re.sub(
        r"\b(PERCENTILE_CONT|PERCENTILE_DISC)\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)",
        _repl, expr, flags=re.IGNORECASE,
    )


def _autofix_expr(expr: str) -> str:
    """Fix common AI expression mistakes before validation.

    Unified pipeline (see module note above). Order matters: each pass rewrites
    text the next one sees, so this ordering is behavior-locked by
    tests/test_semantic_layer.py and tests/test_metric_view_core.py.
    """
    expr = _fix_dquote_identifier(expr)

    def _fix_date_trunc(m):
        interval = m.group(1)
        rest = m.group(2)
        if interval.upper() in _DATE_TRUNC_INTERVALS:
            return f"DATE_TRUNC('{interval}'{rest}"
        return m.group(0)
    expr = re.sub(r"DATE_TRUNC\(\s*([A-Za-z]+)(,)", _fix_date_trunc, expr, flags=re.IGNORECASE)

    def _fix_date_format(m):
        col_part = m.group(1)
        fmt = m.group(2).strip()
        if not (fmt.startswith("'") or fmt.startswith('"')):
            return f"DATE_FORMAT({col_part}, '{fmt}')"
        return m.group(0)
    expr = re.sub(r"DATE_FORMAT\(([^,]+),\s*([^)]+)\)", _fix_date_format, expr, flags=re.IGNORECASE)

    expr = _fix_date_part(expr)
    expr = _fix_datediff(expr)

    def _fix_substr_date(m):
        col = m.group(1).strip()
        length = m.group(2).strip()
        if any(kw in col.lower() for kw in ("date", "time", "dt", "_ts", "created", "updated")):
            fmt = "'yyyy-MM'" if length == "7" else "'yyyy'"
            return f"DATE_FORMAT({col}, {fmt})"
        return m.group(0)
    expr = re.sub(r"SUBSTR\(([^,]+),\s*1\s*,\s*(4|7)\)", _fix_substr_date, expr, flags=re.IGNORECASE)

    expr = _fix_bare_comparison(expr)
    expr = _fix_null_comparison(expr)
    expr = _fix_none_literal(expr)
    expr = _fix_double_commas(expr)
    expr = _fix_position_bare_char(expr)
    expr = _fix_instr_bare_arg(expr)
    expr = _fix_concat_bare_first_arg(expr)
    expr = _fix_case_quoting(expr)
    expr = _fix_unquoted_literals(expr)
    expr = _fix_then_else_literals(expr)
    expr = _fix_in_clause_literals(expr)
    expr = _fix_concat_separators(expr)
    expr = _fix_like_patterns(expr)
    expr = _fix_percentile_cont(expr)
    expr = _fix_bare_whitespace_separator(expr)
    expr = _fix_quoted_computation(expr)
    expr = re.sub(r'\bOVER\s*\(\s*\)', '', expr, flags=re.IGNORECASE)
    return expr


# ── Join normalization + YAML serialization ───────────────────────────
#
# Reconciled from the library + app copies (Phase 0b, verified empirically):
#   * _normalize_joins: uses the LIBRARY implementation (recursive, rewrites the
#     parent short-name -> alias at EVERY nesting level). The app copy only
#     rewrote the top level, leaving nested join ``on`` clauses referencing the
#     parent by TABLE name (e.g. ``customers.geo_id`` instead of ``cust.geo_id``),
#     which Databricks rejects/mis-resolves. The app's _fix_join_alias_refs does
#     NOT repair join ``on`` clauses (only dim/measure/filter exprs), so the app
#     was wrong on nested/snowflake joins. Returns ``defn`` (app callers do
#     ``defn = _normalize_joins(defn)``); still mutates in place for library callers.
#   * _restructure_chained_to_nested / _qualify_nested_refs: identical logic in
#     both copies (app added logging); unified here.
#   * _definition_to_yaml: PURE serialization (the app's model). The join
#     normalization pipeline (_normalize_joins -> _restructure_chained_to_nested
#     -> _qualify_nested_refs) is run by the CALLER, not inside this function.
#     The library's SemanticLayerGenerator._definition_to_yaml still runs that
#     pipeline first and then delegates here, preserving its behavior.


def _normalize_joins(defn: dict) -> dict:
    """Normalize join ``on`` to UC alias format at every nesting level.

    Rewrites ``<parent_short>.<col>`` -> ``<parent_alias>.<col>`` recursively so
    nested joins reference their parent by alias, not table name. Mutates ``defn``
    and returns it.
    """
    source = defn.get("source", "")
    if not source or not defn.get("joins"):
        return defn
    source_short = source.split(".")[-1]

    def _norm(jlist: list, parent_short: str, parent_alias: str) -> None:
        for j in jlist:
            on = j.get("on", "")
            if on and f"{parent_short}." in on:
                j["on"] = on.replace(f"{parent_short}.", f"{parent_alias}.")
            if j.get("joins"):
                j_source = j.get("source", "")
                j_alias = j.get("name", j_source.split(".")[-1] if j_source else "")
                j_short = j_source.split(".")[-1] if j_source else ""
                _norm(j["joins"], j_short, j_alias)

    _norm(defn["joins"], source_short, "source")
    return defn


def _restructure_chained_to_nested(defn: dict) -> dict:
    """Convert flat chained joins into proper nested (snowflake) structure.

    Flat joins whose ``on`` references a sibling alias are moved inside that
    parent as nested children. Joins that can't be chained (cycles / unresolvable
    parents) are dropped along with the dims/measures that reference them.
    """
    joins = defn.get("joins", [])
    if not joins:
        return defn

    ref_pat = re.compile(r"\b([A-Za-z_]\w*)\.\w+")
    join_aliases = {j.get("name", "").lower() for j in joins if j.get("name")}

    root_joins: list = []
    chained: list = []
    for j in joins:
        if j.get("joins"):
            root_joins.append(j)
            continue
        on = j.get("on", "")
        refs = {m.group(1).lower() for m in ref_pat.finditer(on)}
        refs.discard("source")
        own = j.get("name", "").lower()
        if refs & join_aliases - {own}:
            chained.append(j)
        else:
            root_joins.append(j)

    if not chained:
        return defn

    alias_to_join: dict = {}
    def _index(jlist: list) -> None:
        for j in jlist:
            alias = j.get("name", "").lower()
            if alias:
                alias_to_join[alias] = j
            if j.get("joins"):
                _index(j["joins"])
    _index(root_joins)

    dropped: set = set()
    for j in chained:
        on = j.get("on", "")
        refs = {m.group(1).lower() for m in ref_pat.finditer(on)}
        refs.discard("source")
        own = j.get("name", "").lower()
        parent_refs = refs & set(alias_to_join.keys()) - {own}
        if parent_refs:
            parent_alias = next(iter(parent_refs))
            parent = alias_to_join[parent_alias]
            parent.setdefault("joins", []).append(j)
            alias_to_join[own] = j
            logger.info("Nested join '%s' under parent '%s'", j.get("name", "?"), parent_alias)
        else:
            logger.warning(
                "Dropping unchainable join '%s': parent alias not found among root joins",
                j.get("name", "?"),
            )
            dropped.add(own)

    defn["joins"] = root_joins

    if dropped:
        for section in ("dimensions", "measures"):
            items = defn.get(section, [])
            cleaned = []
            for item in items:
                expr_refs = {m.group(1).lower() for m in ref_pat.finditer(item.get("expr", ""))}
                if expr_refs & dropped:
                    logger.warning(
                        "Dropping %s '%s': references dropped join alias",
                        section[:-1], item.get("name", "?"),
                    )
                else:
                    cleaned.append(item)
            defn[section] = cleaned

    return defn


def _qualify_nested_refs(defn: dict) -> dict:
    """Rewrite dim/measure/filter expressions so nested join aliases use full dot-paths.

    Databricks metric views require nested aliases to be referenced through their
    parent chain: ``parent.child.column`` not ``child.column``. Top-level joins can
    be referenced directly.
    """
    joins = defn.get("joins", [])
    if not joins:
        return defn
    top_aliases: set = set()
    alias_path: dict = {}

    def _walk(jlist, prefix=""):
        for j in jlist:
            name = j.get("name", "")
            if not name:
                continue
            path = f"{prefix}.{name}" if prefix else name
            alias_path[name.lower()] = path
            if not prefix:
                top_aliases.add(name.lower())
            if j.get("joins"):
                _walk(j["joins"], path)

    _walk(joins)
    nested = {a: p for a, p in alias_path.items() if a not in top_aliases}
    if not nested:
        return defn
    ref_pat = re.compile(r"\b([A-Za-z_]\w*)\.(\w+)")

    def _rewrite(expr):
        def _sub(m):
            alias_low = m.group(1).lower()
            if alias_low in nested:
                return f"{nested[alias_low]}.{m.group(2)}"
            return m.group(0)
        return ref_pat.sub(_sub, expr)

    for section in ("dimensions", "measures"):
        for item in defn.get(section, []):
            if item.get("expr"):
                item["expr"] = _rewrite(item["expr"])
    if defn.get("filter"):
        defn["filter"] = _rewrite(defn["filter"])
    return defn


class _IndentYamlDumper(yaml.Dumper):
    """Dumper that always indents list items under their parent key.

    Default yaml.Dumper uses compact style where list items sit at the same
    indent as the parent mapping key. Databricks' metric-view YAML parser
    requires the indented style where list items are nested one level deeper
    (matching the format shown in the official documentation).
    """
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)


def _definition_to_yaml(defn: dict, include_materialization: bool = False) -> str:
    """Serialize a JSON definition to the YAML body for CREATE VIEW WITH METRICS.

    PURE serialization -- the join-normalization pipeline (_normalize_joins ->
    _restructure_chained_to_nested -> _qualify_nested_refs) must be run by the
    caller beforehand. Backfills currency_code and normalizes window specs.

    ``include_materialization`` is False by default so dry-run validation never
    emits the materialization block (it would provision a Lakeflow pipeline).
    Real create/apply/export paths pass True.
    """
    source = defn.get("source", "")
    if not source:
        raise ValueError("Metric view definition missing 'source' table")
    mv: dict = {"version": "1.1", "source": source}
    if defn.get("comment"):
        mv["comment"] = defn["comment"]
    if defn.get("filter"):
        mv["filter"] = defn["filter"]
    mv["dimensions"] = [
        {k: v for k, v in ((k, d.get(k)) for k in ("name", "expr", "comment", "display_name", "synonyms")) if v}
        for d in defn.get("dimensions", [])
    ]
    measures_out = []
    for m in defn.get("measures", []):
        entry = {k: v for k, v in ((k, m.get(k)) for k in ("name", "expr", "comment", "display_name", "synonyms")) if v}
        if m.get("format") and isinstance(m["format"], dict):
            fmt = dict(m["format"])
            if fmt.get("type") == "currency" and not fmt.get("currency_code"):
                fmt["currency_code"] = "USD"
            entry["format"] = fmt
        if m.get("window"):
            w = m["window"]
            if isinstance(w, str):
                try:
                    w = json.loads(w)
                except (json.JSONDecodeError, TypeError):
                    w = None
            specs = _normalize_window_specs(w)
            if specs:
                entry["window"] = specs
        measures_out.append(entry)
    mv["measures"] = measures_out
    if defn.get("joins"):
        mv["joins"] = defn["joins"]
    if include_materialization and defn.get("materialization"):
        mv["materialization"] = defn["materialization"]
    return yaml.dump(mv, Dumper=_IndentYamlDumper, default_flow_style=False, sort_keys=False)
