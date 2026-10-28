"""Customer context enrichment for metadata generation prompts.

Stores per-catalog/schema/table/pattern context in a Delta table and resolves
matching entries at prompt time using an in-memory prefetched cache (1 SQL query
per run, 0 per table).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

logger = logging.getLogger(__name__)

# Opt-in 'vector' retrieval reads this Delta-Sync index (built by the app). Suffix is
# single-sourced with the app via the same env var so both agree on the name.
VS_INDEX_SUFFIX = os.environ.get("CONTEXT_VECTOR_INDEX", "customer_context_vs_index")

# Per-entry cap: the max words a SINGLE context entry may hold. Enforced at seed time
# (validate_context_text) and on upload (the app mirrors this). MAX_WORDS kept as a
# backward-compatible alias for external importers.
MAX_WORDS_PER_ENTRY = 500
MAX_WORDS = MAX_WORDS_PER_ENTRY
# Resolve backstop: max words across ALL matching entries combined, ~= per-entry cap x
# the four scope levels. A table that layers catalog+schema+pattern+table context (each
# within the per-entry cap) is therefore NOT silently truncated -- honoring the app's
# advertised "each entry is limited to N words" contract. When the combined context DOES
# exceed this, the LEAST-specific entries are dropped first (retention by specificity;
# see _truncate_preserving_specificity) and a warning is logged at resolve time.
MAX_TOTAL_WORDS = 2000
_SCOPE_ORDER = {"catalog": 0, "schema": 1, "pattern": 2, "table": 3}


def validate_context_text(text: str, max_words: int = MAX_WORDS) -> str:
    """Validate and truncate context text to word limit."""
    if not text or not text.strip():
        raise ValueError("context_text must be non-empty")
    words = text.split()
    if len(words) > max_words:
        logger.warning("Context text truncated from %d to %d words", len(words), max_words)
        return " ".join(words[:max_words])
    return text.strip()


def _scope_id(scope: str) -> str:
    return hashlib.sha256(scope.encode()).hexdigest()[:16]


def ensure_customer_context_table(spark: Any, catalog: str, schema: str) -> None:
    fq = f"{catalog}.{schema}.customer_context"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fq} (
            context_id    STRING NOT NULL,
            scope         STRING NOT NULL,
            scope_type    STRING NOT NULL,
            context_text  STRING NOT NULL,
            context_label STRING,
            priority      INT,
            active        BOOLEAN,
            created_by    STRING,
            created_at    TIMESTAMP,
            updated_at    TIMESTAMP
        ) USING DELTA
    """)


def prefetch_customer_context(spark: Any, catalog: str, schema: str) -> List[Dict[str, Any]]:
    """Load all active customer context rows in one query. Called once per run."""
    fq = f"{catalog}.{schema}.customer_context"
    try:
        rows = spark.sql(
            f"SELECT scope, scope_type, context_text, context_label, priority "
            f"FROM {fq} WHERE active = TRUE"
        ).collect()
        cache = [row.asDict() for row in rows]
        logger.info("Prefetched %d customer context entries", len(cache))
        return cache
    except Exception as exc:
        logger.warning("customer_context table not readable (%s: %s), returning empty cache",
                        type(exc).__name__, exc)
        return []


def _match_rows(cache: List[Dict[str, Any]], full_table_name: str) -> List[Dict[str, Any]]:
    """Return cache rows whose scope matches ``full_table_name`` (any granularity)."""
    parts = full_table_name.split(".")
    catalog = parts[0] if len(parts) >= 1 else ""
    schema_scope = f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else ""
    matches = []
    for row in cache:
        st = row.get("scope_type", "")
        scope = row.get("scope", "")
        if (
            (st == "catalog" and scope == catalog)
            or (st == "schema" and scope == schema_scope)
            or (st == "table" and scope == full_table_name)
            or (st == "pattern" and fnmatch(full_table_name, scope))
        ):
            matches.append(row)
    return matches


def _truncate_preserving_specificity(
    matches: List[Dict[str, Any]], max_total_words: int
) -> Tuple[str, List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Select which matches to keep by specificity, but EMIT them broadest-first.

    Two DELIBERATELY DECOUPLED concerns:

    * RETENTION (which entries survive a tight budget) is by VALUE -- most-specific
      first: table > pattern > schema > catalog. Under budget pressure the table-scoped
      entry survives and the least-specific boilerplate is dropped. (The original code
      kept the HEAD of a broadest-first concatenation, so it silently dropped the table
      entry -- the bug this fixes.)

    * EMISSION ORDER (the order entries appear in the prompt) is broadest-first:
      catalog -> schema -> pattern -> table, lowest-priority first within a scope. This
      matters for OBEDIENCE: a catalog/global directive must LEAD the block or the model
      ignores it in the full generation prompt. Verified on DMVM (epic_emr_demo.allergy,
      3 active directives): catalog-LAST -> 0/3 obeyed; catalog-FIRST -> 2/3 obeyed. An
      earlier version emitted most-specific-first and regressed obedience to 0/3.

    KNOWN TRADEOFF (retention vs. obedience): because retention is most-specific-first,
    a tight budget drops the CATALOG/global directive FIRST -- i.e. the very entry the
    broadest-first emission exists to make the model obey. This only bites above the
    MAX_TOTAL_WORDS backstop (many near-cap entries on one table), which real customer
    context does not approach; specificity-retention (table caveat must survive) was the
    deliberate choice. Revisit only if that budget is realistically hit.

    Entries are kept WHOLE (an atomic caveat is never half-emitted). When an entry does
    not fit, it is dropped but scanning CONTINUES -- a later, smaller entry may still fit.
    NOTE: continue-scanning is by remaining budget, so within one scope a smaller lower-
    priority entry can be kept while a larger higher-priority one is dropped (priority
    orders emission, and is a retention tiebreaker only among same-size entries).

    Returns ``(text, dropped, partial)``:
      - ``dropped``  -- rows fully excluded (NOT injected at all).
      - ``partial``  -- the single most-specific row whose head was kept because it alone
        exceeded the whole budget (so the result isn't empty); reported separately from
        ``dropped`` since it IS partially injected. Normally None (per-entry cap < budget).
    Pure -- no SQL, no logging -- unit-testable in isolation.
    """
    # RETENTION pass: most-specific-first, so the table entry survives a tight budget.
    by_specificity = sorted(
        matches,
        key=lambda r: (
            -_SCOPE_ORDER.get(r.get("scope_type", ""), -1),
            -int(r.get("priority") or 0),
        ),
    )
    kept_rows: List[Dict[str, Any]] = []
    dropped: List[Dict[str, Any]] = []
    partial: Optional[Dict[str, Any]] = None
    partial_text: str = ""
    remaining = max_total_words
    for r in by_specificity:
        entry_words = str(r.get("context_text") or "").split()
        if not entry_words:
            continue
        if len(entry_words) <= remaining:
            kept_rows.append(r)                          # entry kept whole (atomic caveat)
            remaining -= len(entry_words)
        elif not kept_rows and partial is None:
            # Most-specific entry alone exceeds the whole budget: keep its head so the
            # result isn't empty. Partially injected -> tracked as `partial`, not dropped.
            partial = r
            partial_text = " ".join(entry_words[:remaining])
            remaining = 0
        else:
            # Doesn't fit whole. Drop it but keep scanning: a later, smaller entry may
            # still fit the remaining budget.
            dropped.append(r)

    # EMISSION pass: broadest-first (general context leads, specific refinements follow).
    emit = sorted(
        kept_rows,
        key=lambda r: (
            _SCOPE_ORDER.get(r.get("scope_type", ""), 99),
            int(r.get("priority") or 0),
        ),
    )
    texts = [" ".join(str(r.get("context_text") or "").split()) for r in emit]
    if partial is not None:
        texts.append(partial_text)                        # most-specific -> emitted last
    return "\n".join(texts), dropped, partial


_TOKEN_RE = re.compile(r"[a-z0-9_]+")


def _tokens(text: str) -> set:
    return set(_TOKEN_RE.findall((text or "").lower()))


def _lexical_score(signature_tokens: set, entry: Dict[str, Any]) -> float:
    """Relevance of one entry to a table signature = token overlap, length-normalized. Pure."""
    etoks = _tokens(f"{entry.get('context_text', '')} {entry.get('context_label', '')}")
    if not etoks or not signature_tokens:
        return 0.0
    return len(signature_tokens & etoks) / (len(etoks) ** 0.5)


def _select_ranked(
    matches: List[Dict[str, Any]], signature: str, top_k: int
) -> List[Dict[str, Any]]:
    """Keep the ``top_k`` in-scope entries most relevant to the table (lexical overlap), but
    ALWAYS retain catalog-scope entries (global directives are meant to apply regardless of
    lexical similarity). Pure. ``matches`` already passed the hierarchy filter -- this only
    thins a crowded scope; it never crosses scopes. No-op when matches already fit top_k.
    """
    sig = _tokens(signature)
    if not sig or top_k <= 0:
        return matches
    always = [m for m in matches if m.get("scope_type") == "catalog"]
    rest = [m for m in matches if m.get("scope_type") != "catalog"]
    if len(rest) <= top_k:
        return matches   # nothing to thin
    rest.sort(key=lambda m: (
        -_lexical_score(sig, m),
        _SCOPE_ORDER.get(m.get("scope_type", ""), 99),   # tie-break: more specific first
        -int(m.get("priority") or 0),
    ))
    # top_k applies to the ranked non-catalog entries; catalog directives are kept on top.
    return always + rest[:top_k]


def resolve_customer_context_with_report(
    cache: List[Dict[str, Any]],
    full_table_name: str,
    max_words: int = MAX_TOTAL_WORDS,
    mode: str = "exact",
    signature: str = "",
    top_k: int = 8,
) -> Tuple[str, List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Like ``resolve_customer_context`` but also returns what the budget cut.

    Returns ``(text, dropped, partial)`` -- see ``_truncate_preserving_specificity``.
    Both are empty/None unless the combined matching context exceeded ``max_words``.
    ``mode='ranked'`` first thins a crowded scope to the ``top_k`` entries most lexically
    relevant to ``signature`` (the table's name + columns), keeping catalog directives; the
    hierarchy filter and broadest-first emission are unchanged. Pure Python, no SQL.
    """
    if not cache:
        return "", [], None
    matches = _match_rows(cache, full_table_name)
    if not matches:
        return "", [], None
    if mode == "ranked":
        matches = _select_ranked(matches, signature, top_k)
    return _truncate_preserving_specificity(matches, max_words)


def resolve_customer_context(
    cache: List[Dict[str, Any]],
    full_table_name: str,
    max_words: int = MAX_TOTAL_WORDS,
    mode: str = "exact",
    signature: str = "",
    top_k: int = 8,
) -> str:
    """Resolve matching context entries from the prefetched cache. Pure Python, no SQL.

    Entries are emitted broadest-first (catalog -> schema -> pattern -> table) so a
    global directive leads the block, and concatenated up to ``max_words`` (the combined
    budget across all matching granularities); retention under budget pressure is
    most-specific-first (see ``_truncate_preserving_specificity``). If the budget forces
    content to be dropped or truncated, a warning is logged naming the affected scopes --
    resolve-time truncation is no longer silent.

    ``mode='ranked'`` (opt-in) first thins a crowded scope to the ``top_k`` entries most
    lexically relevant to ``signature`` (the table name + columns), always keeping catalog
    directives; the hierarchy filter and emission order are unchanged. Default ``'exact'`` is
    the historical behavior.
    """
    text, dropped, partial = resolve_customer_context_with_report(
        cache, full_table_name, max_words, mode=mode, signature=signature, top_k=top_k
    )
    if dropped or partial is not None:
        affected = list(dropped)
        if partial is not None:
            affected.append(partial)
        logger.warning(
            "customer_context truncated for %s: kept %d word(s) (budget %d); %d entr%s "
            "cut [%s]%s. Generated metadata may omit that context.",
            full_table_name,
            len(text.split()),
            max_words,
            len(affected),
            "y" if len(affected) == 1 else "ies",
            ", ".join(f"{r.get('scope_type', '?')}:{r.get('scope', '?')}" for r in affected),
            " (most-specific entry partially kept)" if partial is not None else "",
        )
    return text


# ---------------------------------------------------------------------------
# Opt-in 'vector' retrieval (scalable): per-table scope-prefiltered HYBRID Vector Search.
#
# Instead of loading the whole customer_context table into memory and scanning it per table,
# `vector` mode queries the customer_context_vs_index for JUST this table -- with the scope
# hierarchy pushed into the index as a PREFILTER so the search never scans out-of-scope rows,
# and HYBRID (semantic ANN + BM25 keyword) ranking so exact tokens (table/column names, codes)
# are caught alongside meaning. Pattern-scoped entries (wildcards, not an equality IN) are few
# and are supplied from a small prefetched pattern cache. Everything then flows through the SAME
# `_truncate_preserving_specificity` budget/ordering as every other mode.
#
# FAIL-OPEN: any failure (index missing/not-ready, VS error, empty result) raises so the caller
# falls back to a BOUNDED per-table SQL read -- never the whole-table load, and never a broken run.
# ---------------------------------------------------------------------------

_CC_VS_COLUMNS = ["context_id", "context_text", "scope", "scope_type", "context_label", "priority"]


def context_vs_index_name(catalog: str, schema: str) -> str:
    return f"{catalog}.{schema}.{VS_INDEX_SUFFIX}"


def _exact_scopes(full_table_name: str) -> List[str]:
    """The hierarchy scopes expressible as equality for a table: catalog, catalog.schema, table."""
    parts = full_table_name.split(".")
    scopes: List[str] = []
    if len(parts) >= 1 and parts[0]:
        scopes.append(parts[0])
    if len(parts) >= 2 and parts[1]:
        scopes.append(f"{parts[0]}.{parts[1]}")
    if len(parts) >= 3 and parts[2]:
        scopes.append(full_table_name)
    return scopes


def prefetch_customer_context_patterns(spark: Any, catalog: str, schema: str) -> List[Dict[str, Any]]:
    """Load ONLY pattern-scoped active rows (small, bounded). Used by 'vector' mode so wildcard
    scopes -- which can't be an equality prefilter -- are still honored without loading the table."""
    fq = f"{catalog}.{schema}.customer_context"
    try:
        rows = spark.sql(
            f"SELECT scope, scope_type, context_text, context_label, priority "
            f"FROM {fq} WHERE active = TRUE AND scope_type = 'pattern'"
        ).collect()
        return [row.asDict() for row in rows]
    except Exception as exc:
        logger.warning("pattern-context prefetch failed (%s: %s); patterns omitted in vector mode",
                       type(exc).__name__, exc)
        return []


def _query_context_vs_index(
    catalog: str, schema: str, full_table_name: str, signature: str, top_k: int,
) -> List[Dict[str, Any]]:
    """Scope-prefiltered HYBRID query against the context VS index. Returns row dicts (the
    requested columns only). Raises on any failure so the caller can fall back."""
    from databricks.sdk import WorkspaceClient

    idx = context_vs_index_name(catalog, schema)
    scopes = _exact_scopes(full_table_name)
    if not scopes:
        return []
    filters = json.dumps({"scope": scopes, "active": True})
    w = WorkspaceClient()
    resp = w.vector_search_indexes.query_index(
        index_name=idx,
        columns=_CC_VS_COLUMNS,
        query_text=signature or full_table_name,
        query_type="HYBRID",
        num_results=max(int(top_k or 8), 1),
        filters_json=filters,
    )
    # Map data_array rows to dicts via the manifest column names (a trailing score column is
    # ignored by keying on the names we asked for).
    col_names: List[str] = []
    try:
        col_names = [c.name for c in resp.manifest.columns]  # type: ignore[attr-defined]
    except Exception:
        col_names = list(_CC_VS_COLUMNS)
    data = []
    try:
        data = resp.result.data_array or []  # type: ignore[attr-defined]
    except Exception:
        data = []
    out: List[Dict[str, Any]] = []
    for row in data:
        d = dict(zip(col_names, row))
        out.append({k: d.get(k) for k in _CC_VS_COLUMNS})
    return out


def _dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    uniq = []
    for r in rows:
        key = (r.get("scope"), r.get("scope_type"), r.get("context_text"))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)
    return uniq


def resolve_customer_context_vector(
    spark: Any,
    catalog: str,
    schema: str,
    full_table_name: str,
    signature: str,
    top_k: int = 8,
    max_words: int = MAX_TOTAL_WORDS,
    pattern_cache: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Opt-in scalable retrieval. Tries the scope-prefiltered HYBRID VS query (+ matching
    patterns from ``pattern_cache``); on ANY failure falls back to a BOUNDED per-table SQL read.
    Never loads the whole customer_context table. Result is budgeted/ordered identically to the
    other modes, so only retrieval differs."""
    pattern_matches = _match_rows(pattern_cache or [], full_table_name)
    try:
        vs_rows = _query_context_vs_index(catalog, schema, full_table_name, signature, top_k)
        matches = _dedupe_rows(list(vs_rows) + list(pattern_matches))
        source = "vector"
    except Exception as exc:
        logger.warning(
            "customer_context vector retrieval unavailable for %s (%s: %s); "
            "falling back to a bounded scope-filtered read.",
            full_table_name, type(exc).__name__, exc,
        )
        matches = _match_rows(_bounded_context_read(spark, catalog, schema, full_table_name),
                              full_table_name)
        source = "fallback"
    if not matches:
        return ""
    text, dropped, partial = _truncate_preserving_specificity(matches, max_words)
    if dropped or partial is not None:
        logger.info("customer_context[%s] budget trimmed for %s (kept %d words).",
                    source, full_table_name, len(text.split()))
    return text


def _bounded_context_read(
    spark: Any, catalog: str, schema: str, full_table_name: str
) -> List[Dict[str, Any]]:
    """Fallback for vector mode: read ONLY the rows that could match this table -- the exact
    hierarchy scopes plus all (few) patterns -- never the whole table. Scope values are UC
    identifiers; quote-escaped for safe interpolation."""
    fq = f"{catalog}.{schema}.customer_context"
    scopes = _exact_scopes(full_table_name)
    in_list = ", ".join("'" + s.replace("'", "''") + "'" for s in scopes) or "''"
    try:
        rows = spark.sql(
            f"SELECT scope, scope_type, context_text, context_label, priority FROM {fq} "
            f"WHERE active = TRUE AND (scope IN ({in_list}) OR scope_type = 'pattern')"
        ).collect()
        return [row.asDict() for row in rows]
    except Exception as exc:
        logger.warning("bounded context read failed for %s (%s: %s)",
                       full_table_name, type(exc).__name__, exc)
        return []


def seed_customer_context_table(
    spark: Any, catalog: str, schema: str, yaml_dir: str
) -> int:
    """Read YAML files from a directory and MERGE into customer_context Delta table."""
    ensure_customer_context_table(spark, catalog, schema)
    fq = f"{catalog}.{schema}.customer_context"
    now = datetime.now(timezone.utc).isoformat()
    all_rows: List[Dict[str, Any]] = []

    yaml_path = Path(yaml_dir)
    if not yaml_path.is_dir():
        logger.warning("customer_context YAML dir not found: %s", yaml_dir)
        return 0

    for f in sorted(yaml_path.glob("*.yaml")) + sorted(yaml_path.glob("*.yml")):
        try:
            data = yaml.safe_load(f.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("Failed to parse %s: %s", f, e)
            continue

        # `data or {}` guards an empty/whitespace/all-comment file (safe_load -> None);
        # `... or []` guards an explicit `contexts:` with no value (parses to None).
        for entry in (data or {}).get("contexts") or []:
            scope = entry.get("scope", "")
            scope_type = entry.get("scope_type", "")
            if not scope or scope_type not in ("catalog", "schema", "table", "pattern"):
                logger.warning("Skipping invalid entry in %s: scope=%s scope_type=%s", f.name, scope, scope_type)
                continue
            # Tolerate a missing/blank/non-numeric priority (YAML `priority:` -> None,
            # or a typo like `priority: high`) rather than aborting the whole seed.
            try:
                priority = int(entry.get("priority") or 0)
            except (TypeError, ValueError):
                logger.warning("Non-numeric priority in %s for scope=%s; defaulting to 0", f.name, scope)
                priority = 0
            text = validate_context_text(entry.get("context_text", ""))
            all_rows.append({
                "context_id": _scope_id(scope),
                "scope": scope,
                "scope_type": scope_type,
                "context_text": text,
                "context_label": entry.get("context_label", ""),
                "priority": priority,
                "active": True,
                "created_by": "yaml_seed",
                "created_at": now,
                "updated_at": now,
            })

    if not all_rows:
        logger.info("No customer context entries found in %s", yaml_dir)
        return 0

    from pyspark.sql import Row
    rows = [Row(**r) for r in all_rows]
    df = spark.createDataFrame(rows)
    df.createOrReplaceTempView("_customer_context_seed")

    # MERGE: Upserts YAML-derived rows into `{catalog}.{schema}.customer_context`, matching
    # on deterministic `context_id` (`_scope_id(scope)`).
    # WHY: Operators manage curated prompts/snippets (catalog/schema/table/pattern scoped)
    # in Git-controlled YAML; merging into Delta lets runtime enrichment (`resolve_*`) read
    # consistent UC state without manual deletes when files change.
    # COEXISTENCE with the app UI (same table, same context_id key): the WHEN MATCHED
    # branch updates only the YAML-authored content fields (text/label/priority) + updated_at,
    # and deliberately does NOT touch `active`, `created_at`, or `created_by`. This preserves a
    # UI soft-delete (`active=FALSE`, set by DELETE /api/customer-context/{id}), the original
    # creation timestamp, AND the original provenance/author across a re-seed, so YAML seeding
    # is genuinely idempotent and does not clobber operator edits. `scope`/`scope_type` are
    # derived from `context_id` (the merge key) so they are invariant and need no update.
    # New scopes still INSERT with active=TRUE and created_by='yaml_seed'.
    # TRADEOFFS: rows removed from YAML are not retired here (they linger until deactivated in
    # the UI or cleaned separately). Explicit column lists must track schema changes.

    spark.sql(f"""
        MERGE INTO {fq} AS tgt
        USING _customer_context_seed AS src
        ON tgt.context_id = src.context_id
        WHEN MATCHED THEN UPDATE SET
            tgt.context_text = src.context_text,
            tgt.context_label = src.context_label,
            tgt.priority = src.priority,
            tgt.updated_at = src.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    spark.sql("DROP VIEW IF EXISTS _customer_context_seed")
    logger.info("Seeded %d customer context entries from %s", len(all_rows), yaml_dir)
    return len(all_rows)


# ---------------------------------------------------------------------------
# Customer acronyms -> acro_content (app-managed, scope-aware)
#
# The global baseline in variables.yml:acro_content is never touched -- these are an
# additional, per-catalog/schema layer managed from the app UI and merged ON TOP of the
# baseline at run time (schema scope beats catalog scope beats baseline). Stored in a Delta
# table so the app and the job share one source of truth; resolved once per run.
# ---------------------------------------------------------------------------

ACRONYMS_TABLE = "customer_acronyms"


def ensure_customer_acronyms_table(spark: Any, catalog: str, schema: str) -> None:
    fq = f"{catalog}.{schema}.{ACRONYMS_TABLE}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fq} (
            acronym_id   STRING NOT NULL,
            scope        STRING NOT NULL,
            scope_type   STRING NOT NULL,
            abbreviation STRING NOT NULL,
            expansion    STRING NOT NULL,
            notes        STRING,
            active       BOOLEAN,
            created_by   STRING,
            created_at   TIMESTAMP,
            updated_at   TIMESTAMP
        ) USING DELTA
    """)


def prefetch_acronyms(spark: Any, catalog: str, schema: str) -> List[Dict[str, Any]]:
    """Load active acronym rows once per run. Tolerant of a missing table (returns [])."""
    fq = f"{catalog}.{schema}.{ACRONYMS_TABLE}"
    try:
        rows = spark.sql(
            f"SELECT scope, scope_type, abbreviation, expansion, notes "
            f"FROM {fq} WHERE active = TRUE"
        ).collect()
        return [r.asDict() for r in rows]
    except Exception as exc:
        logger.info("customer_acronyms not readable (%s), skipping acronym merge", type(exc).__name__)
        return []


def _acro_source_scopes(source_tables: Any) -> Tuple[set, set]:
    """Derive the set of source catalogs and ``catalog.schema`` scopes from the tables
    being documented in this run. Wildcards are fine: ``cat.sch.*`` still yields catalog
    ``cat`` and schema ``cat.sch``."""
    catalogs: set = set()
    schemas: set = set()
    for t in (source_tables or []):
        parts = str(t).split(".")
        if len(parts) >= 1 and parts[0]:
            catalogs.add(parts[0])
        if len(parts) >= 2 and parts[0] and parts[1]:
            schemas.add(f"{parts[0]}.{parts[1]}")
    return catalogs, schemas


def _acro_scope_matches(row: Dict[str, Any], source_catalogs: set, source_schemas: set) -> bool:
    """Does an acronym row apply to any SOURCE table in this run?

    Scope is matched against the tables being documented (their catalog /
    ``catalog.schema``), NOT the output/results schema -- consistent with how
    ``customer_context`` resolves scope (``_match_rows``). A catalog-scoped acronym
    applies when any source table is in that catalog; a schema-scoped acronym applies
    when any source table is in that schema.
    """
    st = (row.get("scope_type") or "").lower()
    scope = row.get("scope") or ""
    if st == "catalog":
        return scope in source_catalogs
    if st == "schema":
        return scope in source_schemas
    return False


def _coerce_acro_dict(base: Any) -> Dict[str, Any]:
    """Return a mutable dict copy of acro_content. The baseline ships as a JSON string inside
    variables.yml; parse it so scoped entries can be layered in. If it cannot be parsed, start
    from an empty dict (so uploaded acronyms still apply) and warn rather than corrupt output."""
    import json
    if isinstance(base, dict):
        return dict(base)
    if isinstance(base, str) and base.strip():
        try:
            parsed = json.loads(base)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            logger.warning("acro_content is not JSON-parseable; scoped acronyms will replace it")
    return {}


def merge_acronyms_into_acro_content(
    base: Any, rows: List[Dict[str, Any]], source_tables: Any
) -> Any:
    """Overlay scoped acronym rows onto the baseline acro_content. Returns the baseline
    unchanged (same object) when no scoped rows match, so a deployment with no uploaded
    acronyms behaves exactly as before. Schema-scoped entries win over catalog-scoped.

    ``source_tables`` is the list of tables being documented in this run
    (``config.table_names``); acronym scope is matched against those SOURCE tables'
    catalog / ``catalog.schema`` -- NOT the output/results schema. (Historically this
    matched against ``config.schema_name``, the OUTPUT schema, so a schema-scoped acronym
    on the data schema never matched -- issue surfaced in field testing.)"""
    source_catalogs, source_schemas = _acro_source_scopes(source_tables)
    scoped = [r for r in rows if _acro_scope_matches(r, source_catalogs, source_schemas)]
    if not scoped:
        return base
    merged = _coerce_acro_dict(base)
    # catalog scope first, schema scope second so the more specific one wins on conflict
    scoped.sort(key=lambda r: 0 if (r.get("scope_type") or "").lower() == "catalog" else 1)
    for r in scoped:
        abbr = (r.get("abbreviation") or "").strip()
        if not abbr:
            continue
        expansion = (r.get("expansion") or "").strip()
        notes = (r.get("notes") or "").strip()
        merged[abbr] = f"{expansion} ({notes})" if notes else expansion
    return merged
