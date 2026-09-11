"""Customer context enrichment for metadata generation prompts.

Stores per-catalog/schema/table/pattern context in a Delta table and resolves
matching entries at prompt time using an in-memory prefetched cache (1 SQL query
per run, 0 per table).
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

logger = logging.getLogger(__name__)

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
) -> Tuple[str, List[Dict[str, Any]]]:
    """Order matches most-specific-first, then concatenate up to ``max_total_words``.

    Retention is by VALUE: table > pattern > schema > catalog, highest-priority first
    within a scope. Under budget pressure the table-scoped entry survives and the
    least-specific boilerplate is dropped -- the reverse of the original code, which
    sorted broadest-first and kept the head, silently dropping the table entry.

    Returns ``(text, dropped)`` where ``dropped`` lists the rows fully or partially cut
    to fit the budget. Pure -- no SQL, no logging -- unit-testable in isolation.
    """
    ordered = sorted(
        matches,
        key=lambda r: (
            -_SCOPE_ORDER.get(r.get("scope_type", ""), -1),
            -int(r.get("priority") or 0),
        ),
    )
    kept: List[str] = []
    dropped: List[Dict[str, Any]] = []
    remaining = max_total_words
    stop = False  # once a whole entry can't fit, drop it and everything less specific
    for r in ordered:
        entry_words = str(r.get("context_text") or "").split()
        if not entry_words:
            continue
        if stop:
            dropped.append(r)
        elif len(entry_words) <= remaining:
            kept.append(" ".join(entry_words))          # entry kept whole (atomic caveat)
            remaining -= len(entry_words)
        elif not kept:
            # Most-specific entry alone exceeds the whole budget: keep its head so the
            # result isn't empty, then stop. (Per-entry cap normally prevents this.)
            kept.append(" ".join(entry_words[:remaining]))
            remaining = 0
            dropped.append(r)
            stop = True
        else:
            # Doesn't fit whole, and it is less specific than what we've kept: drop it
            # (and all remaining) rather than emit a dangling half-caveat.
            dropped.append(r)
            stop = True
    return "\n".join(kept), dropped


def resolve_customer_context_with_report(
    cache: List[Dict[str, Any]],
    full_table_name: str,
    max_words: int = MAX_TOTAL_WORDS,
) -> Tuple[str, List[Dict[str, Any]]]:
    """Like ``resolve_customer_context`` but also returns the dropped rows.

    ``dropped`` is empty unless the combined matching context exceeded ``max_words``
    (in which case the least-specific entries were cut). Pure Python, no SQL -- used by
    the app's resolve-preview endpoint to show what would be truncated.
    """
    if not cache:
        return "", []
    matches = _match_rows(cache, full_table_name)
    if not matches:
        return "", []
    return _truncate_preserving_specificity(matches, max_words)


def resolve_customer_context(
    cache: List[Dict[str, Any]],
    full_table_name: str,
    max_words: int = MAX_TOTAL_WORDS,
) -> str:
    """Resolve matching context entries from the prefetched cache. Pure Python, no SQL.

    Entries are ordered most-specific-first and concatenated up to ``max_words`` (the
    combined budget across all matching granularities). If the budget forces content to
    be dropped, a warning is logged naming the dropped scopes -- resolve-time truncation
    is no longer silent.
    """
    text, dropped = resolve_customer_context_with_report(cache, full_table_name, max_words)
    if dropped:
        logger.warning(
            "customer_context truncated for %s: kept %d word(s) (budget %d); dropped %d "
            "less-specific entr%s [%s]. Generated metadata may omit that context.",
            full_table_name,
            len(text.split()),
            max_words,
            len(dropped),
            "y" if len(dropped) == 1 else "ies",
            ", ".join(f"{r.get('scope_type', '?')}:{r.get('scope', '?')}" for r in dropped),
        )
    return text


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
