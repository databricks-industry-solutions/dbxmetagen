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
from typing import Any, Dict, List

import yaml

logger = logging.getLogger(__name__)

MAX_WORDS = 500
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
    except Exception:
        logger.debug("customer_context table not readable, returning empty cache")
        return []


def resolve_customer_context(
    cache: List[Dict[str, Any]],
    full_table_name: str,
    max_words: int = MAX_WORDS,
) -> str:
    """Resolve matching context entries from prefetched cache. Pure Python, no SQL."""
    if not cache:
        return ""

    parts = full_table_name.split(".")
    catalog = parts[0] if len(parts) >= 1 else ""
    schema_scope = f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else ""

    matches = []
    for row in cache:
        st = row.get("scope_type", "")
        scope = row.get("scope", "")
        hit = (
            (st == "catalog" and scope == catalog)
            or (st == "schema" and scope == schema_scope)
            or (st == "table" and scope == full_table_name)
            or (st == "pattern" and fnmatch(full_table_name, scope))
        )
        if hit:
            matches.append(row)

    if not matches:
        return ""

    matches.sort(key=lambda r: (_SCOPE_ORDER.get(r.get("scope_type", ""), 9), r.get("priority", 0)))
    combined = "\n".join(r.get("context_text", "") for r in matches)
    words = combined.split()
    return " ".join(words[:max_words]) if words else ""


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

        for entry in data.get("contexts", []):
            scope = entry.get("scope", "")
            scope_type = entry.get("scope_type", "")
            if not scope or scope_type not in ("catalog", "schema", "table", "pattern"):
                logger.warning("Skipping invalid entry in %s: scope=%s scope_type=%s", f.name, scope, scope_type)
                continue
            text = validate_context_text(entry.get("context_text", ""))
            all_rows.append({
                "context_id": _scope_id(scope),
                "scope": scope,
                "scope_type": scope_type,
                "context_text": text,
                "context_label": entry.get("context_label", ""),
                "priority": int(entry.get("priority", 0)),
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
    spark.sql(f"""
        MERGE INTO {fq} AS tgt
        USING _customer_context_seed AS src
        ON tgt.context_id = src.context_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    spark.sql("DROP VIEW IF EXISTS _customer_context_seed")
    logger.info("Seeded %d customer context entries from %s", len(all_rows), yaml_dir)
    return len(all_rows)
