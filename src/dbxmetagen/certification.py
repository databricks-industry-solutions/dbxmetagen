"""Certified-asset apply policy — shared by the generation job, the review job, and the app.

Metadata *generation* always runs; this module only gates whether generated DDL OVERWRITES the
comments/tags on a Unity Catalog asset that carries the governed ``certification_status = certified``
tag. It never alters the asset itself or its certification status -- only the comment/description
and the domain/sensitivity/ontology tags dbxmetagen produces.

``certified_apply_policy`` (variables.yml, default ``warn``):
  * ``block`` — never apply to a certified asset. Hard: no per-run override. Fails CLOSED
    (aborts the apply) if certification cannot be read, so a read error can never silently
    overwrite a certified asset. Requires the running principal to read
    ``system.information_schema.{table,column}_tags``.
  * ``warn``  — legacy behavior stays ON (certified assets ARE applied) but they are surfaced:
    the app warns before overwrite, and a per-run ``skip_certified_assets`` flag lets a user
    opt INTO skipping instead. Fails OPEN (applies) if certification cannot be read.
  * ``allow`` — no gate; apply freely (no UC read performed).

Resolution (``normalize_policy`` / ``resolve_skip_certified``) is pure and unit-tested. The
live UC read (``read_certified_targets``) is Spark-based so it works inside the metadata jobs;
the app has its own warehouse-based reader (``_read_uc_current``) and only needs the pure parts.
"""

from __future__ import annotations

import logging
import re
from typing import Iterable, List, Set, Tuple

logger = logging.getLogger(__name__)

CERT_TAG_NAME = "certification_status"
CERT_TAG_VALUE = "certified"
VALID_POLICIES = ("warn", "block", "allow")
DEFAULT_POLICY = "warn"

_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")


def normalize_policy(value) -> str:
    """Coerce any policy input to one of VALID_POLICIES, defaulting to ``warn``."""
    v = str(value or "").strip().lower()
    return v if v in VALID_POLICIES else DEFAULT_POLICY


def resolve_skip_certified(policy, skip_flag) -> bool:
    """Whether certified assets should be SKIPPED when applying.

    block -> always skip (hard). allow -> never skip. warn -> skip only if the per-run
    ``skip_certified_assets`` flag is set (default off = legacy apply-with-warning).
    """
    policy = normalize_policy(policy)
    if policy == "block":
        return True
    if policy == "allow":
        return False
    return bool(_as_bool(skip_flag))


def _as_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() == "true"
    return bool(v)


def _norm_fqn(s: str) -> str:
    """Normalize a (possibly backtick-quoted) dotted identifier to lowercase, backtick-free."""
    return (s or "").replace("`", "").strip().lower()


# --- DDL target parsing (used by the review-job file path, which has DDL strings, not a DF) --
# Column patterns are tried first; an ``ALTER TABLE ... ALTER COLUMN`` is column-level even
# though it starts with ALTER TABLE, so the table patterns must not match it.
_COL_COMMENT_ON = re.compile(r"COMMENT\s+ON\s+COLUMN\s+(.+?)\.`?([^\s`.]+)`?\s+IS\b", re.I | re.S)
_COL_ALTER = re.compile(
    r"ALTER\s+TABLE\s+(\S+)\s+ALTER\s+COLUMN\s+`?([^\s`]+?)`?\s+(?:COMMENT|SET\s+TAGS)\b",
    re.I | re.S,
)
_COL_SET_TAG_ON = re.compile(r"SET\s+TAG\s+ON\s+COLUMN\s+(.+?)\.`?([^\s`.]+)`?\s", re.I | re.S)
_TBL_COMMENT_ON = re.compile(r"COMMENT\s+ON\s+TABLE\s+(\S+)\s+IS\b", re.I | re.S)
_TBL_ALTER = re.compile(
    r"ALTER\s+TABLE\s+(\S+)\s+(?:SET\s+TAGS|UNSET\s+TAGS|SET\s+TBLPROPERTIES|ADD\s+CONSTRAINT|DROP\s+CONSTRAINT)\b",
    re.I | re.S,
)
_TBL_SET_TAG_ON = re.compile(r"SET\s+TAG\s+ON\s+TABLE\s+(\S+)\s", re.I | re.S)


def parse_ddl_target(ddl: str) -> Tuple[str, str]:
    """Return ``(table_fqn, column_or_empty)`` (both normalized) for one DDL statement.

    Returns ``("", "")`` when the statement targets no identifiable table (e.g. a view
    create), so the caller applies it unconditionally.
    """
    if not ddl:
        return "", ""
    for rx in (_COL_COMMENT_ON, _COL_ALTER, _COL_SET_TAG_ON):
        m = rx.search(ddl)
        if m:
            return _norm_fqn(m.group(1)), _norm_fqn(m.group(2))
    for rx in (_TBL_COMMENT_ON, _TBL_ALTER, _TBL_SET_TAG_ON):
        m = rx.search(ddl)
        if m:
            return _norm_fqn(m.group(1)), ""
    return "", ""


def read_certified_targets(spark, fqns: Iterable[str]) -> Tuple[Set[str], Set[Tuple[str, str]]]:
    """Read which of ``fqns`` (``catalog.schema.table``) are certified at table or column level.

    Returns ``(certified_tables, certified_columns)`` where ``certified_tables`` is a set of
    lowercased ``catalog.schema.table`` and ``certified_columns`` a set of
    ``(lowercased_fqn, lowercased_column)``. Reads ``system.information_schema.{table,column}_tags``
    grouped by (catalog, schema). Raises on read failure — the CALLER decides fail-open vs
    fail-closed per policy (block fails closed, warn fails open).
    """
    certified_tables: Set[str] = set()
    certified_columns: Set[Tuple[str, str]] = set()

    triples = []
    for f in fqns:
        parts = (f or "").split(".")
        if len(parts) == 3 and all(_IDENT_RE.match(p) for p in parts):
            triples.append((parts[0], parts[1], parts[2]))
    if not triples:
        return certified_tables, certified_columns

    target_keys = {(c.lower(), s.lower(), t.lower()) for c, s, t in triples}
    scopes = sorted({(c, s) for c, s, _ in triples})

    for cat, sch in scopes:
        # cat/sch are identifier-validated above, so literal interpolation is injection-safe.
        trows = spark.sql(
            f"SELECT lower(table_name) AS t FROM system.information_schema.table_tags "
            f"WHERE catalog_name = '{cat}' AND schema_name = '{sch}' "
            f"AND lower(tag_name) = '{CERT_TAG_NAME}' AND lower(tag_value) = '{CERT_TAG_VALUE}'"
        ).collect()
        for r in trows:
            key = (cat.lower(), sch.lower(), r["t"])
            if key in target_keys:
                certified_tables.add(".".join(key))
        crows = spark.sql(
            f"SELECT lower(table_name) AS t, lower(column_name) AS col "
            f"FROM system.information_schema.column_tags "
            f"WHERE catalog_name = '{cat}' AND schema_name = '{sch}' "
            f"AND lower(tag_name) = '{CERT_TAG_NAME}' AND lower(tag_value) = '{CERT_TAG_VALUE}'"
        ).collect()
        for r in crows:
            key = (cat.lower(), sch.lower(), r["t"])
            if key in target_keys:
                certified_columns.add((".".join(key), r["col"]))
    return certified_tables, certified_columns


def partition_ddls_by_certification(
    ddls: List[str], certified_tables: Set[str], certified_columns: Set[Tuple[str, str]]
) -> Tuple[List[str], List[str]]:
    """Split DDL strings into ``(non_certified, certified)`` by their target.

    A table-level DDL is certified iff its table is; a column-level DDL is certified iff its
    column OR its table is (a certified table protects its own column comments too). Pure.
    """
    non_certified: List[str] = []
    certified: List[str] = []
    for ddl in ddls:
        tbl, col = parse_ddl_target(ddl)
        hit = False
        if tbl:
            if col:
                hit = (tbl, col) in certified_columns or tbl in certified_tables
            else:
                hit = tbl in certified_tables
        (certified if hit else non_certified).append(ddl)
    return non_certified, certified


def gate_ddls(spark, ddls: List[str], policy, skip_flag) -> Tuple[List[str], List[str], str]:
    """Resolve the policy and return ``(to_apply, certified_hits, action)``.

    ``action`` is one of ``"skipped"`` (certified excluded from to_apply), ``"warned"``
    (certified kept in to_apply but surfaced for logging), or ``"allow"`` (no gate).
    Fail-closed on a read error under ``block``; fail-open under ``warn``.
    """
    policy = normalize_policy(policy)
    if policy == "allow" or not ddls:
        return list(ddls), [], "allow"

    skip = resolve_skip_certified(policy, skip_flag)
    targets = {parse_ddl_target(d)[0] for d in ddls}
    targets.discard("")
    try:
        cert_tables, cert_cols = read_certified_targets(spark, targets)
    except Exception as exc:  # noqa: BLE001 — policy decides how to treat a read failure
        if policy == "block":
            raise RuntimeError(
                "certified_apply_policy=block but certification could not be verified "
                f"({type(exc).__name__}: {exc}). Grant the running principal SELECT on "
                "system.information_schema.table_tags/column_tags, or set certified_apply_policy=warn."
            ) from exc
        logger.warning(
            "Could not read certification (%s: %s); applying under warn policy (fail-open).",
            type(exc).__name__, exc,
        )
        return list(ddls), [], "warned"

    non_cert, cert_hits = partition_ddls_by_certification(list(ddls), cert_tables, cert_cols)
    if skip:
        if cert_hits:
            logger.warning("Skipping %d DDL statement(s) on certified assets (policy=%s).",
                           len(cert_hits), policy)
        return non_cert, cert_hits, "skipped"
    if cert_hits:
        logger.warning(
            "WARN: applying %d DDL statement(s) to CERTIFIED assets (certified_apply_policy=warn, "
            "skip_certified_assets not set). The existing comments/tags on those assets will be "
            "overwritten; the assets and their certified status are unchanged.", len(cert_hits),
        )
    return list(ddls), cert_hits, "warned"
