"""Governed UC-Domains tag publishing (roadmap UCG-4 plumbing + UCG-3 domain mode).

In Unity Catalog, native **Domains** ARE governed tags: membership is conferred by applying ONE
governed tag whose value slash-encodes the subdomain (e.g. ``banking/retail_banking``) — not the
two ungoverned ``domain`` + ``subdomain`` tags dbxmetagen writes by default. This module is a
SEPARATE, opt-in, downstream reader: it reads already-persisted domain/subdomain predictions and
builds/applies the single governed tag. It NEVER edits the existing ungoverned path
(`processing._create_table_domain_ddl_func`, the app's two-key builder), which stays the default
(backward-compat + federation).

Safety contract (roadmap Decision 1): **default-off**, **capability-gated**, **graceful no-op** —
if governed tags are disabled, the key is unconfigured, or the governed tag / ASSIGN privilege is
absent, publishing produces zero statements (or per-statement skips) and never fails a run.

Open unknowns resolved by CONFIG, not code (verify in a Domains-enabled workspace, then set):
the exact governed tag KEY (`governed_domain_tag_name`), the subdomain separator
(`governed_domain_separator`, UC convention ``Parent/Sub``), and the value length cap
(`governed_tag_max_len`, UC limit 256). Pure helpers here are unit-tested; the SQL runner is
injected so the app (warehouse `execute_sql`) and the job (`spark.sql`) share one implementation.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

AUDIT_TABLE = "governed_tag_apply_log"

GOVERNED_TAG_HINT = (
    "The governed domain tag or the ASSIGN/APPLY TAG privilege appears to be missing. Create the "
    "governed tag (UC → Tags) with the configured key and allowed values, grant the app/principal "
    "ASSIGN + APPLY TAG, or leave governed tags disabled — the ungoverned domain/subdomain tags are "
    "unaffected."
)


def _esc(s: str) -> str:
    """Escape single quotes for a SQL string literal (same convention as the existing builders)."""
    return (s or "").replace("'", "''")


def normalize_governed_domain_value(
    domain: Any, subdomain: Any, sep: str = "/", max_len: int = 256
) -> str:
    """Build the single governed value from domain (+ optional subdomain). Pure.

    ``banking`` + ``retail_banking`` -> ``banking/retail_banking``; a missing/``None`` subdomain
    yields just the domain; the result is trimmed to ``max_len`` (UC governed-tag limit). Returns
    ``""`` when there is no usable domain (caller skips that row).
    """
    d = str(domain or "").strip()
    if not d or d.lower() == "none":
        return ""
    s = str(subdomain or "").strip()
    value = f"{d}{sep}{s}" if s and s.lower() != "none" else d
    if max_len and len(value) > max_len:
        value = value[:max_len]
    return value


def resolve_fqn(row: Dict[str, Any]) -> str:
    """Return a fully-qualified ``catalog.schema.table`` from a KB/log row. Pure.

    Accepts either a pre-qualified ``table_name`` (contains two dots) or discrete
    catalog/schema/(table_short_name|table_name) parts.
    """
    tn = str(row.get("table_name") or "").strip()
    if tn.count(".") == 2:
        return tn
    cat = str(row.get("catalog") or "").strip()
    sch = str(row.get("schema") or "").strip()
    tbl = str(row.get("table_short_name") or tn).strip()
    if cat and sch and tbl:
        return f"{cat}.{sch}.{tbl}"
    return tn


def build_governed_tag_ddl(fqn: str, value: str, tag_key: str) -> str:
    """One ``ALTER TABLE … SET TAGS`` statement for the governed key. Pure."""
    return f"ALTER TABLE {fqn} SET TAGS ('{_esc(tag_key)}' = '{_esc(value)}')"


def gate(config) -> Tuple[bool, Optional[str]]:
    """Whether governed-tag publishing may proceed. Returns ``(ok, reason_if_not)``. Pure.

    Off unless ``enable_governed_tags`` is true AND a ``governed_domain_tag_name`` is set — the two
    conditions that keep an unverified/unconfigured deployment a safe no-op.
    """
    if not getattr(config, "enable_governed_tags", False):
        return False, "governed tags disabled (enable_governed_tags=false)"
    if not str(getattr(config, "governed_domain_tag_name", "") or "").strip():
        return False, "no governed_domain_tag_name configured (set it after verifying the UC Domains tag key)"
    return True, None


def build_statements(config, rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Build governed-tag statements for rows with a domain. Returns ``(to_apply, skipped)``. Pure.

    ``to_apply`` items: ``{fqn, value, ddl}``; ``skipped`` items: ``{fqn, reason}`` (no domain).
    """
    tag_key = str(getattr(config, "governed_domain_tag_name", "") or "").strip()
    sep = str(getattr(config, "governed_domain_separator", "/") or "/")
    max_len = int(getattr(config, "governed_tag_max_len", 256) or 256)
    to_apply: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for r in rows or []:
        fqn = resolve_fqn(r)
        value = normalize_governed_domain_value(r.get("domain"), r.get("subdomain"), sep, max_len)
        if not fqn or not value:
            skipped.append({"fqn": fqn, "reason": "no domain prediction"})
            continue
        to_apply.append({"fqn": fqn, "value": value, "ddl": build_governed_tag_ddl(fqn, value, tag_key)})
    return to_apply, skipped


def _is_capability_error(msg: str) -> bool:
    """True when an apply error looks like a missing governed tag / privilege (→ graceful skip)."""
    m = (msg or "").lower()
    return ("permission_denied" in m and "tag" in m) or "governed" in m or "tag policy" in m \
        or "not allowed" in m and "tag" in m


def apply_statements(run_sql: Callable[[str], Any], to_apply: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Apply built statements via an injected SQL runner. NEVER raises.

    Governed-tag capability errors (missing tag/policy or ASSIGN) are recorded as ``skipped`` with a
    hint rather than failing the run; other errors are recorded as ``errors``. Returns a summary with
    ``applied`` / ``skipped`` / ``errors`` lists. The runner is the app's ``execute_sql`` or a
    ``spark.sql`` wrapper.
    """
    applied: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for item in to_apply:
        try:
            run_sql(item["ddl"])
            applied.append({"fqn": item["fqn"], "value": item["value"]})
        except Exception as exc:  # noqa: BLE001 — never fail the run on a tag apply
            msg = str(exc)
            if _is_capability_error(msg):
                skipped.append({"fqn": item["fqn"], "value": item["value"], "reason": "capability", "hint": GOVERNED_TAG_HINT})
                logger.warning("Governed tag skipped for %s (capability): %s", item["fqn"], msg)
            else:
                errors.append({"fqn": item["fqn"], "value": item["value"], "error": msg})
                logger.error("Governed tag apply failed for %s: %s", item["fqn"], msg)
    return {"applied": applied, "skipped": skipped, "errors": errors,
            "capability_missing": bool(skipped) and not applied}


def ensure_audit_table(run_sql: Callable[[str], Any], catalog: str, schema: str) -> None:
    """Create the additive audit table if absent. Best-effort (swallows errors)."""
    fq = f"{catalog}.{schema}.{AUDIT_TABLE}"
    try:
        run_sql(
            f"CREATE TABLE IF NOT EXISTS {fq} (fqn STRING, tag_key STRING, value STRING, "
            f"status STRING, detail STRING, run_id STRING, applied_at TIMESTAMP) USING DELTA"
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not ensure %s: %s", fq, exc)


import re as _re

_IDENT = _re.compile(r"^[A-Za-z0-9_.*]+$")


def read_domain_rows(query_fn: Callable[[str], List[Dict[str, Any]]], catalog: str, schema: str,
                     table_names: str = "") -> List[Dict[str, Any]]:
    """Read latest domain/subdomain per table via an injected query runner that returns list[dict].

    Prefers ``table_knowledge_base`` (the same source the app review uses); falls back to the
    latest ``metadata_type='domain'`` rows in ``metadata_generation_log``. ``table_names`` (a comma
    list of bare or fully-qualified names, ``*`` wildcards ignored) optionally scopes the result.
    """
    scope = ""
    names = [n.strip() for n in (table_names or "").split(",") if n.strip() and "*" not in n and _IDENT.match(n.strip())]
    if names:
        shorts = ",".join(f"'{n.split('.')[-1].lower()}'" for n in names)
        fqns = ",".join(f"'{n.lower()}'" for n in names)
        scope = f" AND (lower(table_name) IN ({fqns},{shorts}))"
    kb = f"{catalog}.{schema}.table_knowledge_base"
    try:
        return query_fn(
            f"SELECT table_name, catalog, `schema`, table_short_name, domain, subdomain "
            f"FROM {kb} WHERE domain IS NOT NULL AND domain <> '' AND lower(domain) <> 'none'{scope}"
        )
    except Exception as exc:  # noqa: BLE001
        logger.info("table_knowledge_base not readable (%s); falling back to metadata_generation_log", exc)
        log = f"{catalog}.{schema}.metadata_generation_log"
        return query_fn(
            f"SELECT table_name, catalog, `schema`, domain, subdomain FROM {log} "
            f"WHERE metadata_type = 'domain' AND domain IS NOT NULL AND domain <> '' "
            f"AND lower(domain) <> 'none'{scope} QUALIFY ROW_NUMBER() OVER "
            f"(PARTITION BY table_name ORDER BY _created_at DESC) = 1"
        )


def write_audit(run_sql: Callable[[str], Any], catalog: str, schema: str, tag_key: str,
                result: Dict[str, Any], run_id: str = "") -> None:
    """Append apply outcomes to the audit table. Best-effort (never raises)."""
    fq = f"{catalog}.{schema}.{AUDIT_TABLE}"
    def _ins(fqn, value, status, detail):
        try:
            run_sql(
                f"INSERT INTO {fq} VALUES ('{_esc(fqn)}','{_esc(tag_key)}','{_esc(value)}',"
                f"'{status}','{_esc(detail)[:500]}','{_esc(run_id)}', current_timestamp())"
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("audit insert failed for %s: %s", fqn, exc)
    for a in result.get("applied", []):
        _ins(a["fqn"], a.get("value", ""), "applied", "")
    for s in result.get("skipped", []):
        _ins(s["fqn"], s.get("value", ""), "skipped", s.get("reason", ""))
    for e in result.get("errors", []):
        _ins(e["fqn"], e.get("value", ""), "error", e.get("error", ""))


def run_sync(query_fn: Callable[[str], List[Dict[str, Any]]], run_sql: Callable[[str], Any],
             config, run_id: str = "") -> Dict[str, Any]:
    """End-to-end governed-tag publish shared by the job and the app endpoint. NEVER raises.

    gate → read domain rows → build → apply → audit. Returns a summary dict (status noop/done).
    ``query_fn`` returns list[dict]; ``run_sql`` executes DDL (app: execute_sql; job: spark.sql wrapper).
    """
    ok, reason = gate(config)
    if not ok:
        logger.info("Governed tag sync no-op: %s", reason)
        return {"status": "noop", "reason": reason, "applied": 0, "skipped": 0, "errors": 0}
    cat, sch = config.catalog_name, config.schema_name
    ensure_audit_table(run_sql, cat, sch)
    rows = read_domain_rows(query_fn, cat, sch, getattr(config, "table_names", "") or "")
    to_apply, pre_skipped, _ = gate_and_build(config, rows)
    result = apply_statements(run_sql, to_apply)
    write_audit(run_sql, cat, sch, config.governed_domain_tag_name, result, run_id)
    return {
        "status": "done",
        "applied": len(result["applied"]),
        "skipped": len(result["skipped"]) + len(pre_skipped),
        "errors": len(result["errors"]),
        "capability_missing": result.get("capability_missing", False),
        "tag_key": config.governed_domain_tag_name,
        "details": result,
    }


def gate_and_build(
    config, rows: List[Dict[str, Any]], capability: Optional[Dict[str, Any]] = None
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Optional[str]]:
    """Convenience: apply the gate, then build statements. Returns ``(to_apply, skipped, reason)``.

    ``reason`` is non-None (and ``to_apply`` empty) when the feature is a no-op — disabled,
    unconfigured, or (if a ``capability`` dict is passed) the governed tag is reported absent.
    """
    ok, reason = gate(config)
    if not ok:
        return [], [{"fqn": resolve_fqn(r), "reason": reason} for r in (rows or [])], reason
    if capability is not None and not capability.get("present", True):
        reason = capability.get("reason") or "governed tag capability absent"
        return [], [{"fqn": resolve_fqn(r), "reason": reason} for r in (rows or [])], reason
    to_apply, skipped = build_statements(config, rows)
    return to_apply, skipped, None
