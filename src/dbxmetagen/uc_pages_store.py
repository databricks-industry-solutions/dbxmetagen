"""Persistence for UC Pages generation: the editable system of record + ingested source documents.

Tables (all additive, in the dbxmetagen results schema; created on first use):

* ``page_drafts``: one row per Page dbxmetagen manages. Plan, curation, written content,
  quality, and UC sync state. Users edit it in the app (or with SQL); publishing reads from it.
* ``page_evidence``: claim-level provenance: which evidence supports which statement, its
  authority (human / governed / inferred), source reference and excerpt.
* ``page_sources`` + ``page_source_chunks``: documents ingested for Page generation (glossaries,
  data dictionaries, policy docs), chunked by heading so each chunk is citable.
* ``uc_pages_log``: publish audit (one row per page per publish).

SQL is built here with **bound parameters only**; values never get interpolated into SQL text.
``run_sql(sql, params)`` is injected (app warehouse / job spark), same contract as ``uc_pages``.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

RunSql = Callable[[str, Dict[str, Any]], List[Dict[str, Any]]]
Fq = Callable[[str], str]

RUNS = "page_runs"
DRAFTS = "page_drafts"
EVIDENCE = "page_evidence"
SOURCES = "page_sources"
CHUNKS = "page_source_chunks"
LOG = "uc_pages_log"

# Columns written from Python dicts. ARRAY columns are passed as JSON and parsed with from_json.
DRAFT_COLUMNS: List[Tuple[str, str]] = [
    ("page_id", "STRING"), ("run_id", "STRING"), ("kind", "STRING"), ("display_name", "STRING"),
    ("description", "STRING"), ("body", "STRING"), ("body_cited", "STRING"), ("synonyms", "ARRAY<STRING>"),
    ("offered_evidence_ids", "ARRAY<STRING>"),
    ("domain", "STRING"), ("domain_label", "STRING"),
    ("related_tables", "ARRAY<STRING>"), ("related_metric_views", "ARRAY<STRING>"),
    ("relations", "STRING"), ("open_questions", "ARRAY<STRING>"),
    ("evidence_ids", "ARRAY<STRING>"), ("evidence_level", "STRING"), ("flags", "STRING"),
    ("selected", "BOOLEAN"), ("status", "STRING"), ("quality_score", "DOUBLE"), ("quality", "STRING"),
    ("scope_catalog", "STRING"), ("scope_schema", "STRING"), ("prompt", "STRING"),
    ("uc_page_name", "STRING"), ("uc_state", "STRING"), ("uc_links", "STRING"), ("published_hash", "STRING"),
    ("published_at", "TIMESTAMP"), ("published_by", "STRING"), ("edited_by_user", "BOOLEAN"),
    ("version", "INT"), ("created_by", "STRING"), ("created_at", "TIMESTAMP"),
    ("updated_by", "STRING"), ("updated_at", "TIMESTAMP"),
]
JSON_COLUMNS = {"relations", "flags", "quality", "uc_links"}
ARRAY_COLUMNS = {c for c, t in DRAFT_COLUMNS if t.startswith("ARRAY")}
# Fields a user may edit from the app; anything else is system-managed.
EDITABLE_FIELDS = {"kind", "display_name", "description", "body", "synonyms", "domain", "domain_label",
                   "related_tables", "related_metric_views", "relations", "selected", "open_questions"}
STATUSES = ("planned", "drafted", "published", "edited_in_uc", "discarded")

EVIDENCE_COLUMNS: List[Tuple[str, str]] = [
    ("evidence_key", "STRING"), ("page_id", "STRING"), ("run_id", "STRING"), ("evidence_id", "STRING"),
    ("section", "STRING"), ("claim", "STRING"), ("evidence_type", "STRING"), ("authority", "STRING"),
    ("source_ref", "STRING"), ("label", "STRING"), ("excerpt", "STRING"), ("created_at", "TIMESTAMP"),
]
SOURCE_COLUMNS: List[Tuple[str, str]] = [
    ("source_id", "STRING"), ("title", "STRING"), ("uri", "STRING"), ("source_type", "STRING"),
    ("scope", "STRING"), ("char_count", "BIGINT"), ("chunk_count", "INT"), ("content_hash", "STRING"),
    ("active", "BOOLEAN"), ("created_by", "STRING"), ("created_at", "TIMESTAMP"),
]
CHUNK_COLUMNS: List[Tuple[str, str]] = [
    ("chunk_id", "STRING"), ("source_id", "STRING"), ("ordinal", "INT"), ("heading", "STRING"),
    ("text", "STRING"), ("created_at", "TIMESTAMP"),
]
LOG_COLUMNS: List[Tuple[str, str]] = [
    ("request_id", "STRING"), ("page_id", "STRING"), ("page_name", "STRING"), ("display_name", "STRING"),
    ("action", "STRING"), ("state", "STRING"), ("status", "STRING"), ("error", "STRING"),
    ("linked_assets", "STRING"), ("prompt", "STRING"), ("created_by", "STRING"), ("created_at", "TIMESTAMP"),
]

RUN_COLUMNS: List[Tuple[str, str]] = [
    ("run_id", "STRING"), ("prompt", "STRING"), ("scope_catalog", "STRING"), ("scope_schema", "STRING"),
    ("scope_tables", "ARRAY<STRING>"), ("settings", "STRING"), ("evidence", "STRING"),
    ("model", "STRING"), ("created_by", "STRING"), ("created_at", "TIMESTAMP"),
]

_COMMENTS = {
    RUNS: "dbxmetagen UC Pages: generation runs with the exact evidence snapshot the model saw",
    DRAFTS: "dbxmetagen UC Pages: editable drafts, curation, quality and UC sync state",
    EVIDENCE: "dbxmetagen UC Pages: claim-level provenance for each Page",
    SOURCES: "dbxmetagen UC Pages: documents ingested as Page sources",
    CHUNKS: "dbxmetagen UC Pages: citable chunks of ingested source documents",
    LOG: "dbxmetagen UC Pages: publish audit log",
}
_COLS = {RUNS: RUN_COLUMNS, DRAFTS: DRAFT_COLUMNS, EVIDENCE: EVIDENCE_COLUMNS, SOURCES: SOURCE_COLUMNS,
         CHUNKS: CHUNK_COLUMNS, LOG: LOG_COLUMNS}
ALL_TABLES = (RUNS, DRAFTS, EVIDENCE, SOURCES, CHUNKS, LOG)


def ddl(table: str, fq: Fq) -> str:
    cols = ",\n  ".join(f"{c} {t}" for c, t in _COLS[table])
    return f"CREATE TABLE IF NOT EXISTS {fq(table)} (\n  {cols}\n) USING DELTA COMMENT '{_COMMENTS[table]}'"


def ensure_tables(run_sql: RunSql, fq: Fq, tables: Iterable[str] = ALL_TABLES) -> None:
    for t in tables:
        run_sql(ddl(t, fq), {})


def _insert(run_sql: RunSql, fq: Fq, table: str, rows: List[Dict[str, Any]], batch: int = 25) -> None:
    cols = [c for c, _ in _COLS[table]]
    types = dict(_COLS[table])
    for i in range(0, len(rows), batch):
        params, values = {}, []
        for j, r in enumerate(rows[i:i + batch]):
            ph = []
            for c in cols:
                k = f"{c}_{j}"
                params[k] = _encode(c, types[c], r.get(c))
                ph.append(_select_expr(k, types[c]))
            values.append("(" + ", ".join(ph) + ")")
        run_sql(f"INSERT INTO {fq(table)} ({', '.join(cols)}) VALUES {', '.join(values)}", params)


def save_run(run_sql: RunSql, fq: Fq, run: Dict[str, Any]) -> None:
    _insert(run_sql, fq, RUNS, [{**run, "settings": json.dumps(run.get("settings") or {}),
                                 "evidence": json.dumps(run.get("evidence") or [], ensure_ascii=False)}])


def get_run(run_sql: RunSql, fq: Fq, run_id: str) -> Optional[Dict[str, Any]]:
    rows = run_sql(f"SELECT * FROM {fq(RUNS)} WHERE run_id = :rid LIMIT 1", {"rid": run_id})
    if not rows:
        return None
    r = dict(rows[0])
    for c in ("settings", "evidence"):
        try:
            r[c] = json.loads(r.get(c) or ("{}" if c == "settings" else "[]"))
        except Exception:
            r[c] = {} if c == "settings" else []
    return r


# ---------------------------------------------------------------------------
# Value encoding (Python dict <-> bound parameters)
# ---------------------------------------------------------------------------

def _encode(col: str, typ: str, value: Any) -> Optional[str]:
    if value is None:
        return None
    if typ.startswith("ARRAY"):
        return json.dumps([str(v) for v in (value or [])], ensure_ascii=False)
    if col in JSON_COLUMNS:
        return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
    if typ == "BOOLEAN":
        return "true" if value in (True, "true", "True", 1, "1") else "false"
    return str(value)


def _select_expr(param: str, typ: str) -> str:
    if typ.startswith("ARRAY"):
        return f"from_json(:{param}, '{typ}')"
    if typ == "STRING":
        return f":{param}"
    return f"CAST(:{param} AS {typ})"


def decode_draft(row: Dict[str, Any]) -> Dict[str, Any]:
    """DB row -> Python dict (JSON columns parsed, arrays as lists, booleans as bool)."""
    out = dict(row)
    for c in JSON_COLUMNS:
        v = out.get(c)
        if isinstance(v, str) and v.strip():
            try:
                out[c] = json.loads(v)
            except Exception:
                pass
        elif v in (None, ""):
            out[c] = {} if c in ("quality", "uc_links") else []
    for c in ARRAY_COLUMNS:
        v = out.get(c)
        if isinstance(v, str):
            try:
                out[c] = json.loads(v)
            except Exception:
                out[c] = [x for x in v.split(",") if x]
        elif v is None:
            out[c] = []
    for c in ("selected", "edited_by_user"):
        if isinstance(out.get(c), str):
            out[c] = out[c].lower() == "true"
    if out.get("quality_score") not in (None, ""):
        try:
            out["quality_score"] = float(out["quality_score"])
        except (TypeError, ValueError):
            out["quality_score"] = None
    return out


# ---------------------------------------------------------------------------
# page_drafts
# ---------------------------------------------------------------------------

def build_upsert_drafts(rows: List[Dict[str, Any]], fq: Fq, *, columns: Optional[Iterable[str]] = None) -> Tuple[str, Dict[str, Any]]:
    """One MERGE for several drafts keyed on page_id. ``columns`` limits which fields are updated
    on match (default: all). All values are bound parameters. Returns (sql, params)."""
    cols = [c for c, _ in DRAFT_COLUMNS]
    types = dict(DRAFT_COLUMNS)
    update_cols = [c for c in (columns or cols) if c in types and c not in ("page_id", "created_at", "created_by")]
    params: Dict[str, Any] = {}
    selects = []
    for i, r in enumerate(rows):
        parts = []
        for c in cols:
            p = f"{c}_{i}"
            params[p] = _encode(c, types[c], r.get(c))
            parts.append(f"{_select_expr(p, types[c])} AS {c}")
        selects.append("SELECT " + ", ".join(parts))
    src = " UNION ALL ".join(selects)
    sets = ", ".join(f"t.{c} = s.{c}" for c in update_cols)
    sql = (f"MERGE INTO {fq(DRAFTS)} t USING ({src}) s ON t.page_id = s.page_id "
           f"WHEN MATCHED THEN UPDATE SET {sets} "
           f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})")
    return sql, params


def upsert_drafts(run_sql: RunSql, fq: Fq, rows: List[Dict[str, Any]], *, columns: Optional[Iterable[str]] = None,
                  batch: int = 8) -> None:
    for i in range(0, len(rows), batch):
        sql, params = build_upsert_drafts(rows[i:i + batch], fq, columns=columns)
        run_sql(sql, params)


def list_drafts(run_sql: RunSql, fq: Fq, *, run_id: str = "", status: str = "", page_ids: Optional[List[str]] = None,
                include_discarded: bool = False, limit: int = 500) -> List[Dict[str, Any]]:
    where, params = ["1 = 1"], {}
    if run_id:
        where.append("run_id = :run_id")
        params["run_id"] = run_id
    if status:
        where.append("status = :status")
        params["status"] = status
    elif not include_discarded:
        where.append("status != 'discarded'")
    if page_ids:
        ph = ", ".join(f":p{i}" for i in range(len(page_ids)))
        params.update({f"p{i}": v for i, v in enumerate(page_ids)})
        where.append(f"page_id IN ({ph})")
    rows = run_sql(f"SELECT * FROM {fq(DRAFTS)} WHERE {' AND '.join(where)} "
                   f"ORDER BY updated_at DESC, display_name LIMIT {int(limit)}", params)
    return [decode_draft(r) for r in rows]


def list_runs(run_sql: RunSql, fq: Fq, limit: int = 50) -> List[Dict[str, Any]]:
    return run_sql(
        f"SELECT run_id, max(prompt) AS prompt, max(scope_catalog) AS scope_catalog, max(scope_schema) AS scope_schema, "
        f"count(*) AS pages, sum(CASE WHEN status = 'published' THEN 1 ELSE 0 END) AS published, "
        f"min(created_at) AS created_at, max(created_by) AS created_by FROM {fq(DRAFTS)} "
        f"WHERE status != 'discarded' GROUP BY run_id ORDER BY min(created_at) DESC LIMIT {int(limit)}", {})


def apply_edit(draft: Dict[str, Any], patch: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """Apply a user edit to a draft (only EDITABLE_FIELDS). Pure. Returns (updated, changed_fields).
    Editing content after publish leaves status 'published' but the hash mismatch marks it as
    needing a re-publish (``needs_republish`` is derived by the API)."""
    out = dict(draft)
    changed = []
    for k, v in (patch or {}).items():
        if k not in EDITABLE_FIELDS:
            continue
        if out.get(k) != v:
            out[k] = v
            changed.append(k)
    if changed and any(c in changed for c in ("display_name", "description", "body", "synonyms")):
        out["edited_by_user"] = True
    return out, changed


def merge_drafts(keep: Dict[str, Any], others: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge candidates that are the same concept into ``keep``: union synonyms (others' names
    become synonyms), assets, evidence and relations; strongest evidence level wins. Pure."""
    from dbxmetagen.uc_pages import sanitize_synonyms, AUTHORITY_ORDER

    out = dict(keep)
    syn = list(keep.get("synonyms") or [])
    for o in others:
        syn += [o.get("display_name", "")] + list(o.get("synonyms") or [])
        for k in ("related_tables", "related_metric_views", "evidence_ids"):
            out[k] = list(dict.fromkeys(list(out.get(k) or []) + list(o.get(k) or [])))
        out["relations"] = list(out.get("relations") or []) + [r for r in (o.get("relations") or []) if r not in (out.get("relations") or [])]
        lvl = o.get("evidence_level")
        if AUTHORITY_ORDER.get(lvl, 0) > AUTHORITY_ORDER.get(out.get("evidence_level"), 0):
            out["evidence_level"] = lvl
    out["synonyms"] = sanitize_synonyms(syn, out.get("display_name", ""))
    return out


# ---------------------------------------------------------------------------
# page_evidence
# ---------------------------------------------------------------------------

def replace_evidence(run_sql: RunSql, fq: Fq, page_id: str, run_id: str, provenance: List[Dict[str, Any]],
                     now: str, batch: int = 40) -> None:
    """Replace a page's provenance rows (DELETE for the page, then bound INSERTs)."""
    run_sql(f"DELETE FROM {fq(EVIDENCE)} WHERE page_id = :pid", {"pid": page_id})
    rows = [{**p, "page_id": page_id, "run_id": run_id, "created_at": now, "evidence_key": f"{page_id}::{n:04d}"}
            for n, p in enumerate(provenance)]
    _insert(run_sql, fq, EVIDENCE, rows, batch=batch)


def get_evidence(run_sql: RunSql, fq: Fq, page_id: str) -> List[Dict[str, Any]]:
    return run_sql(f"SELECT evidence_id, section, claim, evidence_type, authority, source_ref, label, excerpt "
                   f"FROM {fq(EVIDENCE)} WHERE page_id = :pid ORDER BY evidence_key", {"pid": page_id})


# ---------------------------------------------------------------------------
# Source documents: chunk + store
# ---------------------------------------------------------------------------

_HEADING_RE = re.compile(r"^\s*(#{1,6}\s+.+|[A-Z][A-Za-z0-9 /&()'-]{2,80}:?)\s*$")


def parsed_document_to_text(parsed: Any) -> str:
    """ai_parse_document returns a JSON VARIANT; extract readable text (elements' content in order).
    Plain strings pass through unchanged."""
    if isinstance(parsed, str):
        s = parsed.strip()
        if not s.startswith("{"):
            return parsed
        try:
            parsed = json.loads(s)
        except Exception:
            return parsed
    if not isinstance(parsed, dict):
        return str(parsed or "")
    doc = parsed.get("document") or parsed
    parts = []
    for el in doc.get("elements") or doc.get("pages") or []:
        if isinstance(el, dict):
            c = el.get("content") or el.get("text") or ""
            t = str(el.get("type") or "").lower()
            if c:
                parts.append(f"## {c}" if t in ("title", "section_header", "heading") else str(c))
    return "\n\n".join(parts) if parts else json.dumps(parsed)[:200000]


def chunk_text(text: str, *, max_chars: int = 1500, min_chars: int = 200) -> List[Dict[str, Any]]:
    """Split a document into citable chunks: by Markdown/Title headings, then paragraphs, packing
    up to ``max_chars``. Each chunk keeps its nearest heading. Pure."""
    heading = ""
    sections: List[Tuple[str, List[str]]] = []
    cur: List[str] = []
    for line in str(text or "").splitlines():
        s = line.strip()
        is_head = bool(s) and (s.startswith("#") or (len(s) < 80 and _HEADING_RE.match(s) and not s.endswith(".")
                                                    and len(s.split()) <= 10 and not s.startswith(("-", "*", "|"))))
        if is_head and s.startswith("#"):
            if cur:
                sections.append((heading, cur))
            heading, cur = s.lstrip("#").strip(), []
        else:
            cur.append(line)
    if cur:
        sections.append((heading, cur))
    chunks: List[Dict[str, Any]] = []
    for head, lines in sections:
        paras = [p.strip() for p in re.split(r"\n\s*\n", "\n".join(lines)) if p.strip()]
        buf = ""
        for p in paras:
            while len(p) > max_chars:  # hard-split very long paragraphs
                cut = p.rfind(" ", 0, max_chars)
                cut = cut if cut > max_chars // 2 else max_chars
                if buf:
                    chunks.append({"heading": head, "text": buf})
                    buf = ""
                chunks.append({"heading": head, "text": p[:cut].strip()})
                p = p[cut:].strip()
            if buf and len(buf) + len(p) + 2 > max_chars:
                chunks.append({"heading": head, "text": buf})
                buf = p
            else:
                buf = f"{buf}\n\n{p}" if buf else p
        if buf:
            if chunks and len(buf) < min_chars and chunks[-1]["heading"] == head and len(chunks[-1]["text"]) + len(buf) < max_chars * 1.3:
                chunks[-1]["text"] += "\n\n" + buf
            else:
                chunks.append({"heading": head, "text": buf})
    for i, c in enumerate(chunks):
        c["ordinal"] = i
    return chunks


def source_id_for(title: str, text: str) -> str:
    return hashlib.sha256(f"{title}\n{text}".encode("utf-8")).hexdigest()[:16]


def store_source(run_sql: RunSql, fq: Fq, *, title: str, text: str, uri: str = "", source_type: str = "upload",
                 scope: str = "", created_by: str = "", now: str) -> Dict[str, Any]:
    """Chunk + store one document (idempotent: same title+text = same source_id, re-ingest replaces chunks)."""
    sid = source_id_for(title, text)
    chunks = chunk_text(text)
    run_sql(f"DELETE FROM {fq(CHUNKS)} WHERE source_id = :sid", {"sid": sid})
    _insert(run_sql, fq, CHUNKS, [{"chunk_id": f"{sid}:{c['ordinal']}", "source_id": sid, "ordinal": c["ordinal"],
                                   "heading": c["heading"], "text": c["text"], "created_at": now} for c in chunks])
    row = {"source_id": sid, "title": title, "uri": uri, "source_type": source_type, "scope": scope,
           "char_count": len(text), "chunk_count": len(chunks), "content_hash": sid, "active": True,
           "created_by": created_by, "created_at": now}
    scols = [c for c, _ in SOURCE_COLUMNS]
    stypes = dict(SOURCE_COLUMNS)
    params = {c: _encode(c, stypes[c], row[c]) for c in scols}
    sel = ", ".join(f"{_select_expr(c, stypes[c])} AS {c}" for c in scols)
    run_sql(f"MERGE INTO {fq(SOURCES)} t USING (SELECT {sel}) s ON t.source_id = s.source_id "
            f"WHEN MATCHED THEN UPDATE SET t.title = s.title, t.uri = s.uri, t.scope = s.scope, t.active = true, "
            f"t.chunk_count = s.chunk_count, t.char_count = s.char_count "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(scols)}) VALUES ({', '.join('s.' + c for c in scols)})", params)
    return {**row, "chunks": len(chunks)}


def list_sources(run_sql: RunSql, fq: Fq) -> List[Dict[str, Any]]:
    return run_sql(f"SELECT source_id, title, uri, source_type, scope, char_count, chunk_count, created_by, created_at "
                   f"FROM {fq(SOURCES)} WHERE active = true ORDER BY created_at DESC LIMIT 500", {})


def deactivate_source(run_sql: RunSql, fq: Fq, source_id: str) -> None:
    run_sql(f"UPDATE {fq(SOURCES)} SET active = false WHERE source_id = :sid", {"sid": source_id})


def source_urls(run_sql: RunSql, fq: Fq, chunk_ids: Iterable[str]) -> List[str]:
    """http(s) URIs of the source documents behind the given chunk ids (for SOURCE links)."""
    sids = sorted({str(c).split(":", 1)[0] for c in chunk_ids if c})
    if not sids:
        return []
    ph = ", ".join(f":s{i}" for i in range(len(sids)))
    rows = run_sql(f"SELECT uri FROM {fq(SOURCES)} WHERE source_id IN ({ph})", {f"s{i}": s for i, s in enumerate(sids)})
    return [r["uri"] for r in rows if str(r.get("uri") or "").lower().startswith(("http://", "https://"))]


def write_log(run_sql: RunSql, fq: Fq, rows: List[Dict[str, Any]]) -> None:
    if rows:
        _insert(run_sql, fq, LOG, rows)
