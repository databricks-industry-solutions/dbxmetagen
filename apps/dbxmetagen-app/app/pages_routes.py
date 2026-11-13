"""UC Pages generation API (router). Opt-in via ENABLE_UC_PAGES; logic lives in dbxmetagen.uc_pages*.

Flow: sources (ingest documents) → plan (candidates, read-only toward UC) → curate drafts (edit /
merge / discard / select) → write (+evaluate, background) → evaluate on demand → publish (the ONLY
external write, explicit user action, Draft by default). Every step persists to the editable
``page_*`` tables in the app's results schema, which are the system of record.

Helpers from api_server are imported lazily (the app runs as ``uvicorn api_server:app``, so this
module is imported from api_server and the back-import resolves to the loaded module).
"""

from __future__ import annotations

import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from dbxmetagen import uc_pages as up
from dbxmetagen import uc_pages_pipeline as pp
from dbxmetagen import uc_pages_store as us

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/pages", tags=["uc-pages"])

ENABLED = os.environ.get("ENABLE_UC_PAGES", "false").strip().lower() == "true"
_DOMAIN_RE = re.compile(r"^domains/[A-Za-z0-9_-]+$")
_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_tasks: Dict[str, Dict[str, Any]] = {}
_status_cache: Dict[str, Any] = {}


def _srv():
    import api_server  # noqa: WPS433 (lazy back-import, see module doc)
    return api_server


def _require():
    if not ENABLED:
        raise HTTPException(403, detail="UC Pages generation is disabled. Set the bundle variable enable_uc_pages=true "
                                        "(ENABLE_UC_PAGES on the app) and redeploy.")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _user() -> str:
    try:
        return _srv()._get_effective_client().current_user.me().user_name
    except Exception:
        return ""


def _api_do(method: str, path: str, query: Optional[dict] = None, body: Optional[dict] = None):
    """Pages/Domains REST as the EFFECTIVE identity (the user under OBO, else the app SP)."""
    return _srv()._get_effective_client().api_client.do(method, path, query=query, body=body)


def _run_sql(sql: str, params: Optional[dict] = None) -> List[dict]:
    srv = _srv()
    return srv.execute_sql(sql, timeout=120, parameters=[
        srv.StatementParameterListItem(name=k, value=None if v is None else str(v)) for k, v in (params or {}).items()])


def _fq(t: str) -> str:
    return _srv().fq(t)


def _llm_factory(model: Optional[str] = None):
    """Batched AI_QUERY: N prompts in ONE statement (parallel on the warehouse). Plain AI_QUERY with
    failOnError => false (no responseFormat; not all endpoints support it)."""
    srv = _srv()
    endpoint = srv._safe_model_endpoint(model or os.environ.get("LLM_MODEL"))

    def llm(prompts: List[str]):
        out = []
        for i in range(0, len(prompts), 10):  # bounded statement size
            chunk = prompts[i:i + 10]
            vals = ", ".join(f"({j}, :p{j})" for j in range(len(chunk)))
            sql = ("SELECT k, q.r.result AS text, q.r.errorMessage AS err FROM (SELECT CAST(k AS INT) AS k, "
                   f"AI_QUERY('{endpoint}', p, failOnError => false) AS r FROM VALUES {vals} AS t(k, p)) q ORDER BY k")
            try:
                rows = srv.execute_sql(sql, timeout=900, parameters=[
                    srv.StatementParameterListItem(name=f"p{j}", value=p[:400000]) for j, p in enumerate(chunk)])
                by_k = {int(r["k"]): (r.get("text") or "", r.get("err")) for r in rows}
            except Exception as exc:
                by_k = {j: ("", str(exc)[:500]) for j in range(len(chunk))}
            out += [by_k.get(j, ("", "no response")) for j in range(len(chunk))]
        return out
    return llm


def _judge_models() -> List[str]:
    """Reviewer endpoint(s). Comma-separated = panel (scores averaged). 'none'/blank = the writer
    model. A different family than the writer avoids self-preference bias."""
    raw = (os.environ.get("PAGES_JUDGE_MODEL") or "").strip()
    models = [m.strip() for m in raw.split(",") if m.strip() and m.strip().lower() != "none"]
    return models or [m for m in [os.environ.get("LLM_MODEL", "")] if m]


def _judge():
    models = _judge_models()
    if not models:
        return None
    judges = [_llm_factory(m) for m in models]
    return judges if len(judges) > 1 else judges[0]


def _probe(refresh: bool = False) -> dict:
    key = _srv()._obo_token_var.get(None) or "sp"
    hit = _status_cache.get(key)
    if refresh or not hit or time.time() - hit[0] > 300:
        _status_cache[key] = (time.time(), up.probe(_api_do))
    return _status_cache[key][1]


def _ensure_tables():
    try:
        us.ensure_tables(_run_sql, _fq)
    except Exception as exc:
        raise HTTPException(500, detail=f"Could not create the Pages tables in the results schema: {exc}")


def _view(d: dict) -> dict:
    """Draft as returned to the UI, with derived flags."""
    d = dict(d)
    d["needs_republish"] = bool(d.get("uc_page_name")) and bool(d.get("published_hash")) and \
        up.content_hash(d.get("display_name"), d.get("description"), d.get("body"), d.get("synonyms")) != d["published_hash"]
    return d


# ---------------------------------------------------------------- status + reference data

@router.get("/status")
def status(refresh: bool = False):
    srv = _srv()
    if not ENABLED:
        return {"enabled": False, "available": False, "domains": [], "reason": "Disabled (enable_uc_pages=false).",
                "identity": srv._auth_identity_label()}
    return {"enabled": True, **_probe(refresh), "identity": srv._auth_identity_label(),
            "writer_model": os.environ.get("LLM_MODEL", ""), "judge_models": _judge_models()}


@router.get("/kinds")
def kinds():
    return {"kinds": [{"key": k, **{f: v[f] for f in ("label", "help", "sections", "default")}} for k, v in up.KINDS.items()]}


@router.get("/uc")
def uc_pages(domain: str = ""):
    _require()
    if domain and not _DOMAIN_RE.match(domain):
        raise HTTPException(400, detail="domain must look like domains/{id}")
    try:
        pages = up.list_pages(_api_do, domain or None, limit=1000)
    except Exception as exc:
        raise HTTPException(502, detail=f"Could not list Pages: {_srv()._sanitize_sdk_error(exc)}")
    return {"pages": [{k: p.get(k) for k in ("name", "display_name", "domain", "state", "update_time")} for p in pages]}


# ---------------------------------------------------------------- sources (ingestion)

@router.get("/sources")
def list_sources():
    _require()
    _ensure_tables()
    return {"sources": us.list_sources(_run_sql, _fq)}


@router.post("/sources")
def add_source(file: Optional[UploadFile] = File(None), volume_path: str = Form(""), title: str = Form(""),
               url: str = Form(""), scope: str = Form(""), text: str = Form("")):
    """Ingest a document as a Page source: upload, UC Volume path, or pasted text. Same parsing lanes
    as customer context: .txt/.md as text, .pdf/.docx/.pptx via ai_parse_document, .csv/.xlsx
    serialized row by row. The document is chunked by heading so each chunk is citable."""
    _require()
    srv = _srv()
    if text.strip():
        raw_text, name = text, title or "Pasted text"
    else:
        raw, name = srv._resolve_ingest_source(file, volume_path)
        ext = name.rsplit(".", 1)[-1].lower() if "." in name else "txt"
        if ext in srv._CC_BINARY_EXTS:
            raw_text = us.parsed_document_to_text(srv._cc_parse_binary_to_text(raw, ext, name))
        elif ext in srv._CC_TABULAR_EXTS:
            raw_text = srv._cc_tabular_to_text(raw, ext)
        elif ext in srv._CC_TEXT_EXTS or ext in ("json", "yaml", "yml"):
            raw_text = raw.decode("utf-8", errors="replace")
        else:
            raise HTTPException(400, detail=f"Unsupported file type '.{ext}'. Use .txt/.md/.pdf/.docx/.pptx/.csv/.xlsx.")
    if not raw_text.strip():
        raise HTTPException(400, detail="No text could be extracted from that source.")
    if url and not url.lower().startswith(("http://", "https://")):
        raise HTTPException(400, detail="url must start with http:// or https://")
    _ensure_tables()
    res = us.store_source(_run_sql, _fq, title=(title or name)[:200], text=raw_text[:2_000_000], uri=url or volume_path or name,
                          source_type="volume" if volume_path else ("text" if text.strip() else "upload"),
                          scope=scope[:400], created_by=_user(), now=_now())
    return {"source": res}


@router.delete("/sources/{source_id}")
def remove_source(source_id: str):
    _require()
    if not _ID_RE.match(source_id):
        raise HTTPException(400, detail="bad source id")
    us.deactivate_source(_run_sql, _fq, source_id)
    return {"ok": True}


# ---------------------------------------------------------------- plan

class PlanRequest(BaseModel):
    prompt: str
    catalog: str
    schema_name: str = ""
    tables: List[str] = []
    domain: str = ""
    kinds: List[str] = []
    include_data_products: bool = False
    source_ids: Optional[List[str]] = None
    max_candidates: int = 15
    audience: str = ""
    include: Dict[str, bool] = {}
    model: Optional[str] = None


@router.post("/plan")
def plan(req: PlanRequest):
    """Propose candidate Pages (one model call). Nothing is written to UC; candidates are stored as
    'planned' drafts for curation. Inferred-only candidates are returned but unselected."""
    _require()
    srv = _srv()
    prompt = (req.prompt or "").strip()
    if len(prompt.split()) < 3:
        raise HTTPException(400, detail="Describe the Pages you want (at least a few words).")
    catalog, schema = (req.catalog or "").strip(), (req.schema_name or "").strip()
    if not catalog or not srv._SAFE_IDENT_RE.match(catalog) or not srv._SAFE_IDENT_RE.match(schema):
        raise HTTPException(400, detail="A valid catalog (and optional schema) is required.")
    if req.domain and not _DOMAIN_RE.match(req.domain):
        raise HTTPException(400, detail="domain must look like domains/{id}")
    kinds = [k for k in (req.kinds or up.DEFAULT_KINDS) if k in up.KINDS and k != "data_product"]
    if req.include_data_products:
        kinds.append("data_product")
    status_ = _probe()
    if not status_["available"]:
        raise HTTPException(409, detail=status_["reason"])
    _ensure_tables()
    res = pp.plan_run(run_sql=_run_sql, fq=_fq, llm=_llm_factory(req.model), api_do=_api_do, catalog=catalog, schema=schema,
                      tables=req.tables, prompt=prompt, source_ids=req.source_ids, kinds=kinds, default_domain=req.domain,
                      max_candidates=max(1, min(int(req.max_candidates or 15), up.MAX_CANDIDATES)), audience=req.audience,
                      include=req.include, domains=status_["domains"], user=_user(), now=_now(),
                      model=req.model or os.environ.get("LLM_MODEL", ""))
    if res.get("error"):
        raise HTTPException(422, detail=res["error"])
    return {"run_id": res["run_id"], "candidates": [_view(c) for c in res["candidates"]], "warnings": res["warnings"],
            "context_summary": res["context_summary"]}


# ---------------------------------------------------------------- drafts (the editable store)

@router.get("/runs")
def runs():
    _require()
    _ensure_tables()
    return {"runs": us.list_runs(_run_sql, _fq)}


@router.get("/drafts")
def drafts(run_id: str = "", status: str = ""):
    _require()
    if (run_id and not _ID_RE.match(run_id)) or (status and status not in us.STATUSES):
        raise HTTPException(400, detail="bad filter")
    _ensure_tables()
    return {"drafts": [_view(d) for d in us.list_drafts(_run_sql, _fq, run_id=run_id, status=status)]}


class DraftPatch(BaseModel):
    kind: Optional[str] = None
    display_name: Optional[str] = None
    description: Optional[str] = None
    body: Optional[str] = None
    synonyms: Optional[List[str]] = None
    domain: Optional[str] = None
    domain_label: Optional[str] = None
    related_tables: Optional[List[str]] = None
    related_metric_views: Optional[List[str]] = None
    relations: Optional[List[Dict[str, Any]]] = None
    selected: Optional[bool] = None
    open_questions: Optional[List[str]] = None


@router.patch("/drafts/{page_id}")
def edit_draft(page_id: str, patch: DraftPatch):
    """Edit a draft (the system of record). Only content/curation fields are editable."""
    _require()
    if not _ID_RE.match(page_id):
        raise HTTPException(400, detail="bad page id")
    rows = us.list_drafts(_run_sql, _fq, page_ids=[page_id], include_discarded=True)
    if not rows:
        raise HTTPException(404, detail="Draft not found")
    data = {k: v for k, v in patch.model_dump().items() if v is not None}
    if "domain" in data and data["domain"] and not _DOMAIN_RE.match(data["domain"]):
        raise HTTPException(400, detail="domain must look like domains/{id}")
    if "kind" in data and data["kind"] not in up.KINDS:
        raise HTTPException(400, detail="unknown kind")
    if "synonyms" in data:
        data["synonyms"] = up.sanitize_synonyms(data["synonyms"], data.get("display_name") or rows[0].get("display_name", ""))
    if rows[0].get("uc_page_name") and data.get("domain") and data["domain"] != rows[0].get("domain"):
        raise HTTPException(400, detail="A published Page's Domain cannot be changed (the Pages API makes it immutable).")
    updated, changed = us.apply_edit(rows[0], data)
    if not changed:
        return {"draft": _view(updated), "changed": []}
    updated.update(updated_by=_user(), updated_at=_now())
    us.upsert_drafts(_run_sql, _fq, [updated], columns=changed + ["edited_by_user", "updated_by", "updated_at"])
    return {"draft": _view(updated), "changed": changed}


class MergeRequest(BaseModel):
    keep_id: str
    merge_ids: List[str]


@router.post("/drafts/merge")
def merge(req: MergeRequest):
    """Merge candidates that are the same concept: others' names become synonyms; others discarded."""
    _require()
    ids = [req.keep_id] + list(req.merge_ids)
    if not all(_ID_RE.match(i) for i in ids) or req.keep_id in req.merge_ids:
        raise HTTPException(400, detail="bad ids")
    rows = {d["page_id"]: d for d in us.list_drafts(_run_sql, _fq, page_ids=ids)}
    if req.keep_id not in rows or len(rows) != len(ids):
        raise HTTPException(404, detail="Draft(s) not found")
    if any(rows[i].get("uc_page_name") for i in req.merge_ids):
        raise HTTPException(400, detail="A published Page cannot be merged away; merge into it instead.")
    merged = us.merge_drafts(rows[req.keep_id], [rows[i] for i in req.merge_ids])
    now, user = _now(), _user()
    merged.update(updated_by=user, updated_at=now)
    gone = [{**rows[i], "status": "discarded", "selected": False, "updated_by": user, "updated_at": now} for i in req.merge_ids]
    us.upsert_drafts(_run_sql, _fq, [merged] + gone)
    return {"draft": _view(merged)}


class IdsRequest(BaseModel):
    page_ids: List[str]


@router.post("/drafts/discard")
def discard(req: IdsRequest):
    _require()
    if not req.page_ids or not all(_ID_RE.match(i) for i in req.page_ids):
        raise HTTPException(400, detail="bad ids")
    rows = us.list_drafts(_run_sql, _fq, page_ids=req.page_ids)
    now, user = _now(), _user()
    us.upsert_drafts(_run_sql, _fq, [{**d, "status": "discarded", "selected": False, "updated_by": user, "updated_at": now} for d in rows],
                     columns=["status", "selected", "updated_by", "updated_at"])
    return {"discarded": len(rows), "note": "Discarding a draft never deletes a published UC Page."}


@router.get("/drafts/{page_id}/evidence")
def evidence(page_id: str):
    """Claim-level provenance for one Page: which evidence (with authority) supports which statement."""
    _require()
    if not _ID_RE.match(page_id):
        raise HTTPException(400, detail="bad page id")
    return {"evidence": us.get_evidence(_run_sql, _fq, page_id)}


# ---------------------------------------------------------------- write / evaluate (background)

def _start_task(kind: str, target, **kwargs) -> str:
    tid = uuid.uuid4().hex[:12]
    _tasks[tid] = {"status": "running", "kind": kind, "stage": "starting", "created": time.time(), "started_at": time.time()}

    def run():
        try:
            _tasks[tid]["result"] = target(**kwargs)
            _tasks[tid]["status"] = "done"
        except Exception as exc:  # surfaced to the UI via polling
            logger.exception("uc_pages %s task failed", kind)
            _tasks[tid].update(status="error", error=str(exc)[:1000])
    _srv()._spawn_with_obo(run)
    return tid


@router.get("/tasks/{task_id}")
def task(task_id: str):
    cutoff = time.time() - 3600
    for k in [k for k, v in _tasks.items() if v["created"] < cutoff and k != task_id]:
        _tasks.pop(k, None)
    t = _tasks.get(task_id)
    if not t:
        raise HTTPException(404, detail="Task not found")
    return {**t, "elapsed_seconds": round(time.time() - t["started_at"])}


class WriteRequest(BaseModel):
    page_ids: List[str]
    revise: bool = True
    model: Optional[str] = None


@router.post("/write")
def write(req: WriteRequest):
    """Write + evaluate selected drafts in the background (one batched model call per step).
    Poll /api/pages/tasks/{task_id}."""
    _require()
    if not req.page_ids or len(req.page_ids) > up.MAX_WRITE_BATCH or not all(_ID_RE.match(i) for i in req.page_ids):
        raise HTTPException(400, detail=f"Choose 1-{up.MAX_WRITE_BATCH} drafts to write.")
    user, now = _user(), _now()

    def job():
        res = pp.write_run(run_sql=_run_sql, fq=_fq, llm=_llm_factory(req.model), judge_llm=_judge(),
                           page_ids=req.page_ids, user=user, now=now, revise=req.revise)
        return {"written": sum(1 for r in res if not r.get("error")),
                "errors": [{"page_id": r["page_id"], "display_name": r.get("display_name"), "error": r["error"]} for r in res if r.get("error")]}
    return {"task_id": _start_task("write", job)}


@router.post("/evaluate")
def evaluate(req: IdsRequest):
    """Re-score drafts (e.g. after edits) against their run's evidence. Background task."""
    _require()
    if not req.page_ids or len(req.page_ids) > up.MAX_WRITE_BATCH or not all(_ID_RE.match(i) for i in req.page_ids):
        raise HTTPException(400, detail="bad ids")
    user, now = _user(), _now()

    def job():
        res = pp.evaluate_drafts(run_sql=_run_sql, fq=_fq, page_ids=req.page_ids, judge_llm=_judge(), user=user, now=now)
        return {"evaluated": len(res)}
    return {"task_id": _start_task("evaluate", job)}


# ---------------------------------------------------------------- publish (the only external write)

class PublishRequest(BaseModel):
    page_ids: List[str]
    state: str = "DRAFT"
    force: bool = False


@router.post("/publish")
def publish(req: PublishRequest):
    """Create/update drafts as UC Pages (Draft by default) and reconcile their asset links.
    Pages edited in UC since our last publish are skipped unless ``force``."""
    _require()
    if not req.page_ids or len(req.page_ids) > up.MAX_WRITE_BATCH or not all(_ID_RE.match(i) for i in req.page_ids):
        raise HTTPException(400, detail=f"Publish 1-{up.MAX_WRITE_BATCH} Pages at a time.")
    try:
        metastore_id = _srv()._get_effective_client().metastores.current().metastore_id
    except Exception as exc:
        metastore_id = None
        logger.warning("uc_pages: could not resolve metastore id: %s", exc)
    results = pp.publish_drafts(run_sql=_run_sql, fq=_fq, api_do=_api_do, page_ids=req.page_ids, state=up.normalize_state(req.state),
                                metastore_id=metastore_id, user=_user(), now=_now(), request_id=str(uuid.uuid4()), force=req.force)
    return {"results": results, "ok": sum(r["status"] == "ok" for r in results),
            "partial": sum(r["status"] == "partial" for r in results),
            "edited_in_uc": sum(r["status"] == "edited_in_uc" for r in results),
            "failed": sum(r["status"] == "error" for r in results)}
