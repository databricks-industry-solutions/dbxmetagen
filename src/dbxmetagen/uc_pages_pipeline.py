"""Orchestration for UC Pages generation: plan → write → evaluate → publish, persisting each step.

Shared by the app (warehouse SQL + REST as the effective identity), the batch job (spark) and
the evaluation harness, so every path runs identical logic. All I/O is injected; see
``uc_pages`` for the ``run_sql`` / ``api_do`` / ``llm`` contracts.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

from dbxmetagen import uc_pages as up
from dbxmetagen import uc_pages_quality as uq
from dbxmetagen import uc_pages_store as us

logger = logging.getLogger(__name__)


def plan_run(*, run_sql, fq, llm, api_do, catalog: str, schema: str = "", tables: Optional[List[str]] = None,
             prompt: str, source_ids: Optional[List[str]] = None, kinds: Optional[Sequence[str]] = None,
             default_domain: str = "", max_candidates: int = 15, audience: str = "", include: Optional[Dict[str, bool]] = None,
             domains: Optional[List[Dict[str, Any]]] = None, user: str = "", now: str, model: str = "",
             persist: bool = True) -> Dict[str, Any]:
    """Gather evidence, plan candidates, persist the run snapshot + candidates. Returns
    ``{run_id, candidates, warnings, context_summary, error}``."""
    kinds = list(kinds or up.DEFAULT_KINDS)
    warnings: List[str] = []
    if domains is None:
        status = up.probe(api_do)
        if not status["available"]:
            return {"run_id": "", "candidates": [], "warnings": [], "error": status["reason"]}
        domains = status["domains"]
    ctx = up.gather_context(run_sql, fq, catalog=catalog, schema=schema, tables=tables, prompt=prompt,
                            source_ids=source_ids, include=include)
    if not ctx["tables"] and not ctx["source_chunks"]:
        return {"run_id": "", "candidates": [], "warnings": [], "error": (
            "No generated metadata or source documents found for that scope. Run metadata generation on it "
            "first, or add source documents. Pages are grounded in that evidence.")}
    if ctx["table_candidates"] > len(ctx["tables"]):
        warnings.append(f"Scope has {ctx['table_candidates']} tables; using the {len(ctx['tables'])} most relevant to the prompt.")
    try:
        existing = up.list_pages(api_do, default_domain or None)
    except Exception as exc:
        existing = []
        warnings.append(f"Could not read existing Pages, so duplicates of UC Pages won't be flagged: {up._clip(exc, 160)}")
    existing_drafts = us.list_drafts(run_sql, fq, limit=1000) if persist else []
    res = up.plan_pages(llm=llm, ctx=ctx, prompt=prompt, domains=domains, existing_pages=existing,
                        existing_drafts=existing_drafts, default_domain=default_domain or None, kinds=kinds,
                        max_candidates=max_candidates, audience=audience)
    run_id = up.new_id()
    summary = context_summary(ctx, res["evidence"])
    if res.get("error") and not res["candidates"]:
        return {"run_id": "", "candidates": [], "warnings": warnings, "error": res["error"], "context_summary": summary}
    rows = []
    for c in res["candidates"]:
        rows.append({**c, "run_id": run_id, "scope_catalog": catalog, "scope_schema": schema, "prompt": prompt,
                     "body": "", "version": 1, "created_by": user, "created_at": now, "updated_by": user,
                     "updated_at": now, "edited_by_user": False, "uc_links": {}, "quality": {}})
    if persist:
        us.ensure_tables(run_sql, fq)
        us.save_run(run_sql, fq, {"run_id": run_id, "prompt": prompt, "scope_catalog": catalog, "scope_schema": schema,
                                  "scope_tables": tables or [], "evidence": res["evidence"], "model": model,
                                  "settings": {"kinds": kinds, "default_domain": default_domain, "max_candidates": max_candidates,
                                               "audience": audience, "include": include or {}, "source_ids": source_ids or []},
                                  "created_by": user, "created_at": now})
        us.upsert_drafts(run_sql, fq, rows)
    return {"run_id": run_id, "candidates": rows, "warnings": warnings, "error": None, "context_summary": summary,
            "evidence": res["evidence"], "domains": domains}


def context_summary(ctx: Dict[str, Any], evidence: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_auth: Dict[str, int] = {}
    by_type: Dict[str, int] = {}
    for e in evidence:
        by_auth[e["authority"]] = by_auth.get(e["authority"], 0) + 1
        by_type[e["type"]] = by_type.get(e["type"], 0) + 1
    return {"tables": [t["table_name"] for t in ctx.get("tables") or []], "evidence_by_authority": by_auth,
            "evidence_by_type": by_type, "source_chunks": len(ctx.get("source_chunks") or [])}


def _quality_record(w: Dict[str, Any], e: Dict[str, Any]) -> Dict[str, Any]:
    return {k: e.get(k) for k in ("score", "deterministic", "judge_score", "blocking", "failed", "checks")} | \
           {"judge_detail": e.get("judge"), "citation_stats": w.get("citation_stats")}


def write_run(*, run_sql, fq, llm, page_ids: List[str], judge_llm=None, user: str = "", now: str,
              drafts: Optional[List[Dict[str, Any]]] = None, run: Optional[Dict[str, Any]] = None,
              persist: bool = True, audience: str = "", revise: bool = True) -> List[Dict[str, Any]]:
    """Write + evaluate the given planned drafts (all from one run). Returns updated drafts
    (with ``error`` on per-page failure).

    With a judge and ``revise``, pages with reviewer findings (unsupported claims, low human-context
    fidelity, failed grounding/definition checks) get ONE revision pass; the revision is kept only
    if it scores higher, and both scores are recorded in ``quality.revision``."""
    drafts = drafts if drafts is not None else us.list_drafts(run_sql, fq, page_ids=page_ids)
    if not drafts:
        return []
    run_ids = {d["run_id"] for d in drafts}
    if len(run_ids) != 1:
        raise ValueError("Write pages from one planning run at a time.")
    run = run if run is not None else us.get_run(run_sql, fq, next(iter(run_ids)))
    if not run:
        raise ValueError("The planning run for these drafts was not found.")
    evidence = run["evidence"]
    scope_tables = sorted({t for d in drafts for t in d.get("related_tables") or []} |
                          {t for e in evidence for t in e.get("tables") or [] if e["type"] == "table_comment"})
    written = up.write_pages(llm=llm, candidates=drafts, evidence=evidence,
                             audience=audience or (run.get("settings") or {}).get("audience", ""))
    ok = [w for w in written if not w.get("error")]
    evals = uq.evaluate_pages(ok, evidence=evidence, scope_tables=scope_tables, llm=judge_llm)
    by_id = {e["page_id"]: e for e in evals}
    revision_meta: Dict[str, Dict[str, Any]] = {}
    if judge_llm and revise:
        issues = {pid: uq.revision_issues(e) for pid, e in by_id.items()}
        issues = {pid: v for pid, v in issues.items() if v}
        if issues:
            revised = up.revise_pages(llm=llm, pages=[w for w in ok if w["page_id"] in issues],
                                      issues_by_page=issues, evidence=evidence)
            good = [r for r in revised if not r.get("revision_error")]
            rev_evals = {e["page_id"]: e for e in uq.evaluate_pages(good, evidence=evidence, scope_tables=scope_tables, llm=judge_llm)}
            for r in good:
                before, after = by_id[r["page_id"]], rev_evals.get(r["page_id"])
                kept = bool(after) and after["score"] > before["score"]
                revision_meta[r["page_id"]] = {"attempted": True, "kept": kept, "score_before": before["score"],
                                               "score_after": after["score"] if after else None,
                                               "issues": issues[r["page_id"]][:8]}
                if kept:
                    ok = [r if w["page_id"] == r["page_id"] else w for w in ok]
                    by_id[r["page_id"]] = after
    for w in ok:
        e = by_id.get(w["page_id"]) or {}
        w["quality_score"] = e.get("score")
        w["quality"] = _quality_record(w, e) | ({"revision": revision_meta[w["page_id"]]} if w["page_id"] in revision_meta else {})
        w["updated_by"], w["updated_at"] = user, now
        w["version"] = int(w.get("version") or 1) + (1 if w.get("body") else 0)
        if e.get("blocking"):
            w["selected"] = False
            w["flags"] = list(w.get("flags") or []) + [{"code": "quality_blocking", "message": "Blocking quality issue: " + ", ".join(e["blocking"])}]
    if persist:
        us.upsert_drafts(run_sql, fq, ok)
        for w in ok:
            us.replace_evidence(run_sql, fq, w["page_id"], w["run_id"], w.get("provenance") or [], now)
    final = {w["page_id"]: w for w in ok}
    return [final.get(w["page_id"], w) for w in written]  # failed pages keep their ``error``


def evaluate_drafts(*, run_sql, fq, page_ids: List[str], judge_llm=None, user: str = "", now: str,
                    persist: bool = True) -> List[Dict[str, Any]]:
    """Re-score drafts (e.g. after a user edit) against their run's evidence."""
    drafts = us.list_drafts(run_sql, fq, page_ids=page_ids)
    out = []
    for run_id in {d["run_id"] for d in drafts}:
        run = us.get_run(run_sql, fq, run_id) or {"evidence": []}
        group = [d for d in drafts if d["run_id"] == run_id]
        for d in group:  # keep last citation stats unless the body was edited (citations are stripped then)
            stats = (d.get("quality") or {}).get("citation_stats")
            if stats and not d.get("edited_by_user"):
                d["citation_stats"] = stats
        scope = sorted({t for e in run["evidence"] for t in e.get("tables") or [] if e["type"] == "table_comment"})
        evals = uq.evaluate_pages(group, evidence=run["evidence"], scope_tables=scope, llm=judge_llm)
        for d, e in zip(group, evals):
            d["quality_score"] = e["score"]
            d["quality"] = _quality_record(d, e) | {"re_evaluated_after_edit": bool(d.get("edited_by_user"))}
            d["updated_by"], d["updated_at"] = user, now
            out.append(d)
    if persist and out:
        us.upsert_drafts(run_sql, fq, out, columns=["quality_score", "quality", "updated_by", "updated_at"])
    return out


def publish_drafts(*, run_sql, fq, api_do, page_ids: List[str], state: str = "DRAFT", metastore_id: Optional[str],
                   user: str = "", now: str, request_id: str, force: bool = False) -> List[Dict[str, Any]]:
    """Publish drafts to UC, then persist UC names/links/hashes and an audit row per page."""
    drafts = us.list_drafts(run_sql, fq, page_ids=page_ids)
    account = up.account_id(api_do)
    known = {d["page_id"]: d["uc_page_name"] for d in us.list_drafts(run_sql, fq, status="published", limit=2000) if d.get("uc_page_name")}
    src_urls = {}
    for d in drafts:
        chunk_ids = [r["source_ref"] for r in us.get_evidence(run_sql, fq, d["page_id"]) if r.get("evidence_type") == "source_doc"]
        src_urls[d["page_id"]] = us.source_urls(run_sql, fq, chunk_ids)
    results = up.publish(api_do, drafts, state=state, metastore_id=metastore_id, account=account,
                         source_urls_by_page=src_urls, force=force, known_page_names=known)
    updates = []
    for d, r in zip(drafts, results):
        if r["status"] in ("ok", "partial"):
            updates.append({**d, "uc_page_name": r["uc_page_name"], "uc_state": r.get("uc_state"), "uc_links": r["uc_links"],
                            "published_hash": r.get("published_hash"), "published_at": now, "published_by": user,
                            "status": "published", "edited_by_user": False, "updated_by": user, "updated_at": now})
        elif r["status"] == "edited_in_uc":
            updates.append({**d, "status": "edited_in_uc", "updated_by": user, "updated_at": now})
    if updates:
        us.upsert_drafts(run_sql, fq, updates)
    us.write_log(run_sql, fq, up.audit_rows(results, prompt=(drafts[0].get("prompt") if drafts else ""), created_by=user,
                                            now=now, request_id=request_id))
    return results
