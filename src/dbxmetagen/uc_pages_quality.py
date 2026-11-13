"""Objective quality evaluation for UC Pages (generated or human-written).

Two layers, both reported per check so a score is always explainable:

1. **Deterministic checks**: cheap, reproducible, no model. Structure and form rules from the
   Pages docs + ISO 704 definition practice, calibrated on 40 real published Pages (median body
   ~670 chars, 92% one-sentence description, 90% synonyms, "Definition" the dominant first section),
   plus grounding checks only we can do (citation coverage/validity, assets in scope, PII).
2. **Rubric judge**: one LLM call per page scoring faithfulness to the cited evidence, definition
   quality, usefulness, human-context fidelity and granularity on 1-5, with the unsupported claims
   it found. Faithfulness is judged against the SAME evidence the writer saw.

Score (0-100) = 50% weighted deterministic pass rate + 50% judge mean (when judged). Blocking
failures (PII, out-of-scope assets, invalid citations, no definition) cap the score at 40 and mark
the page ``blocking`` so the UI can refuse to pre-select it for publish.

The evaluator is itself validated (see tests + the eval harness): real human Pages should score
well on form checks, and deliberately degraded variants must be caught.
"""

from __future__ import annotations

import json
import re
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from dbxmetagen.uc_pages import KINDS, BODY_TARGET_CHARS, norm_name, extract_json, render_evidence, is_table_shaped

Llm = Callable[[List[str]], List[Tuple[str, Optional[str]]]]

# Purpose-statement / non-definition openings (ISO 704: a definition says what something IS).
_PURPOSE_RE = re.compile(
    r"^\s*(this (page|term|metric|concept|document)\b|used (to|for|in)\b|it is used\b|helps?\b|"
    r"important\b|refers to how\b|provides\b|allows\b|enables\b|tracks\b|this is\b)", re.I)
_PURPOSE_ANYWHERE_RE = re.compile(r"\b(is (very )?important (for|to)|is used (for|in|to) (reporting|analysis|tracking))\b", re.I)
_PII_RES = {
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@(?!example\.com\b)[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "phone": re.compile(r"(?<![\w.])(\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}\b"),
    "card": re.compile(r"\b(?:\d[ -]?){13,16}\b"),
}
_FQN_RE = re.compile(r"`([A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+)`")

# check id -> (weight, blocking, human label)
CHECKS: Dict[str, Tuple[float, bool, str]] = {
    "has_description": (2.0, True, "Has a short description"),
    "description_is_definition": (2.0, False, "Description defines the concept (not a purpose statement)"),
    "description_not_circular": (1.5, False, "Description does not define the term by itself"),
    "description_concise": (1.0, False, "Description is one concise sentence"),
    "has_definition_section": (1.0, False, "Body opens with the kind's defining section"),
    "body_length_in_range": (1.0, False, f"Body length {BODY_TARGET_CHARS[0]}-{BODY_TARGET_CHARS[1] + 1500} chars"),
    "has_synonyms": (0.5, False, "Lists synonyms or abbreviations"),
    "links_assets": (1.0, False, "Links the tables / metric views that represent it"),
    "metric_links_metric_view": (1.5, False, "Metric Page links its governed metric view"),
    "not_table_shaped": (1.0, False, "Describes a concept, not a single table"),
    "no_pii": (3.0, True, "No personal data in the text"),
    "assets_in_scope": (2.0, True, "Every referenced table exists in scope"),
    "citations_valid": (2.0, True, "All citations point to real evidence"),
    "citation_coverage": (2.0, False, "At least 80% of statements are cited"),
    "has_human_evidence": (1.0, False, "Grounded in at least one human-provided source"),
    "has_domain": (1.0, False, "Assigned to a Domain"),
}

JUDGE_CRITERIA = {
    "faithfulness": "Every statement is supported by the cited evidence; nothing is invented.",
    "definition_quality": "The description is a precise definition (broader kind + distinguishing traits) that separates this concept from neighbours.",
    "usefulness": "An analyst or AI agent could use the Page to find and correctly query the concept (names tables/columns/metric views, caveats, grain).",
    "human_context_fidelity": "Facts and rules from human evidence (customer context, documents, acronyms) are reflected and never contradicted.",
    "granularity": "The Page covers exactly one business concept at the right level (not a table dump, not a grab-bag).",
}


def _first_heading(body: str) -> str:
    m = re.search(r"^#{1,6}\s+(.+)$", body or "", flags=re.M)
    return norm_name(m.group(1)) if m else ""


def _sentences(text: str) -> int:
    return len([s for s in re.split(r"(?<=[.!?])\s+(?=[A-Z])", (text or "").strip()) if s.strip()])


def deterministic_checks(page: Dict[str, Any], *, scope_tables: Optional[Iterable[str]] = None,
                         evidence_ids: Optional[Iterable[str]] = None,
                         evidence_by_id: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Dict[str, Any]]:
    """Run every applicable deterministic check. Returns ``{check_id: {pass, applicable, detail}}``.

    Grounding checks (citations, human evidence, assets in scope) are not applicable when the
    caller has no evidence/scope (e.g. scoring a human-written Page from UC).
    """
    name = str(page.get("display_name") or "")
    desc = str(page.get("description") or "").strip()
    body = str(page.get("body") or "")
    kind = page.get("kind") or ""
    res: Dict[str, Dict[str, Any]] = {}

    def put(cid, ok, detail="", applicable=True):
        res[cid] = {"pass": bool(ok), "applicable": applicable, "detail": detail}

    put("has_description", bool(desc), "" if desc else "Description is empty.")
    purpose = bool(_PURPOSE_RE.search(desc) or _PURPOSE_ANYWHERE_RE.search(desc))
    put("description_is_definition", bool(desc) and not purpose,
        "Reads as a purpose/usage statement; state what the concept IS." if purpose else "")
    nn = norm_name(name)
    # A leading "<term> is/are/means" restates the subject and is stripped before the test.
    # Calibrated on 40 human Pages: genus+differentia often reuses the head noun ("Raw Material: A
    # material used as input…"), which is correct. Circular = the FULL term phrase recurs in the
    # first sentence, or a one-word term is the head of its own definition ("Encounter: A clinical
    # encounter…").
    raw_first = re.split(r"(?<=[.;:])\s", desc, maxsplit=1)[0] if desc else ""
    first_sentence = norm_name(raw_first)
    if nn:
        first_sentence = re.sub(rf"^(a |an |the )?{re.escape(nn)}( is| are| means| refers to)?\s*", "", first_sentence)
    if len(nn.split()) > 1:
        circular = f" {nn} " in f" {first_sentence} "
    else:
        circular = bool(nn) and bool(re.match(rf"^(a |an |the )?(\w+ )?{re.escape(nn)}s?\b", first_sentence))
    put("description_not_circular", not circular,
        "The term is used inside its own definition." if circular else "")
    concise = bool(desc) and len(desc) <= 300 and (_sentences(desc) <= 2 or (len(desc) <= 200 and _sentences(desc) <= 3))
    put("description_concise", concise, "" if concise else f"{len(desc)} chars / {_sentences(desc)} sentences; aim for one sentence under 300 chars.")
    expected = norm_name(KINDS.get(kind, {}).get("sections", ["Definition"])[0]) if kind else "definition"
    first = _first_heading(body)
    put("has_definition_section", first in (expected, "definition", "rule", "purpose") or first.startswith("definition"),
        "" if first else "Body has no headings.")
    # Measure the substantive body: exclude the steward questions and the provenance footer.
    core = re.split(r"\n## Open questions for the steward\b|\n---\n_Drafted with dbxmetagen", body)[0]
    blen = len(core.strip())
    lo, hi = BODY_TARGET_CHARS[0], BODY_TARGET_CHARS[1] + 1000
    put("body_length_in_range", lo <= blen <= hi, f"{blen} chars (real Pages: median ~670, p90 ~3300)")
    put("has_synonyms", bool(page.get("synonyms")), "")
    assets = list(page.get("related_tables") or []) + list(page.get("related_metric_views") or [])
    # Links are unknowable for Pages read back from UC (no list-links API) -> not applicable there.
    links_known = "related_tables" in page or "related_metric_views" in page
    put("links_assets", bool(assets), "" if assets else "No related tables or metric views linked.",
        applicable=links_known and (kind not in ("term",) or bool(assets)))
    if kind == "metric":
        has_mv = bool(page.get("related_metric_views"))
        put("metric_links_metric_view", has_mv, "" if has_mv else "Metric Pages should link the metric view that governs the calculation.")
    shaped = kind != "data_product" and is_table_shaped(name, scope_tables or [])
    put("not_table_shaped", not shaped, "Name is a table identifier; document tables in a wiki." if shaped else "")
    text = " ".join([name, desc, body, " ".join(page.get("synonyms") or [])])
    hits = [k for k, rx in _PII_RES.items() if rx.search(text)]
    put("no_pii", not hits, f"Possible {', '.join(hits)} in text." if hits else "")

    scope = {str(t).lower() for t in (scope_tables or [])}
    mvs = {str(m).lower() for m in page.get("related_metric_views") or []}
    if scope:
        refs = {m.lower() for m in _FQN_RE.findall(body)}
        bad = sorted(r for r in refs if r not in scope and r not in mvs)
        put("assets_in_scope", not bad, f"Out-of-scope references: {', '.join(bad[:5])}" if bad else "")
    else:
        put("assets_in_scope", True, "", applicable=False)
    stats = page.get("citation_stats") or {}
    if evidence_ids is not None or stats:
        invalid = stats.get("invalid_ids") or []
        put("citations_valid", not invalid, f"Unknown evidence ids: {', '.join(invalid[:5])}" if invalid else "")
        lines, cited = stats.get("content_lines", 0), stats.get("cited_lines", 0)
        cov = (cited / lines) if lines else 0.0
        put("citation_coverage", cov >= 0.8, f"{cited}/{lines} statements cited ({cov:.0%}).")
    else:
        put("citations_valid", True, "", applicable=False)
        put("citation_coverage", True, "", applicable=False)
    if evidence_by_id is not None:
        human = any((evidence_by_id.get(i) or {}).get("authority") == "human" for i in page.get("evidence_ids") or [])
        put("has_human_evidence", human, "" if human else "Only AI-generated or governed metadata supports this Page.")
    else:
        put("has_human_evidence", True, "", applicable=False)
    put("has_domain", bool(page.get("domain")), "")
    return res


def build_judge_prompt(page: Dict[str, Any], evidence_text: str) -> str:
    crit = "\n".join(f"- {k}: {v}" for k, v in JUDGE_CRITERIA.items())
    has_ev = bool(evidence_text.strip())
    return (
        "You are a strict reviewer of Unity Catalog Pages (governed business glossary entries). Score the "
        "Page on each criterion from 1 (poor) to 5 (excellent). Be skeptical: a 5 means a careful data "
        "steward would publish it unchanged.\n\n"
        f"CRITERIA:\n{crit}\n\n"
        + ("Judge faithfulness and human_context_fidelity ONLY against the EVIDENCE below; anything not in it is unsupported.\n"
           if has_ev else "No evidence is available: score faithfulness and human_context_fidelity as null.\n")
        + "List up to 5 specific unsupported or wrong statements, and up to 3 concrete improvements.\n\n"
        'Return ONLY JSON: {"scores":{"faithfulness":1-5|null,"definition_quality":1-5,"usefulness":1-5,'
        '"human_context_fidelity":1-5|null,"granularity":1-5},"unsupported_claims":["..."],"improvements":["..."]}\n\n'
        f"PAGE\nName: {page.get('display_name')}\nKind: {page.get('kind') or 'unknown'}\n"
        f"Description: {page.get('description')}\nSynonyms: {', '.join(page.get('synonyms') or [])}\n"
        f"Related assets: {', '.join(list(page.get('related_tables') or []) + list(page.get('related_metric_views') or []))}\n"
        f"Body:\n{page.get('body')}\n\n"
        + (f"EVIDENCE:\n{evidence_text}" if has_ev else "")
    )


def parse_judge(text: str) -> Dict[str, Any]:
    d = extract_json(text)
    scores = {}
    for k in JUDGE_CRITERIA:
        v = (d.get("scores") or {}).get(k)
        try:
            v = float(v) if v is not None else None
        except (TypeError, ValueError):
            v = None
        scores[k] = None if v is None else max(1.0, min(5.0, v))
    return {"scores": scores, "unsupported_claims": [str(x) for x in (d.get("unsupported_claims") or [])][:5],
            "improvements": [str(x) for x in (d.get("improvements") or [])][:3], "parsed": bool(d)}


def score(checks: Dict[str, Dict[str, Any]], judge: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Combine check results (+ optional judge) into a 0-100 score with an explanation. Pure."""
    num = den = 0.0
    blocking = []
    for cid, r in checks.items():
        if not r["applicable"]:
            continue
        w, block, _ = CHECKS[cid]
        den += w
        num += w if r["pass"] else 0.0
        if block and not r["pass"]:
            blocking.append(cid)
    det = (num / den * 100) if den else 0.0
    judged = [v for v in ((judge or {}).get("scores") or {}).values() if v is not None]
    jscore = ((sum(judged) / len(judged) - 1) / 4 * 100) if judged else None
    total = det if jscore is None else 0.5 * det + 0.5 * jscore
    if blocking:
        total = min(total, 40.0)
    return {"score": round(total, 1), "deterministic": round(det, 1),
            "judge_score": None if jscore is None else round(jscore, 1), "blocking": blocking,
            "failed": [cid for cid, r in checks.items() if r["applicable"] and not r["pass"]]}


def evaluate_pages(pages: List[Dict[str, Any]], *, evidence: Optional[List[Dict[str, Any]]] = None,
                   scope_tables: Optional[Iterable[str]] = None, llm: Optional[Llm] = None) -> List[Dict[str, Any]]:
    """Evaluate pages; returns one ``{page_id, display_name, checks, judge, **score}`` each.
    With ``llm`` the rubric judge runs (one batched call); without it, deterministic only."""
    ev_by_id = {e["id"]: e for e in evidence or []}
    scope_tables = list(scope_tables or [])
    results = []
    prompts = []
    for p in pages:
        checks = deterministic_checks(p, scope_tables=scope_tables,
                                      evidence_ids=(p.get("evidence_ids") if evidence is not None else None),
                                      evidence_by_id=(ev_by_id if evidence is not None else None))
        results.append({"page_id": p.get("page_id"), "display_name": p.get("display_name"), "checks": checks, "judge": None})
        if llm:
            ids = p.get("evidence_ids") or []
            prompts.append(build_judge_prompt(p, render_evidence(evidence or [], ids=ids) if evidence else ""))
    if llm and prompts:
        from dbxmetagen.uc_pages import _call_with_retry
        panel = list(llm) if isinstance(llm, (list, tuple)) else [llm]
        per_judge = [_call_with_retry(j, prompts, ok=lambda t: parse_judge(t)["parsed"]) for j in panel]
        for i, r in enumerate(results):
            r["judge"] = combine_judgments([parse_judge(out[i][0]) if not out[i][1] else {"error": out[i][1], "scores": {}}
                                            for out in per_judge])
    for r in results:
        r.update(score(r["checks"], r["judge"] if r["judge"] and not r["judge"].get("error") else None))
    return results


def combine_judgments(judgments: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Average a judge panel: per-criterion mean of non-null scores; findings unioned. A single
    judgment passes through unchanged. All-error → an error record (score uses deterministic only)."""
    ok = [j for j in judgments if not j.get("error") and j.get("parsed", True)]
    if not ok:
        return {"error": "; ".join(str(j.get("error")) for j in judgments if j.get("error")) or "unparseable", "scores": {}}
    if len(ok) == 1:
        return ok[0]
    scores = {}
    for k in JUDGE_CRITERIA:
        vals = [j["scores"].get(k) for j in ok if j["scores"].get(k) is not None]
        scores[k] = round(sum(vals) / len(vals), 2) if vals else None
    claims = list(dict.fromkeys(c for j in ok for c in j.get("unsupported_claims") or []))[:8]
    improvements = list(dict.fromkeys(c for j in ok for c in j.get("improvements") or []))[:5]
    return {"scores": scores, "unsupported_claims": claims, "improvements": improvements, "parsed": True,
            "panel": [j["scores"] for j in ok]}


def revision_issues(evaluation: Dict[str, Any], *, min_judge: float = 4.0) -> List[str]:
    """Reviewer findings worth one revision pass: low faithfulness / human-context fidelity, the
    judge's unsupported claims, and failed grounding checks. Empty list = leave the page alone."""
    issues: List[str] = []
    j = evaluation.get("judge") or {}
    s = j.get("scores") or {}
    low = [k for k in ("faithfulness", "human_context_fidelity") if s.get(k) is not None and s[k] < min_judge]
    if low:
        issues += list(j.get("unsupported_claims") or []) or [f"Low {k.replace('_', ' ')}." for k in low]
    for cid in ("citation_coverage", "citations_valid", "assets_in_scope", "no_pii", "description_is_definition",
                "description_not_circular", "description_concise"):
        c = (evaluation.get("checks") or {}).get(cid)
        if c and c["applicable"] and not c["pass"]:
            issues.append(f"{CHECKS[cid][2]} failed: {c['detail']}".strip(": "))
    c = (evaluation.get("checks") or {}).get("body_length_in_range")
    if c and c["applicable"] and not c["pass"] and "chars" in c["detail"]:
        n = int(re.match(r"(\d+)", c["detail"]).group(1)) if re.match(r"(\d+)", c["detail"]) else 0
        if n > BODY_TARGET_CHARS[1]:
            issues.append(f"The body is {n} characters; cut it to under {BODY_TARGET_CHARS[1]} by removing repetition "
                          "and facts that belong on other Pages, keeping every human-evidence fact.")
    return issues


def summarize(evals: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate a set of evaluations: mean scores, per-check pass rates, per-criterion means."""
    if not evals:
        return {"n": 0}
    n = len(evals)
    pass_rates = {}
    for cid in CHECKS:
        app = [e["checks"][cid] for e in evals if cid in e["checks"] and e["checks"][cid]["applicable"]]
        if app:
            pass_rates[cid] = round(sum(c["pass"] for c in app) / len(app), 3)
    crit = {}
    for k in JUDGE_CRITERIA:
        vals = [e["judge"]["scores"].get(k) for e in evals if e.get("judge") and e["judge"].get("scores")]
        vals = [v for v in vals if v is not None]
        if vals:
            crit[k] = round(sum(vals) / len(vals), 2)
    return {"n": n, "mean_score": round(sum(e["score"] for e in evals) / n, 1),
            "mean_deterministic": round(sum(e["deterministic"] for e in evals) / n, 1),
            "blocking_rate": round(sum(bool(e["blocking"]) for e in evals) / n, 3),
            "check_pass_rates": pass_rates, "judge_means": crit}


def to_json(evaluation: Dict[str, Any]) -> str:
    return json.dumps(evaluation, ensure_ascii=False, default=str)
