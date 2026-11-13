"""Reference-based evaluation of UC Pages generation against a gold file.

A gold file (YAML/JSON) states what a good Page set for a scope MUST capture, authored from
HUMAN sources only (customer context, policy docs), so it is independent of AI-generated metadata:

```yaml
scope: {catalog: my_catalog, schema: my_schema}
prompt: "Create Pages for ..."
expected_concepts:            # concept recall (matched by name / alias / synonym)
  - {name: Allergy, aliases: [Allergy and intolerance, Allergy assertion]}
required_facts:               # human-context capture (judged)
  - {id: F1, statement: "NKA rows are documented negative assertions, not missing data."}
forbidden:                    # contradictions of human context (judged; must be absent)
  - {id: X1, statement: "NKA rows represent missing or unknown allergy data."}
```

Metrics: ``concept_recall``, ``fact_recall`` (stated = 1, partial = 0.5), ``contradictions``,
``unmatched_pages``, plus the reference-free quality summary from ``uc_pages_quality``.

``baseline_pages`` reproduces a naive one-shot generator (all context in one prompt, no evidence
ids, no plan, no templates) so improvements are measured against a fixed comparator.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional, Tuple

from dbxmetagen import uc_pages as up
from dbxmetagen import uc_pages_quality as uq

Llm = Callable[[List[str]], List[Tuple[str, Optional[str]]]]


def load_gold(path: str) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        text = f.read()
    if path.endswith((".yaml", ".yml")):
        import yaml
        return yaml.safe_load(text)
    return json.loads(text)


def concept_recall(pages: List[Dict[str, Any]], expected: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Which expected concepts have a Page (name, alias, or any synonym match; token-subset tolerant)."""
    def keys(p):
        ks = {up.norm_name(p.get("display_name"))} | {up.norm_name(s) for s in p.get("synonyms") or []}
        return {k for k in ks if k}

    page_keys = [(p, keys(p)) for p in pages]
    matched, missing, used = [], [], set()
    for c in expected:
        names = {up.norm_name(c["name"])} | {up.norm_name(a) for a in c.get("aliases") or []}
        hit = None
        for p, ks in page_keys:
            if names & ks or any(n and (set(n.split()) <= set(k.split()) or set(k.split()) <= set(n.split()))
                                 for n in names for k in ks if len(k.split()) <= 6):
                hit = p
                break
        if hit:
            matched.append({"concept": c["name"], "page": hit.get("display_name")})
            used.add(hit.get("display_name"))
        else:
            missing.append(c["name"])
    n = len(expected)
    return {"recall": round(len(matched) / n, 3) if n else None, "matched": matched, "missing": missing,
            "unmatched_pages": [p.get("display_name") for p in pages if p.get("display_name") not in used]}


def build_fact_prompt(pages: List[Dict[str, Any]], facts: List[Dict[str, Any]], forbidden: List[Dict[str, Any]]) -> str:
    corpus = "\n\n".join(f"=== PAGE: {p.get('display_name')}\nDescription: {p.get('description')}\n{p.get('body')}"
                         for p in pages)
    fl = "\n".join(f"- {f['id']}: {f['statement']}" for f in facts)
    xl = "\n".join(f"- {x['id']}: {x['statement']}" for x in forbidden)
    return (
        "You are auditing a set of glossary Pages against reference facts. For each REQUIRED fact, decide "
        "whether the Pages state it: 'stated' (clearly and correctly, possibly paraphrased), 'partial' "
        "(incomplete or vaguer than the fact), 'absent', or 'contradicted' (the Pages say the opposite). "
        "For each FORBIDDEN statement, decide whether any Page asserts it ('asserted') or not ('not_asserted'). "
        "Open questions that merely ask about something do not count as stating or asserting it. Quote the "
        "page name you relied on.\n\n"
        f"REQUIRED FACTS:\n{fl}\n\nFORBIDDEN STATEMENTS:\n{xl or '- (none)'}\n\n"
        'Return ONLY JSON: {"facts":{"F1":{"status":"stated|partial|absent|contradicted","page":"..."}},'
        '"forbidden":{"X1":{"status":"asserted|not_asserted","page":"..."}}}\n\n'
        f"PAGES:\n{corpus}"
    )


def fact_recall(pages: List[Dict[str, Any]], gold: Dict[str, Any], judge: Llm) -> Dict[str, Any]:
    facts, forbidden = gold.get("required_facts") or [], gold.get("forbidden") or []
    if not pages or not (facts or forbidden):
        return {"fact_recall": 0.0 if facts else None, "facts": {}, "contradictions": [], "forbidden": {}}
    text, err = up._call_with_retry(judge, [build_fact_prompt(pages, facts, forbidden)],
                                    ok=lambda t: bool(up.extract_json(t).get("facts")))[0]
    d = up.extract_json(text) if not err else {}
    fres = d.get("facts") or {}
    pts = {"stated": 1.0, "partial": 0.5}
    score = sum(pts.get(str((fres.get(f["id"]) or {}).get("status")), 0.0) for f in facts)
    contradicted = [f["id"] for f in facts if (fres.get(f["id"]) or {}).get("status") == "contradicted"]
    fb = d.get("forbidden") or {}
    asserted = [x["id"] for x in forbidden if (fb.get(x["id"]) or {}).get("status") == "asserted"]
    return {"fact_recall": round(score / len(facts), 3) if facts else None, "facts": fres, "forbidden": fb,
            "contradictions": sorted(set(contradicted + asserted)), "judge_error": err}


def build_baseline_prompt(prompt: str, context_text: str, max_pages: int) -> str:
    return (
        "You are a data steward writing Unity Catalog Pages (a business glossary). "
        f"USER REQUEST:\n{prompt}\n\nWrite at most {max_pages} Pages, one per business concept. Ground every "
        "statement in the CONTEXT. related_tables must be copied from the context. body is Markdown with "
        "## Definition, ## Business rules, ## How it is represented in the data, ## Notes.\n"
        'Return ONLY JSON: {"pages":[{"display_name":"...","description":"...","synonyms":["..."],'
        '"related_tables":["..."],"body":"..."}]}\n\n'
        f"CONTEXT:\n{context_text}"
    )


def baseline_pages(*, llm: Llm, evidence: List[Dict[str, Any]], prompt: str, max_pages: int = 10,
                   attempts: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """Naive comparator: one prompt, all evidence as plain text (no ids), no plan/templates/citations.

    A one-shot response holding many full Pages can exceed the model's output limit and come back
    as truncated (unparseable) JSON. That is a real failure mode of the design, so it is RECORDED in
    ``attempts`` and the page count is halved until a parse succeeds (min 3) to still compare quality.
    """
    ctx = "\n\n".join(f"({e['label']})\n{e['text']}" for e in evidence)
    n, pages = max_pages, []
    while n >= 1:
        text, err = llm([build_baseline_prompt(prompt, ctx[:up.DEFAULT_CONTEXT_CHARS], n)])[0]
        pages = (up.extract_json(text).get("pages") or []) if not err else []
        if attempts is not None:
            attempts.append({"max_pages": n, "response_chars": len(text or ""), "parsed_pages": len(pages), "error": err})
        if pages or n <= 3:
            break
        n = max(3, n // 2)
    out = []
    for i, p in enumerate(pages[:max_pages]):
        if isinstance(p, dict) and p.get("display_name"):
            out.append({"page_id": f"baseline-{i}", "kind": "", "display_name": p["display_name"],
                        "description": p.get("description") or "", "synonyms": p.get("synonyms") or [],
                        "related_tables": p.get("related_tables") or [], "body": p.get("body") or ""})
    return out


def evaluate_set(pages: List[Dict[str, Any]], *, gold: Dict[str, Any], judge: Llm, evidence: List[Dict[str, Any]],
                 scope_tables: List[str], cited: bool) -> Dict[str, Any]:
    """All metrics for one Page set. ``cited`` = pages carry citation stats / evidence ids (our
    pipeline); the baseline is judged for faithfulness against ALL evidence instead."""
    if cited:
        evals = uq.evaluate_pages(pages, evidence=evidence, scope_tables=scope_tables, llm=judge)
    else:
        all_ids = [e["id"] for e in evidence]
        evals = uq.evaluate_pages([{**p, "evidence_ids": all_ids} for p in pages], evidence=evidence,
                                  scope_tables=scope_tables, llm=judge)
        for e in evals:  # citation checks are meaningless for an uncited generator
            for cid in ("citations_valid", "citation_coverage", "has_human_evidence"):
                e["checks"][cid]["applicable"] = False
            e.update(uq.score(e["checks"], e["judge"] if e.get("judge") and not e["judge"].get("error") else None))
    return {"n_pages": len(pages), "concepts": concept_recall(pages, gold.get("expected_concepts") or []),
            "facts": fact_recall(pages, gold, judge), "quality": uq.summarize(evals),
            "per_page": [{"display_name": e["display_name"], "score": e["score"], "failed": e["failed"],
                          "judge": (e.get("judge") or {}).get("scores"),
                          "unsupported": (e.get("judge") or {}).get("unsupported_claims")} for e in evals],
            "mean_body_chars": round(sum(len(p.get("body") or "") for p in pages) / len(pages)) if pages else 0}
