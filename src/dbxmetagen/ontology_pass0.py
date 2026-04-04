"""Pass 0: cheap candidate shortlist before tier-1 LLM screening."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set

_TOKEN_RE = re.compile(r"[a-z0-9]+", re.I)


def _tokens(text: str) -> Set[str]:
    return {t.lower() for t in _TOKEN_RE.findall(text or "") if len(t) > 1}


def pass0_keyword_candidates(
    *,
    table_name: str,
    columns: str,
    sample: str,
    tier1: List[Dict[str, Any]],
    max_candidates: int,
    min_candidates: int,
    domain_hint: Optional[str] = None,
    domain_entity_affinity: Optional[Dict[str, Set[str]]] = None,
) -> List[Dict[str, Any]]:
    """Score tier-1 rows by token overlap + optional domain affinity boost.

    Returns a **subset** of ``tier1`` dicts (same shape), length between
    ``min_candidates`` and ``max_candidates`` when possible.
    """
    if not tier1:
        return []
    blob = f"{table_name} {columns} {sample}".lower()
    words = _tokens(blob)
    if domain_hint and domain_entity_affinity:
        aff = domain_entity_affinity.get(domain_hint.lower())
    else:
        aff = None

    scored: List[tuple[float, Dict[str, Any]]] = []
    for row in tier1:
        name = row.get("name", "")
        desc = (row.get("description") or "")[:400]
        ent_blob = f"{name} {desc}".lower()
        etoks = _tokens(ent_blob)
        overlap = len(words & etoks)
        if name:
            nl = name.lower()
            if nl in blob or nl.replace("_", "") in blob.replace("_", ""):
                overlap += 3
        score = float(overlap)
        if aff and name in aff:
            score += 2.5
        scored.append((score, row))

    scored.sort(key=lambda x: (-x[0], x[1].get("name", "")))
    if not any(s[0] > 0 for s in scored):
        out = tier1[:max_candidates]
        return out if len(out) >= min_candidates else tier1[: min(len(tier1), max_candidates)]

    positive = [row for s, row in scored if s > 0]
    if len(positive) < min_candidates:
        seen = {r["name"] for r in positive if r.get("name")}
        for s, row in scored:
            if row.get("name") not in seen:
                positive.append(row)
                seen.add(row["name"])
            if len(positive) >= min_candidates:
                break
    out = positive[:max_candidates]
    if len(out) < min_candidates:
        for s, row in scored:
            if row not in out:
                out.append(row)
            if len(out) >= min_candidates:
                break
    return out[:max_candidates]
