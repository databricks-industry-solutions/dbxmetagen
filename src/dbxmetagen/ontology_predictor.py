"""Progressive entity and edge prediction using tiered ontology indexes.

Entity prediction uses three passes of increasing detail, where each tier
acts as an index into the next:

  Pass 1 -- Broad screen against the full tier-1 index (name + description
            for every entity in the ontology). Returns 1-5 candidates.
  Pass 2 -- Confirm against tier-2 profiles scoped to those candidates
            (source, URI, parent, top edges). If confidence >= 0.75 the
            result is final.
  Pass 3 -- Deep classification against tier-3 full profiles (keywords,
            relationships, attributes). Only fires when Pass 2 confidence
            is below 0.75.

Edge prediction uses two or three passes (tier-1 broad, tier-2 confirm,
optional tier-3 deep when Pass 2 requests it) with a Python-side
domain/range pre-filter before the first LLM call.

Pass 0 (optional): keyword / domain-affinity shortlist of tier-1 entities
before Pass 1 to reduce prompt size. Modes: ``off``, ``keyword``, ``hybrid``
(keyword only until embedding wiring exists).
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

import yaml

from dbxmetagen.ontology_index import OntologyIndexLoader
from dbxmetagen.ontology_pass0 import pass0_keyword_candidates
from dbxmetagen.ontology_vector_index import query_entities as vs_query_entities
from dbxmetagen.ontology_vector_index import query_edges as vs_query_edges

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class PredictionResult:
    table_name: str
    predicted_entity: str
    source_ontology: str
    equivalent_class_uri: Optional[str]
    confidence_score: float
    rationale: str
    matched_properties: List[str] = field(default_factory=list)
    passes_run: int = 1
    needs_human_review: bool = False


@dataclass
class EdgePredictionResult:
    from_table: str
    to_table: str
    predicted_edge: str
    edge_uri: Optional[str]
    inverse: Optional[str]
    source_ontology: str
    confidence_score: float
    rationale: str
    passes_run: int = 1


# ---------------------------------------------------------------------------
# Prompt templates
# ---------------------------------------------------------------------------

PASS1_USER = """Classify this database table to a business entity.

ENTITY INDEX:
{tier1_yaml}

TABLE:
Name: {table_name}
Columns: {columns}
Description: {sample}

Return JSON: {{"top_candidates": ["Name1","Name2"], "confidence": "high|medium|low", "reasoning": "one sentence"}}
Return 1-2 candidates if confidence is high, up to 5 if low."""

PASS2_USER = """Confirm entity classification. Candidate profiles:

{tier2_yaml}

TABLE:
Name: {table_name}
Columns: {columns}
Description: {sample}

Prior candidates: {candidates}

Return JSON: {{"predicted_entity": "Name", "equivalent_class_uri": "http://...", "confidence_score": 0.0-1.0, "rationale": "one sentence", "needs_deep_pass": true|false}}
Candidates and URIs must come only from the ENTITY INDEX / profiles above (active bundle). Set needs_deep_pass=true only if confidence_score < 0.75."""

PASS3_USER = """Final classification. Full profiles:

{tier3_yaml}

TABLE:
Name: {table_name}
Columns: {columns}
Description: {sample}

Remaining: {candidates}

Return JSON: {{"predicted_entity": "Name", "equivalent_class_uri": "http://...", "confidence_score": 0.0-1.0, "rationale": "two sentences", "matched_properties": ["col -> ontology_property"]}}
Use only entity names and URIs from the profiles above."""

SYSTEM_PROMPT = (
    "You are an ontology classification expert. "
    "Match database tables to entities from the active ontology bundle index shown in the prompt. "
    "Do not invent classes from other standards. Return ONLY valid JSON."
)

EDGE_PASS1_USER = """Match this foreign key relationship to a formal ontology edge.

EDGE INDEX:
{tier1_yaml}

FK: {from_table}.{from_column} -> {to_table}.{to_column}
Source entity: {src_entity}
Target entity: {dst_entity}

Return JSON: {{"top_candidates": ["edge1","edge2"], "confidence": "high|medium|low", "reasoning": "one sentence"}}"""

EDGE_PASS2_USER = """Confirm edge classification. Candidate profiles:

{tier2_yaml}

FK: {from_table}.{from_column} -> {to_table}.{to_column}
Source entity: {src_entity}
Target entity: {dst_entity}
Prior candidates: {candidates}

Return JSON: {{"predicted_edge": "name", "edge_uri": "http://...", "inverse": "name_or_null", "confidence_score": 0.0-1.0, "rationale": "one sentence", "needs_deep_pass": true|false}}
Use only edges from the EDGE INDEX / profiles. Set needs_deep_pass=true only if confidence_score < 0.75."""

EDGE_PASS3_USER = """Final edge classification. Tier-3 profiles add bundle edge_catalog fields (category, symmetric, canonical domain/range/inverse when present) and domain_profile / range_profile summaries for endpoint classes—use them to disambiguate.

{tier3_yaml}

FK: {from_table}.{from_column} -> {to_table}.{to_column}
Source entity: {src_entity}
Target entity: {dst_entity}
Prior candidates: {candidates}

Return JSON: {{"predicted_edge": "name", "edge_uri": "http://...", "inverse": "name_or_null", "confidence_score": 0.0-1.0, "rationale": "two sentences"}}
Use only edges and URIs from the profiles above."""


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_json(text: str) -> Dict[str, Any]:
    """Extract JSON from LLM response, handling markdown fences."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)
    return json.loads(text)


def _tier1_to_yaml(entries: List[Dict]) -> str:
    """Format tier-1 entries as compact YAML for prompt injection."""
    return yaml.dump(entries, default_flow_style=False, sort_keys=False)


def _safe_parse_response(
    llm_fn: Callable, system: str, user: str, context: str,
) -> Optional[Dict[str, Any]]:
    """Call LLM and parse JSON response. Returns None on any failure."""
    try:
        raw = llm_fn(system, user)
        return _parse_json(raw)
    except (json.JSONDecodeError, ValueError, TypeError) as e:
        logger.warning("Pass failed for %s: %s", context, e)
        return None
    except Exception as e:
        logger.warning("LLM call failed for %s: %s", context, e)
        return None


def _edge_scoped_yaml_dump(data: Dict[str, Any]) -> str:
    """Stable YAML for comparing tier-2 vs tier-3 scoped edge payloads."""
    return yaml.dump(data, default_flow_style=False, sort_keys=True)


def _safe_float(val: Any, default: float = 0.0) -> float:
    """Coerce to float without crashing on non-numeric LLM output."""
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _validate_pass1(resp: Dict) -> bool:
    """Check that a Pass 1 response has the expected shape."""
    return isinstance(resp.get("top_candidates"), list)


def _validate_pass2(resp: Dict) -> bool:
    """Check that a Pass 2/3 response has the expected shape."""
    return "predicted_entity" in resp


# ---------------------------------------------------------------------------
# Entity prediction
# ---------------------------------------------------------------------------

def predict_entity(
    table_name: str,
    columns: str,
    sample: str,
    loader: OntologyIndexLoader,
    llm_fn: Callable[[str, str], str],
    *,
    pass0_mode: str = "off",
    pass0_max_candidates: int = 48,
    pass0_min_candidates: int = 12,
    domain_hint: Optional[str] = None,
    domain_entity_affinity: Optional[Dict[str, Set[str]]] = None,
    vs_index: Optional[str] = None,
    vs_bundle: Optional[str] = None,
    vs_endpoint: str = "dbxmetagen-vs",
    vs_num_results: int = 8,
) -> PredictionResult:
    """Progressive entity classification using tiered ontology indexes.

    Pass 0 (optional) shortlists tier-1 rows by keyword overlap + domain
    affinity. Pass 1 screens that list (or full tier-1 when pass0_mode=off).
    Pass 2 confirms with scoped tier-2 profiles. Pass 3 fires when Pass 2
    requests deep pass or confidence < 0.75.

    When ``pass0_mode="vector"`` and ``vs_index`` is set, Pass 0 + Pass 1 are
    replaced entirely by a single HYBRID Vector Search query. The returned
    entity names are used directly as Pass 2 candidates, saving one LLM call.

    Args:
        table_name: Short table name.
        columns: Column summary string from _get_column_summary.
        sample: Table description text (e.g. "Description: ...").
        loader: OntologyIndexLoader with tier files for the active bundle.
        llm_fn: Callable(system_prompt, user_prompt) -> raw LLM text.
        pass0_mode: ``off`` | ``keyword`` | ``hybrid`` | ``vector``.
        pass0_max_candidates: Cap Pass 0 shortlist size.
        pass0_min_candidates: Minimum entities to send to Pass 1 when possible.
        domain_hint: Optional domain key for affinity boost.
        domain_entity_affinity: Optional map domain -> set of entity names.
        vs_index: Fully-qualified VS index name for vector mode.
        vs_bundle: Bundle name filter for VS queries.
        vs_endpoint: VS endpoint name (default: dbxmetagen-vs).
        vs_num_results: Number of VS results to retrieve.

    Returns:
        PredictionResult with entity name, URI, confidence, and pass count.
    """
    # --- Vector mode: replace Pass 0 + Pass 1 with a single VS query ---
    if pass0_mode == "vector" and not (vs_index and vs_bundle):
        logger.warning(
            "pass0_mode='vector' but vs_index=%r / vs_bundle=%r missing; falling back to keyword path",
            vs_index, vs_bundle,
        )
    if pass0_mode == "vector" and vs_index and vs_bundle:
        table_blob = f"{table_name} {columns} {sample}"
        try:
            vs_results = vs_query_entities(
                fq_index=vs_index, table_blob=table_blob,
                bundle=vs_bundle, num_results=vs_num_results,
                endpoint_name=vs_endpoint,
            )
        except Exception as e:
            logger.warning(
                "VS query failed for %s, falling back to keyword/tier-1 path: %s",
                table_name, e,
            )
            vs_results = None
        if vs_results is not None:
            candidates = [r["name"] for r in vs_results if r.get("name")]
            if not candidates:
                return PredictionResult(
                    table_name=table_name, predicted_entity="Unknown",
                    source_ontology="", equivalent_class_uri=None,
                    confidence_score=0.0,
                    rationale="Vector search returned no entity candidates",
                    passes_run=0, needs_human_review=True,
                )
            logger.info(
                "Vector retrieval for %s: %d candidates from VS (%s)",
                table_name, len(candidates), ", ".join(candidates[:5]),
            )
            return _run_pass2_and_pass3(
                table_name, columns, sample, candidates, loader, llm_fn,
                passes_offset=0,
            )

    # --- Standard path: Pass 0 + Pass 1: Broad screen ---
    tier1_full = loader.get_entities_tier1()
    if not tier1_full:
        return PredictionResult(
            table_name=table_name, predicted_entity="Unknown",
            source_ontology="", equivalent_class_uri=None,
            confidence_score=0.0, rationale="No tier indexes available",
        )

    if pass0_mode in ("keyword", "hybrid"):
        tier1 = pass0_keyword_candidates(
            table_name=table_name,
            columns=columns,
            sample=sample,
            tier1=tier1_full,
            max_candidates=pass0_max_candidates,
            min_candidates=pass0_min_candidates,
            domain_hint=domain_hint,
            domain_entity_affinity=domain_entity_affinity,
        )
        logger.info(
            "Pass 0 (%s): %d tier-1 candidates (from %d)",
            pass0_mode, len(tier1), len(tier1_full),
        )
    else:
        tier1 = tier1_full

    prompt1 = PASS1_USER.format(
        tier1_yaml=_tier1_to_yaml(tier1),
        table_name=table_name, columns=columns, sample=sample,
    )
    resp1 = _safe_parse_response(llm_fn, SYSTEM_PROMPT, prompt1, f"Pass1:{table_name}")
    if not resp1 or not _validate_pass1(resp1):
        return PredictionResult(
            table_name=table_name, predicted_entity="Unknown",
            source_ontology="", equivalent_class_uri=None,
            confidence_score=0.0, rationale="Pass 1 failed (bad LLM response)",
            passes_run=1, needs_human_review=True,
        )

    candidates = resp1.get("top_candidates", [])
    if not candidates:
        return PredictionResult(
            table_name=table_name, predicted_entity="Unknown",
            source_ontology="", equivalent_class_uri=None,
            confidence_score=0.0, rationale="No candidates from Pass 1",
            passes_run=1, needs_human_review=True,
        )

    logger.info("Pass 1 for %s: %d candidates, confidence=%s",
                table_name, len(candidates), resp1.get("confidence", "low"))

    return _run_pass2_and_pass3(
        table_name, columns, sample, candidates, loader, llm_fn,
        passes_offset=1,
    )


def _run_pass2_and_pass3(
    table_name: str,
    columns: str,
    sample: str,
    candidates: List[str],
    loader: OntologyIndexLoader,
    llm_fn: Callable[[str, str], str],
    passes_offset: int = 1,
) -> PredictionResult:
    """Shared Pass 2 + Pass 3 logic used by both standard and vector paths."""
    tier2 = loader.get_entities_tier2_scoped(candidates)
    tier2_yaml = yaml.dump(tier2, default_flow_style=False, sort_keys=False)
    prompt2 = PASS2_USER.format(
        tier2_yaml=tier2_yaml, table_name=table_name,
        columns=columns, sample=sample, candidates=", ".join(candidates),
    )
    resp2 = _safe_parse_response(llm_fn, SYSTEM_PROMPT, prompt2, f"Pass2:{table_name}")
    if not resp2 or not _validate_pass2(resp2):
        uri = loader.get_uri(candidates[0])
        return PredictionResult(
            table_name=table_name, predicted_entity=candidates[0],
            source_ontology="", equivalent_class_uri=uri,
            confidence_score=0.3,
            rationale="Pass 2 failed, using best candidate",
            passes_run=passes_offset, needs_human_review=True,
        )

    score2 = _safe_float(resp2.get("confidence_score"), 0.0)
    needs_deep = resp2.get("needs_deep_pass", score2 < 0.75)

    if not needs_deep:
        pred_ent = resp2.get("predicted_entity", candidates[0])
        uri = resp2.get("equivalent_class_uri") or loader.get_uri(pred_ent)
        canon_so = _entity_bundle_source_ontology(tier2, None, pred_ent)
        return PredictionResult(
            table_name=table_name,
            predicted_entity=pred_ent,
            source_ontology=canon_so,
            equivalent_class_uri=uri,
            confidence_score=score2,
            rationale=resp2.get("rationale", ""),
            passes_run=passes_offset + 1,
            needs_human_review=score2 < 0.6,
        )

    logger.info("Pass 2 for %s: score=%.2f, proceeding to Pass 3", table_name, score2)

    tier3 = loader.get_entities_tier3_scoped(candidates)
    tier3_yaml = yaml.dump(tier3, default_flow_style=False, sort_keys=False)
    prompt3 = PASS3_USER.format(
        tier3_yaml=tier3_yaml, table_name=table_name,
        columns=columns, sample=sample, candidates=", ".join(candidates),
    )
    resp3 = _safe_parse_response(llm_fn, SYSTEM_PROMPT, prompt3, f"Pass3:{table_name}")
    if not resp3 or not _validate_pass2(resp3):
        pred_ent = resp2.get("predicted_entity", candidates[0])
        uri = resp2.get("equivalent_class_uri") or loader.get_uri(pred_ent)
        canon_so = _entity_bundle_source_ontology(tier2, tier3, pred_ent)
        return PredictionResult(
            table_name=table_name,
            predicted_entity=pred_ent,
            source_ontology=canon_so,
            equivalent_class_uri=uri,
            confidence_score=score2,
            rationale=resp2.get("rationale", "Pass 3 failed, returning Pass 2 result"),
            passes_run=passes_offset + 1, needs_human_review=True,
        )

    score3 = _safe_float(resp3.get("confidence_score"), 0.0)
    entity = resp3.get("predicted_entity", candidates[0])
    uri = resp3.get("equivalent_class_uri") or loader.get_uri(entity)
    canon_so3 = _entity_bundle_source_ontology(tier2, tier3, entity)
    return PredictionResult(
        table_name=table_name,
        predicted_entity=entity,
        source_ontology=canon_so3,
        equivalent_class_uri=uri,
        confidence_score=score3,
        rationale=resp3.get("rationale", ""),
        matched_properties=resp3.get("matched_properties", []),
        passes_run=passes_offset + 2,
        needs_human_review=score3 < 0.6,
    )


def _entity_bundle_source_ontology(
    tier2: Optional[Dict[str, Any]],
    tier3: Optional[Dict[str, Any]],
    entity_name: str,
) -> str:
    """Provenance label from scoped entity indexes (active bundle), not LLM text."""
    if not entity_name:
        return ""
    for tier in (tier3, tier2):
        if not tier or entity_name not in tier:
            continue
        node = tier[entity_name]
        if not isinstance(node, dict):
            continue
        so = node.get("source_ontology")
        if so:
            return str(so)
    return ""


def _edge_bundle_source_ontology(
    tier2: Optional[Dict[str, Any]],
    tier3: Optional[Dict[str, Any]],
    edge_name: str,
) -> str:
    """Prefer provenance from scoped edge indexes (active bundle), not LLM text."""
    if not edge_name:
        return ""
    for tier in (tier3, tier2):
        if not tier or edge_name not in tier:
            continue
        node = tier[edge_name]
        if not isinstance(node, dict):
            continue
        so = node.get("source_ontology")
        if so:
            return str(so)
        dp = node.get("domain_profile")
        if isinstance(dp, dict) and dp.get("source_ontology"):
            return str(dp["source_ontology"])
    return ""


def _edge_bundle_uri(
    tier2: Optional[Dict[str, Any]],
    tier3: Optional[Dict[str, Any]],
    edge_name: str,
) -> str:
    """Canonical edge URI from scoped edge indexes when present."""
    if not edge_name:
        return ""
    for tier in (tier3, tier2):
        if not tier or edge_name not in tier:
            continue
        node = tier[edge_name]
        if not isinstance(node, dict):
            continue
        u = node.get("edge_uri") or node.get("uri")
        if u:
            return str(u)
    return ""


# ---------------------------------------------------------------------------
# Edge prediction
# ---------------------------------------------------------------------------

def predict_edge(
    *,
    src_entity: str,
    dst_entity: str,
    loader: OntologyIndexLoader,
    llm_fn: Callable[[str, str], str],
    from_table: str = "",
    from_column: str = "",
    to_table: str = "",
    to_column: str = "",
    vs_index: Optional[str] = None,
    vs_bundle: Optional[str] = None,
    vs_endpoint: str = "dbxmetagen-vs",
    vs_num_results: int = 8,
) -> EdgePredictionResult:
    """Two- or three-pass edge classification using tiered ontology indexes.

    Pass 1 screens tier-1 edges pre-filtered by domain/range match in Python.
    Pass 2 confirms against scoped tier-2 edge profiles. Pass 3 runs when
    tier-3 is strictly richer than tier-2 for the scoped candidates and
    Pass 2 sets needs_deep_pass or low confidence.

    When ``vs_index`` is set, Pass 1 is replaced by a Vector Search query
    and candidates are fed directly to Pass 2.
    """
    _default = EdgePredictionResult(
        from_table=from_table, to_table=to_table,
        predicted_edge="references", edge_uri=None, inverse=None,
        source_ontology="", confidence_score=0.0,
        rationale="No edge tier indexes available",
    )

    ctx = f"Edge:{from_table}->{to_table}"

    # --- Vector mode for edge retrieval ---
    if vs_index and vs_bundle:
        fk_blob = (
            f"{from_table}.{from_column} references {to_table}.{to_column}. "
            f"Source entity: {src_entity}. Target entity: {dst_entity}."
        )
        try:
            vs_results = vs_query_edges(
                fq_index=vs_index, fk_blob=fk_blob,
                bundle=vs_bundle, src_entity=src_entity, dst_entity=dst_entity,
                num_results=vs_num_results, endpoint_name=vs_endpoint,
            )
        except Exception as e:
            logger.warning("VS edge query failed for %s, falling back to tier-1: %s", ctx, e)
            vs_results = []
        candidates = [r["name"] for r in vs_results if r.get("name")]
        if candidates:
            logger.info(
                "Vector edge retrieval for %s: %d candidates", ctx, len(candidates),
            )
            return _run_edge_pass2_and_pass3(
                candidates=candidates, loader=loader, llm_fn=llm_fn,
                from_table=from_table, from_column=from_column,
                to_table=to_table, to_column=to_column,
                src_entity=src_entity, dst_entity=dst_entity,
            )
        logger.info("Vector edge retrieval returned 0 candidates, falling back to tier-1")

    all_edges_t1 = loader.get_edges_tier1()
    if not all_edges_t1:
        return _default

    scoped = [
        e for e in all_edges_t1
        if _edge_domain_matches(e, src_entity, dst_entity)
    ]
    if not scoped:
        scoped = all_edges_t1[:30]

    prompt1 = EDGE_PASS1_USER.format(
        tier1_yaml=_tier1_to_yaml(scoped),
        from_table=from_table, from_column=from_column,
        to_table=to_table, to_column=to_column,
        src_entity=src_entity, dst_entity=dst_entity,
    )
    resp1 = _safe_parse_response(llm_fn, SYSTEM_PROMPT, prompt1, f"Pass1:{ctx}")
    if not resp1 or not _validate_pass1(resp1):
        return EdgePredictionResult(
            from_table=from_table, to_table=to_table,
            predicted_edge="references", edge_uri=None, inverse=None,
            source_ontology="", confidence_score=0.0,
            rationale="Edge Pass 1 failed (bad LLM response)", passes_run=1,
        )

    candidates = resp1.get("top_candidates", [])
    if not candidates:
        return EdgePredictionResult(
            from_table=from_table, to_table=to_table,
            predicted_edge="references", edge_uri=None, inverse=None,
            source_ontology="", confidence_score=0.0,
            rationale="No edge candidates from Pass 1", passes_run=1,
        )

    return _run_edge_pass2_and_pass3(
        candidates=candidates, loader=loader, llm_fn=llm_fn,
        from_table=from_table, from_column=from_column,
        to_table=to_table, to_column=to_column,
        src_entity=src_entity, dst_entity=dst_entity,
    )


def _run_edge_pass2_and_pass3(
    *,
    candidates: List[str],
    loader: OntologyIndexLoader,
    llm_fn: Callable[[str, str], str],
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
    src_entity: str,
    dst_entity: str,
) -> EdgePredictionResult:
    """Shared Pass 2 + Pass 3 edge logic used by both standard and vector paths."""
    ctx = f"Edge:{from_table}->{to_table}"
    tier2 = loader.get_edges_tier2_scoped(candidates)
    tier2_yaml_str = _edge_scoped_yaml_dump(tier2)
    prompt2 = EDGE_PASS2_USER.format(
        tier2_yaml=tier2_yaml_str,
        from_table=from_table, from_column=from_column,
        to_table=to_table, to_column=to_column,
        src_entity=src_entity, dst_entity=dst_entity,
        candidates=", ".join(candidates),
    )
    resp2 = _safe_parse_response(llm_fn, SYSTEM_PROMPT, prompt2, f"Pass2:{ctx}")
    if not resp2 or "predicted_edge" not in resp2:
        return EdgePredictionResult(
            from_table=from_table, to_table=to_table,
            predicted_edge=candidates[0], edge_uri=None, inverse=None,
            source_ontology="", confidence_score=0.3,
            rationale="Edge Pass 2 failed, using best candidate",
            passes_run=1,
        )

    score2 = _safe_float(resp2.get("confidence_score"), 0.0)
    needs_deep = resp2.get("needs_deep_pass", score2 < 0.75)
    tier3 = loader.get_edges_tier3_scoped(candidates)
    tier3_redundant = bool(tier3) and _edge_scoped_yaml_dump(tier3) == tier2_yaml_str
    if needs_deep and tier3 and not tier3_redundant:
        tier3_yaml = yaml.dump(tier3, default_flow_style=False, sort_keys=False)
        prompt3 = EDGE_PASS3_USER.format(
            tier3_yaml=tier3_yaml,
            from_table=from_table, from_column=from_column,
            to_table=to_table, to_column=to_column,
            src_entity=src_entity, dst_entity=dst_entity,
            candidates=", ".join(candidates),
        )
        resp3 = _safe_parse_response(llm_fn, SYSTEM_PROMPT, prompt3, f"Pass3:{ctx}")
        if resp3 and "predicted_edge" in resp3:
            pe = resp3.get("predicted_edge", resp2.get("predicted_edge", "references"))
            canon_so = _edge_bundle_source_ontology(tier2, tier3, pe)
            canon_uri = _edge_bundle_uri(tier2, tier3, pe)
            return EdgePredictionResult(
                from_table=from_table, to_table=to_table,
                predicted_edge=pe,
                edge_uri=canon_uri or resp3.get("edge_uri") or resp2.get("edge_uri"),
                inverse=resp3.get("inverse") or resp2.get("inverse"),
                source_ontology=canon_so,
                confidence_score=_safe_float(resp3.get("confidence_score"), score2),
                rationale=resp3.get("rationale", resp2.get("rationale", "")),
                passes_run=3,
            )

    pe2 = resp2.get("predicted_edge", "references")
    canon_so2 = _edge_bundle_source_ontology(tier2, tier3, pe2)
    canon_uri2 = _edge_bundle_uri(tier2, tier3, pe2)
    return EdgePredictionResult(
        from_table=from_table, to_table=to_table,
        predicted_edge=pe2,
        edge_uri=canon_uri2 or resp2.get("edge_uri"),
        inverse=resp2.get("inverse"),
        source_ontology=canon_so2,
        confidence_score=score2,
        rationale=resp2.get("rationale", ""),
        passes_run=2,
    )


def _edge_domain_matches(edge: Dict, src_entity: str, dst_entity: str) -> bool:
    domain = edge.get("domain")
    range_ = edge.get("range")
    if domain and domain != "Any":
        if isinstance(domain, list):
            if src_entity not in domain:
                return False
        elif domain != src_entity:
            return False
    if range_ and range_ != "Any":
        if isinstance(range_, list):
            if dst_entity not in range_:
                return False
        elif range_ != dst_entity:
            return False
    return True
