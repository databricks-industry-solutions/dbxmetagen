"""Three-pass entity and edge prediction for large ontologies.

Pass 1: Broad screen against tier-1 summaries (name + short description).
Pass 2: Confirm against tier-2 scoped profiles (source, URI, parent, top edges).
Pass 3: Deep classification against tier-3 full profiles (only ~20% of tables).

Gate logic:
- Pass 1 high confidence + 1 candidate -> skip straight to Pass 2 with that candidate
- Pass 2 confidence_score >= 0.75 -> skip Pass 3 (majority path)
- Pass 3 only fires on low-confidence tables
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import yaml

from dbxmetagen.ontology_index import OntologyIndexLoader

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
Sample (5 rows):
{sample}

Return JSON: {{"top_candidates": ["Name1","Name2"], "confidence": "high|medium|low", "reasoning": "one sentence"}}
Return 1-2 candidates if confidence is high, up to 5 if low."""

PASS2_USER = """Confirm entity classification. Candidate profiles:

{tier2_yaml}

TABLE:
Name: {table_name}
Columns: {columns}
Sample: {sample}

Prior candidates: {candidates}

Return JSON: {{"predicted_entity": "Name", "source_ontology": "FHIR R4|OMOP CDM|Schema.org", "equivalent_class_uri": "http://...", "confidence_score": 0.0-1.0, "rationale": "one sentence", "needs_deep_pass": true|false}}
Set needs_deep_pass=true only if confidence_score < 0.75."""

PASS3_USER = """Final classification. Full profiles:

{tier3_yaml}

TABLE:
Name: {table_name}
Columns: {columns}
Sample: {sample}

Remaining: {candidates}

Return JSON: {{"predicted_entity": "Name", "source_ontology": "...", "equivalent_class_uri": "http://...", "confidence_score": 0.0-1.0, "rationale": "two sentences", "matched_properties": ["col -> ontology_property"]}}"""

SYSTEM_PROMPT = (
    "You are an ontology classification expert. "
    "Match database tables to formal ontology entities from published standards "
    "(FHIR R4, OMOP CDM, Schema.org). Return ONLY valid JSON."
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

Return JSON: {{"predicted_edge": "name", "edge_uri": "http://...", "inverse": "name_or_null", "source_ontology": "...", "confidence_score": 0.0-1.0, "rationale": "one sentence", "needs_deep_pass": true|false}}"""


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


# ---------------------------------------------------------------------------
# Entity prediction
# ---------------------------------------------------------------------------

def predict_entity(
    table_name: str,
    columns: str,
    sample: str,
    loader: OntologyIndexLoader,
    llm_fn: Callable[[str, str], str],
) -> PredictionResult:
    """Three-pass entity classification against tiered ontology indexes.
    
    Args:
        table_name: Fully qualified or short table name.
        columns: Comma-separated column names and types.
        sample: Text representation of sample rows.
        loader: OntologyIndexLoader with tier files.
        llm_fn: Callable(system_prompt, user_prompt) -> str response.
    
    Returns:
        PredictionResult with classification, URI, and confidence.
    """
    # --- Pass 1: Broad screen ---
    tier1 = loader.get_entities_tier1()
    if not tier1:
        return PredictionResult(
            table_name=table_name, predicted_entity="Unknown",
            source_ontology="", equivalent_class_uri=None,
            confidence_score=0.0, rationale="No tier indexes available",
        )

    prompt1 = PASS1_USER.format(
        tier1_yaml=_tier1_to_yaml(tier1),
        table_name=table_name, columns=columns, sample=sample,
    )
    resp1 = _parse_json(llm_fn(SYSTEM_PROMPT, prompt1))
    candidates = resp1.get("top_candidates", [])
    p1_confidence = resp1.get("confidence", "low")

    if not candidates:
        return PredictionResult(
            table_name=table_name, predicted_entity="Unknown",
            source_ontology="", equivalent_class_uri=None,
            confidence_score=0.0, rationale="No candidates from Pass 1",
            passes_run=1, needs_human_review=True,
        )

    logger.info("Pass 1 for %s: %d candidates, confidence=%s", table_name, len(candidates), p1_confidence)

    # --- Pass 2: Confirm ---
    tier2 = loader.get_entities_tier2_scoped(candidates)
    tier2_yaml = yaml.dump(tier2, default_flow_style=False, sort_keys=False)
    prompt2 = PASS2_USER.format(
        tier2_yaml=tier2_yaml, table_name=table_name,
        columns=columns, sample=sample, candidates=", ".join(candidates),
    )
    resp2 = _parse_json(llm_fn(SYSTEM_PROMPT, prompt2))

    score2 = float(resp2.get("confidence_score", 0.0))
    needs_deep = resp2.get("needs_deep_pass", score2 < 0.75)

    if not needs_deep:
        uri = resp2.get("equivalent_class_uri") or loader.get_uri(resp2.get("predicted_entity", ""))
        return PredictionResult(
            table_name=table_name,
            predicted_entity=resp2.get("predicted_entity", candidates[0]),
            source_ontology=resp2.get("source_ontology", ""),
            equivalent_class_uri=uri,
            confidence_score=score2,
            rationale=resp2.get("rationale", ""),
            passes_run=2,
            needs_human_review=score2 < 0.6,
        )

    logger.info("Pass 2 for %s: score=%.2f, proceeding to Pass 3", table_name, score2)

    # --- Pass 3: Deep ---
    tier3 = loader.get_entities_tier3_scoped(candidates)
    tier3_yaml = yaml.dump(tier3, default_flow_style=False, sort_keys=False)
    prompt3 = PASS3_USER.format(
        tier3_yaml=tier3_yaml, table_name=table_name,
        columns=columns, sample=sample, candidates=", ".join(candidates),
    )
    resp3 = _parse_json(llm_fn(SYSTEM_PROMPT, prompt3))

    score3 = float(resp3.get("confidence_score", 0.0))
    entity = resp3.get("predicted_entity", candidates[0])
    uri = resp3.get("equivalent_class_uri") or loader.get_uri(entity)
    return PredictionResult(
        table_name=table_name,
        predicted_entity=entity,
        source_ontology=resp3.get("source_ontology", ""),
        equivalent_class_uri=uri,
        confidence_score=score3,
        rationale=resp3.get("rationale", ""),
        matched_properties=resp3.get("matched_properties", []),
        passes_run=3,
        needs_human_review=score3 < 0.6,
    )


# ---------------------------------------------------------------------------
# Edge prediction
# ---------------------------------------------------------------------------

def predict_edge(
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
    src_entity: str,
    dst_entity: str,
    loader: OntologyIndexLoader,
    llm_fn: Callable[[str, str], str],
) -> EdgePredictionResult:
    """Two/three-pass edge classification using tiered ontology indexes.
    
    Pre-filters edge candidates by domain/range in Python before any LLM call.
    """
    all_edges_t1 = loader.get_edges_tier1()
    if not all_edges_t1:
        return EdgePredictionResult(
            from_table=from_table, to_table=to_table,
            predicted_edge="references", edge_uri=None, inverse=None,
            source_ontology="", confidence_score=0.0,
            rationale="No edge tier indexes available",
        )

    # Pre-filter: only edges whose domain/range match our entity types
    scoped = [
        e for e in all_edges_t1
        if _edge_domain_matches(e, src_entity, dst_entity)
    ]
    if not scoped:
        scoped = all_edges_t1[:30]  # fallback to top 30

    prompt1 = EDGE_PASS1_USER.format(
        tier1_yaml=_tier1_to_yaml(scoped),
        from_table=from_table, from_column=from_column,
        to_table=to_table, to_column=to_column,
        src_entity=src_entity, dst_entity=dst_entity,
    )
    resp1 = _parse_json(llm_fn(SYSTEM_PROMPT, prompt1))
    candidates = resp1.get("top_candidates", [])

    if not candidates:
        return EdgePredictionResult(
            from_table=from_table, to_table=to_table,
            predicted_edge="references", edge_uri=None, inverse=None,
            source_ontology="", confidence_score=0.0,
            rationale="No edge candidates from Pass 1", passes_run=1,
        )

    # Pass 2
    tier2 = loader.get_edges_tier2_scoped(candidates)
    prompt2 = EDGE_PASS2_USER.format(
        tier2_yaml=yaml.dump(tier2, default_flow_style=False, sort_keys=False),
        from_table=from_table, from_column=from_column,
        to_table=to_table, to_column=to_column,
        src_entity=src_entity, dst_entity=dst_entity,
        candidates=", ".join(candidates),
    )
    resp2 = _parse_json(llm_fn(SYSTEM_PROMPT, prompt2))

    return EdgePredictionResult(
        from_table=from_table, to_table=to_table,
        predicted_edge=resp2.get("predicted_edge", "references"),
        edge_uri=resp2.get("edge_uri"),
        inverse=resp2.get("inverse"),
        source_ontology=resp2.get("source_ontology", ""),
        confidence_score=float(resp2.get("confidence_score", 0.0)),
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
