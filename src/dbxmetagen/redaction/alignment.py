"""
Entity alignment functions for combining multiple PHI detection methods.

This module provides functions to align and merge entities detected by different
methods (e.g., Presidio, GLiNER, and AI-based detection) to create a consensus view
with weighted confidence scoring.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from .utils import is_fuzzy_match, is_overlap
from .config import (
    DEFAULT_FUZZY_MATCH_THRESHOLD,
    SOURCE_WEIGHTS,
    EXACT_MATCH_SCORE,
    OVERLAP_MATCH_SCORE,
    CONFIDENCE_THRESHOLDS,
    REQUIRED_ENTITY_FIELDS,
)


class MatchType(Enum):
    """Type of match between two entities."""

    EXACT = "exact"
    OVERLAP_FUZZY = "overlap_fuzzy"
    NO_MATCH = "no_match"


@dataclass
class Entity:
    """
    Normalized entity representation.

    Attributes:
        entity: The text content of the entity
        start: Starting character position
        end: Ending character position
        entity_type: Type of entity (e.g., PERSON, EMAIL)
        doc_id: Document identifier
        source: Detection source (presidio, gliner, ai)
        score: Confidence score from the detection method
        extra_fields: Additional fields from source (not used in alignment)
    """

    entity: str
    start: int
    end: int
    entity_type: Optional[str] = None
    doc_id: Optional[str] = None
    source: Optional[str] = None
    score: Optional[float] = None
    extra_fields: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate required fields."""
        if self.entity is None or self.start is None or self.end is None:
            raise ValueError("Entity must have entity, start, and end fields")


def normalize_entity(
    entity_dict: Dict[str, Any], source: str, doc_id: Optional[str] = None
) -> Entity:
    """
    Convert various entity dictionary formats to normalized Entity object.

    Handles extra fields gracefully by storing them in extra_fields without
    raising errors. Only requires the minimal set of fields (entity, start, end).

    Args:
        entity_dict: Dictionary representation of entity from detection method
        source: Source of detection (presidio, gliner, ai)
        doc_id: Document identifier (overrides doc_id in entity_dict if provided)

    Returns:
        Normalized Entity object

    Raises:
        ValueError: If required fields are missing

    Examples:
        >>> ent_dict = {"entity": "John", "start": 0, "end": 4, "extra": "value"}
        >>> entity = normalize_entity(ent_dict, "presidio")
        >>> entity.extra_fields["extra"]
        'value'
    """
    # Validate required fields
    missing_fields = REQUIRED_ENTITY_FIELDS - set(entity_dict.keys())
    if missing_fields:
        raise ValueError(f"Missing required fields: {missing_fields}")

    # Extract standard fields
    entity_text = str(entity_dict["entity"])
    start = int(entity_dict["start"])
    end = int(entity_dict["end"])
    entity_type = entity_dict.get("entity_type")
    if entity_type is not None:
        entity_type = str(entity_type)

    # Use provided doc_id or fall back to dict value
    entity_doc_id = doc_id if doc_id is not None else entity_dict.get("doc_id")
    if entity_doc_id is not None:
        entity_doc_id = str(entity_doc_id)

    # Extract score (different field names for different sources)
    score = entity_dict.get("score")
    if score is not None:
        score = float(score)

    # Collect extra fields (everything not in standard set)
    standard_fields = {"entity", "start", "end", "entity_type", "doc_id", "score"}
    extra_fields = {k: v for k, v in entity_dict.items() if k not in standard_fields}

    return Entity(
        entity=entity_text,
        start=start,
        end=end,
        entity_type=entity_type,
        doc_id=entity_doc_id,
        source=source,
        score=score,
        extra_fields=extra_fields,
    )


def calculate_match_score(
    entity1: Entity,
    entity2: Entity,
    fuzzy_threshold: int = DEFAULT_FUZZY_MATCH_THRESHOLD,
) -> Tuple[float, MatchType]:
    """
    Calculate match quality score between two entities.

    Scoring logic:
    - Exact match (text, start, end): 1.0
    - Overlap + fuzzy text match: 0.7
    - No match: 0.0

    Args:
        entity1: First entity to compare
        entity2: Second entity to compare
        fuzzy_threshold: Threshold for fuzzy string matching (0-100)

    Returns:
        Tuple of (match_score, match_type)

    Examples:
        >>> e1 = Entity("John", 0, 4, "PERSON")
        >>> e2 = Entity("John", 0, 4, "PERSON")
        >>> score, match_type = calculate_match_score(e1, e2)
        >>> score
        1.0
        >>> match_type
        <MatchType.EXACT: 'exact'>
    """
    # Exact match
    if (
        entity1.entity == entity2.entity
        and entity1.start == entity2.start
        and entity1.end == entity2.end
    ):
        return EXACT_MATCH_SCORE, MatchType.EXACT

    # Check for overlap with fuzzy text match
    if is_overlap(entity1.start, entity1.end, entity2.start, entity2.end):
        if is_fuzzy_match(entity1.entity, entity2.entity, threshold=fuzzy_threshold):
            return OVERLAP_MATCH_SCORE, MatchType.OVERLAP_FUZZY

    return 0.0, MatchType.NO_MATCH


def find_best_match(
    entity: Entity,
    candidates: List[Entity],
    fuzzy_threshold: int = DEFAULT_FUZZY_MATCH_THRESHOLD,
) -> Tuple[Optional[Entity], float, MatchType]:
    """
    Find the best matching entity from a list of candidates.

    Prefers exact matches, then overlap matches. Returns the highest scoring
    match above the fuzzy threshold.

    Args:
        entity: Entity to find a match for
        candidates: List of candidate entities to match against
        fuzzy_threshold: Threshold for fuzzy string matching (0-100)

    Returns:
        Tuple of (best_match_entity, match_score, match_type)
        Returns (None, 0.0, NO_MATCH) if no suitable match found

    Examples:
        >>> target = Entity("John Smith", 0, 10, "PERSON")
        >>> candidates = [Entity("John", 0, 4, "PERSON"), Entity("Smith", 5, 10, "PERSON")]
        >>> best, score, match_type = find_best_match(target, candidates)
    """
    best_match = None
    best_score = 0.0
    best_type = MatchType.NO_MATCH

    for candidate in candidates:
        score, match_type = calculate_match_score(entity, candidate, fuzzy_threshold)

        # Prefer exact matches, otherwise take highest score
        if match_type == MatchType.EXACT:
            return candidate, score, match_type
        elif score > best_score:
            best_match = candidate
            best_score = score
            best_type = match_type

    return best_match, best_score, best_type


def merge_entities(entities: List[Entity], match_type: MatchType) -> Dict[str, Any]:
    """
    Merge multiple matched entities from different sources into one result.

    Uses the longest entity text and combines scores from all sources.
    Selects entity_type with preference: ai > presidio > gliner > first available.

    Args:
        entities: List of matched entities from different sources (1-3)
        match_type: Type of match between entities

    Returns:
        Dictionary with merged entity data including all source scores

    Examples:
        >>> e1 = Entity("John", 0, 4, "PERSON", source="ai")
        >>> e2 = Entity("John", 0, 4, "PERSON", source="presidio", score=0.9)
        >>> merged = merge_entities([e1, e2], MatchType.EXACT)
        >>> merged["sources"]
        ['ai', 'presidio']
    """
    if not entities:
        raise ValueError("Cannot merge empty list of entities")

    # Select the longest entity text (most specific)
    longest_entity = max(entities, key=lambda e: len(e.entity))

    # Collect sources and scores
    sources = [e.source for e in entities if e.source]
    scores = {"presidio_score": None, "gliner_score": None, "ai_score": None}

    for entity in entities:
        if entity.source and entity.score is not None:
            scores[f"{entity.source}_score"] = entity.score

    # Select entity_type with preference order
    entity_type = None
    type_preference = ["ai", "presidio", "gliner"]

    for source_name in type_preference:
        for entity in entities:
            if entity.source == source_name and entity.entity_type:
                entity_type = entity.entity_type
                break
        if entity_type:
            break

    # Fallback to first available entity_type
    if not entity_type:
        for entity in entities:
            if entity.entity_type:
                entity_type = entity.entity_type
                break

    if not entity_type:
        entity_type = "UNKNOWN"

    # Get doc_id from first available
    doc_id = next((e.doc_id for e in entities if e.doc_id), None)

    return {
        "entity": longest_entity.entity,
        "entity_type": entity_type,
        "start": longest_entity.start,
        "end": longest_entity.end,
        "doc_id": doc_id,
        "presidio_score": scores["presidio_score"],
        "gliner_score": scores["gliner_score"],
        "ai_score": scores["ai_score"],
        "sources": sources,
        "match_type": match_type.value,
    }


def calculate_confidence(merged_entity: Dict[str, Any], match_type: MatchType) -> str:
    """
    Calculate confidence level using weighted scoring based on source agreement.

    Weighted scoring formula:
    - Each source contributes its weight if present and has a good score
    - Match type affects the weights (exact match = full weight, overlap = reduced)
    - Final score is compared against thresholds to determine confidence level

    Args:
        merged_entity: Merged entity dictionary with source scores
        match_type: Type of match (exact, overlap, etc.)

    Returns:
        Confidence level: "high", "medium", or "low"

    Examples:
        >>> entity = {
        ...     "sources": ["presidio", "ai"],
        ...     "presidio_score": 0.9,
        ...     "ai_score": None
        ... }
        >>> calculate_confidence(entity, MatchType.EXACT)
        'high'
    """
    sources = merged_entity.get("sources", [])

    if not sources:
        return "low"

    # Calculate weighted score
    total_weight = 0.0
    match_multiplier = 1.0 if match_type == MatchType.EXACT else 0.8

    for source in sources:
        if source in SOURCE_WEIGHTS:
            score_key = f"{source}_score"
            score = merged_entity.get(score_key)

            # Source contributes its weight, scaled by its confidence score
            if score is not None and score >= 0.5:
                # High scoring sources contribute full weight
                contribution = SOURCE_WEIGHTS[source] * match_multiplier
                if score >= 0.7:
                    contribution *= 1.0
                else:
                    contribution *= 0.7
                total_weight += contribution
            else:
                # Low/no score still contributes partial weight for presence
                total_weight += SOURCE_WEIGHTS[source] * 0.3 * match_multiplier

    # Determine confidence level based on weighted score
    if total_weight >= CONFIDENCE_THRESHOLDS["high"]:
        return "high"
    elif total_weight >= CONFIDENCE_THRESHOLDS["medium"]:
        return "medium"
    else:
        return "low"


class MultiSourceAligner:
    """
    Orchestrates entity alignment across multiple detection sources.

    Supports flexible alignment of entities from 1-3 sources (Presidio, GLiNER, AI)
    with intelligent matching and confidence scoring.

    Attributes:
        fuzzy_threshold: Threshold for fuzzy string matching (0-100)
    """

    def __init__(self, fuzzy_threshold: int = DEFAULT_FUZZY_MATCH_THRESHOLD):
        """
        Initialize the multi-source aligner.

        Args:
            fuzzy_threshold: Threshold for fuzzy string matching (0-100)
        """
        self.fuzzy_threshold = fuzzy_threshold

    def align(
        self,
        doc_id: str,
        presidio_entities: Optional[List[Dict[str, Any]]] = None,
        gliner_entities: Optional[List[Dict[str, Any]]] = None,
        ai_entities: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Align entities from multiple sources for a single document.

        Args:
            doc_id: Document identifier
            presidio_entities: List of entities from Presidio (optional)
            gliner_entities: List of entities from GLiNER (optional)
            ai_entities: List of entities from AI detection (optional)

        Returns:
            List of aligned entities with confidence scores

        Examples:
            >>> aligner = MultiSourceAligner()
            >>> aligned = aligner.align(
            ...     "doc1",
            ...     presidio_entities=[{"entity": "John", "start": 0, "end": 4}],
            ...     ai_entities=[{"entity": "John", "start": 0, "end": 4}]
            ... )
        """
        # Normalize all entities
        normalized_entities = {
            "presidio": self._normalize_entities(presidio_entities, "presidio", doc_id),
            "gliner": self._normalize_entities(gliner_entities, "gliner", doc_id),
            "ai": self._normalize_entities(ai_entities, "ai", doc_id),
        }

        # Track which entities have been matched
        used_entities = {"presidio": set(), "gliner": set(), "ai": set()}

        results = []

        # Start with the source that has the most entities (likely most complete)
        primary_source = max(
            normalized_entities.keys(), key=lambda s: len(normalized_entities[s])
        )

        # Match entities from all sources
        for primary_idx, primary_entity in enumerate(
            normalized_entities[primary_source]
        ):
            matches = [primary_entity]
            match_types = [MatchType.EXACT]  # Primary entity matches itself exactly
            used_entities[primary_source].add(primary_idx)

            # Find matches in other sources
            for other_source in ["presidio", "gliner", "ai"]:
                if other_source == primary_source:
                    continue

                candidates = [
                    entity
                    for idx, entity in enumerate(normalized_entities[other_source])
                    if idx not in used_entities[other_source]
                ]

                if candidates:
                    best_match, _score, match_type = find_best_match(
                        primary_entity, candidates, self.fuzzy_threshold
                    )

                    if best_match and match_type != MatchType.NO_MATCH:
                        matches.append(best_match)
                        match_types.append(match_type)
                        # Mark as used
                        idx = normalized_entities[other_source].index(best_match)
                        used_entities[other_source].add(idx)

            # Determine overall match type (use worst match type)
            overall_match_type = (
                MatchType.EXACT
                if all(mt == MatchType.EXACT for mt in match_types)
                else (
                    match_types[0] if len(match_types) == 1 else MatchType.OVERLAP_FUZZY
                )
            )

            # Merge matched entities
            merged = merge_entities(matches, overall_match_type)
            merged["confidence"] = calculate_confidence(merged, overall_match_type)
            results.append(merged)

        # Add unmatched entities from other sources
        for source in ["presidio", "gliner", "ai"]:
            if source == primary_source:
                continue

            for idx, entity in enumerate(normalized_entities[source]):
                if idx not in used_entities[source]:
                    merged = merge_entities([entity], MatchType.EXACT)
                    merged["confidence"] = calculate_confidence(merged, MatchType.EXACT)
                    results.append(merged)

        # Clean up results (remove internal fields)
        cleaned_results = []
        for result in results:
            cleaned = {
                "entity": result["entity"],
                "entity_type": result["entity_type"],
                "start": result["start"],
                "end": result["end"],
                "doc_id": result["doc_id"],
                "presidio_score": result["presidio_score"],
                "gliner_score": result["gliner_score"],
                "ai_score": result["ai_score"],
                "confidence": result["confidence"],
            }
            cleaned_results.append(cleaned)

        return cleaned_results

    def _normalize_entities(
        self, entities: Optional[List[Dict[str, Any]]], source: str, doc_id: str
    ) -> List[Entity]:
        """
        Normalize a list of entity dictionaries, filtering by doc_id.

        Args:
            entities: List of entity dictionaries (or None)
            source: Source name (presidio, gliner, ai)
            doc_id: Document identifier to filter by

        Returns:
            List of normalized Entity objects for this document
        """
        if not entities:
            return []

        normalized = []
        for entity_dict in entities:
            try:
                # Check if entity belongs to this document
                entity_doc_id = entity_dict.get("doc_id")
                if entity_doc_id is not None and str(entity_doc_id) != str(doc_id):
                    continue

                entity = normalize_entity(entity_dict, source, doc_id)
                normalized.append(entity)
            except (ValueError, KeyError, TypeError):
                # Skip malformed entities
                continue

        return normalized


def align_entities_multi_source(
    presidio_entities: Optional[List[Dict[str, Any]]],
    gliner_entities: Optional[List[Dict[str, Any]]],
    ai_entities: Optional[List[Dict[str, Any]]],
    doc_id: str,
    fuzzy_threshold: int = DEFAULT_FUZZY_MATCH_THRESHOLD,
) -> List[Dict[str, Any]]:
    """
    Align entities from multiple sources (1-3) for a single document.

    This is a convenience function that wraps MultiSourceAligner for
    simpler usage in pandas UDFs and standalone processing.

    Args:
        presidio_entities: List of entities from Presidio (optional)
        gliner_entities: List of entities from GLiNER (optional)
        ai_entities: List of entities from AI detection (optional)
        doc_id: Document identifier
        fuzzy_threshold: Threshold for fuzzy string matching (0-100)

    Returns:
        List of aligned entities with confidence scores

    Examples:
        >>> aligned = align_entities_multi_source(
        ...     presidio_entities=[{"entity": "John", "start": 0, "end": 4}],
        ...     gliner_entities=None,
        ...     ai_entities=[{"entity": "John", "start": 0, "end": 4}],
        ...     doc_id="doc1"
        ... )
    """
    aligner = MultiSourceAligner(fuzzy_threshold=fuzzy_threshold)
    return aligner.align(
        doc_id=doc_id,
        presidio_entities=presidio_entities,
        gliner_entities=gliner_entities,
        ai_entities=ai_entities,
    )


def align_entities_udf(
    fuzzy_threshold: int = DEFAULT_FUZZY_MATCH_THRESHOLD,
    include_presidio: bool = True,
    include_gliner: bool = False,
    include_ai: bool = True,
):
    """
    Create a Pandas UDF for aligning entities from multiple detection sources.

    Supports flexible combinations of Presidio, GLiNER, and AI detection sources.
    The UDF expects columns for each enabled source.

    Args:
        fuzzy_threshold: Threshold for fuzzy string matching (0-100)
        include_presidio: Whether to include Presidio entities (default: True)
        include_gliner: Whether to include GLiNER entities (default: False)
        include_ai: Whether to include AI entities (default: True)

    Returns:
        A Pandas UDF that aligns entities from enabled detection methods

    Examples:
        >>> # Two sources: AI + Presidio
        >>> align_udf = align_entities_udf(include_gliner=False)
        >>> df = df.withColumn("aligned",
        ...     align_udf(col("ai_entities"), col("presidio_entities"),
        ...               col("gliner_entities"), col("doc_id")))

        >>> # All three sources
        >>> align_udf = align_entities_udf(include_presidio=True,
        ...                                 include_gliner=True,
        ...                                 include_ai=True)
        >>> df = df.withColumn("aligned",
        ...     align_udf(col("ai_entities"), col("presidio_entities"),
        ...               col("gliner_entities"), col("doc_id")))
    """
    # Define the schema for the output
    entity_struct = StructType(
        [
            StructField("entity", StringType()),
            StructField("entity_type", StringType()),
            StructField("start", IntegerType()),
            StructField("end", IntegerType()),
            StructField("doc_id", StringType()),
            StructField("presidio_score", DoubleType()),
            StructField("gliner_score", DoubleType()),
            StructField("ai_score", DoubleType()),
            StructField("confidence", StringType()),
        ]
    )
    result_type = ArrayType(entity_struct)

    @pandas_udf(result_type)
    def _align_udf(
        ai_col: pd.Series,
        presidio_col: pd.Series,
        gliner_col: pd.Series,
        doc_id_col: pd.Series,
    ) -> pd.Series:
        """
        Align entities for each row in the batch.

        Note: All columns are always passed but only used if the corresponding
        include flag is True. This maintains a consistent UDF signature.
        """
        results = []

        for ai_ents, presidio_ents, gliner_ents, doc_id in zip(
            ai_col, presidio_col, gliner_col, doc_id_col
        ):
            aligned = align_entities_multi_source(
                presidio_entities=presidio_ents if include_presidio else None,
                gliner_entities=gliner_ents if include_gliner else None,
                ai_entities=ai_ents if include_ai else None,
                doc_id=doc_id,
                fuzzy_threshold=fuzzy_threshold,
            )
            results.append(aligned)

        return pd.Series(results)

    return _align_udf


# Legacy function for backward compatibility
def align_entities_row(
    ai_entities: List[Dict[str, Any]],
    presidio_entities: List[Dict[str, Any]],
    doc_id: str,
    fuzzy_threshold: int = DEFAULT_FUZZY_MATCH_THRESHOLD,
) -> List[Dict[str, Any]]:
    """
    Align entities from AI and Presidio detection for a single document.

    **DEPRECATED**: Use align_entities_multi_source() instead for better flexibility.

    This function is maintained for backward compatibility with existing code.
    It wraps the new multi-source alignment function.

    Args:
        ai_entities: List of entities detected by AI
        presidio_entities: List of entities detected by Presidio
        doc_id: Document identifier
        fuzzy_threshold: Threshold for fuzzy string matching (0-100)

    Returns:
        List of aligned entities with confidence scores
    """
    return align_entities_multi_source(
        presidio_entities=presidio_entities,
        gliner_entities=None,
        ai_entities=ai_entities,
        doc_id=doc_id,
        fuzzy_threshold=fuzzy_threshold,
    )
