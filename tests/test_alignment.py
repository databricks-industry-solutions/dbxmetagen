"""
Unit tests for alignment functions.

Run with: pytest tests/test_alignment.py -v
"""

import pytest
from pyspark.sql import SparkSession
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dbxmetagen.redaction.alignment import (
    Entity,
    normalize_entity,
    find_best_match,
    merge_entities,
    calculate_confidence,
    MultiSourceAligner,
    MatchType,
)


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for tests."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("alignment-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


class TestEntity:
    """Tests for Entity dataclass."""

    def test_create_entity(self):
        """Test basic entity creation."""
        entity = Entity(
            entity="John Smith",
            start=0,
            end=11,
            entity_type="PERSON",
            doc_id="1",
            source="presidio",
            score=0.95,
        )
        assert entity.entity == "John Smith"
        assert entity.start == 0
        assert entity.end == 11
        assert entity.entity_type == "PERSON"
        assert entity.source == "presidio"
        assert entity.score == 0.95

    def test_entity_validation(self):
        """Test that entity validates required fields."""
        with pytest.raises(ValueError):
            Entity(entity=None, start=0, end=10)

        with pytest.raises(ValueError):
            Entity(entity="John", start=None, end=10)

        with pytest.raises(ValueError):
            Entity(entity="John", start=0, end=None)


class TestNormalizeEntity:
    """Tests for entity normalization."""

    def test_normalize_basic(self):
        """Test basic entity normalization."""
        entity_dict = {
            "entity": "John Smith",
            "start": 0,
            "end": 11,
            "entity_type": "PERSON",
            "score": 0.95,
        }
        entity = normalize_entity(entity_dict, source="presidio", doc_id="1")

        assert entity.entity == "John Smith"
        assert entity.start == 0
        assert entity.end == 11
        assert entity.entity_type == "PERSON"
        assert entity.source == "presidio"
        assert entity.doc_id == "1"
        assert entity.score == 0.95

    def test_normalize_with_extra_fields(self):
        """Test normalization with extra fields."""
        entity_dict = {
            "entity": "John Smith",
            "start": 0,
            "end": 11,
            "extra_field": "extra_value",
            "another_field": 123,
        }
        entity = normalize_entity(entity_dict, source="ai", doc_id="1")

        assert entity.entity == "John Smith"
        assert entity.extra_fields["extra_field"] == "extra_value"
        assert entity.extra_fields["another_field"] == 123


class TestFindBestMatch:
    """Tests for finding best matching entity."""

    def test_exact_match(self):
        """Test finding exact match."""
        entity1 = Entity("John Smith", 0, 11, "PERSON", source="presidio")
        entity2 = Entity("John Smith", 0, 11, "PERSON", source="ai")
        entity3 = Entity("Jane Doe", 20, 28, "PERSON", source="ai")

        candidates = [entity2, entity3]
        best_match, score, match_type = find_best_match(entity1, candidates)

        assert best_match == entity2
        assert match_type == MatchType.EXACT

    def test_overlap_match(self):
        """Test finding overlap match."""
        entity1 = Entity("John Smith", 0, 11, "PERSON", source="presidio")
        entity2 = Entity("Smith", 5, 11, "PERSON", source="ai")
        entity3 = Entity("Jane Doe", 20, 28, "PERSON", source="ai")

        candidates = [entity2, entity3]
        best_match, score, match_type = find_best_match(entity1, candidates)

        assert best_match == entity2
        assert match_type == MatchType.OVERLAP_FUZZY

    def test_no_match(self):
        """Test when no match is found."""
        entity1 = Entity("John Smith", 0, 11, "PERSON", source="presidio")
        entity2 = Entity("Jane Doe", 20, 28, "PERSON", source="ai")

        candidates = [entity2]
        best_match, score, match_type = find_best_match(entity1, candidates)

        assert best_match is None
        assert match_type == MatchType.NO_MATCH


class TestMergeEntities:
    """Tests for merging entities."""

    def test_merge_two_entities(self):
        """Test merging two matched entities."""
        entity1 = Entity("John Smith", 0, 11, "PERSON", source="presidio", score=0.9)
        entity2 = Entity("John Smith", 0, 11, "PERSON", source="ai", score=0.95)

        merged = merge_entities([entity1, entity2], MatchType.EXACT)

        assert merged["entity"] == "John Smith"
        assert merged["start"] == 0
        assert merged["end"] == 11
        assert merged["entity_type"] == "PERSON"
        assert "presidio" in merged["sources"]
        assert "ai" in merged["sources"]
        assert merged["presidio_score"] == 0.9
        assert merged["ai_score"] == 0.95

    def test_merge_selects_longest(self):
        """Test that merge selects longest entity text."""
        entity1 = Entity("John", 0, 4, "PERSON", source="presidio")
        entity2 = Entity("John Smith", 0, 11, "PERSON", source="ai")

        merged = merge_entities([entity1, entity2], MatchType.OVERLAP_FUZZY)

        # Should select the longer "John Smith"
        assert merged["entity"] == "John Smith"
        assert merged["start"] == 0
        assert merged["end"] == 11

    def test_merge_entity_type_preference(self):
        """Test entity type preference (ai > presidio > gliner)."""
        entity1 = Entity("John", 0, 4, "PERSON", source="presidio")
        entity2 = Entity("John", 0, 4, "NAME", source="ai")

        merged = merge_entities([entity1, entity2], MatchType.EXACT)

        # Should prefer ai's entity type
        assert merged["entity_type"] == "NAME"


class TestCalculateConfidence:
    """Tests for confidence calculation."""

    def test_high_confidence_two_sources(self):
        """Test high confidence with two sources."""
        merged = {
            "sources": ["presidio", "ai"],
            "presidio_score": 0.9,
            "ai_score": 0.95,
        }
        confidence = calculate_confidence(merged, MatchType.EXACT)
        assert confidence == "high"

    def test_medium_confidence_one_source(self):
        """Test medium confidence with one source."""
        merged = {
            "sources": ["presidio"],
            "presidio_score": 0.9,
            "ai_score": None,
            "gliner_score": None,
        }
        confidence = calculate_confidence(merged, MatchType.EXACT)
        assert confidence == "medium"

    def test_low_confidence_overlap_match(self):
        """Test low confidence with overlap match."""
        merged = {
            "sources": ["presidio"],
            "presidio_score": 0.6,
            "ai_score": None,
            "gliner_score": None,
        }
        confidence = calculate_confidence(merged, MatchType.OVERLAP_FUZZY)
        # Overlap match typically gets lower confidence
        assert confidence in ["medium", "low"]


class TestMultiSourceAligner:
    """Tests for multi-source alignment."""

    def test_align_two_sources_exact_match(self):
        """Test aligning two sources with exact match."""
        aligner = MultiSourceAligner(fuzzy_threshold=80)

        presidio_entities = [
            {"entity": "John Smith", "start": 0, "end": 11, "score": 0.9}
        ]
        ai_entities = [{"entity": "John Smith", "start": 0, "end": 11, "score": 0.95}]

        aligned = aligner.align(
            doc_id="1", presidio_entities=presidio_entities, ai_entities=ai_entities
        )

        # Should produce one aligned entity
        assert len(aligned) == 1
        assert aligned[0]["entity"] == "John Smith"
        assert aligned[0]["presidio_score"] is not None
        assert aligned[0]["ai_score"] is not None

    def test_align_two_sources_different_boundaries(self):
        """Test aligning entities with different boundaries (longest wins)."""
        aligner = MultiSourceAligner(fuzzy_threshold=80)

        presidio_entities = [{"entity": "John", "start": 0, "end": 4, "score": 0.9}]
        ai_entities = [{"entity": "John Smith", "start": 0, "end": 11, "score": 0.95}]

        aligned = aligner.align(
            doc_id="1", presidio_entities=presidio_entities, ai_entities=ai_entities
        )

        # Should produce one aligned entity with longest text
        assert len(aligned) == 1
        assert aligned[0]["entity"] == "John Smith"
        assert aligned[0]["end"] == 11

    def test_align_union_behavior(self):
        """Test that alignment includes all unmatched entities (union)."""
        aligner = MultiSourceAligner(fuzzy_threshold=80)

        presidio_entities = [
            {"entity": "John Smith", "start": 0, "end": 11, "score": 0.9},
            {"entity": "Unique Presidio", "start": 20, "end": 35, "score": 0.8},
        ]
        ai_entities = [
            {"entity": "John Smith", "start": 0, "end": 11, "score": 0.95},
            {"entity": "Unique AI", "start": 40, "end": 49, "score": 0.85},
        ]

        aligned = aligner.align(
            doc_id="1", presidio_entities=presidio_entities, ai_entities=ai_entities
        )

        # Should produce 3 entities: 1 matched + 2 unmatched
        assert len(aligned) == 3

        # Check that all unique entities are present
        entities_text = [e["entity"] for e in aligned]
        assert "John Smith" in entities_text
        assert "Unique Presidio" in entities_text
        assert "Unique AI" in entities_text

    def test_align_three_sources(self):
        """Test aligning three sources."""
        aligner = MultiSourceAligner(fuzzy_threshold=80)

        presidio_entities = [
            {"entity": "John Smith", "start": 0, "end": 11, "score": 0.9}
        ]
        ai_entities = [{"entity": "John Smith", "start": 0, "end": 11, "score": 0.95}]
        gliner_entities = [
            {"entity": "John Smith", "start": 0, "end": 11, "score": 0.88}
        ]

        aligned = aligner.align(
            doc_id="1",
            presidio_entities=presidio_entities,
            ai_entities=ai_entities,
            gliner_entities=gliner_entities,
        )

        # Should produce one aligned entity with all three sources
        assert len(aligned) == 1
        assert aligned[0]["entity"] == "John Smith"
        assert aligned[0]["presidio_score"] is not None
        assert aligned[0]["ai_score"] is not None
        assert aligned[0]["gliner_score"] is not None

    def test_align_no_overlap(self):
        """Test aligning entities with no overlap."""
        aligner = MultiSourceAligner(fuzzy_threshold=80)

        presidio_entities = [
            {"entity": "John Smith", "start": 0, "end": 11, "score": 0.9}
        ]
        ai_entities = [{"entity": "Jane Doe", "start": 20, "end": 28, "score": 0.95}]

        aligned = aligner.align(
            doc_id="1", presidio_entities=presidio_entities, ai_entities=ai_entities
        )

        # Should produce two separate entities (no match)
        assert len(aligned) == 2

        entities_text = [e["entity"] for e in aligned]
        assert "John Smith" in entities_text
        assert "Jane Doe" in entities_text

    def test_align_primary_source_selection(self):
        """Test that alignment starts with source with most entities."""
        aligner = MultiSourceAligner(fuzzy_threshold=80)

        # AI has more entities, should be primary source
        presidio_entities = [{"entity": "John", "start": 0, "end": 4, "score": 0.9}]
        ai_entities = [
            {"entity": "John Smith", "start": 0, "end": 11, "score": 0.95},
            {"entity": "Jane Doe", "start": 20, "end": 28, "score": 0.93},
            {"entity": "Bob Wilson", "start": 40, "end": 50, "score": 0.91},
        ]

        aligned = aligner.align(
            doc_id="1", presidio_entities=presidio_entities, ai_entities=ai_entities
        )

        # Should have 3 aligned entities (AI's count, with presidio merged to first)
        assert len(aligned) == 3


class TestAlignmentEdgeCases:
    """Tests for alignment edge cases."""

    def test_align_empty_sources(self):
        """Test alignment with empty sources."""
        aligner = MultiSourceAligner(fuzzy_threshold=80)

        aligned = aligner.align(
            doc_id="1", presidio_entities=[], ai_entities=[], gliner_entities=[]
        )

        assert len(aligned) == 0

    def test_align_none_sources(self):
        """Test alignment with None sources."""
        aligner = MultiSourceAligner(fuzzy_threshold=80)

        aligned = aligner.align(
            doc_id="1", presidio_entities=None, ai_entities=None, gliner_entities=None
        )

        assert len(aligned) == 0

    def test_align_single_source(self):
        """Test alignment with only one source."""
        aligner = MultiSourceAligner(fuzzy_threshold=80)

        presidio_entities = [
            {"entity": "John Smith", "start": 0, "end": 11, "score": 0.9}
        ]

        aligned = aligner.align(
            doc_id="1",
            presidio_entities=presidio_entities,
            ai_entities=None,
            gliner_entities=None,
        )

        # Should still produce aligned result with single source
        assert len(aligned) == 1
        assert aligned[0]["entity"] == "John Smith"
        assert aligned[0]["presidio_score"] == 0.9


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
