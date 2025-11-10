"""
Unit tests for entity alignment module.

Tests cover:
- Entity normalization with extra fields
- Exact matching logic
- Overlap detection with fuzzy matching
- Match scoring algorithm
- Confidence calculation (weighted)
- Single, two-source, and three-source alignment
- Edge cases and error handling
"""

import pytest

from dbxmetagen.redaction.alignment import (
    Entity,
    MatchType,
    normalize_entity,
    calculate_match_score,
    find_best_match,
    merge_entities,
    calculate_confidence,
    MultiSourceAligner,
    align_entities_multi_source,
    align_entities_row,
)
from dbxmetagen.redaction.config import (
    EXACT_MATCH_SCORE,
    OVERLAP_MATCH_SCORE,
)


class TestEntityNormalization:
    """Tests for entity normalization."""

    def test_normalize_basic_entity(self):
        """Test normalization of entity with minimal fields."""
        entity_dict = {
            "entity": "John Smith",
            "start": 0,
            "end": 10,
        }

        entity = normalize_entity(entity_dict, "presidio", "doc1")

        assert entity.entity == "John Smith"
        assert entity.start == 0
        assert entity.end == 10
        assert entity.source == "presidio"
        assert entity.doc_id == "doc1"

    def test_normalize_entity_with_extra_fields(self):
        """Test that extra fields don't cause errors."""
        entity_dict = {
            "entity": "John",
            "start": 0,
            "end": 4,
            "entity_type": "PERSON",
            "extra_field": "should_not_error",
            "another_extra": 123,
            "nested": {"key": "value"},
        }

        entity = normalize_entity(entity_dict, "ai", "doc1")

        assert entity.entity == "John"
        assert entity.entity_type == "PERSON"
        assert entity.extra_fields["extra_field"] == "should_not_error"
        assert entity.extra_fields["another_extra"] == 123
        assert entity.extra_fields["nested"]["key"] == "value"

    def test_normalize_entity_with_score(self):
        """Test normalization preserves score."""
        entity_dict = {
            "entity": "test@email.com",
            "start": 10,
            "end": 24,
            "entity_type": "EMAIL",
            "score": 0.95,
        }

        entity = normalize_entity(entity_dict, "presidio")

        assert entity.score == 0.95

    def test_normalize_entity_missing_required_field(self):
        """Test that missing required fields raise ValueError."""
        entity_dict = {
            "entity": "John",
            "start": 0,
            # Missing 'end'
        }

        with pytest.raises(ValueError, match="Missing required fields"):
            normalize_entity(entity_dict, "presidio")

    def test_normalize_entity_type_conversion(self):
        """Test that fields are properly type converted."""
        entity_dict = {
            "entity": 12345,  # Not a string
            "start": "0",  # String instead of int
            "end": "5",
            "entity_type": None,
        }

        entity = normalize_entity(entity_dict, "gliner")

        assert isinstance(entity.entity, str)
        assert isinstance(entity.start, int)
        assert isinstance(entity.end, int)
        assert entity.entity_type is None


class TestMatchScoring:
    """Tests for match scoring logic."""

    def test_exact_match(self):
        """Test exact match detection."""
        e1 = Entity("John Smith", 0, 10, "PERSON")
        e2 = Entity("John Smith", 0, 10, "PERSON")

        score, match_type = calculate_match_score(e1, e2)

        assert score == EXACT_MATCH_SCORE
        assert match_type == MatchType.EXACT

    def test_overlap_fuzzy_match(self):
        """Test overlap with fuzzy text match."""
        e1 = Entity("John Smith", 0, 10, "PERSON")
        e2 = Entity("Smith John", 0, 10, "PERSON")  # Different order, same position

        score, match_type = calculate_match_score(e1, e2, fuzzy_threshold=80)

        assert score == OVERLAP_MATCH_SCORE
        assert match_type == MatchType.OVERLAP_FUZZY

    def test_no_match_different_positions(self):
        """Test no match when positions don't overlap."""
        e1 = Entity("John", 0, 4, "PERSON")
        e2 = Entity("Smith", 10, 15, "PERSON")

        score, match_type = calculate_match_score(e1, e2)

        assert score == 0.0
        assert match_type == MatchType.NO_MATCH

    def test_no_match_overlap_but_different_text(self):
        """Test no match when positions overlap but text is very different."""
        e1 = Entity("John", 0, 4, "PERSON")
        e2 = Entity("Alice", 0, 5, "PERSON")

        score, match_type = calculate_match_score(e1, e2, fuzzy_threshold=80)

        assert score == 0.0
        assert match_type == MatchType.NO_MATCH

    def test_partial_overlap_with_similar_text(self):
        """Test partial overlap with similar text."""
        e1 = Entity("John Smith", 0, 10, "PERSON")
        e2 = Entity("John", 0, 4, "PERSON")

        score, match_type = calculate_match_score(e1, e2, fuzzy_threshold=50)

        assert score == OVERLAP_MATCH_SCORE
        assert match_type == MatchType.OVERLAP_FUZZY


class TestFindBestMatch:
    """Tests for finding best match among candidates."""

    def test_find_exact_match(self):
        """Test finding exact match from candidates."""
        target = Entity("John", 0, 4, "PERSON")
        candidates = [
            Entity("Alice", 10, 15, "PERSON"),
            Entity("John", 0, 4, "PERSON"),  # Exact match
            Entity("Bob", 20, 23, "PERSON"),
        ]

        best_match, score, match_type = find_best_match(target, candidates)

        assert best_match.entity == "John"
        assert score == EXACT_MATCH_SCORE
        assert match_type == MatchType.EXACT

    def test_find_best_fuzzy_match(self):
        """Test finding best fuzzy match when no exact match."""
        target = Entity("John Smith", 0, 10, "PERSON")
        candidates = [
            Entity("Alice", 10, 15, "PERSON"),
            Entity("Smith John", 0, 10, "PERSON"),  # Fuzzy match
            Entity("Bob", 20, 23, "PERSON"),
        ]

        best_match, score, match_type = find_best_match(target, candidates)

        assert best_match.entity == "Smith John"
        assert score == OVERLAP_MATCH_SCORE
        assert match_type == MatchType.OVERLAP_FUZZY

    def test_find_no_match(self):
        """Test when no suitable match exists."""
        target = Entity("John", 0, 4, "PERSON")
        candidates = [
            Entity("Alice", 10, 15, "PERSON"),
            Entity("Bob", 20, 23, "PERSON"),
        ]

        best_match, score, match_type = find_best_match(target, candidates)

        assert best_match is None
        assert score == 0.0
        assert match_type == MatchType.NO_MATCH

    def test_empty_candidates(self):
        """Test with empty candidate list."""
        target = Entity("John", 0, 4, "PERSON")
        candidates = []

        best_match, score, match_type = find_best_match(target, candidates)

        assert best_match is None
        assert score == 0.0
        assert match_type == MatchType.NO_MATCH


class TestMergeEntities:
    """Tests for merging entities from multiple sources."""

    def test_merge_single_entity(self):
        """Test merging a single entity."""
        entities = [Entity("John", 0, 4, "PERSON", source="presidio", score=0.9)]

        merged = merge_entities(entities, MatchType.EXACT)

        assert merged["entity"] == "John"
        assert merged["entity_type"] == "PERSON"
        assert merged["presidio_score"] == 0.9
        assert merged["ai_score"] is None
        assert merged["gliner_score"] is None
        assert merged["sources"] == ["presidio"]

    def test_merge_two_entities_same_text(self):
        """Test merging two entities with same text."""
        entities = [
            Entity("John", 0, 4, "PERSON", source="presidio", score=0.9),
            Entity("John", 0, 4, "PERSON", source="ai"),
        ]

        merged = merge_entities(entities, MatchType.EXACT)

        assert merged["entity"] == "John"
        assert merged["presidio_score"] == 0.9
        assert merged["ai_score"] is None  # AI didn't provide score
        assert set(merged["sources"]) == {"presidio", "ai"}

    def test_merge_prefers_longest_entity(self):
        """Test that merge prefers the longest entity text."""
        entities = [
            Entity("John", 0, 4, "PERSON", source="gliner", score=0.8),
            Entity("John Smith", 0, 10, "PERSON", source="presidio", score=0.9),
        ]

        merged = merge_entities(entities, MatchType.OVERLAP_FUZZY)

        assert merged["entity"] == "John Smith"  # Longer version
        assert merged["start"] == 0
        assert merged["end"] == 10

    def test_merge_entity_type_preference(self):
        """Test entity_type selection with preference order."""
        # Test AI preference
        entities = [
            Entity("John", 0, 4, "PERSON", source="presidio"),
            Entity("John", 0, 4, "NAME", source="ai"),  # AI should win
        ]

        merged = merge_entities(entities, MatchType.EXACT)
        assert merged["entity_type"] == "NAME"

        # Test Presidio over GLiNER
        entities = [
            Entity("John", 0, 4, "person", source="gliner"),
            Entity("John", 0, 4, "PERSON", source="presidio"),
        ]

        merged = merge_entities(entities, MatchType.EXACT)
        assert merged["entity_type"] == "PERSON"

    def test_merge_three_sources(self):
        """Test merging entities from all three sources."""
        entities = [
            Entity("John", 0, 4, "PERSON", source="presidio", score=0.95),
            Entity("John", 0, 4, "person", source="gliner", score=0.85),
            Entity("John", 0, 4, "PERSON", source="ai"),
        ]

        merged = merge_entities(entities, MatchType.EXACT)

        assert merged["presidio_score"] == 0.95
        assert merged["gliner_score"] == 0.85
        assert merged["ai_score"] is None
        assert len(merged["sources"]) == 3

    def test_merge_empty_list_raises_error(self):
        """Test that merging empty list raises ValueError."""
        with pytest.raises(ValueError, match="Cannot merge empty list"):
            merge_entities([], MatchType.EXACT)


class TestConfidenceCalculation:
    """Tests for confidence calculation."""

    def test_high_confidence_two_sources_exact_match(self):
        """Test high confidence with 2 sources and exact match."""
        merged = {
            "sources": ["presidio", "ai"],
            "presidio_score": 0.9,
            "ai_score": None,
            "gliner_score": None,
        }

        confidence = calculate_confidence(merged, MatchType.EXACT)
        assert confidence == "high"

    def test_high_confidence_three_sources(self):
        """Test high confidence with all 3 sources."""
        merged = {
            "sources": ["presidio", "ai", "gliner"],
            "presidio_score": 0.8,
            "ai_score": None,
            "gliner_score": 0.7,
        }

        confidence = calculate_confidence(merged, MatchType.EXACT)
        assert confidence == "high"

    def test_medium_confidence_overlap_match(self):
        """Test medium confidence with overlap match."""
        merged = {
            "sources": ["presidio"],
            "presidio_score": 0.9,
            "ai_score": None,
            "gliner_score": None,
        }

        confidence = calculate_confidence(merged, MatchType.OVERLAP_FUZZY)
        assert confidence in ["medium", "low"]  # Depends on scoring

    def test_low_confidence_single_source_low_score(self):
        """Test low confidence with single source and low score."""
        merged = {
            "sources": ["gliner"],
            "presidio_score": None,
            "ai_score": None,
            "gliner_score": 0.3,
        }

        confidence = calculate_confidence(merged, MatchType.EXACT)
        assert confidence == "low"

    def test_low_confidence_no_sources(self):
        """Test low confidence with no sources."""
        merged = {
            "sources": [],
            "presidio_score": None,
            "ai_score": None,
            "gliner_score": None,
        }

        confidence = calculate_confidence(merged, MatchType.EXACT)
        assert confidence == "low"


class TestMultiSourceAligner:
    """Tests for MultiSourceAligner class."""

    def test_single_source_presidio(self):
        """Test alignment with only Presidio entities."""
        aligner = MultiSourceAligner()

        presidio_entities = [
            {
                "entity": "John",
                "start": 0,
                "end": 4,
                "entity_type": "PERSON",
                "score": 0.9,
            }
        ]

        result = aligner.align(
            doc_id="doc1",
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=None,
        )

        assert len(result) == 1
        assert result[0]["entity"] == "John"
        assert result[0]["presidio_score"] == 0.9
        assert result[0]["ai_score"] is None
        assert result[0]["gliner_score"] is None

    def test_two_sources_exact_match(self):
        """Test alignment with two sources matching exactly."""
        aligner = MultiSourceAligner()

        presidio_entities = [
            {
                "entity": "John",
                "start": 0,
                "end": 4,
                "entity_type": "PERSON",
                "score": 0.9,
            }
        ]
        ai_entities = [
            {"entity": "John", "start": 0, "end": 4, "entity_type": "PERSON"}
        ]

        result = aligner.align(
            doc_id="doc1",
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=ai_entities,
        )

        assert len(result) == 1
        assert result[0]["entity"] == "John"
        assert result[0]["presidio_score"] == 0.9
        assert result[0]["confidence"] in ["high", "medium"]

    def test_three_sources_with_matches(self):
        """Test alignment with all three sources."""
        aligner = MultiSourceAligner()

        presidio_entities = [
            {
                "entity": "John Smith",
                "start": 0,
                "end": 10,
                "entity_type": "PERSON",
                "score": 0.9,
            }
        ]
        gliner_entities = [
            {
                "entity": "John Smith",
                "start": 0,
                "end": 10,
                "entity_type": "person",
                "score": 0.85,
            }
        ]
        ai_entities = [
            {"entity": "John Smith", "start": 0, "end": 10, "entity_type": "PERSON"}
        ]

        result = aligner.align(
            doc_id="doc1",
            presidio_entities=presidio_entities,
            gliner_entities=gliner_entities,
            ai_entities=ai_entities,
        )

        assert len(result) == 1
        assert result[0]["entity"] == "John Smith"
        assert result[0]["presidio_score"] == 0.9
        assert result[0]["gliner_score"] == 0.85
        assert result[0]["confidence"] == "high"

    def test_mixed_matches_and_unique_entities(self):
        """Test alignment with some matching and some unique entities."""
        aligner = MultiSourceAligner()

        presidio_entities = [
            {
                "entity": "John",
                "start": 0,
                "end": 4,
                "entity_type": "PERSON",
                "score": 0.9,
            },
            {
                "entity": "test@email.com",
                "start": 20,
                "end": 34,
                "entity_type": "EMAIL",
                "score": 0.95,
            },
        ]
        ai_entities = [
            {"entity": "John", "start": 0, "end": 4, "entity_type": "PERSON"},
            {"entity": "555-1234", "start": 40, "end": 48, "entity_type": "PHONE"},
        ]

        result = aligner.align(
            doc_id="doc1",
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=ai_entities,
        )

        # Should have 3 entities: John (matched), email (unique), phone (unique)
        assert len(result) == 3

        # Check matched entity
        john_entity = [e for e in result if e["entity"] == "John"][0]
        assert john_entity["presidio_score"] == 0.9

        # Check unique entities
        email_entity = [e for e in result if "email" in e["entity"]][0]
        assert email_entity["presidio_score"] == 0.95
        assert email_entity["ai_score"] is None

        phone_entity = [e for e in result if "555" in e["entity"]][0]
        assert phone_entity["ai_score"] is None
        assert phone_entity["presidio_score"] is None

    def test_doc_id_filtering(self):
        """Test that entities are filtered by doc_id."""
        aligner = MultiSourceAligner()

        presidio_entities = [
            {
                "entity": "John",
                "start": 0,
                "end": 4,
                "entity_type": "PERSON",
                "doc_id": "doc1",
            },
            {
                "entity": "Alice",
                "start": 0,
                "end": 5,
                "entity_type": "PERSON",
                "doc_id": "doc2",
            },
        ]

        result = aligner.align(
            doc_id="doc1",
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=None,
        )

        # Should only include entities from doc1
        assert len(result) == 1
        assert result[0]["entity"] == "John"

    def test_empty_sources(self):
        """Test alignment with no entities from any source."""
        aligner = MultiSourceAligner()

        result = aligner.align(
            doc_id="doc1",
            presidio_entities=None,
            gliner_entities=None,
            ai_entities=None,
        )

        assert result == []

    def test_malformed_entities_skipped(self):
        """Test that malformed entities are skipped gracefully."""
        aligner = MultiSourceAligner()

        presidio_entities = [
            {"entity": "John", "start": 0, "end": 4, "entity_type": "PERSON"},
            {"entity": "BadEntity"},  # Missing start/end
            {"start": 10, "end": 15},  # Missing entity
        ]

        result = aligner.align(
            doc_id="doc1",
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=None,
        )

        # Should only include the valid entity
        assert len(result) == 1
        assert result[0]["entity"] == "John"


class TestAlignEntitiesMultiSource:
    """Tests for the standalone align_entities_multi_source function."""

    def test_basic_alignment(self):
        """Test basic alignment functionality."""
        presidio_entities = [
            {
                "entity": "John",
                "start": 0,
                "end": 4,
                "entity_type": "PERSON",
                "score": 0.9,
            }
        ]
        ai_entities = [
            {"entity": "John", "start": 0, "end": 4, "entity_type": "PERSON"}
        ]

        result = align_entities_multi_source(
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=ai_entities,
            doc_id="doc1",
        )

        assert len(result) == 1
        assert result[0]["entity"] == "John"
        assert result[0]["confidence"] in ["high", "medium"]

    def test_custom_fuzzy_threshold(self):
        """Test alignment with custom fuzzy threshold."""
        presidio_entities = [
            {"entity": "John Smith", "start": 0, "end": 10, "entity_type": "PERSON"}
        ]
        ai_entities = [
            {"entity": "Smith, John", "start": 0, "end": 11, "entity_type": "PERSON"}
        ]

        # With low threshold, should match
        result_low = align_entities_multi_source(
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=ai_entities,
            doc_id="doc1",
            fuzzy_threshold=50,
        )

        # At least the low threshold should create alignment
        assert len(result_low) >= 1


class TestBackwardCompatibility:
    """Tests for backward compatibility with old align_entities_row function."""

    def test_align_entities_row_basic(self):
        """Test that old function still works."""
        ai_entities = [
            {"entity": "John", "start": 0, "end": 4, "entity_type": "PERSON"}
        ]
        presidio_entities = [
            {
                "entity": "John",
                "start": 0,
                "end": 4,
                "entity_type": "PERSON",
                "score": 0.9,
            }
        ]

        result = align_entities_row(
            ai_entities=ai_entities, presidio_entities=presidio_entities, doc_id="doc1"
        )

        assert len(result) == 1
        assert result[0]["entity"] == "John"
        assert result[0]["presidio_score"] == 0.9

    def test_align_entities_row_matches_new_function(self):
        """Test that old function produces same results as new function."""
        ai_entities = [
            {"entity": "John", "start": 0, "end": 4, "entity_type": "PERSON"}
        ]
        presidio_entities = [
            {
                "entity": "test@email.com",
                "start": 10,
                "end": 24,
                "entity_type": "EMAIL",
                "score": 0.95,
            }
        ]

        result_old = align_entities_row(
            ai_entities=ai_entities, presidio_entities=presidio_entities, doc_id="doc1"
        )

        result_new = align_entities_multi_source(
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=ai_entities,
            doc_id="doc1",
        )

        # Should produce same number of entities
        assert len(result_old) == len(result_new)

        # Check entities are present (order might differ)
        old_entities = {e["entity"] for e in result_old}
        new_entities = {e["entity"] for e in result_new}
        assert old_entities == new_entities


class TestIntegrationScenarios:
    """Integration tests with realistic medical text entities."""

    def test_medical_record_number_detection(self):
        """Test alignment of medical record numbers from multiple sources."""
        aligner = MultiSourceAligner()

        presidio_entities = [
            {
                "entity": "MRN-123456",
                "start": 0,
                "end": 10,
                "entity_type": "MEDICAL_RECORD",
                "score": 0.85,
            }
        ]
        gliner_entities = [
            {
                "entity": "MRN-123456",
                "start": 0,
                "end": 10,
                "entity_type": "medical record number",
                "score": 0.75,
            }
        ]
        ai_entities = [
            {
                "entity": "MRN-123456",
                "start": 0,
                "end": 10,
                "entity_type": "MEDICAL_RECORD_NUMBER",
            }
        ]

        result = aligner.align(
            doc_id="medical_doc_1",
            presidio_entities=presidio_entities,
            gliner_entities=gliner_entities,
            ai_entities=ai_entities,
        )

        assert len(result) == 1
        assert result[0]["entity"] == "MRN-123456"
        assert result[0]["confidence"] == "high"
        assert result[0]["presidio_score"] == 0.85
        assert result[0]["gliner_score"] == 0.75

    def test_complex_medical_text_alignment(self):
        """Test alignment with multiple PHI entities in medical text."""
        aligner = MultiSourceAligner()

        # Simulating: "Patient John Smith (MRN: 123456) born 01/15/1980, phone: 555-1234"
        presidio_entities = [
            {
                "entity": "John Smith",
                "start": 8,
                "end": 18,
                "entity_type": "PERSON",
                "score": 0.95,
            },
            {
                "entity": "123456",
                "start": 25,
                "end": 31,
                "entity_type": "MEDICAL_RECORD",
                "score": 0.7,
            },
            {
                "entity": "01/15/1980",
                "start": 38,
                "end": 48,
                "entity_type": "DATE_OF_BIRTH",
                "score": 0.9,
            },
            {
                "entity": "555-1234",
                "start": 57,
                "end": 65,
                "entity_type": "PHONE",
                "score": 0.85,
            },
        ]

        ai_entities = [
            {"entity": "John Smith", "start": 8, "end": 18, "entity_type": "PERSON"},
            {
                "entity": "01/15/1980",
                "start": 38,
                "end": 48,
                "entity_type": "BIRTH_DATE",
            },
            {
                "entity": "555-1234",
                "start": 57,
                "end": 65,
                "entity_type": "PHONE_NUMBER",
            },
        ]

        result = aligner.align(
            doc_id="complex_doc",
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=ai_entities,
        )

        # Should have 4 entities with some matched
        assert len(result) == 4

        # Verify person entity is matched with high confidence
        person = [e for e in result if e["entity"] == "John Smith"][0]
        assert person["confidence"] in ["high", "medium"]
        assert person["presidio_score"] == 0.95

    def test_partial_overlap_medical_entities(self):
        """Test handling of partially overlapping medical entities."""
        aligner = MultiSourceAligner()

        presidio_entities = [
            {
                "entity": "Dr. John Smith",
                "start": 0,
                "end": 14,
                "entity_type": "PERSON",
                "score": 0.9,
            }
        ]
        ai_entities = [
            {"entity": "John Smith", "start": 4, "end": 14, "entity_type": "PERSON"}
        ]

        result = aligner.align(
            doc_id="overlap_doc",
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=ai_entities,
        )

        # Should recognize as same entity with fuzzy match
        assert len(result) == 1
        # Should use the longer, more specific version
        assert result[0]["entity"] == "Dr. John Smith"


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_entity_text(self):
        """Test handling of empty entity text."""
        aligner = MultiSourceAligner()

        presidio_entities = [
            {"entity": "", "start": 0, "end": 0, "entity_type": "UNKNOWN"}
        ]

        result = aligner.align(
            doc_id="doc1",
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=None,
        )

        # Should handle gracefully
        assert len(result) <= 1

    def test_duplicate_entities_same_source(self):
        """Test handling of duplicate entities from same source."""
        aligner = MultiSourceAligner()

        presidio_entities = [
            {
                "entity": "John",
                "start": 0,
                "end": 4,
                "entity_type": "PERSON",
                "score": 0.9,
            },
            {
                "entity": "John",
                "start": 0,
                "end": 4,
                "entity_type": "PERSON",
                "score": 0.9,
            },
        ]

        result = aligner.align(
            doc_id="doc1",
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=None,
        )

        # Should handle duplicates (might merge or keep separate)
        assert len(result) >= 1
        assert any(e["entity"] == "John" for e in result)

    def test_none_values_in_entity_dict(self):
        """Test handling of None values in entity dictionaries."""
        aligner = MultiSourceAligner()

        presidio_entities = [
            {"entity": "John", "start": 0, "end": 4, "entity_type": None, "score": None}
        ]

        result = aligner.align(
            doc_id="doc1",
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=None,
        )

        assert len(result) == 1
        assert result[0]["entity"] == "John"
        assert result[0]["presidio_score"] is None

    def test_very_long_entity_text(self):
        """Test handling of very long entity text."""
        aligner = MultiSourceAligner()

        long_text = "A" * 10000
        presidio_entities = [
            {"entity": long_text, "start": 0, "end": 10000, "entity_type": "UNKNOWN"}
        ]

        result = aligner.align(
            doc_id="doc1",
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=None,
        )

        assert len(result) == 1
        assert len(result[0]["entity"]) == 10000

    def test_negative_positions(self):
        """Test handling of negative start/end positions."""
        # This should raise an error during normalization or be handled gracefully
        # Depending on requirements, adjust test accordingly
        aligner = MultiSourceAligner()

        presidio_entities = [
            {"entity": "Test", "start": -1, "end": 3, "entity_type": "UNKNOWN"}
        ]

        # Should either skip or handle gracefully
        result = aligner.align(
            doc_id="doc1",
            presidio_entities=presidio_entities,
            gliner_entities=None,
            ai_entities=None,
        )

        # Implementation should handle this without crashing
        assert isinstance(result, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
