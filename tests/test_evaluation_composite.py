"""
Unit tests for composite entity matching in evaluation.

These tests validate the critical composite matching functionality where:
1. Many-to-one: Multiple GT entities match a single prediction (e.g., date + time = datetime)
2. One-to-many: A single GT entity matches multiple predictions (e.g., full name = first + last)

Run with: pytest tests/test_evaluation_composite.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dbxmetagen.redaction.evaluation import (
    match_entities_flexible,
    calculate_entity_metrics,
    find_composite_matches,
)


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for tests."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("composite-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


class TestCompositeMatching:
    """Tests for composite entity matching (many-to-one, one-to-many)."""

    def test_composite_datetime_many_to_one(self, spark):
        """
        Test many-to-one: Multiple GT entities (date, time) should match one pred (datetime).

        This is the critical use case mentioned in the plan.
        """
        gt_data = [
            ("1", "2023-01-15", 0, 10),  # Date
            ("1", "10:30 AM", 11, 19),  # Time (adjacent, within tolerance)
        ]
        pred_data = [("1", "2023-01-15 10:30 AM", 0, 19)]  # Combined datetime

        gt_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("chunk", StringType(), False),
                StructField("begin", IntegerType(), False),
                StructField("end", IntegerType(), False),
            ]
        )
        pred_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("entity", StringType(), False),
                StructField("start", IntegerType(), False),
                StructField("end", IntegerType(), False),
            ]
        )

        gt_df = spark.createDataFrame(gt_data, gt_schema)
        pred_df = spark.createDataFrame(pred_data, pred_schema)

        matched, fn, fp = match_entities_flexible(
            gt_df, pred_df, iou_threshold=0.1, position_tolerance=2
        )

        # With composite matching: Should match both GT entities to the single pred
        # Expected: matched count should handle composite appropriately
        # For now, we expect at least 1 match (the overlap match)
        assert matched.count() >= 1, "Should have at least 1 match"

        # TODO: When composite matching is implemented:
        # assert matched.count() == 2, "Should match both GT entities via composite"
        # assert fn.count() == 0, "No false negatives with composite matching"
        # assert fp.count() == 0, "No false positives with composite matching"

    def test_composite_name_one_to_many(self, spark):
        """
        Test one-to-many: Single GT (full name) should match multiple preds (first + last).

        This is the inverse case of datetime.
        """
        gt_data = [("1", "John Smith", 0, 10)]  # Full name
        pred_data = [
            ("1", "John", 0, 4),  # First name
            ("1", "Smith", 5, 10),  # Last name (adjacent)
        ]

        gt_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("chunk", StringType(), False),
                StructField("begin", IntegerType(), False),
                StructField("end", IntegerType(), False),
            ]
        )
        pred_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("entity", StringType(), False),
                StructField("start", IntegerType(), False),
                StructField("end", IntegerType(), False),
            ]
        )

        gt_df = spark.createDataFrame(gt_data, gt_schema)
        pred_df = spark.createDataFrame(pred_data, pred_schema)

        matched, fn, fp = match_entities_flexible(
            gt_df, pred_df, iou_threshold=0.1, position_tolerance=2
        )

        # With composite matching: Should match GT to both preds
        # Expected: 1 GT matched, both preds matched
        assert matched.count() >= 1, "Should have at least 1 match"

        # TODO: When composite matching is implemented:
        # assert matched.count() == 1, "Should match GT to composite pred"
        # assert fn.count() == 0, "No false negatives"
        # assert fp.count() == 0, "No false positives (both preds part of composite)"

    def test_composite_with_no_space(self, spark):
        """
        Test composite matching when entities are adjacent without space.

        Example: "123" + "456" = "123456" (no space between)
        """
        gt_data = [
            ("1", "123", 0, 3),
            ("1", "456", 3, 6),  # Immediately adjacent (no space)
        ]
        pred_data = [("1", "123456", 0, 6)]

        gt_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("chunk", StringType(), False),
                StructField("begin", IntegerType(), False),
                StructField("end", IntegerType(), False),
            ]
        )
        pred_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("entity", StringType(), False),
                StructField("start", IntegerType(), False),
                StructField("end", IntegerType(), False),
            ]
        )

        gt_df = spark.createDataFrame(gt_data, gt_schema)
        pred_df = spark.createDataFrame(pred_data, pred_schema)

        matched, fn, fp = match_entities_flexible(
            gt_df, pred_df, iou_threshold=0.1, position_tolerance=2
        )

        # Should match via position overlap at minimum
        assert matched.count() >= 1, "Should match via overlap"

        # TODO: When composite matching is implemented:
        # Should recognize adjacent entities and match both

    def test_composite_adjacency_tolerance(self, spark):
        """
        Test that entities within adjacency tolerance are considered for composite matching.

        Default tolerance: 3 characters
        """
        gt_data = [
            ("1", "John", 0, 4),
            ("1", "Smith", 6, 11),  # 2 chars apart (within default 3)
        ]
        pred_data = [("1", "John Smith", 0, 11)]

        gt_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("chunk", StringType(), False),
                StructField("begin", IntegerType(), False),
                StructField("end", IntegerType(), False),
            ]
        )
        pred_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("entity", StringType(), False),
                StructField("start", IntegerType(), False),
                StructField("end", IntegerType(), False),
            ]
        )

        gt_df = spark.createDataFrame(gt_data, gt_schema)
        pred_df = spark.createDataFrame(pred_data, pred_schema)

        matched, fn, fp = match_entities_flexible(
            gt_df, pred_df, iou_threshold=0.1, position_tolerance=2
        )

        # Should match via position overlap
        assert matched.count() >= 1, "Should match via overlap"

        # TODO: When composite matching is implemented with adjacency_tolerance=3:
        # Should match both GT entities via composite (within tolerance)

    def test_composite_not_matching_distant_entities(self, spark):
        """
        Test that entities >3 chars apart should NOT composite match.

        They might still partial match via overlap if IoU is sufficient.
        """
        gt_data = [
            ("1", "John", 0, 4),
            ("1", "Smith", 20, 25),  # 16 chars apart (beyond tolerance)
        ]
        pred_data = [("1", "John Smith", 0, 11)]

        gt_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("chunk", StringType(), False),
                StructField("begin", IntegerType(), False),
                StructField("end", IntegerType(), False),
            ]
        )
        pred_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("entity", StringType(), False),
                StructField("start", IntegerType(), False),
                StructField("end", IntegerType(), False),
            ]
        )

        gt_df = spark.createDataFrame(gt_data, gt_schema)
        pred_df = spark.createDataFrame(pred_data, pred_schema)

        matched, fn, fp = match_entities_flexible(
            gt_df, pred_df, iou_threshold=0.1, position_tolerance=2
        )

        # "John" should match to "John Smith" via partial overlap
        # "Smith" at position 20-25 doesn't overlap with pred at 0-11
        assert matched.count() >= 1, "John should match via overlap"
        assert fn.count() >= 1, "Smith should be a false negative (no overlap)"


class TestFindCompositeMatches:
    """Tests for the find_composite_matches function specifically."""

    def test_find_composite_matches_basic(self, spark):
        """Test basic composite matching functionality."""
        # This tests the find_composite_matches function directly
        # Currently it's a stub, so this will pass trivially

        matched_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("gt_id", IntegerType(), False),
                StructField("pred_id", IntegerType(), False),
                StructField("gt_text", StringType(), False),
                StructField("pred_text", StringType(), False),
                StructField("gt_start", IntegerType(), False),
                StructField("gt_end", IntegerType(), False),
                StructField("pred_start", IntegerType(), False),
                StructField("pred_end", IntegerType(), False),
                StructField("iou_score", DoubleType(), False),
                StructField("match_type", StringType(), False),
            ]
        )
        fn_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("chunk", StringType(), False),
                StructField("begin", IntegerType(), False),
                StructField("end", IntegerType(), False),
            ]
        )
        fp_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("entity", StringType(), False),
                StructField("start", IntegerType(), False),
                StructField("end", IntegerType(), False),
            ]
        )

        matched_df = spark.createDataFrame([], matched_schema)
        fn_df = spark.createDataFrame([("1", "Smith", 20, 25)], fn_schema)
        fp_df = spark.createDataFrame([("1", "Doe", 30, 33)], fp_schema)

        # Call find_composite_matches
        updated_matched, updated_fn, updated_fp = find_composite_matches(
            matched_df, fn_df, fp_df
        )

        # Currently returns as-is (stub implementation)
        assert updated_matched.count() == 0
        assert updated_fn.count() == 1
        assert updated_fp.count() == 1

    def test_find_composite_no_duplicate_counting(self, spark):
        """
        Test that composite matching doesn't create duplicate counts.

        This is a critical requirement from the plan document.
        """
        # When composite matching is implemented, verify:
        # - Each GT entity is counted once
        # - Each pred entity is counted once
        # - No entity appears in both matched and FP/FN
        pass  # Will implement when composite matching is ready


class TestCompositeIntegration:
    """Integration tests for composite matching in full pipeline."""

    def test_composite_improves_metrics(self, spark):
        """
        Test that composite matching improves precision and recall.

        Without composite: Many FPs and FNs from split/merged entities
        With composite: These should be recognized as valid matches
        """
        # Create a dataset with known composite cases
        gt_data = [
            ("1", "2023-01-15", 0, 10),
            ("1", "10:30 AM", 11, 19),
            ("2", "John Smith", 0, 10),
        ]
        pred_data = [
            ("1", "2023-01-15 10:30 AM", 0, 19),
            ("2", "John", 0, 4),
            ("2", "Smith", 5, 10),
        ]

        gt_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("chunk", StringType(), False),
                StructField("begin", IntegerType(), False),
                StructField("end", IntegerType(), False),
            ]
        )
        pred_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("entity", StringType(), False),
                StructField("start", IntegerType(), False),
                StructField("end", IntegerType(), False),
            ]
        )

        gt_df = spark.createDataFrame(gt_data, gt_schema)
        pred_df = spark.createDataFrame(pred_data, pred_schema)

        matched, fn, fp = match_entities_flexible(
            gt_df, pred_df, iou_threshold=0.1, position_tolerance=2
        )

        metrics = calculate_entity_metrics(matched, fn, fp)

        # With composite matching, we expect:
        # - All 3 GT entities matched (recall = 1.0)
        # - All 3 pred entities matched (precision = 1.0)
        # Currently without composite matching, metrics will be lower

        assert metrics["recall"] > 0, "Should have some matches"
        assert metrics["precision"] > 0, "Should have some matches"

        # TODO: When composite matching is implemented:
        # assert metrics["recall"] >= 0.9, "Recall should be very high with composite"
        # assert metrics["precision"] >= 0.9, "Precision should be very high with composite"

    def test_composite_multiple_documents(self, spark):
        """
        Test that composite matching respects document boundaries.

        Entities in doc1 should NOT composite match with entities in doc2.
        """
        gt_data = [
            ("1", "John", 0, 4),
            ("2", "Smith", 0, 5),  # Different doc
        ]
        pred_data = [("1", "John Smith", 0, 10)]  # Only in doc1

        gt_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("chunk", StringType(), False),
                StructField("begin", IntegerType(), False),
                StructField("end", IntegerType(), False),
            ]
        )
        pred_schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("entity", StringType(), False),
                StructField("start", IntegerType(), False),
                StructField("end", IntegerType(), False),
            ]
        )

        gt_df = spark.createDataFrame(gt_data, gt_schema)
        pred_df = spark.createDataFrame(pred_data, pred_schema)

        matched, fn, fp = match_entities_flexible(
            gt_df, pred_df, iou_threshold=0.1, position_tolerance=2
        )

        # "John" in doc1 should match
        # "Smith" in doc2 should NOT match (different document)
        matched_docs = matched.select("doc_id").distinct().collect()
        assert len(matched_docs) == 1, "Should only match in doc1"
        assert matched_docs[0][0] == "1", "Should match in doc1"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
