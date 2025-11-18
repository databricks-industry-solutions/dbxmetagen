"""
Unit tests for evaluation functions.

Run with: pytest tests/test_evaluation.py -v
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
    normalize_whitespace,
    calculate_iou,
    match_entities_flexible,
    calculate_entity_metrics,
)


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for tests."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("evaluation-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


class TestNormalizeWhitespace:
    """Tests for whitespace normalization."""

    def test_basic_normalization(self):
        """Test basic whitespace normalization."""
        assert normalize_whitespace("John  Smith") == "John Smith"
        assert normalize_whitespace("  John Smith  ") == "John Smith"
        assert normalize_whitespace("John\nSmith") == "John Smith"
        assert normalize_whitespace("John\t\tSmith") == "John Smith"

    def test_multiple_spaces(self):
        """Test multiple consecutive spaces."""
        assert normalize_whitespace("John     Smith") == "John Smith"

    def test_mixed_whitespace(self):
        """Test mixed whitespace characters."""
        assert normalize_whitespace("John \n\t Smith") == "John Smith"

    def test_empty_string(self):
        """Test empty string."""
        assert normalize_whitespace("") == ""
        assert normalize_whitespace("   ") == ""

    def test_none(self):
        """Test None input."""
        assert normalize_whitespace(None) == ""


class TestCalculateIoU:
    """Tests for IoU calculation."""

    def test_perfect_overlap(self):
        """Test identical spans."""
        assert calculate_iou(0, 10, 0, 10) == 1.0
        assert calculate_iou(5, 15, 5, 15) == 1.0

    def test_no_overlap(self):
        """Test non-overlapping spans."""
        assert calculate_iou(0, 10, 10, 20) == 0.0
        assert calculate_iou(0, 10, 11, 20) == 0.0

    def test_partial_overlap(self):
        """Test partially overlapping spans."""
        # Overlap: 5 chars, Union: 15 chars, IoU = 5/15 = 0.333...
        iou = calculate_iou(0, 10, 5, 15)
        assert abs(iou - 0.333) < 0.01

    def test_fifty_percent_overlap(self):
        """Test 50% overlap."""
        # Span1: 0-10 (10 chars), Span2: 5-15 (10 chars)
        # Overlap: 5-10 (5 chars), Union: 0-15 (15 chars)
        # IoU = 5/15 = 0.333
        iou = calculate_iou(0, 10, 5, 15)
        assert abs(iou - 0.333) < 0.01

    def test_one_contains_other(self):
        """Test when one span contains another."""
        # Span1: 0-20, Span2: 5-15
        # Overlap: 10 chars, Union: 20 chars, IoU = 0.5
        iou = calculate_iou(0, 20, 5, 15)
        assert iou == 0.5

    def test_off_by_one(self):
        """Test the critical 957770228 case."""
        # GT: (1, 10) = length 9, Pred: (1, 9) = length 8
        # Overlap: 1-9 = 8 chars, Union: 1-10 = 9 chars
        # IoU = 8/9 = 0.888...
        iou = calculate_iou(1, 10, 1, 9)
        assert abs(iou - 0.888) < 0.01


class TestMatchEntitiesFlexible:
    """Tests for flexible entity matching."""

    def test_exact_match_same_position(self, spark):
        """Test exact match with identical positions."""
        gt_data = [("1", "John Smith", 0, 11)]
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
            gt_df, pred_df, iou_threshold=0.5, position_tolerance=2
        )

        assert matched.count() == 1
        assert fn.count() == 0
        assert fp.count() == 0

        match_type = matched.select("match_type").collect()[0][0]
        assert match_type == "exact"

    def test_exact_match_whitespace_normalized(self, spark):
        """Test exact match with different whitespace."""
        gt_data = [("1", "John  Smith", 0, 11)]
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
            gt_df, pred_df, iou_threshold=0.5, position_tolerance=2
        )

        assert matched.count() == 1
        assert fn.count() == 0
        assert fp.count() == 0

        match_type = matched.select("match_type").collect()[0][0]
        assert match_type == "exact"

    def test_exact_match_position_tolerance(self, spark):
        """Test the critical 957770228 case - position off by 1."""
        gt_data = [("1", "957770228", 1, 10)]
        pred_data = [("1", "957770228", 1, 9)]

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
            gt_df, pred_df, iou_threshold=0.5, position_tolerance=2
        )

        # Should match exactly because text is identical and position diff <= 2
        assert matched.count() == 1, f"Expected 1 match, got {matched.count()}"
        assert fn.count() == 0, f"Expected 0 FN, got {fn.count()}"
        assert fp.count() == 0, f"Expected 0 FP, got {fp.count()}"

        match_type = matched.select("match_type").collect()[0][0]
        assert match_type == "exact", f"Expected 'exact' match, got '{match_type}'"

    def test_partial_match_low_iou(self, spark):
        """Test partial match with low IoU."""
        gt_data = [("1", "John Smith", 0, 10)]
        pred_data = [("1", "Smith", 5, 10)]

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

        # Should match partially (IoU = 5/10 = 0.5 > 0.1)
        assert matched.count() == 1
        assert fn.count() == 0
        assert fp.count() == 0

        match_type = matched.select("match_type").collect()[0][0]
        assert match_type == "partial"

    def test_no_match_different_text_no_overlap(self, spark):
        """Test no match when text differs and no position overlap."""
        gt_data = [("1", "John Smith", 0, 10)]
        pred_data = [("1", "Jane Doe", 20, 28)]

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

        assert matched.count() == 0
        assert fn.count() == 1
        assert fp.count() == 1

    def test_multiple_entities_one_to_one(self, spark):
        """Test multiple entities with one-to-one matching."""
        gt_data = [
            ("1", "John Smith", 0, 10),
            ("1", "Jane Doe", 20, 28),
        ]
        pred_data = [
            ("1", "John Smith", 0, 10),
            ("1", "Jane Doe", 20, 28),
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
            gt_df, pred_df, iou_threshold=0.5, position_tolerance=2
        )

        assert matched.count() == 2
        assert fn.count() == 0
        assert fp.count() == 0

    def test_greedy_matching_prefers_higher_iou(self, spark):
        """Test that greedy matching selects best match."""
        # GT has one entity, Pred has two overlapping entities
        # Should match to the one with higher IoU
        gt_data = [("1", "John Smith", 0, 10)]
        pred_data = [
            ("1", "John Smith", 0, 10),  # Perfect match (IoU = 1.0)
            ("1", "Smith", 5, 10),  # Partial match (IoU = 0.5)
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

        # Should match to the perfect match, leaving one FP
        assert matched.count() == 1
        assert fn.count() == 0
        assert fp.count() == 1

        # Check that it matched to the exact one
        matched_entity = matched.select("pred_text").collect()[0][0]
        assert matched_entity == "John Smith"


class TestCalculateEntityMetrics:
    """Tests for entity metrics calculation."""

    def test_perfect_metrics(self, spark):
        """Test metrics with all matches."""
        matched_data = [
            ("1", 1, 1, "John", "John", 0, 4, 0, 4, 1.0, "exact"),
            ("1", 2, 2, "Smith", "Smith", 5, 10, 5, 10, 1.0, "exact"),
        ]
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

        matched_df = spark.createDataFrame(matched_data, matched_schema)
        fn_df = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("doc_id", StringType(), False),
                    StructField("chunk", StringType(), False),
                    StructField("begin", IntegerType(), False),
                    StructField("end", IntegerType(), False),
                ]
            ),
        )
        fp_df = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("doc_id", StringType(), False),
                    StructField("entity", StringType(), False),
                    StructField("start", IntegerType(), False),
                    StructField("end", IntegerType(), False),
                ]
            ),
        )

        metrics = calculate_entity_metrics(matched_df, fn_df, fp_df)

        assert metrics["tp"] == 2
        assert metrics["fp"] == 0
        assert metrics["fn"] == 0
        assert metrics["precision"] == 1.0
        assert metrics["recall"] == 1.0
        assert metrics["f1"] == 1.0
        assert metrics["exact_matches"] == 2
        assert metrics["partial_matches"] == 0

    def test_mixed_metrics(self, spark):
        """Test metrics with mixed results."""
        matched_data = [("1", 1, 1, "John", "John", 0, 4, 0, 4, 1.0, "exact")]
        fn_data = [("1", "Smith", 5, 10)]
        fp_data = [("1", "Doe", 15, 18)]

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

        matched_df = spark.createDataFrame(matched_data, matched_schema)
        fn_df = spark.createDataFrame(
            fn_data,
            StructType(
                [
                    StructField("doc_id", StringType(), False),
                    StructField("chunk", StringType(), False),
                    StructField("begin", IntegerType(), False),
                    StructField("end", IntegerType(), False),
                ]
            ),
        )
        fp_df = spark.createDataFrame(
            fp_data,
            StructType(
                [
                    StructField("doc_id", StringType(), False),
                    StructField("entity", StringType(), False),
                    StructField("start", IntegerType(), False),
                    StructField("end", IntegerType(), False),
                ]
            ),
        )

        metrics = calculate_entity_metrics(matched_df, fn_df, fp_df)

        assert metrics["tp"] == 1
        assert metrics["fp"] == 1
        assert metrics["fn"] == 1
        assert metrics["precision"] == 0.5  # 1/(1+1)
        assert metrics["recall"] == 0.5  # 1/(1+1)
        assert abs(metrics["f1"] - 0.5) < 0.01


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
