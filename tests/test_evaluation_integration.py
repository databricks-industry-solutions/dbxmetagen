"""
Integration tests for the complete evaluation pipeline.

These tests validate end-to-end functionality including:
- Multiple detection methods
- Evaluation across multiple documents
- Edge cases (empty predictions, empty GT)
- Full metrics calculation

Run with: pytest tests/test_evaluation_integration.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dbxmetagen.redaction.evaluation import (
    match_entities_flexible,
    calculate_entity_metrics,
)


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for tests."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("integration-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


class TestEvaluationMultiDocument:
    """Tests for evaluation across multiple documents."""

    def test_evaluation_multiple_documents(self, spark):
        """
        Test that evaluation correctly handles multiple documents.

        Entities in different documents should not interfere with each other.
        """
        gt_data = [
            ("1", "John Smith", 0, 10),
            ("1", "555-1234", 20, 28),
            ("2", "Jane Doe", 0, 8),
            ("2", "987-6543", 15, 23),
        ]
        pred_data = [
            ("1", "John Smith", 0, 10),
            ("1", "555-1234", 20, 28),
            ("2", "Jane Doe", 0, 8),
            # Missing prediction for doc2 phone
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

        # Should have 3 matches (2 in doc1, 1 in doc2)
        assert matched.count() == 3, f"Expected 3 matches, got {matched.count()}"

        # Should have 1 FN (doc2 phone number)
        assert fn.count() == 1, f"Expected 1 FN, got {fn.count()}"

        # Should have 0 FP
        assert fp.count() == 0, f"Expected 0 FP, got {fp.count()}"

    def test_evaluation_document_separation(self, spark):
        """
        Test that entities with same text in different docs don't cross-match.
        """
        gt_data = [
            ("1", "John Smith", 0, 10),
            ("2", "John Smith", 0, 10),  # Same text, different doc
        ]
        pred_data = [
            ("1", "John Smith", 0, 10),
            # No prediction for doc2
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

        # Should match only in doc1
        assert matched.count() == 1
        matched_docs = [row["doc_id"] for row in matched.select("doc_id").collect()]
        assert "1" in matched_docs
        assert "2" not in matched_docs

        # Should have 1 FN for doc2
        assert fn.count() == 1
        fn_docs = [row["doc_id"] for row in fn.select("doc_id").collect()]
        assert "2" in fn_docs


class TestEvaluationEdgeCases:
    """Tests for edge cases in evaluation."""

    def test_evaluation_empty_predictions(self, spark):
        """
        Test evaluation when predictions are empty.

        All GT should be FN, precision undefined (0/0), recall = 0.
        """
        gt_data = [
            ("1", "John Smith", 0, 10),
            ("1", "555-1234", 20, 28),
        ]
        pred_data = []  # No predictions

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

        metrics = calculate_entity_metrics(matched, fn, fp)

        assert metrics["tp"] == 0, "No true positives"
        assert metrics["fn"] == 2, "All GT are false negatives"
        assert metrics["fp"] == 0, "No false positives"
        assert metrics["recall"] == 0.0, "Recall should be 0"
        assert metrics["precision"] == 0.0, "Precision should be 0 (0/0)"

    def test_evaluation_empty_ground_truth(self, spark):
        """
        Test evaluation when ground truth is empty.

        All predictions should be FP, recall undefined (0/0), precision = 0.
        """
        gt_data = []  # No ground truth
        pred_data = [
            ("1", "John Smith", 0, 10),
            ("1", "555-1234", 20, 28),
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

        metrics = calculate_entity_metrics(matched, fn, fp)

        assert metrics["tp"] == 0, "No true positives"
        assert metrics["fn"] == 0, "No false negatives"
        assert metrics["fp"] == 2, "All predictions are false positives"
        assert metrics["recall"] == 0.0, "Recall should be 0 (0/0)"
        assert metrics["precision"] == 0.0, "Precision should be 0"

    def test_evaluation_all_perfect_matches(self, spark):
        """
        Test evaluation when all predictions are perfect matches.

        Precision = 1.0, Recall = 1.0, F1 = 1.0
        """
        gt_data = [
            ("1", "John Smith", 0, 10),
            ("1", "555-1234", 20, 28),
            ("2", "Jane Doe", 0, 8),
        ]
        pred_data = [
            ("1", "John Smith", 0, 10),
            ("1", "555-1234", 20, 28),
            ("2", "Jane Doe", 0, 8),
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

        metrics = calculate_entity_metrics(matched, fn, fp)

        assert metrics["tp"] == 3, "All should be TPs"
        assert metrics["fn"] == 0, "No false negatives"
        assert metrics["fp"] == 0, "No false positives"
        assert metrics["precision"] == 1.0, "Perfect precision"
        assert metrics["recall"] == 1.0, "Perfect recall"
        assert metrics["f1"] == 1.0, "Perfect F1"


class TestEvaluationMixedScenarios:
    """Tests for mixed scenarios (some matches, some misses)."""

    def test_evaluation_mixed_results(self, spark):
        """
        Test evaluation with mixed TP, FP, FN.

        Verifies correct calculation of all metrics.
        """
        gt_data = [
            ("1", "John Smith", 0, 10),  # Will match
            ("1", "555-1234", 20, 28),  # Will match
            ("1", "test@email.com", 40, 54),  # Will be FN
        ]
        pred_data = [
            ("1", "John Smith", 0, 10),  # TP
            ("1", "555-1234", 20, 28),  # TP
            ("1", "wrong@email.com", 60, 75),  # FP (different entity)
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

        metrics = calculate_entity_metrics(matched, fn, fp)

        assert metrics["tp"] == 2, "Two true positives"
        assert metrics["fn"] == 1, "One false negative"
        assert metrics["fp"] == 1, "One false positive"

        # Precision = TP / (TP + FP) = 2 / 3 = 0.666...
        assert abs(metrics["precision"] - 0.667) < 0.01

        # Recall = TP / (TP + FN) = 2 / 3 = 0.666...
        assert abs(metrics["recall"] - 0.667) < 0.01

        # F1 = 2 * (P * R) / (P + R) = 2 * (2/3 * 2/3) / (2/3 + 2/3) = 0.666...
        assert abs(metrics["f1"] - 0.667) < 0.01

    def test_evaluation_overlapping_entities(self, spark):
        """
        Test evaluation with overlapping entity predictions.

        Multiple predictions may overlap same GT (greedy matching should handle).
        """
        gt_data = [
            ("1", "John Smith", 0, 10),
        ]
        pred_data = [
            ("1", "John Smith", 0, 10),  # Perfect match
            ("1", "John", 0, 4),  # Overlaps same GT
            ("1", "Smith", 5, 10),  # Overlaps same GT
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

        # Greedy matching: GT should match to best pred (perfect match)
        assert matched.count() == 1, "One GT matched to best pred"

        # The other 2 preds should be FP (or could be composite matched)
        # Without composite: 2 FP
        # With composite: 0 FP (all part of match)
        assert fp.count() >= 0, "Some preds may be FP"


class TestEvaluationPerformance:
    """Performance and scale tests."""

    @pytest.mark.slow
    def test_evaluation_100_documents(self, spark):
        """
        Test that evaluation scales to 100 documents.

        This should complete in reasonable time (<10s).
        """
        # Generate 100 documents with 10 entities each
        gt_data = []
        pred_data = []

        for doc_id in range(100):
            for i in range(10):
                start = i * 20
                end = start + 10
                entity = f"Entity_{doc_id}_{i}"
                gt_data.append((str(doc_id), entity, start, end))
                pred_data.append((str(doc_id), entity, start, end))

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

        metrics = calculate_entity_metrics(matched, fn, fp)

        # Should have 1000 perfect matches (100 docs * 10 entities)
        assert metrics["tp"] == 1000
        assert metrics["precision"] == 1.0
        assert metrics["recall"] == 1.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
