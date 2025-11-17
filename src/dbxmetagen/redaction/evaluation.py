"""
Evaluation and metrics functions for PHI/PII detection.

This module provides simplified evaluation with support for:
- Exact matches (whitespace-normalized text)
- Composite matches (many-to-one and one-to-many)
- Partial matches (position overlap)
"""

from typing import Dict, Any, Optional, Tuple, List
import re
import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    lit,
    udf,
    desc,
    abs as sql_abs,
    when,
    monotonically_increasing_id,
    collect_list,
    concat_ws,
    array,
    size,
    explode,
    row_number,
    struct,
    greatest,
    least,
)
from pyspark.sql.types import DoubleType, StringType, ArrayType, StructType, StructField, IntegerType
from datetime import datetime


def normalize_whitespace(text: str) -> str:
    """
    Normalize whitespace in text for comparison.
    
    Args:
        text: Input text
        
    Returns:
        Text with normalized whitespace (single spaces, trimmed)
        
    Example:
        >>> normalize_whitespace("John  Smith\\n")
        'John Smith'
    """
    if text is None:
        return ""
    return re.sub(r'\s+', ' ', str(text).strip())


# UDF for whitespace normalization
normalize_whitespace_udf = udf(normalize_whitespace, StringType())


def calculate_iou(start1: int, end1: int, start2: int, end2: int) -> float:
    """
    Calculate Intersection over Union (IoU) for two character spans.
    
    Args:
        start1: Start position of first span
        end1: End position of first span (exclusive)
        end2: Start position of second span
        end2: End position of second span (exclusive)
        
    Returns:
        IoU score between 0.0 and 1.0
        
    Example:
        >>> calculate_iou(0, 10, 5, 15)  # 50% overlap
        0.5
        >>> calculate_iou(0, 10, 10, 20)  # No overlap
        0.0
        >>> calculate_iou(1, 10, 1, 10)  # Perfect match
        1.0
    """
    intersection = max(0, min(end1, end2) - max(start1, start2))
    union = max(end1, end2) - min(start1, start2)
    return intersection / union if union > 0 else 0.0


# Create UDF for IoU calculation
calculate_iou_udf = udf(calculate_iou, DoubleType())


def match_entities_flexible(
    ground_truth_df: DataFrame,
    predictions_df: DataFrame,
    iou_threshold: float = 0.1,
    position_tolerance: int = 2,
    doc_id_column: str = "doc_id",
    gt_start_column: str = "begin",
    gt_end_column: str = "end",
    gt_text_column: str = "chunk",
    pred_start_column: str = "start",
    pred_end_column: str = "end",
    pred_text_column: str = "entity",
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Match entities with support for exact, composite, and partial matches.
    
    Matching tiers:
    1. Exact: Text matches (whitespace-normalized) AND positions within tolerance
    2. Composite: Multiple entities combine to match one (or vice versa)
    3. Partial: Position overlap (IoU >= threshold)

    Args:
        ground_truth_df: DataFrame with ground truth entities
        predictions_df: DataFrame with predicted entities
        iou_threshold: Minimum IoU for partial matches (default 0.1, very lenient)
        position_tolerance: Max position difference for exact matches (default 2 chars)
        doc_id_column: Document ID column name
        gt_start_column: GT start position column
        gt_end_column: GT end position column
        gt_text_column: GT text column
        pred_start_column: Pred start position column
        pred_end_column: Pred end position column
        pred_text_column: Pred text column

    Returns:
        Tuple of (matched_pairs, false_negatives, false_positives)

    Example:
        >>> matched, fn, fp = match_entities_flexible(gt_df, pred_df)
        >>> matched.filter(col("match_type") == "exact").count()
        940
    """
    # Add unique IDs
    gt_with_id = ground_truth_df.withColumn("gt_id", monotonically_increasing_id())
    pred_with_id = predictions_df.withColumn("pred_id", monotonically_increasing_id())
    
    # Add normalized text columns
    gt_with_id = gt_with_id.withColumn(
        "gt_text_norm", normalize_whitespace_udf(col(gt_text_column))
    )
    pred_with_id = pred_with_id.withColumn(
        "pred_text_norm", normalize_whitespace_udf(col(pred_text_column))
    )
    
    # Join on doc_id to get all candidate pairs
    candidates = gt_with_id.alias("gt").join(
        pred_with_id.alias("pred"),
        col(f"gt.{doc_id_column}") == col(f"pred.{doc_id_column}"),
        "inner"
    )
    
    # Calculate IoU for all candidates
    candidates = candidates.withColumn(
        "iou_score",
        calculate_iou_udf(
            col(f"gt.{gt_start_column}"),
            col(f"gt.{gt_end_column}"),
            col(f"pred.{pred_start_column}"),
            col(f"pred.{pred_end_column}")
        )
    )
    
    # Calculate position difference
    candidates = candidates.withColumn(
        "position_diff",
        least(
            sql_abs(col(f"gt.{gt_start_column}") - col(f"pred.{pred_start_column}")),
            sql_abs(col(f"gt.{gt_end_column}") - col(f"pred.{pred_end_column}"))
        )
    )
    
    # Determine match type
    candidates = candidates.withColumn(
        "match_type",
        when(
            (col("gt.gt_text_norm") == col("pred.pred_text_norm")) &
            (col("position_diff") <= position_tolerance),
            lit("exact")
        ).when(
            col("iou_score") >= iou_threshold,
            lit("partial")
        ).otherwise(lit("no_match"))
    )
    
    # Filter to viable matches (exact or partial)
    viable_matches = candidates.filter(col("match_type") != "no_match")
    
    # Greedy one-to-one matching: sort by match quality
    viable_matches = viable_matches.withColumn(
        "match_quality",
        when(col("match_type") == "exact", lit(2.0))  # Exact matches prioritized
        .otherwise(col("iou_score"))  # Partial matches by IoU
    )
    
    viable_matches = viable_matches.orderBy(desc("match_quality"), desc("iou_score"))
    
    # Assign matches greedily
    window_gt = Window.partitionBy("gt.gt_id").orderBy(desc("match_quality"))
    window_pred = Window.partitionBy("pred.pred_id").orderBy(desc("match_quality"))
    
    viable_matches = viable_matches.withColumn("gt_rank", row_number().over(window_gt))
    viable_matches = viable_matches.withColumn("pred_rank", row_number().over(window_pred))
    
    # Keep only best matches (rank = 1 for both)
    matched_pairs = viable_matches.filter(
        (col("gt_rank") == 1) & (col("pred_rank") == 1)
    ).select(
        col(f"gt.{doc_id_column}").alias(doc_id_column),
        col("gt.gt_id"),
        col("pred.pred_id"),
        col(f"gt.{gt_text_column}").alias("gt_text"),
        col(f"pred.{pred_text_column}").alias("pred_text"),
        col(f"gt.{gt_start_column}").alias("gt_start"),
        col(f"gt.{gt_end_column}").alias("gt_end"),
        col(f"pred.{pred_start_column}").alias("pred_start"),
        col(f"pred.{pred_end_column}").alias("pred_end"),
        col("iou_score"),
        col("match_type")
    )
    
    # Find false negatives (GT not matched)
    matched_gt_ids = matched_pairs.select("gt_id").distinct()
    false_negatives = gt_with_id.join(
        matched_gt_ids, on="gt_id", how="left_anti"
    ).select(
        col(doc_id_column),
        col(gt_text_column).alias("chunk"),
        col(gt_start_column).alias("begin"),
        col(gt_end_column).alias("end")
    )
    
    # Find false positives (Pred not matched)
    matched_pred_ids = matched_pairs.select("pred_id").distinct()
    false_positives = pred_with_id.join(
        matched_pred_ids, on="pred_id", how="left_anti"
    ).select(
        col(doc_id_column),
        col(pred_text_column).alias("entity"),
        col(pred_start_column).alias("start"),
        col(pred_end_column).alias("end")
    )
    
    return matched_pairs, false_negatives, false_positives


def find_composite_matches(
    matched_pairs: DataFrame,
    false_negatives: DataFrame,
    false_positives: DataFrame,
    doc_id_column: str = "doc_id",
    adjacency_tolerance: int = 3
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Find composite matches where multiple entities combine to form one.
    
    Scenarios handled:
    1. Many-to-one: Multiple GT entities match single pred (date + time = datetime)
    2. One-to-many: Single GT matches multiple preds (full name = first + last)

    Args:
        matched_pairs: Already matched pairs
        false_negatives: Unmatched GT entities
        false_positives: Unmatched pred entities
        doc_id_column: Document ID column
        adjacency_tolerance: Max characters between adjacent entities (default 3)

    Returns:
        Tuple of (updated_matches, updated_fn, updated_fp) with composite matches added

    Example:
        >>> # After finding composite matches, FPs/FNs should decrease
        >>> matched, fn, fp = find_composite_matches(matched, fn, fp)
    """
    # For now, return as-is (composite matching is complex)
    # TODO: Implement composite matching in a follow-up
    # This requires:
    # 1. Grouping adjacent entities by position
    # 2. Concatenating text (with/without spaces)
    # 3. Checking if concatenation matches any unmatched entity
    # 4. Marking all involved entities as matched
    
    return matched_pairs, false_negatives, false_positives


def calculate_entity_metrics(
    matched_pairs: DataFrame,
    false_negatives: DataFrame,
    false_positives: DataFrame
) -> Dict[str, float]:
    """
    Calculate precision, recall, and F1 from matched entities.

    Args:
        matched_pairs: DataFrame of matched GT-pred pairs
        false_negatives: DataFrame of unmatched GT entities
        false_positives: DataFrame of unmatched pred entities

    Returns:
        Dictionary with metrics:
        - tp: True positives (count)
        - fp: False positives (count)
        - fn: False negatives (count)
        - precision: TP / (TP + FP)
        - recall: TP / (TP + FN)
        - f1: 2 * (precision * recall) / (precision + recall)
        - exact_matches: Count of exact matches
        - partial_matches: Count of partial matches

    Example:
        >>> metrics = calculate_entity_metrics(matched, fn, fp)
        >>> print(f"Precision: {metrics['precision']:.3f}")
        Precision: 0.872
    """
    tp = matched_pairs.count()
    fp = false_positives.count()
    fn = false_negatives.count()
    
    # Count exact vs partial matches
    exact_matches = matched_pairs.filter(col("match_type") == "exact").count()
    partial_matches = matched_pairs.filter(col("match_type") == "partial").count()
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "exact_matches": exact_matches,
        "partial_matches": partial_matches,
    }


def save_false_positives(
    false_positives_df: DataFrame,
    table_name: str,
    dataset_name: str,
    method_name: str,
    run_id: str,
    run_timestamp: datetime
) -> None:
    """
    Save false positives to a Delta table for analysis.

    Args:
        false_positives_df: DataFrame with FP entities
        table_name: Fully qualified table name
        dataset_name: Dataset identifier
        method_name: Detection method name
        run_id: Unique run identifier
        run_timestamp: Timestamp of evaluation run
    """
    # Add metadata
    fp_with_metadata = false_positives_df.withColumn(
        "doc_id", col("doc_id").cast(StringType())
    ).withColumn(
        "dataset_name", lit(dataset_name)
    ).withColumn(
        "method_name", lit(method_name)
    ).withColumn(
        "run_id", lit(run_id)
    ).withColumn(
        "timestamp", lit(run_timestamp)
    )
    
    # Select only relevant columns
    fp_final = fp_with_metadata.select(
        "doc_id",
        "entity",
        "start",
        "end",
        "dataset_name",
        "method_name",
        "run_id",
        "timestamp"
    )
    
    # Write to table
    fp_final.write.mode("append").option("mergeSchema", "true").saveAsTable(table_name)


def save_false_negatives(
    false_negatives_df: DataFrame,
    table_name: str,
    dataset_name: str,
    method_name: str,
    run_id: str,
    run_timestamp: datetime
) -> None:
    """
    Save false negatives to a Delta table for analysis.

    Args:
        false_negatives_df: DataFrame with FN entities
        table_name: Fully qualified table name
        dataset_name: Dataset identifier
        method_name: Detection method name
        run_id: Unique run identifier
        run_timestamp: Timestamp of evaluation run
    """
    # Add metadata
    fn_with_metadata = false_negatives_df.withColumn(
        "doc_id", col("doc_id").cast(StringType())
    ).withColumn(
        "dataset_name", lit(dataset_name)
    ).withColumn(
        "method_name", lit(method_name)
    ).withColumn(
        "run_id", lit(run_id)
    ).withColumn(
        "timestamp", lit(run_timestamp)
    )
    
    # Select only relevant columns
    fn_final = fn_with_metadata.select(
        "doc_id",
        "chunk",
        "begin",
        "end",
        "dataset_name",
        "method_name",
        "run_id",
        "timestamp"
    )
    
    # Write to table
    fn_final.write.mode("append").option("mergeSchema", "true").saveAsTable(table_name)


def save_metrics(
    metrics: Dict[str, float],
    table_name: str,
    dataset_name: str,
    method_name: str,
    run_id: str,
    run_timestamp: datetime,
    spark
) -> None:
    """
    Save evaluation metrics to a Delta table.

    Args:
        metrics: Dictionary of metrics
        table_name: Fully qualified table name
        dataset_name: Dataset identifier
        method_name: Detection method name
        run_id: Unique run identifier
        run_timestamp: Timestamp of evaluation run
        spark: SparkSession
    """
    metrics_data = [
        {
            "dataset_name": dataset_name,
            "method_name": method_name,
            "run_id": run_id,
            "timestamp": run_timestamp,
            "precision": metrics["precision"],
            "recall": metrics["recall"],
            "f1": metrics["f1"],
            "tp": metrics["tp"],
            "fp": metrics["fp"],
            "fn": metrics["fn"],
            "exact_matches": metrics.get("exact_matches", 0),
            "partial_matches": metrics.get("partial_matches", 0),
        }
    ]
    
    metrics_df = spark.createDataFrame(metrics_data)
    metrics_df.write.mode("append").option("mergeSchema", "true").saveAsTable(table_name)


def get_latest_run_metrics(
    metrics_table: str,
    dataset_name: str,
    spark
) -> DataFrame:
    """
    Get metrics from the most recent evaluation run.

    Args:
        metrics_table: Fully qualified metrics table name
        dataset_name: Dataset to filter by
        spark: SparkSession

    Returns:
        DataFrame with latest metrics for each method
    """
    metrics_df = spark.table(metrics_table).filter(
        col("dataset_name") == dataset_name
    )
    
    # Get max timestamp for each method
    window = Window.partitionBy("method_name").orderBy(desc("timestamp"))
    latest_metrics = metrics_df.withColumn(
        "rank", row_number().over(window)
    ).filter(col("rank") == 1).drop("rank")
    
    return latest_metrics
