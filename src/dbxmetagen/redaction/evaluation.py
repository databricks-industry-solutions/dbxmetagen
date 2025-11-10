"""
Evaluation and metrics functions for PHI/PII detection.

This module provides functions to evaluate the performance of PHI detection methods
against ground truth data, calculating standard classification metrics.
"""

from typing import Dict, Any, Optional
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, contains, asc_nulls_last, length


def evaluate_detection(
    ground_truth_df: DataFrame,
    detection_df: DataFrame,
    text_column: str = "text",
    chunk_column: str = "chunk",
    entity_column: str = "entity",
    doc_id_column: str = "doc_id",
    begin_column: str = "begin",
    end_column: str = "end",
    start_column: str = "start",
) -> DataFrame:
    """
    Evaluate detected entities against ground truth.

    Performs an outer join between ground truth and detected entities,
    matching based on:
    - Document ID
    - Text containment (entity in chunk or vice versa)
    - Position overlap

    Args:
        ground_truth_df: DataFrame with ground truth entities
        detection_df: DataFrame with detected entities
        text_column: Name of text column (excluded from output)
        chunk_column: Name of ground truth chunk column
        entity_column: Name of detected entity column
        doc_id_column: Name of document ID column
        begin_column: Name of ground truth start position column
        end_column: Name of ground truth end position column
        start_column: Name of detection start position column

    Returns:
        DataFrame with matched and unmatched entities for evaluation

    Example:
        >>> ground_truth = spark.table("ground_truth")
        >>> detections = spark.table("presidio_results")
        >>> eval_df = evaluate_detection(ground_truth, detections)
        >>> # Count true positives, false positives, false negatives
    """
    # Ensure we have the required columns
    gt_cols = ground_truth_df.columns
    det_cols = detection_df.columns

    # Create aliases to avoid ambiguous column references
    gt = ground_truth_df.alias("gt")
    det = detection_df.alias("det")

    # Perform the evaluation join
    # Improved matching logic: requires position overlap AND text similarity
    eval_df = gt.join(
        det,
        # Matching conditions:
        # 1. Same document
        (col(f"det.{doc_id_column}") == col(f"gt.{doc_id_column}"))
        # 2. Position overlap (ANY overlap, not containment)
        # Check if ranges [det.start, det.end] and [gt.begin, gt.end] overlap
        & (col(f"det.{start_column}") < col(f"gt.{end_column}") + 1)
        & (col(f"det.{end_column}") >= col(f"gt.{begin_column}"))
        # 3. Text similarity check (entity should match chunk or have significant overlap)
        & (
            # Case 1: Exact match (best case)
            (col(f"det.{entity_column}") == col(f"gt.{chunk_column}"))
            # Case 2: Detection is substring of ground truth (e.g., "Smith" in "John Smith")
            | (contains(col(f"gt.{chunk_column}"), col(f"det.{entity_column}")))
            # Case 3: Ground truth is substring of detection (e.g., "John Smith" contains "John")
            # Only allow if detection is not too much longer (prevents "son" matching "Anderson")
            | (
                contains(col(f"det.{entity_column}"), col(f"gt.{chunk_column}"))
                & (
                    length(col(f"det.{entity_column}"))
                    <= length(col(f"gt.{chunk_column}")) * 2
                )
            )
        ),
        how="outer",
    )

    # Drop text column if present and sort results
    if text_column in eval_df.columns:
        eval_df = eval_df.drop(text_column)

    eval_df = eval_df.orderBy(
        asc_nulls_last(col(f"gt.{doc_id_column}")),
        asc_nulls_last(col(f"gt.{begin_column}")),
    )

    return eval_df


def calculate_metrics(
    eval_df: DataFrame,
    total_tokens: int,
    chunk_column: str = "chunk",
    entity_column: str = "entity",
) -> Dict[str, Any]:
    """
    Calculate classification metrics for PHI detection.

    Computes standard binary classification metrics including:
    - True Positives (TP): Correctly identified entities
    - False Positives (FP): Incorrectly identified entities
    - True Negatives (TN): Correctly identified non-entities
    - False Negatives (FN): Missed entities
    - Accuracy, Precision, Recall, Specificity, NPV

    Args:
        eval_df: Result from evaluate_detection()
        total_tokens: Total number of tokens/characters in corpus
        chunk_column: Name of ground truth chunk column
        entity_column: Name of detected entity column

    Returns:
        Dictionary with metrics and contingency table values

    Example:
        >>> metrics = calculate_metrics(eval_df, total_tokens=100000)
        >>> print(f"Precision: {metrics['precision']:.3f}")
        >>> print(f"Recall: {metrics['recall']:.3f}")
    """
    # Calculate counts from evaluation DataFrame
    pos_actual = eval_df.where(col(chunk_column).isNotNull()).count()
    pos_pred = eval_df.where(col(entity_column).isNotNull()).count()
    tp = eval_df.where(
        col(chunk_column).isNotNull() & col(entity_column).isNotNull()
    ).count()

    # Derived counts
    fp = pos_pred - tp
    neg_actual = total_tokens - pos_actual
    tn = neg_actual - fp
    fn = pos_actual - tp
    neg_pred = tn + fn

    # Calculate metrics (with zero-division protection)
    recall = tp / pos_actual if pos_actual > 0 else 0.0
    precision = tp / pos_pred if pos_pred > 0 else 0.0
    specificity = tn / neg_actual if neg_actual > 0 else 0.0
    npv = tn / neg_pred if neg_pred > 0 else 0.0
    accuracy = (
        (tp + tn) / (pos_actual + neg_actual) if (pos_actual + neg_actual) > 0 else 0.0
    )

    # F1 score
    f1 = (
        2 * (precision * recall) / (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )

    return {
        # Contingency table
        "true_positives": tp,
        "false_positives": fp,
        "true_negatives": tn,
        "false_negatives": fn,
        "pos_actual": pos_actual,
        "neg_actual": neg_actual,
        "pos_pred": pos_pred,
        "neg_pred": neg_pred,
        "total_tokens": total_tokens,
        # Metrics
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "specificity": specificity,
        "npv": npv,
        "f1_score": f1,
    }


def format_contingency_table(metrics: Dict[str, Any]) -> pd.DataFrame:
    """
    Format metrics as a contingency table DataFrame.

    Args:
        metrics: Dictionary from calculate_metrics()

    Returns:
        Pandas DataFrame with 2x2 contingency table

    Example:
        >>> metrics = calculate_metrics(eval_df, 100000)
        >>> table = format_contingency_table(metrics)
        >>> display(table)
    """
    tn = metrics["true_negatives"]
    fp = metrics["false_positives"]
    fn = metrics["false_negatives"]
    tp = metrics["true_positives"]

    contingency_data = {
        "": ["Neg_pred", "Pos_pred", "Total"],
        "Neg_actual": [tn, fp, tn + fp],
        "Pos_actual": [fn, tp, fn + tp],
        "Total": [tn + fn, fp + tp, tn + fp + fn + tp],
    }

    return pd.DataFrame(contingency_data)


def format_metrics_summary(metrics: Dict[str, Any]) -> pd.DataFrame:
    """
    Format key metrics as a summary DataFrame.

    Args:
        metrics: Dictionary from calculate_metrics()

    Returns:
        Pandas DataFrame with metric names and values

    Example:
        >>> metrics = calculate_metrics(eval_df, 100000)
        >>> summary = format_metrics_summary(metrics)
        >>> display(summary)
    """
    summary_data = {
        "Metric": ["Accuracy", "Precision", "Recall", "Specificity", "NPV", "F1 Score"],
        "Value": [
            metrics["accuracy"],
            metrics["precision"],
            metrics["recall"],
            metrics["specificity"],
            metrics["npv"],
            metrics["f1_score"],
        ],
    }

    return pd.DataFrame(summary_data)


def metrics_to_long_format(
    metrics: Dict[str, Any], dataset_name: str, method_name: str
) -> pd.DataFrame:
    """
    Convert metrics dictionary to long format for storage and analysis.

    Transforms metrics from a dictionary into a long-format DataFrame with
    one row per metric, suitable for combining results from multiple
    datasets and methods.

    Args:
        metrics: Dictionary from calculate_metrics()
        dataset_name: Name of the dataset being evaluated
        method_name: Name of detection method (e.g., 'presidio', 'ai', 'aligned')

    Returns:
        Pandas DataFrame in long format with columns:
        - dataset_name
        - method_name
        - metric_name
        - metric_value

    Example:
        >>> metrics = calculate_metrics(eval_df, 100000)
        >>> long_df = metrics_to_long_format(metrics, "jsl_48docs", "presidio")
        >>> # Returns: [{'dataset_name': 'jsl_48docs', 'method_name': 'presidio',
        >>> #            'metric_name': 'accuracy', 'metric_value': 0.95}, ...]
    """
    import datetime

    metric_names = [
        "accuracy",
        "precision",
        "recall",
        "specificity",
        "npv",
        "f1_score",
        "true_positives",
        "false_positives",
        "true_negatives",
        "false_negatives",
    ]

    rows = []
    timestamp = datetime.datetime.now()

    for metric_name in metric_names:
        if metric_name in metrics:
            rows.append(
                {
                    "dataset_name": dataset_name,
                    "method_name": method_name,
                    "metric_name": metric_name,
                    "metric_value": float(metrics[metric_name]),
                    "timestamp": timestamp,
                }
            )

    return pd.DataFrame(rows)


def save_evaluation_results(
    spark,
    metrics: Dict[str, Any],
    dataset_name: str,
    method_name: str,
    output_table: str,
    mode: str = "append",
) -> None:
    """
    Save evaluation metrics to a shared table.

    Converts metrics to long format and writes to a table for cross-dataset
    and cross-method comparison.

    Args:
        spark: Active SparkSession
        metrics: Dictionary from calculate_metrics()
        dataset_name: Name of the dataset being evaluated
        method_name: Name of detection method
        output_table: Fully qualified output table name
        mode: Write mode ('append', 'overwrite', etc.)

    Example:
        >>> metrics = calculate_metrics(eval_df, 100000)
        >>> save_evaluation_results(
        ...     spark,
        ...     metrics,
        ...     dataset_name="jsl_48docs",
        ...     method_name="presidio",
        ...     output_table="catalog.schema.phi_evaluation_results"
        ... )
    """
    long_df = metrics_to_long_format(metrics, dataset_name, method_name)
    spark_df = spark.createDataFrame(long_df)
    spark_df.write.mode(mode).option("mergeSchema", "true").saveAsTable(output_table)


def compare_methods_across_datasets(
    spark, evaluation_table: str, metric_name: str = "f1_score"
) -> DataFrame:
    """
    Compare detection methods across multiple datasets.

    Aggregates evaluation results from the shared evaluation table to
    compare performance across datasets and methods.

    Args:
        spark: Active SparkSession
        evaluation_table: Table containing evaluation results
        metric_name: Metric to compare (default: 'f1_score')

    Returns:
        DataFrame with comparison results

    Example:
        >>> comparison = compare_methods_across_datasets(
        ...     spark,
        ...     "catalog.schema.phi_evaluation_results",
        ...     metric_name="f1_score"
        ... )
        >>> display(comparison)
    """
    query = f"""
    SELECT 
        dataset_name,
        method_name,
        metric_value
    FROM {evaluation_table}
    WHERE metric_name = '{metric_name}'
    ORDER BY dataset_name, method_name
    """

    return spark.sql(query)


def get_best_method_per_dataset(
    spark, evaluation_table: str, metric_name: str = "f1_score"
) -> DataFrame:
    """
    Identify the best performing method for each dataset.

    Args:
        spark: Active SparkSession
        evaluation_table: Table containing evaluation results
        metric_name: Metric to use for ranking (default: 'f1_score')

    Returns:
        DataFrame with best method per dataset

    Example:
        >>> best_methods = get_best_method_per_dataset(
        ...     spark,
        ...     "catalog.schema.phi_evaluation_results"
        ... )
    """
    query = f"""
    WITH ranked AS (
        SELECT 
            dataset_name,
            method_name,
            metric_value,
            ROW_NUMBER() OVER (
                PARTITION BY dataset_name 
                ORDER BY metric_value DESC
            ) as rank
        FROM {evaluation_table}
        WHERE metric_name = '{metric_name}'
    )
    SELECT 
        dataset_name,
        method_name,
        metric_value
    FROM ranked
    WHERE rank = 1
    ORDER BY dataset_name
    """

    return spark.sql(query)
