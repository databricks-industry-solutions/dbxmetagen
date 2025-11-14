# Databricks notebook source
# MAGIC %md
# MAGIC # PHI Detection Evaluation
# MAGIC
# MAGIC This notebook evaluates the performance of different PHI detection methods against ground truth data.
# MAGIC
# MAGIC **Detection Methods:**
# MAGIC - Presidio-based detection
# MAGIC - AI-based detection
# MAGIC - Aligned/combined detection
# MAGIC
# MAGIC **Metrics:**
# MAGIC - Accuracy, Precision, Recall, Specificity, NPV, F1 Score
# MAGIC
# MAGIC **Output:**
# MAGIC - Long-format evaluation table for cross-dataset comparison

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt
# MAGIC %pip install /Volumes/dbxmetagen/default/init_scripts/dbxmetagen-0.5.2-py3-none-any.whl
# MAGIC %restart_python

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col, explode, desc
import uuid
from datetime import datetime

from dbxmetagen.redaction import (
    match_entities_one_to_one,
    calculate_entity_metrics,
    save_false_positives,
    save_false_negatives,
    get_latest_run_metrics,
    save_evaluation_results,
    compare_methods_across_datasets,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.text(
    name="ground_truth_table",
    defaultValue="dbxmetagen.eval_data.jsl_48_ground_truth",
    label="0. Ground Truth Table",
)

dbutils.widgets.text(
    name="detection_results_table",
    defaultValue="dbxmetagen.eval_data.jsl_48docs_deduped_detection_results",
    label="1. Detection Results Table",
)

dbutils.widgets.text(
    name="dataset_name",
    defaultValue="jsl_48docs",
    label="2. Dataset Name (for tracking)",
)

dbutils.widgets.text(
    name="evaluation_output_table",
    defaultValue="dbxmetagen.eval_data.phi_evaluation_results",
    label="3. Evaluation Output Table",
)

dbutils.widgets.dropdown(
    name="write_mode",
    defaultValue="append",
    choices=["append", "overwrite"],
    label="4. Write Mode",
)

# Get widget values
ground_truth_table = dbutils.widgets.get("ground_truth_table")
detection_results_table = dbutils.widgets.get("detection_results_table")
dataset_name = dbutils.widgets.get("dataset_name")
evaluation_output_table = dbutils.widgets.get("evaluation_output_table")
write_mode = dbutils.widgets.get("write_mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# Load ground truth data
ground_truth_df = spark.table(ground_truth_table)
print(f"Ground truth records: {ground_truth_df.count()}")
display(ground_truth_df.limit(5))

# COMMAND ----------

# Load detection results
detection_df = spark.table(detection_results_table)
print(f"Detection results: {detection_df.count()}")
display(detection_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data for Evaluation

# COMMAND ----------

# Check which detection methods are available
available_methods = []
if "presidio_results_struct" in detection_df.columns:
    available_methods.append("presidio")
if "gliner_results_struct" in detection_df.columns:
    available_methods.append("gliner")
if "ai_results_struct" in detection_df.columns:
    available_methods.append("ai")
if "aligned_entities" in detection_df.columns:
    available_methods.append("aligned")

print(f"Available detection methods: {available_methods}")

# COMMAND ----------

# Explode results for each method
exploded_results = {}

if "presidio" in available_methods:
    exploded_results["presidio"] = (
        detection_df.select("doc_id", "presidio_results_struct")
        .withColumn("presidio_results_exploded", explode("presidio_results_struct"))
        .select("presidio_results_exploded.*")
    )

if "gliner" in available_methods:
    exploded_results["gliner"] = (
        detection_df.select("doc_id", "gliner_results_struct")
        .withColumn("gliner_results_exploded", explode("gliner_results_struct"))
        .select(
            "doc_id",
            "gliner_results_exploded.entity",
            "gliner_results_exploded.score",
            "gliner_results_exploded.start",
            "gliner_results_exploded.end",
        )
    )

if "ai" in available_methods:
    exploded_results["ai"] = (
        detection_df.select("doc_id", "ai_results_struct")
        .withColumn("ai_results_exploded", explode("ai_results_struct"))
        .select(
            "doc_id",
            "ai_results_exploded.entity",
            "ai_results_exploded.score",
            "ai_results_exploded.start",
            "ai_results_exploded.end",
        )
    )

if "aligned" in available_methods:
    exploded_results["aligned"] = (
        detection_df.select("doc_id", "aligned_entities")
        .withColumn("aligned_entities", explode("aligned_entities"))
        .select(
            "doc_id",
            "aligned_entities.entity",
            "aligned_entities.start",
            "aligned_entities.end",
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Corpus Statistics

# COMMAND ----------

# Calculate total tokens in corpus for metrics
text_dict = (
    ground_truth_df.select("doc_id", "text")
    .distinct()
    .toPandas()
    .to_dict(orient="list")
)
corpus = "\n".join(text_dict["text"])
all_tokens = len(corpus)

print(f"Total tokens in corpus: {all_tokens:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Run ID and Timestamp

# COMMAND ----------

# Generate unique run ID and timestamp for this evaluation
run_id = str(uuid.uuid4())
run_timestamp = datetime.now()

print(f"Run ID: {run_id}")
print(f"Timestamp: {run_timestamp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate Each Detection Method
# MAGIC
# MAGIC Using one-to-one entity matching with IoU threshold to prevent double counting.

# COMMAND ----------

evaluation_results = {}
false_positives_dfs = {}
false_negatives_dfs = {}

# Exploded ground truth for matching
gt_exploded = ground_truth_df.select("doc_id", "chunk", "begin", "end")

for method_name, exploded_df in exploded_results.items():
    print(f"\n{'='*80}")
    print(f"Evaluating: {method_name.upper()}")
    print(f"{'='*80}")

    # Perform one-to-one matching with IoU threshold
    print(f"Matching entities with IoU threshold = 0.5...")
    matched_df, fn_df, fp_df = match_entities_one_to_one(
        ground_truth_df=gt_exploded,
        predictions_df=exploded_df,
        iou_threshold=0.5,
        doc_id_column="doc_id",
        gt_start_column="begin",
        gt_end_column="end",
        gt_text_column="chunk",
        pred_start_column="start",
        pred_end_column="end",
        pred_text_column="entity",
    )

    print(
        f"Matched: {matched_df.count()}, False Negatives: {fn_df.count()}, False Positives: {fp_df.count()}"
    )

    # Calculate entity-level metrics (no TN)
    metrics = calculate_entity_metrics(matched_df, fp_df, fn_df, run_id, run_timestamp)
    evaluation_results[method_name] = metrics
    false_positives_dfs[method_name] = fp_df
    false_negatives_dfs[method_name] = fn_df

    # Display metrics summary
    print(f"\n{method_name.upper()} Metrics Summary:")
    print(f"  True Positives:  {metrics['true_positives']}")
    print(f"  False Positives: {metrics['false_positives']}")
    print(f"  False Negatives: {metrics['false_negatives']}")
    print(f"  Precision:       {metrics['precision']:.4f}")
    print(f"  Recall:          {metrics['recall']:.4f}")
    print(f"  F1 Score:        {metrics['f1_score']:.4f}")

    # Show sample matched entities with scoring details
    if matched_df.count() > 0:
        print(f"\n  Sample Matches (with scoring details):")
        match_samples = matched_df.select(
            col("gt.chunk").alias("ground_truth"),
            col("pred.entity").alias("predicted"),
            "iou_score",
            "exact_text_match",
            "pred_contains_gt",
            "final_score",
        ).limit(5)
        for row in match_samples.collect():
            print(f"    GT: '{row.ground_truth}' | Pred: '{row.predicted}'")
            print(
                f"      IoU: {row.iou_score:.3f} | Final: {row.final_score:.3f} | ExactMatch: {row.exact_text_match} | Contains: {row.pred_contains_gt}"
            )

    # Save to evaluation table
    save_evaluation_results(
        spark=spark,
        metrics=metrics,
        dataset_name=dataset_name,
        method_name=method_name,
        output_table=evaluation_output_table,
        mode=write_mode,
    )

    # Save false positives and false negatives to Delta tables
    save_false_positives(
        spark=spark,
        fp_df=fp_df,
        dataset_name=dataset_name,
        method_name=method_name,
        catalog="dbxmetagen",
        schema="eval_data",
        run_id=run_id,
        timestamp=run_timestamp,
    )

    save_false_negatives(
        spark=spark,
        fn_df=fn_df,
        dataset_name=dataset_name,
        method_name=method_name,
        catalog="dbxmetagen",
        schema="eval_data",
        run_id=run_id,
        timestamp=run_timestamp,
    )

    print(f"\nResults saved to {evaluation_output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare Methods
# MAGIC
# MAGIC Side-by-side comparison of all detection methods.

# COMMAND ----------

comparison_data = {
    "Method": [],
    "Precision": [],
    "Recall": [],
    "F1 Score": [],
}

for method_name, metrics in evaluation_results.items():
    comparison_data["Method"].append(method_name.capitalize())
    comparison_data["Precision"].append(metrics["precision"])
    comparison_data["Recall"].append(metrics["recall"])
    comparison_data["F1 Score"].append(metrics["f1_score"])

comparison_df = pd.DataFrame(comparison_data)
display(comparison_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Dataset Comparison
# MAGIC
# MAGIC View all evaluation results from the shared table.

# COMMAND ----------

try:
    all_results = spark.table(evaluation_output_table)
    print(f"Total evaluation records: {all_results.count()}")
    display(all_results)
except Exception as e:
    print(f"Could not load evaluation table: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## F1 Score Comparison Across Datasets

# COMMAND ----------

try:
    f1_comparison = compare_methods_across_datasets(
        spark=spark, evaluation_table=evaluation_output_table, metric_name="f1_score"
    )
    display(f1_comparison)
except Exception as e:
    print(f"Could not compare across datasets: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Precision-Recall Trade-off

# COMMAND ----------

try:
    # Get only the latest run for each dataset and method
    precision_recall = spark.sql(
        f"""
        WITH latest_runs AS (
            SELECT dataset_name, method_name, MAX(timestamp) as latest_timestamp
            FROM {evaluation_output_table}
            GROUP BY dataset_name, method_name
        )
        SELECT 
            e.dataset_name,
            e.method_name,
            MAX(CASE WHEN e.metric_name = 'precision' THEN e.metric_value END) as precision,
            MAX(CASE WHEN e.metric_name = 'recall' THEN e.metric_value END) as recall,
            MAX(CASE WHEN e.metric_name = 'f1_score' THEN e.metric_value END) as f1_score
        FROM {evaluation_output_table} e
        INNER JOIN latest_runs lr 
            ON e.dataset_name = lr.dataset_name 
            AND e.method_name = lr.method_name 
            AND e.timestamp = lr.latest_timestamp
        WHERE e.metric_name IN ('precision', 'recall', 'f1_score')
        GROUP BY e.dataset_name, e.method_name
        ORDER BY e.dataset_name, f1_score DESC
        """
    )
    display(precision_recall)
except Exception as e:
    print(f"Could not create precision-recall view: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## False Positives Analysis
# MAGIC
# MAGIC Analyze entities that were incorrectly detected.

# COMMAND ----------

for method_name, fp_df in false_positives_dfs.items():
    print(f"\n{'='*80}")
    print(f"{method_name.upper()} - False Positives (Top 20 by frequency)")
    print(f"{'='*80}")

    fp_summary = fp_df.groupBy("entity").count().orderBy(desc("count")).limit(20)
    display(fp_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## False Negatives Analysis
# MAGIC
# MAGIC Analyze entities that were missed by detection methods.

# COMMAND ----------

for method_name, fn_df in false_negatives_dfs.items():
    print(f"\n{'='*80}")
    print(f"{method_name.upper()} - False Negatives (Top 20 by frequency)")
    print(f"{'='*80}")

    fn_summary = fn_df.groupBy("chunk").count().orderBy(desc("count")).limit(20)
    display(fn_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## False Positive Examples with Context
# MAGIC
# MAGIC Sample false positives with their document context.

# COMMAND ----------

for method_name, fp_df in false_positives_dfs.items():
    print(f"\n{'='*80}")
    print(f"{method_name.upper()} - False Positive Examples")
    print(f"{'='*80}")

    fp_examples = fp_df.select("doc_id", "entity", "start", "end").limit(10)
    display(fp_examples)

# COMMAND ----------

# MAGIC %md
# MAGIC ## False Negative Examples with Context
# MAGIC
# MAGIC Sample false negatives with their document context.

# COMMAND ----------

for method_name, fn_df in false_negatives_dfs.items():
    print(f"\n{'='*80}")
    print(f"{method_name.upper()} - False Negative Examples")
    print(f"{'='*80}")

    fn_examples = fn_df.select("doc_id", "chunk", "begin", "end").limit(10)
    display(fn_examples)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visual Inspection with Context
# MAGIC
# MAGIC Show false positives and false negatives with surrounding text context for manual review.

# COMMAND ----------

# Get the full text for each document
doc_text_df = ground_truth_df.select("doc_id", "text").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### False Positives with Context

# COMMAND ----------

from pyspark.sql.functions import (
    substring,
    length,
    when,
    lit,
    concat,
    abs as sql_abs,
    least,
)

for method_name, fp_df in false_positives_dfs.items():
    print(f"\n{'='*80}")
    print(f"{method_name.upper()} - False Positive Examples with Context")
    print(f"{'='*80}")

    # Join with document text
    fp_with_text = fp_df.join(doc_text_df, on="doc_id", how="left")

    # Join with ground truth to find best matching GT entity (within 10 positions tolerance)
    gt_for_matching = (
        ground_truth_df.select("doc_id", "chunk", "begin", "end")
        .withColumnRenamed("chunk", "gt_chunk")
        .withColumnRenamed("begin", "gt_begin")
        .withColumnRenamed("end", "gt_end")
    )

    fp_with_gt = fp_with_text.join(gt_for_matching, on="doc_id", how="left")

    # Calculate position distance and filter to within tolerance
    position_tolerance = 10
    fp_with_gt = fp_with_gt.withColumn(
        "position_distance",
        least(
            sql_abs(col("start") - col("gt_begin")), sql_abs(col("end") - col("gt_end"))
        ),
    ).filter(col("position_distance") <= position_tolerance)

    # Keep only the best match per FP entity (closest GT)
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    window = Window.partitionBy("doc_id", "entity", "start", "end").orderBy(
        "position_distance"
    )
    fp_with_best_gt = (
        fp_with_gt.withColumn("rank", row_number().over(window))
        .filter(col("rank") == 1)
        .drop("rank")
    )

    # Calculate context window (25 chars before and after) for PREDICTED entity
    context_size = 25
    fp_with_context = (
        fp_with_best_gt.withColumn(
            "pred_context_start",
            when(
                col("start") - context_size >= 0, col("start") - context_size
            ).otherwise(0),
        )
        .withColumn(
            "pred_context_end",
            when(
                col("end") + 1 + context_size <= length(col("text")),
                col("end") + 1 + context_size,
            ).otherwise(length(col("text"))),
        )
        .withColumn(
            "pred_text_with_context",
            concat(
                lit("..."),
                substring(
                    col("text"),
                    col("pred_context_start") + 1,
                    col("start") - col("pred_context_start"),
                ),
                lit(">>>"),
                substring(col("text"), col("start") + 1, col("end") + 1 - col("start")),
                lit("<<<"),
                substring(
                    col("text"),
                    col("end") + 2,
                    col("pred_context_end") - col("end") - 1,
                ),
                lit("..."),
            ),
        )
        # Calculate context for GROUND TRUTH entity
        .withColumn(
            "gt_context_start",
            when(
                col("gt_begin") - context_size >= 0, col("gt_begin") - context_size
            ).otherwise(0),
        )
        .withColumn(
            "gt_context_end",
            when(
                col("gt_end") + context_size <= length(col("text")),
                col("gt_end") + context_size,
            ).otherwise(length(col("text"))),
        )
        .withColumn(
            "gt_text_with_context",
            concat(
                lit("..."),
                substring(
                    col("text"),
                    col("gt_context_start") + 1,
                    col("gt_begin") - col("gt_context_start"),
                ),
                lit(">>>"),
                col("gt_chunk"),
                lit("<<<"),
                substring(
                    col("text"),
                    col("gt_end") + 1,
                    col("gt_context_end") - col("gt_end"),
                ),
                lit("..."),
            ),
        )
    )

    # Display sample with both predicted and GT context
    fp_display = fp_with_context.select(
        "doc_id",
        "entity",
        "pred_text_with_context",
        "gt_chunk",
        "gt_text_with_context",
        "position_distance",
    ).limit(10)

    display(fp_display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### False Negatives with Context

# COMMAND ----------

for method_name, fn_df in false_negatives_dfs.items():
    print(f"\n{'='*80}")
    print(f"{method_name.upper()} - False Negative Examples with Context")
    print(f"{'='*80}")

    # Join with document text
    fn_with_text = fn_df.join(doc_text_df, on="doc_id", how="left")

    # Calculate context window (25 chars before and after)
    context_size = 25
    fn_with_context = (
        fn_with_text.withColumn(
            "context_start",
            when(
                col("begin") - context_size >= 0, col("begin") - context_size
            ).otherwise(0),
        )
        .withColumn(
            "context_end",
            when(
                col("end") + context_size <= length(col("text")),
                col("end") + context_size,
            ).otherwise(length(col("text"))),
        )
        .withColumn(
            "text_with_context",
            concat(
                lit("..."),
                substring(
                    col("text"),
                    col("context_start") + 1,
                    col("begin") - col("context_start"),
                ),
                lit(">>>"),
                col("chunk"),
                lit("<<<"),
                substring(col("text"), col("end") + 1, col("context_end") - col("end")),
                lit("..."),
            ),
        )
    )

    # Display sample with context
    fn_display = fn_with_context.select("doc_id", "chunk", "text_with_context").limit(
        10
    )

    display(fn_display)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Latest Run Metrics
# MAGIC
# MAGIC Query the most recent evaluation run.

# COMMAND ----------

try:
    latest_metrics = get_latest_run_metrics(
        spark=spark, evaluation_table=evaluation_output_table, dataset_name=dataset_name
    )
    print(f"Latest run metrics for dataset: {dataset_name}")
    display(latest_metrics)
except Exception as e:
    print(f"Could not load latest run metrics: {e}")
