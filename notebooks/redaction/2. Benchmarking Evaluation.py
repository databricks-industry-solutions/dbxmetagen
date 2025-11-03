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
# MAGIC %pip install -e ../../
# MAGIC %restart_python

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col, explode

from dbxmetagen.redaction import (
    evaluate_detection,
    calculate_metrics,
    format_contingency_table,
    format_metrics_summary,
    save_evaluation_results,
    compare_methods_across_datasets,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.text(
    name="ground_truth_table",
    defaultValue="dbxmetagen.eval_data.jsl_48docs",
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
# MAGIC ## Evaluate Each Detection Method

# COMMAND ----------

evaluation_results = {}

for method_name, exploded_df in exploded_results.items():
    print(f"\n{'='*80}")
    print(f"Evaluating: {method_name.upper()}")
    print(f"{'='*80}")

    # Evaluate detection
    eval_df = evaluate_detection(ground_truth_df, exploded_df)

    # Calculate metrics
    metrics = calculate_metrics(eval_df, all_tokens)
    evaluation_results[method_name] = metrics

    # Display contingency table
    print(f"\n{method_name.upper()} Contingency Table:")
    contingency_df = format_contingency_table(metrics)
    display(contingency_df)

    # Display metrics summary
    print(f"\n{method_name.upper()} Metrics Summary:")
    summary_df = format_metrics_summary(metrics)
    display(summary_df)

    # Save to evaluation table
    save_evaluation_results(
        spark=spark,
        metrics=metrics,
        dataset_name=dataset_name,
        method_name=method_name,
        output_table=evaluation_output_table,
        mode=write_mode,
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
    "Accuracy": [],
}

for method_name, metrics in evaluation_results.items():
    comparison_data["Method"].append(method_name.capitalize())
    comparison_data["Precision"].append(metrics["precision"])
    comparison_data["Recall"].append(metrics["recall"])
    comparison_data["F1 Score"].append(metrics["f1_score"])
    comparison_data["Accuracy"].append(metrics["accuracy"])

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
    precision_recall = spark.sql(
        f"""
        SELECT 
            dataset_name,
            method_name,
            MAX(CASE WHEN metric_name = 'precision' THEN metric_value END) as precision,
            MAX(CASE WHEN metric_name = 'recall' THEN metric_value END) as recall,
            MAX(CASE WHEN metric_name = 'f1_score' THEN metric_value END) as f1_score
        FROM {evaluation_output_table}
        WHERE metric_name IN ('precision', 'recall', 'f1_score')
        GROUP BY dataset_name, method_name
        ORDER BY dataset_name, f1_score DESC
    """
    )
    display(precision_recall)
except Exception as e:
    print(f"Could not create precision-recall view: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## False Negatives Analysis
# MAGIC
# MAGIC Analyze entities that were missed by detection methods.

# COMMAND ----------

if "aligned" in exploded_results:
    aligned_eval_df = evaluate_detection(ground_truth_df, exploded_results["aligned"])

    print("Top Missed Entities (False Negatives):")
    false_negatives = (
        aligned_eval_df.where(col("entity").isNull())
        .select("chunk")
        .groupBy("chunk")
        .count()
        .orderBy(col("count").desc())
    )
    display(false_negatives.limit(20))
