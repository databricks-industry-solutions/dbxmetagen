# Databricks notebook source
# MAGIC %md
# MAGIC # PHI Detection Evaluation
# MAGIC
# MAGIC This notebook evaluates the performance of different PHI detection methods:
# MAGIC - Presidio-based detection
# MAGIC - AI-based detection
# MAGIC - Aligned/combined detection
# MAGIC
# MAGIC Metrics calculated include accuracy, precision, recall, specificity, and NPV.

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------


# COMMAND ----------

# Import necessary libraries
import pandas as pd
import numpy as np
from pyspark.sql.functions import col, explode

# Import evaluation functions from the redaction library
from src.dbxmetagen.redaction.evaluation import (
    evaluate_detection,
    calculate_metrics,
    format_contingency_table,
    format_metrics_summary,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# Load ground truth data
df = spark.table("dbxmetagen.eval_data.jsl_48docs")
display(df)

# COMMAND ----------

# Load detection results
df_results = spark.table("dbxmetagen.eval_data.aligned_entities3")
display(df_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data for Evaluation

# COMMAND ----------

# Explode Presidio results for evaluation
presidio_results_exploded = (
    df_results
    .select("doc_id", "presidio_results_struct")
    .withColumn("presidio_results_exploded", explode("presidio_results_struct"))
    .select("presidio_results_exploded.*")
)

# Explode AI results for evaluation
ai_results_exploded = (
    df_results
    .select("doc_id", "ai_results_struct")
    .withColumn("ai_results_exploded", explode("ai_results_struct"))
    .select(
        "doc_id",
        "ai_results_exploded.entity",
        "ai_results_exploded.score",
        "ai_results_exploded.start",
        "ai_results_exploded.end"
    )
)

# Explode aligned results for evaluation
aligned_results_exploded = (
    df_results
    .select("doc_id", "aligned_entities")
    .withColumn("aligned_entities", explode("aligned_entities"))
    .select(
        "doc_id",
        "aligned_entities.entity",
        "aligned_entities.start",
        "aligned_entities.end"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate Detection Methods

# COMMAND ----------

# Evaluate each detection method against ground truth
presidio_join_df = evaluate_detection(df, presidio_results_exploded)
ai_join_df = evaluate_detection(df, ai_results_exploded)
aligned_join_df = evaluate_detection(df, aligned_results_exploded)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Corpus Statistics

# COMMAND ----------

# Calculate total tokens in corpus for metrics
text_dict = df.select("doc_id", "text").distinct().toPandas().to_dict(orient="list")
corpus = '\n'.join(text_dict['text'])
all_tokens = len(corpus)

print(f"Total tokens in corpus: {all_tokens:,}")

# COMMAND ----------

display(df_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Presidio Detection Metrics

# COMMAND ----------

# Calculate metrics for Presidio
presidio_metrics = calculate_metrics(presidio_join_df, all_tokens)

# Display contingency table
print("Presidio Contingency Table:")
contingency_df = format_contingency_table(presidio_metrics)
display(contingency_df)

# Display summary metrics
print("\nPresidio Metrics Summary:")
summary_df = format_metrics_summary(presidio_metrics)
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI Detection Metrics

# COMMAND ----------

# Calculate metrics for AI detection
ai_metrics = calculate_metrics(ai_join_df, all_tokens)

# Display contingency table
print("AI Detection Contingency Table:")
contingency_df = format_contingency_table(ai_metrics)
display(contingency_df)

# Display summary metrics
print("\nAI Detection Metrics Summary:")
summary_df = format_metrics_summary(ai_metrics)
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aligned Detection Metrics

# COMMAND ----------

# Calculate metrics for aligned detection
aligned_metrics = calculate_metrics(aligned_join_df, all_tokens)

# Display contingency table
print("Aligned Detection Contingency Table:")
contingency_df = format_contingency_table(aligned_metrics)
display(contingency_df)

# Display summary metrics
print("\nAligned Detection Metrics Summary:")
summary_df = format_metrics_summary(aligned_metrics)
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare Methods

# COMMAND ----------

# Create comparison DataFrame
comparison_data = {
    "Method": ["Presidio", "AI", "Aligned"],
    "Precision": [
        presidio_metrics["precision"],
        ai_metrics["precision"],
        aligned_metrics["precision"]
    ],
    "Recall": [
        presidio_metrics["recall"],
        ai_metrics["recall"],
        aligned_metrics["recall"]
    ],
    "F1 Score": [
        presidio_metrics["f1_score"],
        ai_metrics["f1_score"],
        aligned_metrics["f1_score"]
    ],
    "Accuracy": [
        presidio_metrics["accuracy"],
        ai_metrics["accuracy"],
        aligned_metrics["accuracy"]
    ]
}

comparison_df = pd.DataFrame(comparison_data)
display(comparison_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze False Negatives

# COMMAND ----------

# Show false negatives (entities that were missed)
print("False Negatives (Missed Entities):")
display(
    aligned_join_df
    .where(aligned_join_df.entity.isNull())
    .select("chunk")
    .groupBy("chunk")
    .count()
    .orderBy(col("count").desc())
)

# COMMAND ----------


