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
from pyspark.sql.functions import col, explode, desc, abs as sql_abs, least
import uuid
from datetime import datetime

from dbxmetagen.redaction import (
    match_entities_flexible,
    calculate_entity_metrics,
    save_false_positives,
    save_false_negatives,
    save_metrics,
    get_latest_run_metrics,
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

dbutils.widgets.text(
    name="iou_threshold",
    defaultValue="0.1",
    label="5a. IoU Threshold for Partial Matches (0.0-1.0)",
)

dbutils.widgets.text(
    name="position_tolerance",
    defaultValue="2",
    label="5b. Position Tolerance for Exact Matches (chars)",
)

# Get widget values
ground_truth_table = dbutils.widgets.get("ground_truth_table")
detection_results_table = dbutils.widgets.get("detection_results_table")
dataset_name = dbutils.widgets.get("dataset_name")
evaluation_output_table = dbutils.widgets.get("evaluation_output_table")
write_mode = dbutils.widgets.get("write_mode")
iou_threshold = float(dbutils.widgets.get("iou_threshold"))
position_tolerance = int(dbutils.widgets.get("position_tolerance"))

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
        .withColumn("end", col("end") + 1)  # Convert AI's inclusive end to exclusive
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
    print(f"  IoU Threshold: {iou_threshold}, Position Tolerance: {position_tolerance}")
    print(f"{'='*80}")

    # Perform flexible matching (exact, composite, partial)
    print(f"Matching entities...")
    matched_df, fn_df, fp_df = match_entities_flexible(
        ground_truth_df=gt_exploded,
        predictions_df=exploded_df,
        iou_threshold=iou_threshold,
        position_tolerance=position_tolerance,
        doc_id_column="doc_id",
        gt_start_column="begin",
        gt_end_column="end",
        gt_text_column="chunk",
        pred_start_column="start",
        pred_end_column="end",
        pred_text_column="entity",
    )

    # Count match types
    exact_count = matched_df.filter(col("match_type") == "exact").count()
    partial_count = matched_df.filter(col("match_type") == "partial").count()
    total_matched = matched_df.count()
    fn_count = fn_df.count()
    fp_count = fp_df.count()

    print(f"Results:")
    print(f"  Exact Matches:    {exact_count:4,}")
    print(f"  Partial Matches:  {partial_count:4,}")
    print(f"  Total Matched:    {total_matched:4,}")
    print(f"  False Negatives:  {fn_count:4,}")
    print(f"  False Positives:  {fp_count:4,}")

    # Calculate entity-level metrics
    metrics = calculate_entity_metrics(matched_df, fn_df, fp_df)
    evaluation_results[method_name] = metrics
    false_positives_dfs[method_name] = fp_df
    false_negatives_dfs[method_name] = fn_df

    # Display metrics summary
    print(f"\n{method_name.upper()} Metrics Summary:")
    print(f"  True Positives:  {metrics['tp']}")
    print(f"  False Positives: {metrics['fp']}")
    print(f"  False Negatives: {metrics['fn']}")
    print(f"  Precision:       {metrics['precision']:.4f}")
    print(f"  Recall:          {metrics['recall']:.4f}")
    print(f"  F1 Score:        {metrics['f1']:.4f}")

    # Show sample matched entities
    if matched_df.count() > 0:
        print(f"\n  Sample Matches:")
        matched_df.select(
            "doc_id", "gt_text", "pred_text", "iou_score", "match_type"
        ).orderBy(desc("iou_score")).limit(10).show(truncate=False)

    # Save metrics to evaluation table
    save_metrics(
        metrics=metrics,
        table_name=evaluation_output_table,
        dataset_name=dataset_name,
        method_name=method_name,
        run_id=run_id,
        run_timestamp=run_timestamp,
        spark=spark,
    )

    # Save false positives and false negatives to Delta tables
    fp_table_name = f"{evaluation_output_table}_false_positives"
    save_false_positives(
        false_positives_df=fp_df,
        table_name=fp_table_name,
        dataset_name=dataset_name,
        method_name=method_name,
        run_id=run_id,
        timestamp=run_timestamp,
    )

    fn_table_name = f"{evaluation_output_table}_false_negatives"
    save_false_negatives(
        false_negatives_df=fn_df,
        table_name=fn_table_name,
        dataset_name=dataset_name,
        method_name=method_name,
        run_id=run_id,
        run_timestamp=run_timestamp,
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
    comparison_data["F1 Score"].append(metrics["f1"])

comparison_df = pd.DataFrame(comparison_data)
display(comparison_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Alignment Diagnostic Analysis
# MAGIC
# MAGIC Compare individual methods vs aligned results to understand alignment behavior.

# COMMAND ----------

if "aligned" in exploded_results:
    print("=" * 80)
    print("ALIGNMENT DIAGNOSTIC: Understanding Alignment Behavior")
    print("=" * 80)

    # Get counts for each method
    print("\n1. Entity Counts Per Method:")
    for method_name, exploded_df in exploded_results.items():
        count = exploded_df.count()
        unique_count = (
            exploded_df.select("doc_id", "entity", "start", "end").distinct().count()
        )
        print(f"  {method_name:12s}: {count:5,} total | {unique_count:5,} unique")

    # Check if aligned has more entities than sum of individuals
    if "presidio" in exploded_results and "ai" in exploded_results:
        presidio_count = exploded_results["presidio"].count()
        ai_count = exploded_results["ai"].count()
        gliner_count = (
            exploded_results["gliner"].count() if "gliner" in exploded_results else 0
        )
        aligned_count = exploded_results["aligned"].count()

        print(f"\n2. Alignment Union Behavior:")
        print(
            f"  Sum of individual methods: {presidio_count + ai_count + gliner_count:,}"
        )
        print(f"  Aligned result count:      {aligned_count:,}")
        delta = aligned_count - max(presidio_count, ai_count, gliner_count)
        if delta > 0:
            print(
                f"  ⚠ Aligned has {delta} MORE entities than largest source (union behavior)"
            )
        elif delta < 0:
            print(
                f"  ✓ Aligned has {abs(delta)} FEWER entities (dedup/consensus behavior)"
            )

    # Find entities in aligned but not in any individual method (alignment artifacts)
    print(f"\n3. Alignment Artifacts (entities created by alignment):")

    aligned_df = exploded_results["aligned"].select("doc_id", "entity", "start", "end")

    # Union all individual method entities
    individual_union = None
    for method in ["presidio", "ai", "gliner"]:
        if method in exploded_results:
            method_df = exploded_results[method].select(
                "doc_id", "entity", "start", "end"
            )
            if individual_union is None:
                individual_union = method_df
            else:
                individual_union = individual_union.union(method_df)

    if individual_union is not None:
        # Find entities in aligned but NOT in any individual method
        artifacts = aligned_df.join(
            individual_union,
            (aligned_df["doc_id"] == individual_union["doc_id"])
            & (aligned_df["entity"] == individual_union["entity"])
            & (aligned_df["start"] == individual_union["start"])
            & (aligned_df["end"] == individual_union["end"]),
            "left_anti",
        )

        artifact_count = artifacts.count()
        if artifact_count > 0:
            print(
                f"  ⚠ Found {artifact_count} entities in aligned that don't match ANY individual detection"
            )
            print(f"    These are likely boundary modifications by alignment")
            print(f"    Sample artifacts:")
            artifacts.orderBy("doc_id").limit(10).show(truncate=False)
        else:
            print(
                f"  ✓ No artifacts - all aligned entities match individual detections"
            )

    # Find entities in individual methods but missing from aligned
    print(f"\n4. Entities Lost in Alignment:")

    for method in ["presidio", "ai", "gliner"]:
        if method in exploded_results:
            method_df = exploded_results[method].select(
                "doc_id", "entity", "start", "end"
            )

            # Find entities in this method but NOT in aligned
            missing_in_aligned = method_df.join(
                aligned_df,
                (method_df["doc_id"] == aligned_df["doc_id"])
                & (method_df["entity"] == aligned_df["entity"])
                & (method_df["start"] == aligned_df["start"])
                & (method_df["end"] == aligned_df["end"]),
                "left_anti",
            )

            missing_count = missing_in_aligned.count()
            if missing_count > 0:
                print(f"  ⚠ {method:12s}: {missing_count:3,} entities not in aligned")
                print(f"    Sample missing entities from {method}:")
                missing_in_aligned.orderBy("doc_id").limit(5).show(truncate=False)
            else:
                print(f"  ✓ {method:12s}: All entities present in aligned")

    # Specific check for doc_id 2, entity at 4732-4738
    print(f"\n5. Specific Case Investigation (Doc ID 2, pos 4732-4738):")

    for method in ["presidio", "ai", "gliner", "aligned"]:
        if method in exploded_results:
            specific_entity = exploded_results[method].filter(
                (col("doc_id") == "2") & (col("start") == 4732) & (col("end") == 4738)
            )
            count = specific_entity.count()
            if count > 0:
                print(f"  ✓ {method:12s}: Found entity at position 4732-4738")
                specific_entity.select("doc_id", "entity", "start", "end").show(
                    truncate=False
                )
            else:
                print(f"  ✗ {method:12s}: NOT found")

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
    # Simple comparison across datasets using new metrics schema
    f1_comparison = spark.sql(
        f"""
        WITH latest_runs AS (
            SELECT dataset_name, method_name, MAX(timestamp) as latest_timestamp
            FROM {evaluation_output_table}
            GROUP BY dataset_name, method_name
        )
        SELECT 
            e.dataset_name,
            e.method_name,
            e.f1
        FROM {evaluation_output_table} e
        INNER JOIN latest_runs lr 
            ON e.dataset_name = lr.dataset_name 
            AND e.method_name = lr.method_name 
            AND e.timestamp = lr.latest_timestamp
        ORDER BY e.dataset_name, e.f1 DESC
    """
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
            e.precision,
            e.recall,
            e.f1,
            e.exact_matches,
            e.partial_matches
        FROM {evaluation_output_table} e
        INNER JOIN latest_runs lr 
            ON e.dataset_name = lr.dataset_name 
            AND e.method_name = lr.method_name 
            AND e.timestamp = lr.latest_timestamp
        ORDER BY e.dataset_name, e.f1 DESC
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

    # Diagnostic: Check for FPs that look like they should match GT
    print(f"\n  Diagnostic: Checking for potential matching issues...")

    # Check if any FPs have exact text matches with GT (but different positions)
    fp_with_positions = fp_df.select("doc_id", "entity", "start", "end")
    gt_with_positions = gt_exploded.select("doc_id", "chunk", "begin", "end")

    # Find FPs where text matches GT chunk exactly
    potential_matches = fp_with_positions.join(
        gt_with_positions,
        (fp_with_positions["doc_id"] == gt_with_positions["doc_id"])
        & (fp_with_positions["entity"] == gt_with_positions["chunk"]),
        "inner",
    )

    potential_match_count = potential_matches.count()
    if potential_match_count > 0:
        print(
            f"    ⚠ Found {potential_match_count} FPs with text matching GT (position mismatch >5 chars)"
        )
        print(f"      Sample cases:")
        potential_matches.withColumn(
            "position_diff",
            least(
                sql_abs(fp_with_positions["start"] - gt_with_positions["begin"]),
                sql_abs(fp_with_positions["end"] - gt_with_positions["end"]),
            ),
        ).select("doc_id", "entity", "start", "begin", "position_diff").orderBy(
            desc("position_diff")
        ).limit(
            5
        ).show(
            truncate=False
        )

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
