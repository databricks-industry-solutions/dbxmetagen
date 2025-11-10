# Databricks notebook source
# MAGIC %md
# MAGIC # Load and Transform AI4Privacy PII Masking Dataset
# MAGIC
# MAGIC This notebook loads the `ai4privacy/pii-masking-300k` dataset from HuggingFace
# MAGIC and transforms it into the format required for PHI/PII detection benchmarking
# MAGIC and evaluation.
# MAGIC
# MAGIC **Dataset:** [ai4privacy/pii-masking-300k](https://huggingface.co/datasets/ai4privacy/pii-masking-300k)
# MAGIC
# MAGIC **Outputs:**
# MAGIC 1. **Source table** for detection benchmarking (`doc_id`, `text`)
# MAGIC 2. **Ground truth table** for evaluation (`doc_id`, `text`, `chunk`, `begin`, `end`)
# MAGIC
# MAGIC **Schema Mapping:**
# MAGIC - `source_text` → `text` (raw text with PII)
# MAGIC - `privacy_mask` → Ground truth entities with positions
# MAGIC - Auto-generated `doc_id` for tracking

# COMMAND ----------

# MAGIC %pip install datasets
# MAGIC %restart_python

# COMMAND ----------

import pandas as pd
from datasets import load_dataset
from pyspark.sql.functions import (
    col,
    monotonically_increasing_id,
    explode,
    expr,
    substring,
    length as sql_length,
    concat,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.text(
    name="output_catalog", defaultValue="dbxmetagen", label="0. Output Catalog"
)

dbutils.widgets.text(
    name="output_schema", defaultValue="eval_data", label="1. Output Schema"
)

dbutils.widgets.text(
    name="dataset_name",
    defaultValue="ai4privacy_pii_300k",
    label="2. Dataset Name (for table suffix)",
)

dbutils.widgets.text(
    name="sample_size",
    defaultValue="1000",
    label="3. Sample Size (0 for full dataset, max 300k)",
)

dbutils.widgets.dropdown(
    name="split", defaultValue="train", choices=["train"], label="4. Dataset Split"
)

# Get widget values
output_catalog = dbutils.widgets.get("output_catalog")
output_schema = dbutils.widgets.get("output_schema")
dataset_name = dbutils.widgets.get("dataset_name")
sample_size = int(dbutils.widgets.get("sample_size"))
split = dbutils.widgets.get("split")

# Construct output table names
source_table = f"{output_catalog}.{output_schema}.{dataset_name}_source"
ground_truth_table = f"{output_catalog}.{output_schema}.{dataset_name}_ground_truth"

print(f"Source table: {source_table}")
print(f"Ground truth table: {ground_truth_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Dataset from HuggingFace

# COMMAND ----------

print("Loading ai4privacy/pii-masking-300k dataset from HuggingFace...")
print(f"Split: {split}")
if sample_size > 0:
    print(f"Sample size: {sample_size:,} records")
else:
    print("Loading full dataset (300k records)")

# Load dataset
ds = load_dataset("ai4privacy/pii-masking-300k", split=split)

# Sample if requested
if sample_size > 0:
    ds = ds.select(range(min(sample_size, len(ds))))

print(f"Loaded {len(ds):,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Dataset Schema

# COMMAND ----------

print("Dataset features:")
print(ds.features)
print("\nFirst record:")
print(ds[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example Record Analysis
# MAGIC
# MAGIC Let's examine the structure of the `privacy_mask` field to understand how entities are stored.

# COMMAND ----------

example = ds[0]

print("Source text:")
print(example["source_text"])
print("\nTarget text (masked):")
print(example["target_text"])
print("\nPrivacy mask:")
for mask in example["privacy_mask"]:
    print(f"  {mask}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert to Pandas and Spark DataFrame

# COMMAND ----------

# Convert to pandas
print("Converting to pandas DataFrame...")
pandas_df = ds.to_pandas()

# Add doc_id
pandas_df["doc_id"] = [f"doc_{i:06d}" for i in range(len(pandas_df))]

print(f"Created pandas DataFrame with {len(pandas_df):,} rows")
display(pandas_df.head())

# COMMAND ----------

# Convert to Spark DataFrame
print("Converting to Spark DataFrame...")
spark_df = spark.createDataFrame(pandas_df)

print(f"Created Spark DataFrame with {spark_df.count():,} rows")
display(spark_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Source Table (for Detection)
# MAGIC
# MAGIC This table contains the raw text for running detection methods.
# MAGIC
# MAGIC **Schema:** `doc_id`, `text`

# COMMAND ----------

# Create source table with just doc_id and text
source_df = spark_df.select(col("doc_id"), col("source_text").alias("text"))

print(f"Source table schema:")
source_df.printSchema()

display(source_df.limit(10))

# COMMAND ----------

# Save source table
print(f"Saving source table to: {source_table}")
source_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    source_table
)

# Optimize table
spark.sql(f"OPTIMIZE {source_table}")

print(f"✓ Source table saved with {source_df.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Privacy Mask to Ground Truth Format
# MAGIC
# MAGIC The `privacy_mask` array contains dictionaries with entity information.
# MAGIC We need to:
# MAGIC 1. Explode the array to create one row per entity
# MAGIC 2. Extract entity positions (start/end)
# MAGIC 3. Extract the actual entity text from the source_text
# MAGIC 4. Format as: `doc_id`, `text`, `chunk`, `begin`, `end`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze Privacy Mask Structure

# COMMAND ----------

# Examine the structure of privacy_mask
privacy_mask_sample = spark_df.select("doc_id", "privacy_mask").limit(5)

display(privacy_mask_sample)

# COMMAND ----------

# Check a specific privacy_mask entry
sample_record = pandas_df.iloc[0]
print("Sample privacy_mask entry:")
if sample_record["privacy_mask"]:
    for i, mask in enumerate(sample_record["privacy_mask"][:3]):  # Show first 3
        print(f"\nEntry {i}:")
        for key, value in mask.items():
            print(f"  {key}: {value}")
else:
    print("Empty privacy_mask")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explode and Transform Privacy Mask

# COMMAND ----------

# Explode the privacy_mask array
ground_truth_df = spark_df.select(
    col("doc_id"),
    col("source_text").alias("text"),
    explode("privacy_mask").alias("mask"),
)

print("After exploding privacy_mask:")
display(ground_truth_df.limit(5))

# COMMAND ----------

# Extract fields from the mask struct
# The privacy_mask typically contains: start, end, label, and sometimes entity_text
ground_truth_df = ground_truth_df.select(
    col("doc_id"),
    col("text"),
    col("mask.start").alias("begin"),
    col("mask.end").alias("end"),
    col("mask.label").alias("entity_type"),
    # Some datasets include the entity text, others don't
    col("mask.value").alias("chunk_from_mask"),
)

display(ground_truth_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract Entity Text from Source
# MAGIC
# MAGIC If the privacy_mask doesn't include the entity text, we extract it from source_text using the positions.

# COMMAND ----------

# Extract chunk from source text using begin/end positions
# Note: The end position might be exclusive or inclusive depending on the dataset
ground_truth_df = ground_truth_df.withColumn(
    "chunk_from_text",
    substring(col("text"), col("begin") + 1, col("end") - col("begin")),
)

# Use chunk_from_mask if available, otherwise use chunk_from_text
ground_truth_df = ground_truth_df.withColumn(
    "chunk", expr("COALESCE(chunk_from_mask, chunk_from_text)")
)

# Drop intermediate columns
ground_truth_df = ground_truth_df.select(
    "doc_id", "text", "chunk", "begin", "end", "entity_type"
)

print("Ground truth DataFrame with extracted chunks:")
display(ground_truth_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Checks

# COMMAND ----------

# Check for null values
print("Null value counts:")
ground_truth_df.select(
    [(col(c).isNull().cast("int")).alias(c) for c in ground_truth_df.columns]
).agg(*[expr(f"SUM({c}) as {c}_nulls") for c in ground_truth_df.columns]).show()

# COMMAND ----------

# Check chunk lengths
print("Chunk length statistics:")
ground_truth_df.withColumn("chunk_length", sql_length(col("chunk"))).select(
    "chunk_length"
).describe().show()

# COMMAND ----------

# Verify positions match text
print("Verifying that positions match extracted chunks...")

verification = (
    ground_truth_df.withColumn(
        "matches",
        substring(col("text"), col("begin") + 1, col("end") - col("begin"))
        == col("chunk"),
    )
    .select("matches")
    .groupBy("matches")
    .count()
)

display(verification)

# COMMAND ----------

# Show examples where positions might not match
print("Examples where positions might need adjustment:")
mismatches = (
    ground_truth_df.withColumn(
        "extracted", substring(col("text"), col("begin") + 1, col("end") - col("begin"))
    )
    .where(col("extracted") != col("chunk"))
    .limit(10)
)

display(mismatches)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Ground Truth Table

# COMMAND ----------

# Final ground truth schema validation
print("Final ground truth schema:")
ground_truth_df.printSchema()

print(f"\nGround truth statistics:")
print(f"Total entities: {ground_truth_df.count():,}")
print(f"Unique documents: {ground_truth_df.select('doc_id').distinct().count():,}")

# COMMAND ----------

# Save ground truth table
print(f"Saving ground truth table to: {ground_truth_table}")
ground_truth_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    ground_truth_table
)

# Optimize table
spark.sql(f"OPTIMIZE {ground_truth_table}")

print(f"✓ Ground truth table saved with {ground_truth_df.count():,} entities")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

print("=" * 80)
print("DATASET SUMMARY")
print("=" * 80)

print(f"\n📊 Source Table: {source_table}")
source_count = spark.table(source_table).count()
print(f"   Documents: {source_count:,}")

print(f"\n📊 Ground Truth Table: {ground_truth_table}")
gt_count = spark.table(ground_truth_table).count()
gt_docs = spark.table(ground_truth_table).select("doc_id").distinct().count()
print(f"   Entities: {gt_count:,}")
print(f"   Documents: {gt_docs:,}")
print(f"   Avg entities per doc: {gt_count/gt_docs:.1f}")

# Entity type distribution
print(f"\n📊 Entity Type Distribution:")
entity_dist = (
    spark.table(ground_truth_table)
    .groupBy("entity_type")
    .count()
    .orderBy(col("count").desc())
)
display(entity_dist)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Records for Verification

# COMMAND ----------

print("Sample source records:")
display(spark.table(source_table).limit(5))

# COMMAND ----------

print("Sample ground truth records:")
display(spark.table(ground_truth_table).orderBy("doc_id", "begin").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC ✅ Dataset loaded and transformed successfully!
# MAGIC
# MAGIC **Now you can:**
# MAGIC
# MAGIC 1. **Run Detection Benchmarking**
# MAGIC    - Open notebook: `1. Benchmarking Detection.py`
# MAGIC    - Set source table to: `{source_table}`
# MAGIC    - Run detection with Presidio, AI Query, or GLiNER
# MAGIC
# MAGIC 2. **Run Evaluation**
# MAGIC    - Open notebook: `2. Benchmarking Evaluation.py`
# MAGIC    - Set ground truth table to: `{ground_truth_table}`
# MAGIC    - Set detection results table (from step 1)
# MAGIC    - Calculate precision, recall, F1 scores
# MAGIC
# MAGIC **Example usage:**
# MAGIC ```python
# MAGIC # In 1. Benchmarking Detection.py
# MAGIC source_table = "{source_table}"
# MAGIC output_table = "{source_table}_detection_results"
# MAGIC
# MAGIC # In 2. Benchmarking Evaluation.py
# MAGIC ground_truth_table = "{ground_truth_table}"
# MAGIC detection_results_table = "{source_table}_detection_results"
# MAGIC dataset_name = "{dataset_name}"
# MAGIC ```

# COMMAND ----------

print(
    f"""
{'='*80}
SETUP COMPLETE ✓
{'='*80}

📁 Tables Created:
   • Source: {source_table}
   • Ground Truth: {ground_truth_table}

🔍 Next Steps:
   1. Benchmarking Detection → Use: {source_table}
   2. Benchmarking Evaluation → Use: {ground_truth_table}

📝 Copy these values to the next notebooks:
   - Source Table: {source_table}
   - Ground Truth Table: {ground_truth_table}
   - Dataset Name: {dataset_name}
{'='*80}
"""
)

# COMMAND ----------
