# Databricks notebook source
# MAGIC %md
# MAGIC # PII/PHI Redaction
# MAGIC
# MAGIC This notebook takes detection results and applies redaction to create a clean dataset.
# MAGIC
# MAGIC **Redaction Strategies:**
# MAGIC - **Generic**: Replace entities with `[REDACTED]`
# MAGIC - **Typed**: Replace entities with type-specific placeholders like `[PERSON]`, `[EMAIL]`
# MAGIC
# MAGIC **Input:**
# MAGIC - Table with detection results (from Benchmarking Detection notebook)
# MAGIC
# MAGIC **Output:**
# MAGIC - New table with redacted text (suffix: `_redacted`)

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt
# MAGIC %pip install /Volumes/dbxmetagen/default/init_scripts/dbxmetagen-0.5.2-py3-none-any.whl
# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql.functions import col

from dbxmetagen.redaction import create_redacted_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.text(
    name="detection_table",
    defaultValue="dbxmetagen.eval_data.jsl_48docs_deduped_detection_results",
    label="0. Detection Results Table",
)

dbutils.widgets.text(
    name="text_column", defaultValue="text", label="1. Text Column to Redact"
)

dbutils.widgets.text(
    name="entities_column", defaultValue="aligned_entities", label="2. Entities Column"
)

dbutils.widgets.dropdown(
    name="redaction_strategy",
    defaultValue="typed",
    choices=["generic", "typed"],
    label="3. Redaction Strategy",
)

dbutils.widgets.text(
    name="output_table",
    defaultValue="",
    label="4. Output Table (leave blank for auto-suffix)",
)

dbutils.widgets.text(
    name="redacted_suffix", defaultValue="_redacted", label="5. Redacted Column Suffix"
)

# Get widget values
detection_table = dbutils.widgets.get("detection_table")
text_column = dbutils.widgets.get("text_column")
entities_column = dbutils.widgets.get("entities_column")
redaction_strategy = dbutils.widgets.get("redaction_strategy")
output_table = dbutils.widgets.get("output_table")
redacted_suffix = dbutils.widgets.get("redacted_suffix")

# Auto-generate output table name if not provided
if not output_table:
    output_table = f"{detection_table}_redacted"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Detection Results

# COMMAND ----------

detection_df = spark.table(detection_table)

print(f"Loaded {detection_df.count()} records from {detection_table}")
print(f"Columns: {detection_df.columns}")

# Verify required columns exist
if text_column not in detection_df.columns:
    raise ValueError(f"Text column '{text_column}' not found in table")
if entities_column not in detection_df.columns:
    raise ValueError(f"Entities column '{entities_column}' not found in table")

display(detection_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Redaction

# COMMAND ----------

redacted_df = create_redacted_table(
    spark=spark,
    source_df=detection_df,
    text_column=text_column,
    entities_column=entities_column,
    output_table=output_table,
    strategy=redaction_strategy,
    suffix=redacted_suffix,
)

print(f"Redacted table saved to: {output_table}")
print(f"Redaction strategy: {redaction_strategy}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Results

# COMMAND ----------

display(redacted_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare Before and After
# MAGIC
# MAGIC Show side-by-side comparison of original and redacted text.

# COMMAND ----------

redacted_col_name = f"{text_column}{redacted_suffix}"
comparison_df = redacted_df.select(text_column, redacted_col_name, entities_column)

display(comparison_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Redaction Statistics

# COMMAND ----------

stats_df = redacted_df.selectExpr(
    "COUNT(*) as total_documents",
    f"SUM(SIZE({entities_column})) as total_entities_redacted",
    f"AVG(SIZE({entities_column})) as avg_entities_per_doc",
    f"AVG(LENGTH({text_column})) as avg_original_length",
    f"AVG(LENGTH({redacted_col_name})) as avg_redacted_length",
)

display(stats_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Redactions
# MAGIC
# MAGIC View a few examples of redacted text.

# COMMAND ----------

print("=== Sample Redactions ===\n")

samples = redacted_df.select(text_column, redacted_col_name).limit(3).collect()

for i, row in enumerate(samples, 1):
    print(f"Example {i}:")
    print(f"Original: {row[text_column][:200]}...")
    print(f"Redacted: {row[redacted_col_name][:200]}...")
    print("-" * 80)
