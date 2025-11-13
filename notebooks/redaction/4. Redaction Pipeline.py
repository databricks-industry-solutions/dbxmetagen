# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End Redaction Pipeline
# MAGIC
# MAGIC This notebook provides a complete end-to-end pipeline for PII/PHI detection and redaction.
# MAGIC
# MAGIC **Input Modes:**
# MAGIC - **Table + Column**: Specify table name and text column directly
# MAGIC - **Table + Tag**: Query Unity Catalog for columns with specific classification tags
# MAGIC
# MAGIC **Process:**
# MAGIC 1. Load source data (by column name or tag)
# MAGIC 2. Run detection (Presidio, AI Query, or both)
# MAGIC 3. Align results (if using multiple methods)
# MAGIC 4. Apply redaction
# MAGIC 5. Save redacted table

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt
# MAGIC %pip install /Volumes/dbxmetagen/default/init_scripts/dbxmetagen-0.5.2-py3-none-any.whl
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from dbxmetagen.redaction import (
    run_redaction_pipeline,
    run_redaction_pipeline_by_tag,
    get_columns_by_tag,
)
from dbxmetagen.databricks_utils import get_dbr_version

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.dropdown(
    name="input_mode",
    defaultValue="table_column",
    choices=["table_column", "table_tag"],
    label="0. Input Mode",
)

dbutils.widgets.text(
    name="source_table",
    defaultValue="dbxmetagen.eval_data.jsl_48docs",
    label="1. Source Table (fully qualified)",
)

dbutils.widgets.text(
    name="text_column",
    defaultValue="text",
    label="2a. Text Column (for table_column mode)",
)

dbutils.widgets.text(
    name="tag_name",
    defaultValue="data_classification",
    label="2b. Tag Name (for table_tag mode)",
)

dbutils.widgets.text(
    name="tag_value",
    defaultValue="protected",
    label="2c. Tag Value (for table_tag mode)",
)

dbutils.widgets.text(
    name="doc_id_column", defaultValue="doc_id", label="3. Document ID Column"
)

dbutils.widgets.dropdown(
    name="use_presidio",
    defaultValue="true",
    choices=["true", "false"],
    label="4. Use Presidio Detection",
)

dbutils.widgets.dropdown(
    name="use_ai_query",
    defaultValue="true",
    choices=["true", "false"],
    label="5. Use AI Query Detection",
)

dbutils.widgets.dropdown(
    name="use_gliner",
    defaultValue="false",
    choices=["true", "false"],
    label="6. Use GLiNER Detection",
)

dbutils.widgets.dropdown(
    name="redaction_strategy",
    defaultValue="typed",
    choices=["generic", "typed"],
    label="7. Redaction Strategy",
)

dbutils.widgets.dropdown(
    name="endpoint",
    defaultValue="databricks-claude-sonnet-4",
    choices=sorted(
        [
            "databricks-claude-sonnet-4",
            "databricks-gpt-oss-120b",
            "databricks-gpt-oss-20b",
            "databricks-gemma-3-12b",
            "databricks-llama-4-maverick",
            "databricks-meta-llama-3-3-70b-instruct",
            "databricks-meta-llama-3-1-8b-instruct",
        ]
    ),
    label="8. AI Endpoint (for AI Query method)",
)

dbutils.widgets.text(
    name="presidio_score_threshold",
    defaultValue="0.5",
    label="9. Presidio Score Threshold",
)

dbutils.widgets.text(name="num_cores", defaultValue="10", label="10. Number of Cores")

dbutils.widgets.text(
    name="output_table",
    defaultValue="",
    label="11. Output Table (leave blank for auto-suffix)",
)

# Get widget values
input_mode = dbutils.widgets.get("input_mode")
source_table = dbutils.widgets.get("source_table")
text_column = dbutils.widgets.get("text_column")
tag_name = dbutils.widgets.get("tag_name")
tag_value = dbutils.widgets.get("tag_value")
doc_id_column = dbutils.widgets.get("doc_id_column")
use_presidio = dbutils.widgets.get("use_presidio") == "true"
use_ai_query = dbutils.widgets.get("use_ai_query") == "true"
use_gliner = dbutils.widgets.get("use_gliner") == "true"
redaction_strategy = dbutils.widgets.get("redaction_strategy")
endpoint = dbutils.widgets.get("endpoint")
score_threshold = float(dbutils.widgets.get("presidio_score_threshold"))
num_cores = int(dbutils.widgets.get("num_cores"))
output_table = dbutils.widgets.get("output_table")

# Auto-generate output table if not provided
if not output_table:
    output_table = f"{source_table}_redacted"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

if "client" not in get_dbr_version():
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Input Mode

# COMMAND ----------

if input_mode == "table_tag":
    print(f"Input Mode: Table + Tag")
    print(f"Searching for columns with {tag_name}='{tag_value}' in {source_table}")

    # Find columns with the specified tag
    protected_columns = get_columns_by_tag(
        spark=spark, table_name=source_table, tag_name=tag_name, tag_value=tag_value
    )

    if not protected_columns:
        raise ValueError(
            f"No columns found with {tag_name}='{tag_value}' in {source_table}"
        )

    print(f"Found {len(protected_columns)} protected column(s): {protected_columns}")
    text_column = protected_columns[0]
    print(f"Using first column for redaction: {text_column}")
else:
    print(f"Input Mode: Table + Column")
    print(f"Using specified column: {text_column}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Redaction Pipeline

# COMMAND ----------

# Build detection method description
detection_methods = []
if use_presidio:
    detection_methods.append("Presidio")
if use_ai_query:
    detection_methods.append("AI Query")
if use_gliner:
    detection_methods.append("GLiNER")
detection_method = " + ".join(detection_methods) if detection_methods else "None"

print("=" * 80)
print("STARTING REDACTION PIPELINE")
print("=" * 80)
print(f"Source Table: {source_table}")
print(f"Text Column: {text_column}")
print(f"Detection Method: {detection_method}")
print(f"Redaction Strategy: {redaction_strategy}")
print(f"Output Table: {output_table}")
print("=" * 80)

# COMMAND ----------

if input_mode == "table_tag":
    # Run pipeline with tag-based column identification
    result_df = run_redaction_pipeline_by_tag(
        spark=spark,
        source_table=source_table,
        output_table=output_table,
        tag_name=tag_name,
        tag_value=tag_value,
        doc_id_column=doc_id_column,
        use_presidio=use_presidio,
        use_ai_query=use_ai_query,
        use_gliner=use_gliner,
        redaction_strategy=redaction_strategy,
        endpoint=endpoint if use_ai_query else None,
        score_threshold=score_threshold,
        num_cores=num_cores,
    )
else:
    # Run pipeline with explicit column name
    result_df = run_redaction_pipeline(
        spark=spark,
        source_table=source_table,
        text_column=text_column,
        output_table=output_table,
        doc_id_column=doc_id_column,
        use_presidio=use_presidio,
        use_ai_query=use_ai_query,
        use_gliner=use_gliner,
        redaction_strategy=redaction_strategy,
        endpoint=endpoint if use_ai_query else None,
        score_threshold=score_threshold,
        num_cores=num_cores,
        use_aligned=True,
    )

# COMMAND ----------

print("=" * 80)
print("PIPELINE COMPLETE")
print("=" * 80)
print(f"Redacted table saved to: {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Results

# COMMAND ----------

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

redacted_col_name = f"{text_column}_redacted"

summary_df = result_df.selectExpr(
    "COUNT(*) as total_documents",
    f"AVG(LENGTH({text_column})) as avg_original_length",
    f"AVG(LENGTH({redacted_col_name})) as avg_redacted_length",
)

display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Comparisons

# COMMAND ----------

comparison_df = result_df.select(doc_id_column, text_column, redacted_col_name)

display(comparison_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entity Statistics
# MAGIC
# MAGIC Show entity detection counts by method.

# COMMAND ----------

if use_presidio and "presidio_results_struct" in result_df.columns:
    print("=== Presidio Detection Stats ===")
    presidio_stats = result_df.selectExpr(
        "COUNT(*) as total_docs",
        "SUM(SIZE(presidio_results_struct)) as total_entities",
        "AVG(SIZE(presidio_results_struct)) as avg_entities_per_doc",
    )
    display(presidio_stats)

# COMMAND ----------

if use_ai_query and "ai_results_struct" in result_df.columns:
    print("=== AI Query Detection Stats ===")
    ai_stats = result_df.selectExpr(
        "COUNT(*) as total_docs",
        "SUM(SIZE(ai_results_struct)) as total_entities",
        "AVG(SIZE(ai_results_struct)) as avg_entities_per_doc",
    )
    display(ai_stats)

# COMMAND ----------

if use_gliner and "gliner_results_struct" in result_df.columns:
    print("=== GLiNER Detection Stats ===")
    gliner_stats = result_df.selectExpr(
        "COUNT(*) as total_docs",
        "SUM(SIZE(gliner_results_struct)) as total_entities",
        "AVG(SIZE(gliner_results_struct)) as avg_entities_per_doc",
    )
    display(gliner_stats)

# COMMAND ----------

if "aligned_entities" in result_df.columns:
    print("=== Aligned Detection Stats ===")
    aligned_stats = result_df.selectExpr(
        "COUNT(*) as total_docs",
        "SUM(SIZE(aligned_entities)) as total_entities",
        "AVG(SIZE(aligned_entities)) as avg_entities_per_doc",
    )
    display(aligned_stats)
