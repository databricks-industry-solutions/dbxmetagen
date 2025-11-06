# Databricks notebook source
# MAGIC %md
# MAGIC # PII/PHI Detection Benchmarking
# MAGIC
# MAGIC This notebook performs PHI/PII detection on a dataset using configurable detection methods.
# MAGIC
# MAGIC **Detection Methods:**
# MAGIC - **Presidio**: Rule-based and NLP-based detection using Microsoft Presidio
# MAGIC - **AI Query**: LLM-based detection using Databricks AI endpoints
# MAGIC - **All**: Runs both methods and aligns results
# MAGIC
# MAGIC **Outputs:**
# MAGIC - Detection results with entity positions and types
# MAGIC - Aligned entities (when using multiple methods)

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt gliner
# MAGIC %pip install /Volumes/dbxmetagen/default/init_scripts/dbxmetagen-0.5.1-py3-none-any.whl
# MAGIC %restart_python

# COMMAND ----------

# Development: Add repo to path for importing modules
# For production with installed package, comment out the sys.path line
import sys

sys.path.append('../../src')

# COMMAND ----------

import json
from typing import Iterator, Tuple
import pandas as pd
from pyspark.sql.functions import pandas_udf, col
from presidio_analyzer import BatchAnalyzerEngine

# Import helper functions into notebook scope - these will be captured in UDF closures
from dbxmetagen.deterministic_pi import get_analyzer_engine
from dbxmetagen.redaction.config import DEFAULT_PRESIDIO_SCORE_THRESHOLD
from dbxmetagen.redaction.presidio import format_presidio_batch_results
from dbxmetagen.redaction import run_detection_pipeline
from dbxmetagen.databricks_utils import get_dbr_version

# COMMAND ----------

# MAGIC %md
# MAGIC ## Presidio UDF Definition
# MAGIC
# MAGIC Define Presidio UDF inline in notebook to capture helper functions in closure.
# MAGIC This avoids worker import issues - the UDF is serialized with all dependencies.

# COMMAND ----------


def make_presidio_batch_udf(
    score_threshold: float = DEFAULT_PRESIDIO_SCORE_THRESHOLD, add_pci: bool = False
):
    """
    Create a Pandas UDF for batch PHI detection using Presidio.

    Defined inline in notebook to capture helper functions (get_analyzer_engine,
    format_presidio_batch_results) in the closure, avoiding worker import issues.

    Args:
        score_threshold: Minimum confidence score to include results (0.0-1.0)
        add_pci: Whether to add PCI (Payment Card Industry) recognizers

    Returns:
        A Pandas UDF that takes (doc_ids, texts) and returns JSON-serialized results
    """

    @pandas_udf("string")
    def analyze_udf(
        batch_iter: Iterator[Tuple[pd.Series, pd.Series]],
    ) -> Iterator[pd.Series]:
        # Captures get_analyzer_engine from notebook scope
        analyzer = get_analyzer_engine(
            add_pci=add_pci, default_score_threshold=score_threshold
        )
        batch_analyzer = BatchAnalyzerEngine(analyzer_engine=analyzer)

        for doc_ids, texts in batch_iter:
            text_dict = pd.DataFrame({"doc_id": doc_ids, "text": texts}).to_dict(
                orient="list"
            )

            results = batch_analyzer.analyze_dict(
                text_dict,
                language="en",
                keys_to_skip=["doc_id"],
                score_threshold=score_threshold,
                batch_size=20,
                n_process=1,
            )

            # Captures format_presidio_batch_results from notebook scope
            output = format_presidio_batch_results(
                results, score_threshold=score_threshold
            )
            yield pd.Series(output)

    return analyze_udf


# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

dbutils.widgets.text(
    name="source_table",
    defaultValue="dbxmetagen.eval_data.jsl_48docs_deduped",
    label="0. Source Table",
)

dbutils.widgets.text(
    name="doc_id_column", defaultValue="doc_id", label="1. Document ID Column"
)

dbutils.widgets.text(name="text_column", defaultValue="text", label="2. Text Column")

dbutils.widgets.dropdown(
    name="use_presidio",
    defaultValue="true",
    choices=["true", "false"],
    label="3. Use Presidio Detection",
)

dbutils.widgets.dropdown(
    name="use_ai_query",
    defaultValue="true",
    choices=["true", "false"],
    label="4. Use AI Query Detection",
)

dbutils.widgets.dropdown(
    name="use_gliner",
    defaultValue="false",
    choices=["true", "false"],
    label="5. Use GLiNER Detection",
)

dbutils.widgets.dropdown(
    name="endpoint",
    defaultValue="databricks-gpt-oss-120b",
    choices=sorted(
        [
            "databricks-claude-sonnet-4",
            "databricks-gpt-oss-120b",
        ]
    ),
    label="6. AI Endpoint (for AI Query method)",
)

dbutils.widgets.text(
    name="presidio_score_threshold",
    defaultValue="0.5",
    label="5. Presidio Score Threshold",
)

dbutils.widgets.text(name="num_cores", defaultValue="10", label="6. Number of Cores")

dbutils.widgets.text(
    name="output_table",
    defaultValue="",
    label="7. Output Table (leave blank for auto-suffix)",
)

# Get widget values
source_table = dbutils.widgets.get("source_table")
doc_id_column = dbutils.widgets.get("doc_id_column")
text_column = dbutils.widgets.get("text_column")
use_presidio = dbutils.widgets.get("use_presidio") == "true"
use_ai_query = dbutils.widgets.get("use_ai_query") == "true"
use_gliner = dbutils.widgets.get("use_gliner") == "true"
endpoint = dbutils.widgets.get("endpoint")
score_threshold = float(dbutils.widgets.get("presidio_score_threshold"))
num_cores = int(dbutils.widgets.get("num_cores"))
output_table = dbutils.widgets.get("output_table")

# Auto-generate output table name if not provided
if not output_table:
    output_table = f"{source_table}_detection_results"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

if "client" not in get_dbr_version():
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Data

# COMMAND ----------

source_df = spark.table(source_table).select(doc_id_column, col(text_column))

source_df_count = source_df.count()

if source_df_count > 100:
    raise ValueError(
        "Source table has more than 100 documents. Please use a smaller table or increase the limit for evaluation."
    )

print(f"Loaded {source_df_count} documents from {source_table}")

display(source_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Detection
# MAGIC
# MAGIC Applies Presidio UDF defined above (uses closure capture for worker compatibility).
# MAGIC For AI Query and GLiNER, uses pipeline functions.

# COMMAND ----------

from pyspark.sql.functions import from_json

# Start with source data
results_df = source_df

# Apply Presidio detection if enabled (using locally-defined UDF)
if use_presidio:
    print("Running Presidio detection with locally-defined UDF...")
    presidio_udf = make_presidio_batch_udf(score_threshold=score_threshold)

    results_df = (
        results_df.repartition(num_cores)
        .withColumn(
            "presidio_results", presidio_udf(col(doc_id_column), col(text_column))
        )
        .withColumn(
            "presidio_results_struct",
            from_json(
                "presidio_results",
                "array<struct<entity:string, score:double, start:integer, end:integer, doc_id:string>>",
            ),
        )
    )

# For other detection methods, use pipeline (they don't have serialization issues)
if use_ai_query or use_gliner:
    print("Running additional detection methods via pipeline...")
    results_df = run_detection_pipeline(
        spark=spark,
        source_df=results_df,  # Pass results with Presidio if already applied
        doc_id_column=doc_id_column,
        text_column=text_column,
        use_presidio=False,  # Already applied above
        use_ai_query=use_ai_query,
        use_gliner=use_gliner,
        endpoint=endpoint if use_ai_query else None,
        score_threshold=score_threshold,
        num_cores=num_cores,
        align_results=True,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

results_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    output_table
)
print(f"Results saved to table: {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Results

# COMMAND ----------

results_df = spark.read.table(output_table)
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

if use_presidio and "presidio_results_struct" in results_df.columns:
    print("=== Presidio Results ===")
    presidio_summary = results_df.selectExpr(
        f"{doc_id_column}", "SIZE(presidio_results_struct) as entity_count"
    )
    display(presidio_summary)

# COMMAND ----------

if use_ai_query and "ai_results_struct" in results_df.columns:
    print("=== AI Query Results ===")
    ai_summary = results_df.selectExpr(
        f"{doc_id_column}", "SIZE(ai_results_struct) as entity_count"
    )
    display(ai_summary)


# COMMAND ----------

if use_gliner and "gliner_results_struct" in results_df.columns:
    print("=== GLiNER Results ===")
    gliner_summary = results_df.selectExpr(
        f"{doc_id_column}", "SIZE(gliner_results_struct) as entity_count"
    )
    display(gliner_summary)

# COMMAND ----------

if "aligned_entities" in results_df.columns:
    print("=== Aligned Results ===")
    aligned_summary = results_df.selectExpr(
        f"{doc_id_column}", "SIZE(aligned_entities) as entity_count"
    )
    display(aligned_summary)

# COMMAND ----------



