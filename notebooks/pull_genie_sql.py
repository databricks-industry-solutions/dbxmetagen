# Databricks notebook source
# MAGIC %md
# MAGIC # Pull Genie SQL Examples
# MAGIC
# MAGIC Extracts curated example SQL + benchmarks from EXISTING Genie spaces (via
# MAGIC the legacy `data-rooms` REST endpoints) into a CDF-enabled
# MAGIC `genie_sql_examples` Delta table, then provisions/syncs a
# MAGIC `genie_examples_vs_index` Delta Sync index on the shared `dbxmetagen-vs`
# MAGIC endpoint. Downstream, metric-view generation retrieves these exemplars to
# MAGIC seed richer definitions (backlog items 14/15).
# MAGIC
# MAGIC Scope is EXPLICIT: provide `space_ids` (comma-separated) and/or
# MAGIC `title_contains` (comma-separated case-insensitive substrings). The pull
# MAGIC refuses to run with no scope so junk/test spaces are never ingested.

# COMMAND ----------

# MAGIC # Uncomment below when running outside a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("endpoint_name", "dbxmetagen-vs", "VS Endpoint Name")
dbutils.widgets.text("space_ids", "", "Genie Space IDs (comma-separated)")
dbutils.widgets.text("title_contains", "", "Space title substrings (comma-separated)")
dbutils.widgets.text("include_sample_questions", "false", "Include SAMPLE_QUESTIONs (no SQL)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
endpoint_name = dbutils.widgets.get("endpoint_name")

if not catalog_name or not schema_name:
    raise ValueError("Both catalog_name and schema_name are required")

space_ids = [s.strip() for s in dbutils.widgets.get("space_ids").split(",") if s.strip()]
title_contains = [s.strip() for s in dbutils.widgets.get("title_contains").split(",") if s.strip()]
include_samples = dbutils.widgets.get("include_sample_questions").strip().lower() in ("true", "1", "yes")

if not space_ids and not title_contains:
    raise ValueError(
        "Provide an explicit scope: set space_ids and/or title_contains. "
        "The Genie SQL pull will not ingest every space by default."
    )

print(f"Pulling Genie SQL into {catalog_name}.{schema_name}.genie_sql_examples")
print(f"  space_ids: {space_ids or '(none)'}")
print(f"  title_contains: {title_contains or '(none)'}")

# COMMAND ----------

import sys
sys.path.append("../src")  # For git-clone / DAB deployment; pip-installed package works without this

from dbxmetagen.genie_sql_puller import GenieSQLPuller, GenieSQLPullerConfig

config = GenieSQLPullerConfig(
    catalog_name=catalog_name,
    schema_name=schema_name,
    endpoint_name=endpoint_name,
    space_ids=space_ids,
    title_contains=title_contains,
    include_sample_questions=include_samples,
)

result = GenieSQLPuller(spark=spark, config=config).run()

print("Genie SQL pull complete")
print(f"  Examples written: {result.get('examples_written')}")
print(f"  Endpoint:         {result.get('endpoint')}")
print(f"  Index:            {result.get('index')}")

# COMMAND ----------

df = spark.sql(f"""
    SELECT question_type, COUNT(*) AS cnt
    FROM {catalog_name}.{schema_name}.genie_sql_examples
    GROUP BY question_type ORDER BY cnt DESC
""")
display(df)
