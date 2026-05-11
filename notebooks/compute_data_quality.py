# Databricks notebook source
# MAGIC %md
# MAGIC # Compute Data Quality Scores
# MAGIC 
# MAGIC This notebook computes data quality scores based on profiling snapshots.

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("table_names", "", "Table Names (comma-separated, empty for all)")
dbutils.widgets.text("incremental", "true", "Incremental")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")

# COMMAND ----------

import sys
sys.path.append("../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.data_quality import compute_data_quality
from dbxmetagen.table_filter import parse_table_names
table_names = parse_table_names(dbutils.widgets.get("table_names").strip()) or None

incremental = dbutils.widgets.get("incremental").strip().lower() in ("true", "1", "yes")

result = compute_data_quality(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    table_names=table_names,
    incremental=incremental,
)

print(f"Quality scoring complete:")
print(f"  Tables scored: {result['tables_scored']}")
print(f"  Average score: {result['average_score']:.1f}")
print(f"  Low quality tables: {result['low_quality_tables']}")

# COMMAND ----------

# Show quality scores summary
display(spark.sql(f"""
    SELECT 
        table_name,
        overall_score,
        completeness_score,
        uniqueness_score,
        freshness_score,
        consistency_score,
        SIZE(quality_issues) as issue_count
    FROM {catalog_name}.{schema_name}.data_quality_scores
    ORDER BY overall_score ASC
    LIMIT 20
"""))

