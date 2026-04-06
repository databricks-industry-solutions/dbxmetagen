# Databricks notebook source
# MAGIC %md
# MAGIC # Run Profiling
# MAGIC 
# MAGIC This notebook runs the profiling pipeline to capture table and column statistics
# MAGIC for data quality monitoring and anomaly detection.

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("max_tables", "", "Max Tables (empty for all)")
dbutils.widgets.text("incremental", "true", "Incremental (true/false)")
dbutils.widgets.text("table_names", "", "Table Names (comma-separated, empty=all)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
max_tables_str = dbutils.widgets.get("max_tables")
max_tables = int(max_tables_str) if max_tables_str else None
incremental = dbutils.widgets.get("incremental").lower() == "true"
table_names_raw = dbutils.widgets.get("table_names")

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Max Tables: {max_tables}")
print(f"Incremental: {incremental}")
if table_names_raw:
    print(f"Table filter: {table_names_raw}")

# COMMAND ----------

import sys
sys.path.append("../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.profiling import run_profiling
from dbxmetagen.table_filter import parse_table_names

table_names = parse_table_names(table_names_raw) or None

result = run_profiling(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    max_tables=max_tables,
    incremental=incremental,
    table_names=table_names,
)

print(f"Profiling complete:")
print(f"  Tables profiled: {result['tables_profiled']}")
print(f"  Tables failed: {result['tables_failed']}")
print(f"  Total tables: {result['total_tables']}")

# COMMAND ----------

# Show recent snapshots
display(spark.sql(f"""
    SELECT table_name, snapshot_time, row_count, table_size_bytes
    FROM {catalog_name}.{schema_name}.profiling_snapshots
    ORDER BY snapshot_time DESC
    LIMIT 20
"""))

