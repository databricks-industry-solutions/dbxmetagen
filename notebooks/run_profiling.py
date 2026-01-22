# Databricks notebook source
# MAGIC %md
# MAGIC # Run Profiling
# MAGIC 
# MAGIC This notebook runs the profiling pipeline to capture table and column statistics
# MAGIC for data quality monitoring and anomaly detection.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("max_tables", "", "Max Tables (empty for all)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
max_tables_str = dbutils.widgets.get("max_tables")
max_tables = int(max_tables_str) if max_tables_str else None

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Max Tables: {max_tables}")

# COMMAND ----------

from src.dbxmetagen.profiling import run_profiling

result = run_profiling(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    max_tables=max_tables
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

