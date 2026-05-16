# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Extended Metadata
# MAGIC 
# MAGIC Extracts comprehensive metadata from system tables including lineage,
# MAGIC constraints, table properties, and governance information.

# COMMAND ----------

# MAGIC %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("incremental", "true", "Incremental (true/false)")
dbutils.widgets.text("table_names", "", "Table Names (comma-separated, empty=all)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
incremental = dbutils.widgets.get("incremental").lower() == "true"
table_names_raw = dbutils.widgets.get("table_names")

if not catalog_name or not schema_name:
    raise ValueError("Both catalog_name and schema_name are required")

print(f"Extracting extended metadata to {catalog_name}.{schema_name}")
print(f"Incremental: {incremental}")
if table_names_raw:
    print(f"Table filter: {table_names_raw}")

# COMMAND ----------

import sys
sys.path.append("../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.extended_metadata import extract_extended_metadata
from dbxmetagen.table_filter import parse_table_names

table_names = parse_table_names(table_names_raw) or None

result = extract_extended_metadata(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    incremental=incremental,
    table_names=table_names,
)

print(f"Extended metadata extraction complete")
print(f"  Staged tables: {result['staged_count']}")
print(f"  Total records: {result['total_records']}")

# COMMAND ----------

# Show extended metadata summary
display(spark.sql(f"""
    SELECT 
        table_name,
        table_type,
        table_owner,
        column_count,
        SIZE(upstream_tables) as upstream_count,
        SIZE(downstream_tables) as downstream_count,
        SIZE(primary_key_columns) as pk_columns,
        table_size_bytes,
        num_files
    FROM {catalog_name}.{schema_name}.extended_table_metadata
    ORDER BY table_size_bytes DESC NULLS LAST
    LIMIT 20
"""))

