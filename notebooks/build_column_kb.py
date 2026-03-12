# Databricks notebook source
# MAGIC %md
# MAGIC # Build Column Knowledge Base
# MAGIC 
# MAGIC Builds the column_knowledge_base table from metadata_generation_log,
# MAGIC aggregating column-level comments and PI classifications.

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

if not catalog_name or not schema_name:
    raise ValueError("Both catalog_name and schema_name are required")

print(f"Building column knowledge base in {catalog_name}.{schema_name}")

# COMMAND ----------

import sys
sys.path.append("../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.column_knowledge_base import build_column_knowledge_base

result = build_column_knowledge_base(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name
)

print(f"Column knowledge base build complete")
print(f"  Staged columns: {result['staged_count']}")
print(f"  Total records: {result['total_records']}")

# COMMAND ----------

# Show sample of column knowledge base
display(spark.sql(f"""
    SELECT column_id, table_name, column_name, comment, classification, data_type
    FROM {catalog_name}.{schema_name}.column_knowledge_base
    LIMIT 20
"""))

