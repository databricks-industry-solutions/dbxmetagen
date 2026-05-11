# Databricks notebook source
# MAGIC %md
# MAGIC # Build Vector Search Index
# MAGIC
# MAGIC Consolidates table, column, entity, and metric-view metadata into a
# MAGIC `metadata_documents` Delta table, then provisions a Databricks Vector
# MAGIC Search endpoint + Delta Sync index with managed embeddings.

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("endpoint_name", "dbxmetagen-vs", "VS Endpoint Name")
dbutils.widgets.text("sweep_stale_docs", "false", "Sweep Stale Docs")
dbutils.widgets.text("table_names", "", "Table Names (comma-separated, empty for all)")
dbutils.widgets.text("incremental", "true", "Incremental")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
endpoint_name = dbutils.widgets.get("endpoint_name")
sweep_stale_docs = dbutils.widgets.get("sweep_stale_docs").strip().lower() == "true"

if not catalog_name or not schema_name:
    raise ValueError("Both catalog_name and schema_name are required")

print(f"Building vector index for {catalog_name}.{schema_name}")
print(f"VS endpoint: {endpoint_name}")
print(f"Sweep stale docs: {sweep_stale_docs}")

# COMMAND ----------

import sys
sys.path.append("../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.vector_index import build_vector_index
from dbxmetagen.table_filter import parse_table_names
table_names = parse_table_names(dbutils.widgets.get("table_names").strip()) or None

incremental = dbutils.widgets.get("incremental").strip().lower() in ("true", "1", "yes")

result = build_vector_index(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    endpoint_name=endpoint_name,
    sweep_stale_docs=sweep_stale_docs,
    table_names=table_names,
    incremental=incremental,
)

print(f"Vector index build complete")
print(f"  Documents: {result['documents']}")
print(f"  Endpoint:  {result['endpoint']}")
print(f"  Index:     {result['index']}")

# COMMAND ----------

df = spark.sql(f"""
    SELECT doc_type, COUNT(*) AS cnt
    FROM {catalog_name}.{schema_name}.metadata_documents
    GROUP BY doc_type ORDER BY cnt DESC
""")
display(df)
