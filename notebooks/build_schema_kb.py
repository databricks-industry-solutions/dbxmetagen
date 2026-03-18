# Databricks notebook source
# MAGIC %md
# MAGIC # Build Schema Knowledge Base
# MAGIC 
# MAGIC Builds the schema_knowledge_base table by aggregating table-level metadata
# MAGIC and generating AI summaries for each schema.

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("generate_comments", "true", "Generate AI Comments")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
generate_comments = dbutils.widgets.get("generate_comments").lower() == "true"

if not catalog_name or not schema_name:
    raise ValueError("Both catalog_name and schema_name are required")

print(f"Building schema knowledge base in {catalog_name}.{schema_name}")
print(f"Generate AI comments: {generate_comments}")

# COMMAND ----------

import sys
sys.path.append("../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.schema_knowledge_base import build_schema_knowledge_base

result = build_schema_knowledge_base(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    generate_comments=generate_comments
)

print(f"Schema knowledge base build complete")
print(f"  Staged schemas: {result['staged_count']}")
print(f"  Total records: {result['total_records']}")

# COMMAND ----------

# Show schema knowledge base
display(spark.sql(f"""
    SELECT schema_id, catalog, schema_name, domain, has_pii, has_phi, table_count, 
           SUBSTRING(comment, 1, 200) as comment_preview
    FROM {catalog_name}.{schema_name}.schema_knowledge_base
"""))

