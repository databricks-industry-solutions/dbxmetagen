# Databricks notebook source
# MAGIC %md
# MAGIC # Classify Geographic Columns
# MAGIC
# MAGIC Classifies columns as geographic (suitable for location-based filtering) or
# MAGIC non-geographic using keyword matching with LLM fallback.

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("table_names", "", "Table Names (comma-separated, optional)")
dbutils.widgets.text("apply_ddl", "false", "Apply DDL (tags)")
dbutils.widgets.text("tag_key", "geo_classification", "UC Tag Key")
dbutils.widgets.text(
    "config_path", "", "Geo Config Path (legacy, leave blank to use an ontology bundle)"
)

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
raw_tables = dbutils.widgets.get("table_names").strip()
table_names = [t.strip() for t in raw_tables.split(",") if t.strip()] or None
apply_ddl = dbutils.widgets.get("apply_ddl").lower() in ("true", "1", "yes")
tag_key = dbutils.widgets.get("tag_key").strip() or "geo_classification"
config_path = dbutils.widgets.get("config_path")

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Tables: {table_names or '(all)'}")
print(f"Apply DDL: {apply_ddl}")
print(f"Tag key: {tag_key}")

# COMMAND ----------

import sys

sys.path.append("../../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.geo_classifier import classify_columns_geo

result = classify_columns_geo(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    config_path=config_path,
    tag_key=tag_key,
    table_names=table_names,
    apply_ddl=apply_ddl,
)

print(f"Geo classification complete:")
print(f"  Columns classified: {result['classified']}")
print(f"  Geographic: {result['geographic']}")
print(f"  Non-geographic: {result['non_geographic']}")
if "tags_applied" in result:
    print(f"  Tags applied: {result['tags_applied']}")

