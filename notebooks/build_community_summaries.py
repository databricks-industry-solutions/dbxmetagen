# Databricks notebook source
# MAGIC %md
# MAGIC # Build Community Summaries
# MAGIC
# MAGIC Generates LLM summaries for each domain/subdomain community in the metadata catalog.
# MAGIC These summaries are indexed in the Vector Search to answer broad questions without
# MAGIC traversing many individual table nodes.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("model", "databricks-claude-sonnet-4", "LLM Model")
dbutils.widgets.text("min_tables", "2", "Min Tables per Community")
dbutils.widgets.text("table_names", "", "Table Names (comma-separated, empty for all)")
dbutils.widgets.text("incremental", "true", "Incremental")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
model = dbutils.widgets.get("model")
min_tables = int(dbutils.widgets.get("min_tables"))

print(f"Catalog: {catalog_name}, Schema: {schema_name}, Model: {model}, Min tables: {min_tables}")

# COMMAND ----------

import sys
sys.path.append("../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.community_summaries import generate_summaries_batch
from dbxmetagen.table_filter import parse_table_names
table_names = parse_table_names(dbutils.widgets.get("table_names").strip()) or None

incremental = dbutils.widgets.get("incremental").strip().lower() in ("true", "1", "yes")

count = generate_summaries_batch(
    spark, catalog_name, schema_name,
    model=model, min_tables=min_tables,
    table_names=table_names,
    incremental=incremental,
)
print(f"Generated {count} community summaries")

# COMMAND ----------

if count > 0:
    df = spark.table(f"`{catalog_name}`.`{schema_name}`.community_summaries")
    display(df.select("community_id", "domain", "subdomain", "table_count", "summary"))
