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

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
model = dbutils.widgets.get("model")
min_tables = int(dbutils.widgets.get("min_tables"))

print(f"Catalog: {catalog_name}, Schema: {schema_name}, Model: {model}, Min tables: {min_tables}")

# COMMAND ----------

from dbxmetagen.community_summaries import generate_summaries_batch

count = generate_summaries_batch(
    spark, catalog_name, schema_name,
    model=model, min_tables=min_tables,
)
print(f"Generated {count} community summaries")

# COMMAND ----------

if count > 0:
    df = spark.table(f"`{catalog_name}`.`{schema_name}`.community_summaries")
    display(df.select("community_id", "domain", "subdomain", "table_count", "summary"))
