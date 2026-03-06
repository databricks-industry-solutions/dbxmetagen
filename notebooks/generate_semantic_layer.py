# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Semantic Layer
# MAGIC
# MAGIC Reads pending business questions and generates metric view definitions
# MAGIC using AI_QUERY and existing catalog metadata (knowledge bases, FK predictions, ontology).

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("model_endpoint", "databricks-gpt-oss-120b", "Model Endpoint")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
model_endpoint = dbutils.widgets.get("model_endpoint")

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Model: {model_endpoint}")

# COMMAND ----------

import sys
sys.path.append("../")

from dbxmetagen.semantic_layer import SemanticLayerGenerator, SemanticLayerConfig

config = SemanticLayerConfig(
    catalog_name=catalog_name,
    schema_name=schema_name,
    model_endpoint=model_endpoint,
)
gen = SemanticLayerGenerator(spark, config)
gen.create_tables()

# COMMAND ----------

result = gen.generate_metric_views()
print(f"Generation complete:")
print(f"  Generated: {result['generated']}")
print(f"  Validated: {result['validated']}")
print(f"  Failed:    {result['failed']}")

# COMMAND ----------

display(spark.sql(f"""
    SELECT definition_id, metric_view_name, source_table, status, validation_errors, created_at
    FROM {catalog_name}.{schema_name}.metric_view_definitions
    ORDER BY created_at DESC
"""))
