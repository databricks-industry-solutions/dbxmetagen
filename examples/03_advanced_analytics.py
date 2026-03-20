# Databricks notebook source
# MAGIC %md
# MAGIC # dbxmetagen Advanced Analytics
# MAGIC
# MAGIC Optional post-pipeline analytics: foreign key prediction, cluster analysis,
# MAGIC and ontology validation. Run after 02_analytics_pipeline.

# COMMAND ----------
# MAGIC %pip install -qqq git+https://github.com/databricks-industry-solutions/dbxmetagen.git@main
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog_name", "", "Catalog Name (required)")
dbutils.widgets.text("schema_name", "metadata_results", "Output Schema")
dbutils.widgets.dropdown("fk_dry_run", "true", ["true", "false"], "FK Dry Run")

# COMMAND ----------
from pyspark.sql import SparkSession
from dbxmetagen import predict_foreign_keys, validate_ontology

spark = SparkSession.builder.getOrCreate()
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
dry_run = dbutils.widgets.get("fk_dry_run").lower() == "true"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Foreign Key Prediction
# MAGIC Uses column similarity + join validation + AI judgment to predict FK relationships.
# MAGIC Set `FK Dry Run = true` to preview without calling the LLM.

# COMMAND ----------
predict_foreign_keys(spark, catalog, schema, dry_run=dry_run)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ontology Validation

# COMMAND ----------
validate_ontology(spark, catalog, schema)
