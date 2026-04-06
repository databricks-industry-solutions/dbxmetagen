# Databricks notebook source
# MAGIC %md
# MAGIC # dbxmetagen Quickstart
# MAGIC
# MAGIC > **Library usage:** This notebook demonstrates using dbxmetagen as a **pip-installable library**.
# MAGIC > It is intended for embedding dbxmetagen into your own projects or for ad-hoc use.
# MAGIC > For the full deployment with the interactive dashboard, pre-configured jobs, and app,
# MAGIC > see the main repo [README](https://github.com/databricks-industry-solutions/dbxmetagen) and `deploy.sh`.
# MAGIC
# MAGIC Generate table/column descriptions, classify sensitive information (PII/PHI/PCI),
# MAGIC or classify tables into business domains -- all with a single function call.
# MAGIC
# MAGIC **Prerequisites:** A Databricks workspace with Unity Catalog enabled and
# MAGIC access to a foundation model endpoint (e.g. `databricks-claude-sonnet-4-6`).

# COMMAND ----------
# MAGIC %pip install -qqq git+https://github.com/databricks-industry-solutions/dbxmetagen.git@main
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration
# MAGIC Fill in the widgets above after running this cell.

# COMMAND ----------
dbutils.widgets.text("catalog_name", "", "Catalog Name (required)")
dbutils.widgets.text("table_names", "", "Table Names - comma separated (required)")
dbutils.widgets.dropdown("mode", "comment", ["comment", "pi", "domain"], "Mode")
dbutils.widgets.text("schema_name", "metadata_results", "Output Schema")
dbutils.widgets.dropdown("apply_ddl", "false", ["true", "false"], "Apply DDL")
dbutils.widgets.text("model", "databricks-claude-sonnet-4-6", "Model Endpoint")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run

# COMMAND ----------
from dbxmetagen.main import main

main({
    "catalog_name": dbutils.widgets.get("catalog_name"),
    "schema_name": dbutils.widgets.get("schema_name"),
    "table_names": dbutils.widgets.get("table_names"),
    "mode": dbutils.widgets.get("mode"),
    "apply_ddl": dbutils.widgets.get("apply_ddl"),
    "model": dbutils.widgets.get("model"),
    "table_names_source": "parameter",
})

# COMMAND ----------
# MAGIC %md
# MAGIC ## (Optional) Build Analytics Layer
# MAGIC
# MAGIC After generating metadata, you can build the knowledge base, knowledge graph,
# MAGIC embeddings, ontology, and data quality scores. Uncomment and run the cells below.

# COMMAND ----------
# from pyspark.sql import SparkSession
# from dbxmetagen import (
#     build_knowledge_base,
#     build_column_knowledge_base,
#     build_schema_knowledge_base,
#     extract_extended_metadata,
#     build_knowledge_graph,
#     generate_embeddings,
#     build_ontology,
#     build_similarity_edges,
#     compute_data_quality,
#     run_profiling,
# )
#
# spark = SparkSession.builder.getOrCreate()
# catalog = dbutils.widgets.get("catalog_name")
# schema = dbutils.widgets.get("schema_name")
#
# # Stage 1: Knowledge base
# build_knowledge_base(spark, catalog, schema)
# build_column_knowledge_base(spark, catalog, schema)
# build_schema_knowledge_base(spark, catalog, schema)
# extract_extended_metadata(spark, catalog, schema)
#
# # Stage 2: Knowledge graph
# build_knowledge_graph(spark, catalog, schema)
#
# # Stage 3: Embeddings, profiling, ontology
# generate_embeddings(spark, catalog, schema)
# run_profiling(spark, catalog, schema)
# build_ontology(spark, catalog, schema)
#
# # Stage 4: Similarity & quality
# build_similarity_edges(spark, catalog, schema)
# compute_data_quality(spark, catalog, schema)
