# Databricks notebook source
# MAGIC %md
# MAGIC # dbxmetagen Analytics Pipeline
# MAGIC
# MAGIC > **Library usage:** This notebook demonstrates using dbxmetagen as a **pip-installable library**.
# MAGIC > It is intended for embedding dbxmetagen into your own projects or for ad-hoc use.
# MAGIC > For the full deployment with the interactive dashboard, pre-configured jobs, and app,
# MAGIC > see the main repo [README](https://github.com/databricks-industry-solutions/dbxmetagen) and `deploy.sh`.
# MAGIC
# MAGIC After running metadata generation (01_quickstart_metadata), this notebook builds
# MAGIC the analytics layer: knowledge base, knowledge graph, embeddings, ontology,
# MAGIC similarity edges, and data quality scores.
# MAGIC
# MAGIC Run steps sequentially -- each depends on outputs from the previous step.

# COMMAND ----------
# MAGIC %pip install -qqq git+https://github.com/databricks-industry-solutions/dbxmetagen.git@main
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog_name", "", "Catalog Name (required)")
dbutils.widgets.text("schema_name", "metadata_results", "Output Schema")

# COMMAND ----------
from pyspark.sql import SparkSession
from dbxmetagen import (
    build_knowledge_base,
    build_column_knowledge_base,
    build_schema_knowledge_base,
    extract_extended_metadata,
    build_knowledge_graph,
    generate_embeddings,
    build_ontology,
    build_similarity_edges,
    compute_data_quality,
    run_profiling,
)

spark = SparkSession.builder.getOrCreate()
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 1: Knowledge Base

# COMMAND ----------
build_knowledge_base(spark, catalog, schema)
build_column_knowledge_base(spark, catalog, schema)
build_schema_knowledge_base(spark, catalog, schema)
extract_extended_metadata(spark, catalog, schema)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 2: Knowledge Graph

# COMMAND ----------
build_knowledge_graph(spark, catalog, schema)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 3: Embeddings, Profiling, Ontology

# COMMAND ----------
generate_embeddings(spark, catalog, schema)
run_profiling(spark, catalog, schema)
build_ontology(spark, catalog, schema)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 4: Similarity & Quality

# COMMAND ----------
build_similarity_edges(spark, catalog, schema)
compute_data_quality(spark, catalog, schema)
