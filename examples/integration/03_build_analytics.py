# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3: Build Analytics Layer
# MAGIC
# MAGIC Builds the full analytics pipeline on top of knowledge bases:
# MAGIC knowledge graph, ontology, embeddings, profiling, similarity edges,
# MAGIC FK prediction, data quality scores, and ontology validation.
# MAGIC
# MAGIC All steps run on serverless compute (no graphframes needed).
# MAGIC
# MAGIC **Prerequisites:** Run `02_build_knowledge_bases` first.

# COMMAND ----------

# MAGIC %pip install -qqq git+https://github.com/databricks-industry-solutions/dbxmetagen.git@main

# COMMAND ----------

import os
from pyspark.sql import SparkSession

dbutils.widgets.text("catalog_name", os.getenv("CATALOG_NAME", ""), "Catalog Name (required)")
dbutils.widgets.text("schema_name", os.getenv("SCHEMA_NAME", "default"), "Output Schema")
dbutils.widgets.text("ontology_bundle", os.getenv("METAGEN_ONTOLOGY_BUNDLE", "general"), "Ontology Bundle")
dbutils.widgets.text("model_endpoint", os.getenv("METAGEN_MODEL_ENDPOINT", "databricks-claude-sonnet-4-6"), "Model Endpoint")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
ontology_bundle = dbutils.widgets.get("ontology_bundle")
model_endpoint = dbutils.widgets.get("model_endpoint")

spark = SparkSession.builder.getOrCreate()

print(f"Building analytics layer in {catalog_name}.{schema_name}")
print(f"Ontology bundle: {ontology_bundle}")

# COMMAND ----------

# DBTITLE 1,Build Knowledge Graph
from dbxmetagen import build_knowledge_graph

build_knowledge_graph(spark, catalog_name, schema_name)
print("Knowledge graph built")

# COMMAND ----------

# DBTITLE 1,Build Ontology
from dbxmetagen import build_ontology
from dbxmetagen.ontology import resolve_bundle_path

config_path = resolve_bundle_path(ontology_bundle)
if not os.path.exists(config_path):
    nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    bundle_root = "/Workspace" + str(nb_path).rsplit("/", 3)[0]
    config_path = f"{bundle_root}/configurations/ontology_bundles/{ontology_bundle}.yaml"
    print(f"Using workspace-resolved config: {config_path}")

build_ontology(spark, catalog_name, schema_name, config_path=config_path, model_endpoint=model_endpoint)
print("Ontology built")

# COMMAND ----------

# DBTITLE 1,Generate Embeddings
from dbxmetagen import generate_embeddings

generate_embeddings(spark, catalog_name, schema_name)
print("Embeddings generated")

# COMMAND ----------

# DBTITLE 1,Run Profiling
from dbxmetagen import run_profiling

run_profiling(spark, catalog_name, schema_name)
print("Profiling complete")

# COMMAND ----------

# DBTITLE 1,Build Similarity Edges and Predict Foreign Keys
from dbxmetagen import build_similarity_edges, predict_foreign_keys

build_similarity_edges(spark, catalog_name, schema_name)
print("Similarity edges built")

predict_foreign_keys(spark, catalog_name, schema_name)
fk_count = spark.table(f"{catalog_name}.{schema_name}.fk_predictions").count()
print(f"FK predictions: {fk_count} rows")

# COMMAND ----------

# DBTITLE 1,Compute Data Quality Scores
from dbxmetagen import compute_data_quality

compute_data_quality(spark, catalog_name, schema_name)
print("Data quality scores computed")

# COMMAND ----------

# DBTITLE 1,Validate Ontology
from dbxmetagen import validate_ontology

validate_ontology(spark, catalog_name, schema_name)
print("Ontology validation complete")
