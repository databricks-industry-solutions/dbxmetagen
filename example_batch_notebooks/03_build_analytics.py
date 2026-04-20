# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3: Build Analytics Layer
# MAGIC
# MAGIC Builds the full analytics pipeline on top of the knowledge bases:
# MAGIC knowledge graph, ontology, embeddings, profiling, similarity edges,
# MAGIC FK prediction, data quality, and ontology validation.
# MAGIC
# MAGIC This is where dbxmetagen goes beyond descriptions -- it discovers relationships
# MAGIC between tables (FK prediction), classifies columns into ontology entities,
# MAGIC and scores data quality. All of this feeds into Genie space generation.
# MAGIC
# MAGIC **Prerequisites:** Run `02_build_knowledge_bases` first.

# COMMAND ----------

# MAGIC %pip install -qqq https://github.com/databricks-industry-solutions/dbxmetagen/archive/refs/heads/main.zip
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./helpers/common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Knowledge Graph
# MAGIC
# MAGIC Creates `knowledge_graph_nodes` and `knowledge_graph_edges` tables from the
# MAGIC knowledge bases. Nodes are tables/columns; edges are relationships like
# MAGIC "table has column", "column references column", etc.

# COMMAND ----------

from dbxmetagen import build_knowledge_graph

build_knowledge_graph(spark, CATALOG, SCHEMA)
print("Knowledge graph built")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ontology
# MAGIC
# MAGIC Discovers entity types and classifies columns into ontology roles using the
# MAGIC selected ontology bundle (general, healthcare, financial_services, retail_cpg).
# MAGIC This creates `ontology_entities` and `ontology_relations`.

# COMMAND ----------

from dbxmetagen import build_ontology
from dbxmetagen.ontology import resolve_bundle_path

config_path = resolve_bundle_path(ONTOLOGY_BUNDLE)
build_ontology(spark, CATALOG, SCHEMA, config_path=config_path, model_endpoint=MODEL)
print("Ontology built")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Embeddings
# MAGIC
# MAGIC Generates vector embeddings for tables and columns using their AI descriptions.
# MAGIC Used downstream for similarity-based FK prediction and clustering.

# COMMAND ----------

from dbxmetagen import generate_embeddings

generate_embeddings(spark, CATALOG, SCHEMA)
print("Embeddings generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profiling
# MAGIC
# MAGIC Computes column-level statistics (null rate, cardinality, min/max, patterns)
# MAGIC for data quality scoring.

# COMMAND ----------

from dbxmetagen import run_profiling

run_profiling(spark, CATALOG, SCHEMA)
print("Profiling complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Similarity Edges & FK Prediction
# MAGIC
# MAGIC Finds semantically similar columns using embeddings, then uses those candidates
# MAGIC plus join validation and AI judgment to predict foreign key relationships.
# MAGIC Predicted FKs are critical for Genie -- they tell the space how tables join.

# COMMAND ----------

from dbxmetagen import build_similarity_edges, predict_foreign_keys

build_similarity_edges(spark, CATALOG, SCHEMA)
print("Similarity edges built")

predict_foreign_keys(spark, CATALOG, SCHEMA)
show_table("fk_predictions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality & Ontology Validation

# COMMAND ----------

from dbxmetagen import compute_data_quality, validate_ontology

compute_data_quality(spark, CATALOG, SCHEMA)
print("Data quality scores computed")

validate_ontology(spark, CATALOG, SCHEMA)
print("Ontology validation complete")
