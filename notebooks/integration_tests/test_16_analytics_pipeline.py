# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 16: Full Analytics Pipeline
# MAGIC
# MAGIC Validates the full analytics pipeline: embeddings -> similarity -> clustering.
# MAGIC Requires test_12_knowledge_graph to have run first with skip_cleanup=true
# MAGIC (needs graph_nodes with data). This test cleans up the schema when done.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "dev_integration_tests", "Catalog Name")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")
dbutils.widgets.text("graph_test_schema", "", "Graph Test Schema (from test_12)")

catalog_name = dbutils.widgets.get("catalog_name")
test_schema = dbutils.widgets.get("test_schema")
graph_test_schema = dbutils.widgets.get("graph_test_schema").strip()

if not graph_test_schema:
    raise ValueError(
        "graph_test_schema widget is required -- this test must run after "
        "test_12_knowledge_graph with skip_cleanup=true"
    )

print(f"Testing analytics pipeline in {catalog_name}.{graph_test_schema}")

# COMMAND ----------

import sys
sys.path.append("../../src")  # For git-clone or DAB deployment; pip-installed package works without this

# COMMAND ----------
# MAGIC %md
# MAGIC ## Check Prerequisites

# COMMAND ----------

nodes_count = spark.sql(
    f"SELECT COUNT(*) as cnt FROM {catalog_name}.{graph_test_schema}.graph_nodes"
).first().cnt
assert nodes_count > 0, f"graph_nodes is empty -- run test_12 first with skip_cleanup=true"
print(f"graph_nodes has {nodes_count} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Generate Embeddings

# COMMAND ----------

from dbxmetagen.embeddings import EmbeddingConfig, EmbeddingGenerator

emb_config = EmbeddingConfig(
    catalog_name=catalog_name,
    schema_name=graph_test_schema,
)
generator = EmbeddingGenerator(spark, emb_config)
result = generator.run()
print(f"Embeddings generated: {result}")

emb_count = spark.sql(f"""
    SELECT COUNT(*) as cnt FROM {catalog_name}.{graph_test_schema}.graph_nodes
    WHERE embedding IS NOT NULL AND SIZE(embedding) > 0
""").first().cnt
assert emb_count > 0, "No embeddings generated"
print(f"Nodes with embeddings: {emb_count}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Build Similarity Edges

# COMMAND ----------

from dbxmetagen.similarity_edges import SimilarityEdgesConfig, SimilarityEdgeBuilder

sim_config = SimilarityEdgesConfig(
    catalog_name=catalog_name,
    schema_name=graph_test_schema,
    similarity_threshold=0.5,
    max_edges_per_node=5,
)
builder = SimilarityEdgeBuilder(spark, sim_config)
sim_result = builder.run()
print(f"Similarity edges result: {sim_result}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Verify Analytics Tables

# COMMAND ----------

expected_tables = ["graph_nodes", "graph_edges"]
for table in expected_tables:
    count = spark.sql(
        f"SELECT COUNT(*) as cnt FROM {catalog_name}.{graph_test_schema}.{table}"
    ).first().cnt
    print(f"{table}: {count} rows")
    assert count > 0, f"{table} should not be empty after analytics pipeline"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{graph_test_schema} CASCADE")
print(f"[CLEANUP] Dropped test schema: {catalog_name}.{graph_test_schema}")

# COMMAND ----------

print("=" * 60)
print("ALL ANALYTICS PIPELINE INTEGRATION TESTS PASSED")
print("=" * 60)
