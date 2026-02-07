# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 16: Full Analytics Pipeline
# MAGIC
# MAGIC Validates the full analytics pipeline: embeddings -> similarity -> clustering.
# MAGIC Requires test_12_knowledge_graph to have run first (needs graph_nodes with data).

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")

catalog_name = dbutils.widgets.get("catalog_name")
test_schema = dbutils.widgets.get("test_schema")

print(f"Testing analytics pipeline in {catalog_name}.{test_schema}")

# COMMAND ----------

import sys
sys.path.append("../../")  # For DAB deployment; pip-installed package works without this

# COMMAND ----------
# MAGIC %md
# MAGIC ## Check Prerequisites

# COMMAND ----------

# Verify graph_nodes exists and has data
nodes_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog_name}.{test_schema}.graph_nodes").first().cnt
assert nodes_count > 0, f"graph_nodes is empty -- run test_12 first"
print(f"graph_nodes has {nodes_count} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Generate Embeddings

# COMMAND ----------

from dbxmetagen.embeddings import EmbeddingConfig, EmbeddingGenerator

emb_config = EmbeddingConfig(
    catalog_name=catalog_name,
    schema_name=test_schema,
)
generator = EmbeddingGenerator(spark, emb_config)
result = generator.generate_all_embeddings()
print(f"Embeddings generated: {result}")

# Verify embeddings were written
emb_count = spark.sql(f"""
    SELECT COUNT(*) as cnt FROM {catalog_name}.{test_schema}.graph_nodes
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
    schema_name=test_schema,
    similarity_threshold=0.5,  # Lower threshold for test data
    max_edges_per_node=5,
)
builder = SimilarityEdgeBuilder(spark, sim_config)
edges_added = builder.build_edges()
print(f"Similarity edges added: {edges_added}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Verify Analytics Tables

# COMMAND ----------

# Check all expected tables exist
expected_tables = ["graph_nodes", "graph_edges"]
for table in expected_tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog_name}.{test_schema}.{table}").first().cnt
    print(f"{table}: {count} rows")
    assert count > 0, f"{table} should not be empty after analytics pipeline"

print("\nAll analytics pipeline tests passed!")
