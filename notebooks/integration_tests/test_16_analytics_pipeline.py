# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 16: Embeddings and Similarity Edges
# MAGIC
# MAGIC Validates embedding generation and similarity edge building on graph_nodes.
# MAGIC Requires test_12_knowledge_graph to have run first with skip_cleanup=true
# MAGIC (needs graph_nodes with data). This test cleans up the schema when done.
# MAGIC
# MAGIC Note: Clustering (cluster_analysis.py) is NOT tested here -- it requires
# MAGIC MLflow autolog management and is validated separately via the full pipeline job.

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
    similarity_threshold=0.3,
    max_edges_per_node=10,
)
builder = SimilarityEdgeBuilder(spark, sim_config)

# Check if there are enough non-entity nodes with embeddings for meaningful similarity
eligible_count = spark.sql(f"""
    SELECT COUNT(*) as cnt FROM {catalog_name}.{graph_test_schema}.graph_nodes
    WHERE embedding IS NOT NULL AND SIZE(embedding) > 0 AND node_type != 'entity'
""").first().cnt
print(f"  Non-entity nodes with embeddings: {eligible_count}")

sim_result = builder.run()
print(f"Similarity edges result: {sim_result}")

assert sim_result is not None, "SimilarityEdgeBuilder.run() should return a result"
if isinstance(sim_result, dict):
    edges_built = sim_result.get("edges_added", sim_result.get("edges_created", -1))
    print(f"  Edges built: {edges_built}")

sim_edge_count = spark.sql(
    f"SELECT COUNT(*) as cnt FROM {catalog_name}.{graph_test_schema}.graph_edges WHERE edge_type = 'similar_embedding'"
).first().cnt
print(f"  Similarity edges in graph_edges: {sim_edge_count}")

if eligible_count >= 2:
    assert sim_edge_count > 0, "Similarity builder should produce at least one similarity edge"
else:
    print(f"  [SKIP] Only {eligible_count} eligible nodes -- not enough for similarity assertion")

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
