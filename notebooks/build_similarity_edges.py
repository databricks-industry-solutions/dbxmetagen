# Databricks notebook source
# MAGIC %md
# MAGIC # Build Similarity Edges
# MAGIC 
# MAGIC Creates 'similar_embedding' edges between nodes with high cosine similarity,
# MAGIC enabling discovery of semantically related tables, columns, and schemas.
# MAGIC 
# MAGIC Supports two modes:
# MAGIC - **Cross-join** (default): Exhaustive pairwise cosine with 3-tier blocking.
# MAGIC - **ANN** (`use_ann_similarity=true`): Vector Search index + parallel per-node queries.

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("similarity_threshold", "0.8", "Similarity Threshold")
dbutils.widgets.text("max_edges_per_node", "10", "Max Edges Per Node")
dbutils.widgets.text("use_ann_similarity", "true", "Use ANN (Vector Search)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
similarity_threshold = float(dbutils.widgets.get("similarity_threshold"))
max_edges_per_node = int(dbutils.widgets.get("max_edges_per_node"))
use_ann = dbutils.widgets.get("use_ann_similarity").strip().lower() in ("true", "1", "yes")

if not catalog_name or not schema_name:
    raise ValueError("Both catalog_name and schema_name are required")

print(f"Building similarity edges in {catalog_name}.{schema_name}")
print(f"Similarity threshold: {similarity_threshold}")
print(f"Max edges per node: {max_edges_per_node}")
print(f"Mode: {'ANN (Vector Search)' if use_ann else 'cross-join'}")

# COMMAND ----------

import sys
sys.path.append("../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.similarity_edges import build_similarity_edges

result = build_similarity_edges(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    similarity_threshold=similarity_threshold,
    max_edges_per_node=max_edges_per_node,
    use_ann=use_ann,
)

print(f"Similarity edge creation complete")
print(f"  Method: {result['method']}")
print(f"  Edges created: {result['edges_created']}")
print(f"  Threshold used: {result['threshold']}")

# COMMAND ----------

# Show top similar pairs
display(spark.sql(f"""
    SELECT 
        e.src,
        e.dst,
        e.weight as similarity,
        n1.node_type as src_type,
        n2.node_type as dst_type,
        n1.domain as src_domain,
        n2.domain as dst_domain
    FROM {catalog_name}.{schema_name}.graph_edges e
    JOIN {catalog_name}.{schema_name}.graph_nodes n1 ON e.src = n1.id
    JOIN {catalog_name}.{schema_name}.graph_nodes n2 ON e.dst = n2.id
    WHERE e.relationship = 'similar_embedding'
    ORDER BY e.weight DESC
    LIMIT 20
"""))
