# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Embeddings
# MAGIC 
# MAGIC Generates vector embeddings for graph nodes using Databricks Foundation Model (BGE)
# MAGIC via AI_QUERY. Embeddings enable similarity search and clustering.

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("max_nodes", "", "Max Nodes (empty for all)")
dbutils.widgets.text("table_names", "", "Table Names (comma-separated, empty for all)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
max_nodes_str = dbutils.widgets.get("max_nodes")
max_nodes = int(max_nodes_str) if max_nodes_str else None

if not catalog_name or not schema_name:
    raise ValueError("Both catalog_name and schema_name are required")

print(f"Generating embeddings for {catalog_name}.{schema_name}")
print(f"Max nodes: {max_nodes or 'all'}")

# COMMAND ----------

import sys
sys.path.append("../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.embeddings import generate_embeddings
from dbxmetagen.table_filter import parse_table_names
table_names = parse_table_names(dbutils.widgets.get("table_names").strip()) or None

result = generate_embeddings(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    max_nodes=max_nodes,
    table_names=table_names,
)

print(f"Embedding generation complete")
print(f"  Nodes embedded: {result['nodes_embedded']}")
print(f"  Total candidates: {result['total_candidates']}")

# COMMAND ----------

# Show nodes with embeddings
nodes_with_emb = spark.sql(f"""
    SELECT id, node_type, 
           SUBSTRING(comment, 1, 100) as comment_preview,
           SIZE(embedding) as embedding_dim
    FROM {catalog_name}.{schema_name}.graph_nodes
    WHERE embedding IS NOT NULL
    LIMIT 20
""")

print(f"Nodes with embeddings: {nodes_with_emb.count()}")
display(nodes_with_emb)

