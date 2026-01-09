# Databricks notebook source
# MAGIC %md
# MAGIC # Build Knowledge Graph
# MAGIC 
# MAGIC Transforms the `table_knowledge_base` into GraphFrames-compatible node and edge tables
# MAGIC for relationship analysis between tables.
# MAGIC
# MAGIC ## Requirements
# MAGIC - **ML Runtime Required**: GraphFrames requires JVM dependencies not available on serverless
# MAGIC - Depends on `table_knowledge_base` being populated first
# MAGIC
# MAGIC ## Relationships Created
# MAGIC - `same_domain`: Tables in the same business domain
# MAGIC - `same_subdomain`: Tables in the same subdomain
# MAGIC - `same_catalog`: Tables in the same Unity Catalog catalog
# MAGIC - `same_schema`: Tables in the same schema
# MAGIC - `same_security_level`: Tables with matching PII/PHI/PUBLIC classification

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------
# MAGIC %pip install -qqqq -r ../requirements.txt graphframes
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

if not catalog_name or not schema_name:
    raise ValueError("Both catalog_name and schema_name are required")

print(f"Building knowledge graph in {catalog_name}.{schema_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify Knowledge Base Exists

# COMMAND ----------

kb_count = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {catalog_name}.{schema_name}.table_knowledge_base
""").collect()[0]["cnt"]

if kb_count == 0:
    raise ValueError("table_knowledge_base is empty. Run build_knowledge_base first.")

print(f"Found {kb_count} tables in knowledge base")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build Knowledge Graph

# COMMAND ----------

import sys
sys.path.append("../")

from src.dbxmetagen.knowledge_graph import (
    KnowledgeGraphConfig,
    KnowledgeGraphBuilder,
    build_knowledge_graph
)

# Execute the graph building pipeline
result = build_knowledge_graph(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name
)

print(f"Knowledge graph build complete")
print(f"Staged nodes: {result['staged_nodes']}")
print(f"Staged edges: {result['staged_edges']}")
print(f"Total nodes: {result['total_nodes']}")
print(f"Total edges: {result['total_edges']}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## View Graph Statistics

# COMMAND ----------

from pyspark.sql import functions as F

# Node statistics
print("=== Node Statistics ===")
node_stats = spark.sql(f"""
    SELECT 
        COUNT(*) as total_nodes,
        COUNT(DISTINCT catalog) as unique_catalogs,
        COUNT(DISTINCT schema) as unique_schemas,
        COUNT(DISTINCT domain) as unique_domains,
        COUNT(DISTINCT subdomain) as unique_subdomains,
        SUM(CASE WHEN security_level = 'PHI' THEN 1 ELSE 0 END) as phi_tables,
        SUM(CASE WHEN security_level = 'PII' THEN 1 ELSE 0 END) as pii_tables,
        SUM(CASE WHEN security_level = 'PUBLIC' THEN 1 ELSE 0 END) as public_tables
    FROM {catalog_name}.{schema_name}.graph_nodes
""")
node_stats.display()

# COMMAND ----------

# Edge statistics by relationship type
print("=== Edge Statistics by Relationship ===")
edge_stats = spark.sql(f"""
    SELECT 
        relationship,
        COUNT(*) as edge_count
    FROM {catalog_name}.{schema_name}.graph_edges
    GROUP BY relationship
    ORDER BY edge_count DESC
""")
edge_stats.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Example GraphFrames Queries
# MAGIC 
# MAGIC Below are example queries you can run once the graph is built.

# COMMAND ----------

# Create GraphFrame
from graphframes import GraphFrame

nodes = spark.table(f"{catalog_name}.{schema_name}.graph_nodes")
edges = spark.table(f"{catalog_name}.{schema_name}.graph_edges")

g = GraphFrame(nodes, edges)

print(f"GraphFrame created with {g.vertices.count()} vertices and {g.edges.count()} edges")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Query 1: Find tables in the same domain

# COMMAND ----------

# Find all domain relationships
same_domain = g.edges.filter(F.col("relationship") == "same_domain")
print(f"Found {same_domain.count()} same-domain relationships")

# Show sample
same_domain.limit(10).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Query 2: Find tables connected by security level

# COMMAND ----------

# PHI tables connected to other PHI tables
phi_connections = g.find("(a)-[e]->(b)").filter(
    (F.col("a.security_level") == "PHI") & 
    (F.col("b.security_level") == "PHI") &
    (F.col("e.relationship") == "same_security_level")
)

print(f"Found {phi_connections.count()} PHI-to-PHI connections")
phi_connections.select("a.table_name", "b.table_name", "a.domain").limit(10).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Query 3: Motif Finding - Tables bridging domains

# COMMAND ----------

# Find tables that connect different domains through schema relationships
# Pattern: A and B in same schema, B and C in same domain
bridging = g.find("(a)-[e1]->(b); (b)-[e2]->(c)").filter(
    (F.col("e1.relationship") == "same_schema") & 
    (F.col("e2.relationship") == "same_domain")
).select(
    F.col("a.table_name").alias("table_a"),
    F.col("b.table_name").alias("bridge_table"),
    F.col("c.table_name").alias("table_c"),
    F.col("a.domain").alias("domain_a"),
    F.col("c.domain").alias("domain_c")
).filter(
    F.col("domain_a") != F.col("domain_c")  # Different domains
)

if bridging.count() > 0:
    print("Tables that bridge different domains:")
    bridging.limit(10).display()
else:
    print("No cross-domain bridges found (tables may all be in same domain)")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Query 4: Most Connected Tables (Degree Centrality)

# COMMAND ----------

# Count connections per table
degrees = g.degrees.orderBy("degree", ascending=False)
print("Most connected tables:")
degrees.limit(20).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Query 5: Connected Components (Table Clusters)

# COMMAND ----------

# Set checkpoint directory (required for connected components algorithm)
# Note: setCheckpointDir requires DBFS paths, not UC Volume paths
# Using DBFS with unique path per catalog/schema to avoid conflicts
checkpoint_dir = f"dbfs:/tmp/graphframes_checkpoints/{catalog_name}/{schema_name}"
spark.sparkContext.setCheckpointDir(checkpoint_dir)
print(f"Checkpoint directory set to: {checkpoint_dir}")

# Find clusters of related tables
components = g.connectedComponents()

# Count tables per component
cluster_sizes = components.groupBy("component").count().orderBy("count", ascending=False)
print(f"Found {cluster_sizes.count()} connected components")
cluster_sizes.limit(10).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Query 6: PageRank - Most Important Tables

# COMMAND ----------

# Run PageRank to find most "important" tables
pagerank = g.pageRank(resetProbability=0.15, maxIter=10)

top_tables = pagerank.vertices.select(
    "table_name", "domain", "security_level", "pagerank"
).orderBy("pagerank", ascending=False)

print("Top tables by PageRank:")
top_tables.limit(20).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Additional Query Examples
# MAGIC 
# MAGIC See the `GRAPHFRAMES_EXAMPLES` string in `src/dbxmetagen/knowledge_graph.py` for more examples including:
# MAGIC - Finding paths between specific tables
# MAGIC - Security boundary analysis (PHI to PII connections)
# MAGIC - Label propagation for community detection
# MAGIC - Subgraph analysis for specific domains
# MAGIC - Finding isolated tables

