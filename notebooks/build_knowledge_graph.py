# Databricks notebook source
# MAGIC %md
# MAGIC # Build Knowledge Graph
# MAGIC 
# MAGIC Transforms knowledge bases into GraphFrames-compatible node and edge tables
# MAGIC for relationship analysis between tables, columns, and schemas.
# MAGIC
# MAGIC ## Requirements
# MAGIC - **ML Runtime Required**: GraphFrames requires JVM dependencies not available on serverless
# MAGIC - Depends on `table_knowledge_base`, `column_knowledge_base`, and `schema_knowledge_base`
# MAGIC
# MAGIC ## Node Types Created
# MAGIC - `table`: From table_knowledge_base
# MAGIC - `column`: From column_knowledge_base
# MAGIC - `schema`: From schema_knowledge_base
# MAGIC
# MAGIC ## Relationships Created
# MAGIC - `same_domain`: Tables in the same business domain
# MAGIC - `same_subdomain`: Tables in the same subdomain
# MAGIC - `same_catalog`: Tables in the same Unity Catalog catalog
# MAGIC - `same_schema`: Tables in the same schema
# MAGIC - `same_security_level`: Tables with matching PII/PHI/PUBLIC classification
# MAGIC - `contains`: Hierarchical (schema->table, table->column)
# MAGIC - `derives_from`: Lineage relationships

# COMMAND ----------

# MAGIC %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup
# MAGIC 
# MAGIC Libraries (graphframes, dbxmetagen wheel) are installed via job cluster configuration.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("table_names", "", "Table Names (comma-separated, empty=all)")
dbutils.widgets.text("sweep_stale_edges", "false", "Sweep stale edges")
dbutils.widgets.text("incremental", "true", "Incremental (true/false)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
table_names_raw = dbutils.widgets.get("table_names")
sweep_stale = dbutils.widgets.get("sweep_stale_edges").strip().lower() in ("true", "1", "yes")
incremental = dbutils.widgets.get("incremental").strip().lower() == "true"

if not catalog_name or not schema_name:
    raise ValueError("Both catalog_name and schema_name are required")

print(f"Building knowledge graph in {catalog_name}.{schema_name}")
print(f"Incremental: {incremental}")
if table_names_raw:
    print(f"Table filter: {table_names_raw}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify Knowledge Bases Exist

# COMMAND ----------

# Check table knowledge base
table_kb_count = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {catalog_name}.{schema_name}.table_knowledge_base
""").collect()[0]["cnt"]

if table_kb_count == 0:
    raise ValueError("table_knowledge_base is empty. Run build_knowledge_base first.")

print(f"Found {table_kb_count} tables in table_knowledge_base")

# Check column knowledge base (optional but recommended)
try:
    column_kb_count = spark.sql(f"""
        SELECT COUNT(*) as cnt 
        FROM {catalog_name}.{schema_name}.column_knowledge_base
    """).collect()[0]["cnt"]
    print(f"Found {column_kb_count} columns in column_knowledge_base")
except:
    column_kb_count = 0
    print("column_knowledge_base not found - column nodes will be skipped")

# Check schema knowledge base (optional but recommended)
try:
    schema_kb_count = spark.sql(f"""
        SELECT COUNT(*) as cnt 
        FROM {catalog_name}.{schema_name}.schema_knowledge_base
    """).collect()[0]["cnt"]
    print(f"Found {schema_kb_count} schemas in schema_knowledge_base")
except:
    schema_kb_count = 0
    print("schema_knowledge_base not found - schema nodes will be skipped")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build Knowledge Graph

# COMMAND ----------

import sys
sys.path.append("../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.knowledge_graph import (
    ExtendedKnowledgeGraphConfig,
    ExtendedKnowledgeGraphBuilder,
    build_extended_knowledge_graph
)
from dbxmetagen.table_filter import parse_table_names

table_names = parse_table_names(table_names_raw) or None

result = build_extended_knowledge_graph(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    include_columns=(column_kb_count > 0),
    include_schemas=(schema_kb_count > 0),
    table_names=table_names,
    sweep_stale=sweep_stale,
    incremental=incremental,
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

# Node statistics by type
print("=== Node Statistics by Type ===")
node_type_stats = spark.sql(f"""
    SELECT 
        COALESCE(node_type, 'unknown') as node_type,
        COUNT(*) as node_count,
        SUM(CASE WHEN security_level = 'PHI' THEN 1 ELSE 0 END) as phi_count,
        SUM(CASE WHEN security_level = 'PII' THEN 1 ELSE 0 END) as pii_count,
        SUM(CASE WHEN security_level = 'PUBLIC' THEN 1 ELSE 0 END) as public_count
    FROM {catalog_name}.{schema_name}.graph_nodes
    GROUP BY node_type
    ORDER BY node_count DESC
""")
node_type_stats.display()

# COMMAND ----------

# Overall node statistics
print("=== Overall Node Statistics ===")
node_stats = spark.sql(f"""
    SELECT 
        COUNT(*) as total_nodes,
        COUNT(DISTINCT catalog) as unique_catalogs,
        COUNT(DISTINCT schema) as unique_schemas,
        COUNT(DISTINCT domain) as unique_domains,
        SUM(CASE WHEN node_type = 'table' THEN 1 ELSE 0 END) as table_nodes,
        SUM(CASE WHEN node_type = 'column' THEN 1 ELSE 0 END) as column_nodes,
        SUM(CASE WHEN node_type = 'schema' THEN 1 ELSE 0 END) as schema_nodes
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

# Containment hierarchy stats
print("=== Containment Hierarchy ===")
containment_stats = spark.sql(f"""
    SELECT 
        n1.node_type as parent_type,
        n2.node_type as child_type,
        COUNT(*) as edge_count
    FROM {catalog_name}.{schema_name}.graph_edges e
    JOIN {catalog_name}.{schema_name}.graph_nodes n1 ON e.src = n1.id
    JOIN {catalog_name}.{schema_name}.graph_nodes n2 ON e.dst = n2.id
    WHERE e.relationship = 'contains'
    GROUP BY n1.node_type, n2.node_type
    ORDER BY edge_count DESC
""")
containment_stats.display()

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

# PHI nodes connected to other PHI nodes
phi_connections = g.find("(a)-[e]->(b)").filter(
    (F.col("a.security_level") == "PHI") & 
    (F.col("b.security_level") == "PHI") &
    (F.col("e.relationship") == "same_security_level")
)

print(f"Found {phi_connections.count()} PHI-to-PHI connections")
phi_connections.select("a.id", "a.node_type", "b.id", "b.node_type", "e.relationship").limit(10).display()

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

# Run PageRank to find most "important" nodes
pagerank = g.pageRank(resetProbability=0.15, maxIter=10)

top_nodes = pagerank.vertices.select(
    "id", "node_type", "domain", "security_level", "pagerank"
).orderBy("pagerank", ascending=False)

print("Top nodes by PageRank:")
top_nodes.limit(20).display()

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

