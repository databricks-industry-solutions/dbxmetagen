# Databricks notebook source
# MAGIC %md
# MAGIC # Build Knowledge Graph
# MAGIC 
# MAGIC Transforms knowledge bases into node and edge Delta tables
# MAGIC for relationship analysis between tables, columns, and schemas.
# MAGIC
# MAGIC ## Requirements
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

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup
# MAGIC 
# MAGIC The dbxmetagen wheel is installed via the serverless environment configuration.

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


