# Databricks notebook source
# MAGIC %md
# MAGIC # Update Graph with Ontology
# MAGIC 
# MAGIC This notebook updates the knowledge graph with ontology entity relationships.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")

# COMMAND ----------

# Add entity nodes to the graph
spark.sql(f"""
INSERT INTO {catalog_name}.{schema_name}.graph_nodes
SELECT 
    entity_id as id,
    NULL as table_name,
    NULL as catalog,
    NULL as `schema`,
    entity_name as table_short_name,
    NULL as domain,
    NULL as subdomain,
    FALSE as has_pii,
    FALSE as has_phi,
    'PUBLIC' as security_level,
    description as comment,
    'entity' as node_type,
    entity_type as parent_id,
    NULL as data_type,
    confidence as quality_score,
    NULL as embedding,
    created_at,
    updated_at
FROM {catalog_name}.{schema_name}.ontology_entities
WHERE entity_id NOT IN (
    SELECT id FROM {catalog_name}.{schema_name}.graph_nodes
)
""")

print("Added entity nodes to graph")

# COMMAND ----------

# Add instance_of edges from tables to entities
spark.sql(f"""
INSERT INTO {catalog_name}.{schema_name}.graph_edges
SELECT 
    t.table_name as src,
    e.entity_id as dst,
    'instance_of' as relationship,
    e.confidence as weight,
    current_timestamp() as created_at,
    current_timestamp() as updated_at
FROM {catalog_name}.{schema_name}.ontology_entities e
LATERAL VIEW EXPLODE(e.source_tables) exploded AS table_name
JOIN {catalog_name}.{schema_name}.graph_nodes t ON t.id = table_name
WHERE NOT EXISTS (
    SELECT 1 FROM {catalog_name}.{schema_name}.graph_edges existing
    WHERE existing.src = table_name 
      AND existing.dst = e.entity_id 
      AND existing.relationship = 'instance_of'
)
""")

print("Added instance_of edges to graph")

# COMMAND ----------

# Summary
node_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog_name}.{schema_name}.graph_nodes WHERE node_type = 'entity'").collect()[0].cnt
edge_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog_name}.{schema_name}.graph_edges WHERE relationship = 'instance_of'").collect()[0].cnt

print(f"Graph updated:")
print(f"  Entity nodes: {node_count}")
print(f"  Instance_of edges: {edge_count}")

