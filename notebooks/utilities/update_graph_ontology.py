# Databricks notebook source
# MAGIC %md
# MAGIC # Update Graph with Ontology
# MAGIC
# MAGIC Updates the knowledge graph with ontology entity nodes and instance_of edges.
# MAGIC
# MAGIC **Deprecated**: Prefer using `OntologyBuilder._sync_entity_nodes_to_graph()`
# MAGIC and `OntologyBuilder.add_entity_relationships_to_graph()` from the library,
# MAGIC which handle all columns, MERGE-based upserts, and deduplication.

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")

# COMMAND ----------

# Remove legacy per-UUID entity nodes
spark.sql(f"""
DELETE FROM {catalog_name}.{schema_name}.graph_nodes
WHERE node_type = 'entity' AND source_system = 'ontology' AND id NOT LIKE 'entity::%'
""")

# MERGE: One canonical concept node per (entity_name, ontology_bundle).
# ID format: entity::{bundle}::{entity_name} -- deterministic and deduplicated.
spark.sql(f"""
MERGE INTO {catalog_name}.{schema_name}.graph_nodes AS target
USING (
    SELECT CONCAT('entity::', COALESCE(ontology_bundle, '_default'), '::', entity_name) AS canonical_id,
           FIRST(entity_name) AS entity_name,
           FIRST(entity_type) AS entity_type,
           FIRST(description) AS description,
           MAX(confidence) AS confidence,
           MIN(created_at) AS created_at,
           MAX(updated_at) AS updated_at
    FROM {catalog_name}.{schema_name}.ontology_entities
    GROUP BY entity_name, COALESCE(ontology_bundle, '_default')
) AS source
ON target.id = source.canonical_id AND target.node_type = 'entity'

WHEN MATCHED THEN UPDATE SET
    target.table_short_name = source.entity_name,
    target.comment = COALESCE(source.description, target.comment),
    target.quality_score = source.confidence,
    target.ontology_type = source.entity_type,
    target.display_name = source.entity_name,
    target.short_description = COALESCE(source.description, target.short_description),
    target.status = 'primary',
    target.updated_at = source.updated_at

WHEN NOT MATCHED THEN INSERT (
    id, table_name, catalog, `schema`, table_short_name,
    domain, subdomain, has_pii, has_phi, security_level,
    comment, node_type, parent_id, data_type,
    quality_score, embedding,
    ontology_id, ontology_type, display_name, short_description,
    sensitivity, status, source_system, keywords,
    created_at, updated_at
) VALUES (
    source.canonical_id, NULL, NULL, NULL, source.entity_name,
    NULL, NULL, FALSE, FALSE, 'PUBLIC',
    source.description, 'entity', NULL, NULL,
    source.confidence, NULL,
    source.canonical_id, source.entity_type, source.entity_name, source.description,
    'public', 'primary', 'ontology', NULL,
    source.created_at, source.updated_at
)
""")

print("Merged canonical entity concept nodes into graph")

# COMMAND ----------

# DELETE + rebuild instance_of edges pointing to canonical concept nodes.
spark.sql(f"""
DELETE FROM {catalog_name}.{schema_name}.graph_edges
WHERE relationship = 'instance_of'
  AND (source_system IS NULL OR source_system = 'ontology')
""")

# INSERT instance_of edges: src = table FQN, dst = canonical concept node ID.
spark.sql(f"""
INSERT INTO {catalog_name}.{schema_name}.graph_edges
    (src, dst, relationship, weight, edge_id, edge_type, direction,
     join_expression, join_confidence, ontology_rel,
     source_system, status, created_at, updated_at)
SELECT DISTINCT
    tbl AS src,
    CONCAT('entity::', COALESCE(e.ontology_bundle, '_default'), '::', e.entity_name) AS dst,
    'instance_of' AS relationship,
    e.confidence AS weight,
    CONCAT(tbl, '::', CONCAT('entity::', COALESCE(e.ontology_bundle, '_default'), '::', e.entity_name), '::instance_of') AS edge_id,
    'instance_of' AS edge_type,
    'out' AS direction,
    NULL AS join_expression,
    NULL AS join_confidence,
    NULL AS ontology_rel,
    'ontology' AS source_system,
    'candidate' AS status,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
FROM {catalog_name}.{schema_name}.ontology_entities e
LATERAL VIEW EXPLODE(e.source_tables) exploded AS tbl
WHERE e.source_tables IS NOT NULL AND SIZE(e.source_tables) > 0
  AND COALESCE(e.entity_role, 'primary') = 'primary'
""")

print("Added instance_of edges to graph (canonical concept nodes)")

# COMMAND ----------

# Summary
node_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog_name}.{schema_name}.graph_nodes WHERE node_type = 'entity'").collect()[0].cnt
edge_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog_name}.{schema_name}.graph_edges WHERE relationship = 'instance_of'").collect()[0].cnt

print(f"Graph updated:")
print(f"  Entity nodes: {node_count}")
print(f"  Instance_of edges: {edge_count}")
