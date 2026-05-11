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

# MERGE: Upserts ontology entity rows into `graph_nodes`; match key `target.id = source.entity_id`.
#   On match updates table_short_name, comment, quality_score (from confidence), ontology_type,
#   display_name, short_description, status (entity_role), updated_at; on insert creates `node_type='entity'`,
#   `source_system='ontology'`, fills identity/display fields from `ontology_entities` (table/column FK fields nullable).
# WHY: Mirrors discovered ontology entities as first-class graph nodes so dashboards, traversal, and Genie/context
#   consumers see the same entity catalog the ontology pipeline emitted.
# TRADEOFFS: MERGE is idempotent and correct for notebooks, but skips library helpers that dedupe/maintain columns
#   parity; DELETE+reload or deprecated drift vs `OntologyBuilder` sync is possible — prefer library paths for prod.
spark.sql(f"""
MERGE INTO {catalog_name}.{schema_name}.graph_nodes AS target
USING (
    SELECT entity_id, entity_name, entity_type,
           COALESCE(entity_role, 'primary') AS entity_role,
           description, confidence, created_at, updated_at
    FROM {catalog_name}.{schema_name}.ontology_entities
) AS source
ON target.id = source.entity_id

WHEN MATCHED THEN UPDATE SET
    target.table_short_name = source.entity_name,
    target.comment = COALESCE(source.description, target.comment),
    target.quality_score = source.confidence,
    target.ontology_type = source.entity_type,
    target.display_name = source.entity_name,
    target.short_description = COALESCE(source.description, target.short_description),
    target.status = source.entity_role,
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
    source.entity_id, NULL, NULL, NULL, source.entity_name,
    NULL, NULL, FALSE, FALSE, 'PUBLIC',
    source.description, 'entity', NULL, NULL,
    source.confidence, NULL,
    source.entity_id, source.entity_type, source.entity_name, source.description,
    'public', source.entity_role, 'ontology', NULL,
    source.created_at, source.updated_at
)
""")

print("Merged entity nodes into graph")

# COMMAND ----------

# DELETE: Removes rows from `graph_edges` where `relationship = 'instance_of'` and source is ontology
#   (`source_system IS NULL OR source_system = 'ontology'`) so stale links do not coexist with the next INSERT.
# WHY: Instance links are rebuilt fully from current `ontology_entities.source_tables`; partial MERGE-by-edge_id is not used here.
# TRADEOFFS: Brief window/table scan cost on delete-then-insert vs single MERGE sweep; clears legacy NULL source_system edges
#   but treats any non-ontology `instance_of` as out of scope — may orphan if edges were stamped with another system.
spark.sql(f"""
DELETE FROM {catalog_name}.{schema_name}.graph_edges
WHERE relationship = 'instance_of'
  AND (source_system IS NULL OR source_system = 'ontology')
""")

# INSERT: Appends new `instance_of` edges: `src` = physical table id (`graph_nodes.id`), `dst` = `entity_id`,
#   deterministic `edge_id = CONCAT(src,'::',dst,'::instance_of')`, weight from entity confidence,
#   `source_system='ontology'`, status `candidate`, timestamps `current_timestamp`.
# WHY: Materializes table→ontology-entity containment for graph queries and lineage-style navigation after node sync.
# TRADEOFFS: Requires matching `graph_nodes` rows keyed by exploded `source_tables` names JOIN `t.id = table_name`
#   (misses entities whose table names lack node rows); LATERAL EXPLODE duplicates if arrays repeat — normalization is caller's duty.
spark.sql(f"""
INSERT INTO {catalog_name}.{schema_name}.graph_edges
    (src, dst, relationship, weight, edge_id, edge_type, direction,
     join_expression, join_confidence, ontology_rel,
     source_system, status, created_at, updated_at)
SELECT
    t.table_name AS src,
    e.entity_id AS dst,
    'instance_of' AS relationship,
    e.confidence AS weight,
    CONCAT(t.table_name, '::', e.entity_id, '::instance_of') AS edge_id,
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
LATERAL VIEW EXPLODE(e.source_tables) exploded AS table_name
JOIN {catalog_name}.{schema_name}.graph_nodes t ON t.id = table_name
""")

print("Added instance_of edges to graph")

# COMMAND ----------

# Summary
node_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog_name}.{schema_name}.graph_nodes WHERE node_type = 'entity'").collect()[0].cnt
edge_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog_name}.{schema_name}.graph_edges WHERE relationship = 'instance_of'").collect()[0].cnt

print(f"Graph updated:")
print(f"  Entity nodes: {node_count}")
print(f"  Instance_of edges: {edge_count}")
