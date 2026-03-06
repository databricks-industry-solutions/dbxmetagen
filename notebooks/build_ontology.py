# Databricks notebook source
# MAGIC %md
# MAGIC # Build Ontology
# MAGIC 
# MAGIC This notebook discovers and builds the ontology layer from the knowledge base.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("config_path", "configurations/ontology_config.yaml", "Config Path")
dbutils.widgets.text("apply_ddl", "false", "Apply DDL (tags)")
dbutils.widgets.text("ontology_bundle", "", "Ontology Bundle (e.g. healthcare, general)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
config_path = dbutils.widgets.get("config_path")
apply_tags = dbutils.widgets.get("apply_ddl").lower() in ("true", "1", "yes")
ontology_bundle = dbutils.widgets.get("ontology_bundle").strip()

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Config: {config_path}")
print(f"Ontology bundle: {ontology_bundle or '(none – using config_path)'}")
print(f"Apply tags: {apply_tags}")

# COMMAND ----------

import sys
sys.path.append("../")  # For DAB deployment; pip-installed package works without this

from dbxmetagen.ontology import build_ontology, resolve_bundle_path

effective_config = config_path
if ontology_bundle:
    effective_config = resolve_bundle_path(ontology_bundle)
    print(f"Resolved bundle '{ontology_bundle}' -> {effective_config}")

result = build_ontology(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    config_path=effective_config,
    apply_tags=apply_tags,
    ontology_bundle=ontology_bundle,
)

print(f"Ontology build complete:")
print(f"  Table entities discovered: {result['entities_discovered']}")
print(f"  Column entities discovered: {result.get('column_entities_discovered', 0)}")
print(f"  Entity types: {result['entity_types']}")
print(f"  Graph edges added: {result['edges_added']}")
print(f"  UC tags applied: {result.get('tags_applied', 0)}")

# COMMAND ----------

# Show discovered entities
display(spark.sql(f"""
    SELECT 
        entity_name,
        entity_type,
        confidence,
        auto_discovered,
        validated,
        SIZE(source_tables) as source_table_count
    FROM {catalog_name}.{schema_name}.ontology_entities
    ORDER BY confidence DESC
"""))

