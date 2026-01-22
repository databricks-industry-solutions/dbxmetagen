# Databricks notebook source
# MAGIC %md
# MAGIC # Build Ontology
# MAGIC 
# MAGIC This notebook discovers and builds the ontology layer from the knowledge base.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("config_path", "configurations/ontology_config.yaml", "Config Path")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
config_path = dbutils.widgets.get("config_path")

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Config: {config_path}")

# COMMAND ----------

from src.dbxmetagen.ontology import build_ontology

result = build_ontology(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    config_path=config_path
)

print(f"Ontology build complete:")
print(f"  Entities discovered: {result['entities_discovered']}")
print(f"  Entity types: {result['entity_types']}")
print(f"  Graph edges added: {result['edges_added']}")

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

