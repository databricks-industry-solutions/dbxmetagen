# Databricks notebook source
# MAGIC %md
# MAGIC # Validate Ontology
# MAGIC 
# MAGIC This notebook validates discovered entities using AI and suggests improvements.

# COMMAND ----------

# MAGIC %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("validate_columns", "false", "Validate Column Entities (true/false)")
dbutils.widgets.text("force_revalidate", "false", "Force Re-validate All (true/false)")
dbutils.widgets.text("table_names", "", "Table Names (comma-separated, empty for all)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
validate_columns = str(dbutils.widgets.get("validate_columns")).strip().lower() == "true"
force_revalidate = str(dbutils.widgets.get("force_revalidate")).strip().lower() == "true"

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Validate columns: {validate_columns}")
print(f"Force revalidate: {force_revalidate}")

# COMMAND ----------

import sys
sys.path.append("../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.ontology_validator import validate_ontology
from dbxmetagen.table_filter import parse_table_names
table_names = parse_table_names(dbutils.widgets.get("table_names").strip()) or None

result = validate_ontology(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    validate_columns=validate_columns,
    force_revalidate=force_revalidate,
    table_names=table_names,
)

print(f"Validation complete:")
print(f"  Entities validated: {result['entities_validated']}")

# COMMAND ----------

# Show validation summary
recommendations = result.get('recommendations', {})

if recommendations.get('validation_summary'):
    print("Validation Summary by Entity Type:")
    for entity_type, stats in recommendations['validation_summary'].items():
        print(f"  {entity_type}: {stats['validated']}/{stats['total']} validated, avg confidence: {stats['avg_confidence']:.2f}")

# COMMAND ----------

# Show suggested new entities
if recommendations.get('suggested_entities'):
    print(f"\nSuggested New Entities: {len(recommendations['suggested_entities'])}")
    for entity in recommendations['suggested_entities']:
        print(f"  - {entity.get('entity_name', 'Unknown')}: {entity.get('description', '')[:100]}")
else:
    print("\nNo new entity suggestions")

# COMMAND ----------

# Show validated entities
display(spark.sql(f"""
    SELECT 
        entity_name,
        entity_type,
        confidence,
        validated,
        validation_notes
    FROM {catalog_name}.{schema_name}.ontology_entities
    WHERE validated = TRUE
    ORDER BY confidence DESC
"""))

