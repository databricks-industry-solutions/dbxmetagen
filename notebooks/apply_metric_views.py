# Databricks notebook source
# MAGIC %md
# MAGIC # Apply Metric Views & Create Genie Space
# MAGIC
# MAGIC Reads validated metric view definitions and creates them in Unity Catalog.
# MAGIC Optionally creates a Genie space for natural language data exploration.

# COMMAND ----------

# MAGIC %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("warehouse_id", "", "Warehouse ID (for Genie)")
dbutils.widgets.text("genie_space_name", "", "Genie Space Name (optional)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
warehouse_id = dbutils.widgets.get("warehouse_id")
genie_space_name = dbutils.widgets.get("genie_space_name")

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")

# COMMAND ----------

import sys
sys.path.append("../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.semantic_layer import SemanticLayerGenerator, SemanticLayerConfig

config = SemanticLayerConfig(catalog_name=catalog_name, schema_name=schema_name)
gen = SemanticLayerGenerator(spark, config)

result = gen.apply_metric_views()
print(f"Applied: {result['applied']}, Failed: {result['failed']}")

# COMMAND ----------

if genie_space_name and warehouse_id:
    space_id = gen.create_genie_space(genie_space_name, warehouse_id)
    if space_id:
        print(f"Genie space created: {space_id}")
    else:
        print("Genie space creation skipped or failed (see logs)")
else:
    print("Skipping Genie space creation (no name or warehouse_id provided)")

# COMMAND ----------

display(spark.sql(f"""
    SELECT metric_view_name, source_table, status, genie_space_id, applied_at
    FROM {catalog_name}.{schema_name}.metric_view_definitions
    WHERE status = 'applied'
"""))
