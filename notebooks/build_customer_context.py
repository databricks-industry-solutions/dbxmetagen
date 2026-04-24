# Databricks notebook source
# MAGIC %md
# MAGIC # Build Customer Context Table
# MAGIC Seeds the `customer_context` Delta table from YAML files in `configurations/customer_context/`.
# MAGIC Run this once after adding or updating context files, or include it as an early step in the analytics pipeline.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("yaml_dir", "configurations/customer_context", "YAML Directory")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
yaml_dir = dbutils.widgets.get("yaml_dir")

# COMMAND ----------

import os

# Resolve yaml_dir relative to repo root
if not os.path.isabs(yaml_dir):
    for candidate in [yaml_dir, os.path.join("..", yaml_dir)]:
        if os.path.isdir(candidate):
            yaml_dir = candidate
            break

print(f"Catalog: {catalog_name}")
print(f"Schema:  {schema_name}")
print(f"YAML dir: {yaml_dir} (exists: {os.path.isdir(yaml_dir)})")

# COMMAND ----------

from dbxmetagen.customer_context import seed_customer_context_table

count = seed_customer_context_table(spark, catalog_name, schema_name, yaml_dir)
print(f"Seeded {count} customer context entries")

# COMMAND ----------

df = spark.sql(f"SELECT scope, scope_type, context_label, length(context_text) as text_len, priority, active FROM {catalog_name}.{schema_name}.customer_context ORDER BY scope_type, scope")
display(df)
