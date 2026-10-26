# Databricks notebook source
# MAGIC %md
# MAGIC # Apply Metric Views & Create Genie Space
# MAGIC
# MAGIC Reads validated metric view definitions and creates them in Unity Catalog.
# MAGIC Optionally creates a Genie space for natural language data exploration.

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("warehouse_id", "", "Warehouse ID (for Genie)")
dbutils.widgets.text("genie_space_name", "", "Genie Space Name (optional)")
dbutils.widgets.text("genie_space_manager", "", "Grant CAN_MANAGE on the Genie space to (user or SP id)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
warehouse_id = dbutils.widgets.get("warehouse_id")
genie_space_name = dbutils.widgets.get("genie_space_name")
genie_space_manager = dbutils.widgets.get("genie_space_manager").strip()

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
        # The space is owned by whoever runs this job. When that is a run_as service
        # principal, share it so the deployer (default) can see and manage it.
        run_identity = spark.sql("SELECT current_user()").first()[0]
        if genie_space_manager and genie_space_manager != run_identity:
            import re
            from databricks.sdk import WorkspaceClient
            key = ("service_principal_name"
                   if re.fullmatch(r"[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}", genie_space_manager)
                   else "user_name")
            try:
                WorkspaceClient().api_client.do(
                    "PATCH", f"/api/2.0/permissions/genie/{space_id}",
                    body={"access_control_list": [
                        {key: genie_space_manager, "permission_level": "CAN_MANAGE"}]})
                print(f"Granted CAN_MANAGE on Genie space to {genie_space_manager}")
            except Exception as e:
                print(f"WARNING: could not share Genie space with {genie_space_manager}: {e}")
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
