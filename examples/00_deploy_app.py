# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy dbxmetagen App (no CLI / no DABs)
# MAGIC
# MAGIC Deploy the dbxmetagen dashboard directly from your Databricks workspace.
# MAGIC The pre-built frontend is included in the repo -- no Node.js or npm required.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - This repo cloned into your workspace (Git folder)
# MAGIC - A SQL warehouse you can access
# MAGIC - Permission to create Databricks Apps

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------
dbutils.widgets.text("catalog_name", "", "Catalog Name (required)")
dbutils.widgets.text("schema_name", "metadata_results", "Output Schema")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (required)")
dbutils.widgets.text("app_name", "dbxmetagen-app", "App Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
warehouse_id = dbutils.widgets.get("warehouse_id")
app_name = dbutils.widgets.get("app_name")

assert catalog_name, "catalog_name is required"
assert warehouse_id, "warehouse_id is required"

print(f"Catalog:   {catalog_name}")
print(f"Schema:    {schema_name}")
print(f"Warehouse: {warehouse_id}")
print(f"App name:  {app_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Generate app.yaml from template

# COMMAND ----------
import os

repo_root = os.path.dirname(os.getcwd())
app_dir = os.path.join(repo_root, "apps", "dbxmetagen-app", "app")
template_path = os.path.join(app_dir, "app.yaml.template")
output_path = os.path.join(app_dir, "app.yaml")

assert os.path.exists(template_path), f"Template not found: {template_path}"

with open(template_path) as f:
    content = f.read()

content = content.replace("__CATALOG_NAME__", catalog_name)
content = content.replace("__SCHEMA_NAME__", schema_name)

with open(output_path, "w") as f:
    f.write(content)

print(f"Generated {output_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Install Python dependencies
# MAGIC
# MAGIC The app's `requirements.txt` lists runtime dependencies.
# MAGIC These will be installed inside the app container automatically,
# MAGIC but the dbxmetagen wheel needs to be accessible. Install it into
# MAGIC the repo so the app source includes it.

# COMMAND ----------
# MAGIC %pip install -qqq -r ../requirements.txt ..
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Deploy the app
# MAGIC
# MAGIC This creates (or updates) a Databricks App pointing at the app source directory.
# MAGIC After deployment, add your SQL warehouse as a resource via the Apps UI.

# COMMAND ----------
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import AppDeployment, App

w = WorkspaceClient()
app_source_path = os.path.join(
    "/Workspace",
    os.path.relpath(
        os.path.join(os.path.dirname(os.getcwd()), "apps", "dbxmetagen-app", "app"),
        "/Workspace"
    )
)

print(f"App source path: {app_source_path}")
print(f"Deploying app '{app_name}'...")

try:
    existing = w.apps.get(app_name)
    print(f"App '{app_name}' exists, deploying update...")
except Exception:
    print(f"Creating new app '{app_name}'...")
    w.apps.create(App(name=app_name, description="dbxmetagen dashboard"))

deployment = w.apps.deploy(app_name, AppDeployment(source_code_path=app_source_path))
print(f"Deployment started: {deployment.deployment_id}")
print(f"\nNext steps:")
print(f"  1. Go to Workspace > Apps > {app_name}")
print(f"  2. Add your SQL warehouse (ID: {warehouse_id}) as a resource named 'sql_warehouse'")
print(f"  3. The app will be available at its assigned URL once resources are configured")
