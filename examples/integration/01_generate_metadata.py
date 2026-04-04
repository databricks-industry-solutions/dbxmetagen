# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1: Generate Metadata with dbxmetagen
# MAGIC
# MAGIC Runs AI-powered metadata generation (comments, PII classification, or domain classification)
# MAGIC on a set of Unity Catalog tables. Produces `metadata_generation_log` in the target schema.
# MAGIC
# MAGIC **Outputs:** `metadata_generation_log`
# MAGIC
# MAGIC See the [integration README](README.md) for the full pipeline overview.

# COMMAND ----------

import subprocess, sys, os

dbutils.widgets.text("install_source", os.getenv("METAGEN_INSTALL_SOURCE", "git+https://github.com/databricks-industry-solutions/dbxmetagen.git@main"))
src = dbutils.widgets.get("install_source")
subprocess.check_call([sys.executable, "-m", "pip", "install", "-qqq", src])

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
from pyspark.sql import SparkSession

dbutils.widgets.text("catalog_name", os.getenv("CATALOG_NAME", ""), "Catalog Name (required)")
dbutils.widgets.text("schema_name", os.getenv("SCHEMA_NAME", "default"), "Output Schema")
dbutils.widgets.text("volume_name", os.getenv("VOLUME_NAME", "generated_metadata"), "Volume Name")
dbutils.widgets.text("table_names", os.getenv("METAGEN_TABLE_NAMES", ""), "Table Names (required)")
dbutils.widgets.text("model_endpoint", os.getenv("METAGEN_MODEL_ENDPOINT", "databricks-claude-sonnet-4-6"), "Model Endpoint")
dbutils.widgets.text("mode", os.getenv("METAGEN_MODE", "comment"), "Mode (comment/pi/domain)")
dbutils.widgets.text("max_total_tables", os.getenv("METAGEN_MAX_TABLES", "100"), "Max Total Tables")
dbutils.widgets.text("install_source", os.getenv("METAGEN_INSTALL_SOURCE", "git+https://github.com/databricks-industry-solutions/dbxmetagen.git@main"))

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")
table_names = dbutils.widgets.get("table_names")
model_endpoint = dbutils.widgets.get("model_endpoint")
mode = dbutils.widgets.get("mode")
max_total_tables = int(dbutils.widgets.get("max_total_tables"))

spark = SparkSession.builder.getOrCreate()

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Tables: {table_names}")
print(f"Mode: {mode}")
print(f"Model: {model_endpoint}")
print(f"Max total tables: {max_total_tables}")

# COMMAND ----------

# DBTITLE 1,Table Count Safeguard
# Pre-resolve wildcards to catch accidentally large runs before any LLM calls.
if "*" in table_names:
    parts = table_names.strip().split(".")
    cat = parts[0] if len(parts) >= 1 else catalog_name
    sch = parts[1] if len(parts) >= 2 else schema_name
    resolved = spark.sql(f"SHOW TABLES IN `{cat}`.`{sch}`").collect()
    table_count = len(resolved)
else:
    table_count = len([t for t in table_names.split(",") if t.strip()])

if table_count > max_total_tables:
    raise ValueError(
        f"Resolved {table_count} tables, which exceeds the safety limit of {max_total_tables}.\n\n"
        f"Each table triggers multiple LLM calls (one per {20}-column chunk), so processing "
        f"{table_count} tables may take hours and incur significant cost.\n\n"
        f"To proceed intentionally, raise max_total_tables (widget or METAGEN_MAX_TABLES env var).\n"
        f"To narrow the scope, specify explicit table names instead of a wildcard."
    )

print(f"Table count check passed: {table_count} tables (limit: {max_total_tables})")

# COMMAND ----------

# DBTITLE 1,Resolve Overrides YAML (optional)
# If your project ships a metagen_overrides.yml, resolve its workspace path.
# MetadataConfig merges YAML defaults under explicit kwargs, so only overrides matter.
yaml_path = None
try:
    nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    bundle_root = "/Workspace" + str(nb_path).rsplit("/", 2)[0]
    candidate = f"{bundle_root}/configurations/metagen_overrides.yml"
    if os.path.exists(candidate):
        yaml_path = candidate
        print(f"Using overrides YAML: {yaml_path}")
except Exception:
    pass

if not yaml_path:
    print("No overrides YAML found -- using dbxmetagen defaults + explicit params")

# COMMAND ----------

# DBTITLE 1,Run Metadata Generation
from dbxmetagen.main import main

kwargs = {
    "catalog_name": catalog_name,
    "schema_name": schema_name,
    "table_names": table_names,
    "mode": mode,
    "model": model_endpoint,
    "apply_ddl": "false",
    "table_names_source": "parameter",
    "incremental": "true",
    "volume_name": volume_name,
}
if yaml_path:
    kwargs["yaml_file_path"] = yaml_path

main(kwargs)

# COMMAND ----------

# DBTITLE 1,Verify Output
log_table = f"{catalog_name}.{schema_name}.metadata_generation_log"
count = spark.table(log_table).count()
print(f"metadata_generation_log: {count} rows")
display(spark.table(log_table).limit(5))
