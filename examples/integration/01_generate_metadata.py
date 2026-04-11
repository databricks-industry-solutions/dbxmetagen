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

import os
import sys

_mp = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
if _mp not in sys.path:
    sys.path.insert(0, _mp)
try:
    from install_dbxmetagen import install_dbxmetagen
except ImportError:
    sys.path.insert(0, os.path.join(os.getcwd(), "metagen_pipeline"))
    from install_dbxmetagen import install_dbxmetagen

dbutils.widgets.text(
    "install_source",
    os.getenv("METAGEN_INSTALL_SOURCE", "auto"),
)
src = dbutils.widgets.get("install_source")
install_dbxmetagen(src)

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
# Re-declared so DAB base_parameters can pass install_source without "widget not found" errors.
dbutils.widgets.text("install_source", os.getenv("METAGEN_INSTALL_SOURCE", "auto"))

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")
table_names = (dbutils.widgets.get("table_names") or "").strip()
if not table_names:
    table_names = f"{catalog_name}.{schema_name}.*"
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
        f"Each table triggers multiple LLM calls (one per 20-column chunk), so processing "
        f"{table_count} tables may take hours and incur significant cost.\n\n"
        f"To proceed intentionally, raise max_total_tables (widget or METAGEN_MAX_TABLES env var).\n"
        f"To narrow the scope, specify explicit table names instead of a wildcard."
    )

print(f"Table count check passed: {table_count} tables (limit: {max_total_tables})")

# COMMAND ----------

# DBTITLE 1,Resolve bundle YAML
# MetadataConfig.load_yaml() only sets keys present in the YAML file. A *partial* file
# (metagen_overrides.yml) omits most defaults and may cause AttributeError. Prefer the full
# copy of dbxmetagen variables.yml shipped under configurations/.
bundle_root = None
yaml_path = None
yaml_advanced_path = None
using_full_variables = False
try:
    nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    bundle_root = "/Workspace" + str(nb_path).rsplit("/", 2)[0]
    full_var = f"{bundle_root}/configurations/dbxmetagen_variables.yml"
    partial = f"{bundle_root}/configurations/metagen_overrides.yml"
    adv = f"{bundle_root}/configurations/variables.advanced.yml"
    if os.path.exists(full_var):
        yaml_path = full_var
        using_full_variables = True
        if os.path.exists(adv):
            yaml_advanced_path = adv
        print(f"Using full variables YAML: {yaml_path}")
        if yaml_advanced_path:
            print(f"Using advanced YAML: {yaml_advanced_path}")
    elif os.path.exists(partial):
        yaml_path = partial
        print(f"Using partial overrides YAML (legacy): {yaml_path}")
except Exception:
    pass

if not yaml_path:
    print(
        "No configurations/*.yml found under bundle -- deploy the bundle or set yaml paths; "
        "install may fail without full variables."
    )

# COMMAND ----------

# DBTITLE 1,Run Metadata Generation
from dbxmetagen.main import main
from dbxmetagen.user_utils import get_current_user

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
    "current_user": get_current_user(),
}
if yaml_path:
    kwargs["yaml_file_path"] = yaml_path
if yaml_advanced_path:
    kwargs["yaml_advanced_file_path"] = yaml_advanced_path
if yaml_path and not using_full_variables:
    kwargs.update(
        {
            "source_file_path": "table_names.csv",
            "ddl_output_format": "tsv",
            "review_output_file_type": "tsv",
            "control_table": "metadata_control_{}",
        }
    )

main(kwargs)

# COMMAND ----------

# DBTITLE 1,Verify Output
log_table = f"{catalog_name}.{schema_name}.metadata_generation_log"
count = spark.table(log_table).count()
print(f"metadata_generation_log: {count} rows")
display(spark.table(log_table).limit(5))
