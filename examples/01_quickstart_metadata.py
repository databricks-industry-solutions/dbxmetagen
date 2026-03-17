# Databricks notebook source
# MAGIC %md
# MAGIC # dbxmetagen Quickstart
# MAGIC
# MAGIC Generate table/column descriptions, classify sensitive information (PII/PHI/PCI),
# MAGIC or classify tables into business domains -- all with a single function call.
# MAGIC
# MAGIC **Prerequisites:** A Databricks workspace with Unity Catalog enabled and
# MAGIC access to a foundation model endpoint (e.g. `databricks-claude-sonnet-4-6`).

# COMMAND ----------
# MAGIC %pip install -qqq dbxmetagen
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration
# MAGIC Fill in the widgets above after running this cell.

# COMMAND ----------
dbutils.widgets.text("catalog_name", "", "Catalog Name (required)")
dbutils.widgets.text("table_names", "", "Table Names - comma separated (required)")
dbutils.widgets.dropdown("mode", "comment", ["comment", "pi", "domain"], "Mode")
dbutils.widgets.text("schema_name", "metadata_results", "Output Schema")
dbutils.widgets.dropdown("apply_ddl", "false", ["true", "false"], "Apply DDL")
dbutils.widgets.text("model", "databricks-claude-sonnet-4-6", "Model Endpoint")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run

# COMMAND ----------
from dbxmetagen.main import main

main({
    "catalog_name": dbutils.widgets.get("catalog_name"),
    "schema_name": dbutils.widgets.get("schema_name"),
    "table_names": dbutils.widgets.get("table_names"),
    "mode": dbutils.widgets.get("mode"),
    "apply_ddl": dbutils.widgets.get("apply_ddl"),
    "model": dbutils.widgets.get("model"),
    "table_names_source": "parameter",
})
