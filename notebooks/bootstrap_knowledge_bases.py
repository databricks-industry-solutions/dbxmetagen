# Databricks notebook source
# MAGIC %md
# MAGIC # Bootstrap Knowledge Bases
# MAGIC
# MAGIC Populates `table_knowledge_base` and `column_knowledge_base` directly from
# MAGIC `information_schema` -- pure SQL, zero LLM calls.  This enables profiling
# MAGIC and extended-metadata extraction to run *before* comment generation so that
# MAGIC comments are produced once with full context.

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("table_names", "", "Comma-separated FQNs or wildcards (e.g. cat.schema.*)")
dbutils.widgets.text("catalog_name", "", "Output catalog for KB tables")
dbutils.widgets.text("schema_name", "", "Output schema for KB tables")
dbutils.widgets.dropdown("populate_log", "true", ["true", "false"], "Also write to metadata_generation_log")

table_names_raw = dbutils.widgets.get("table_names")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
populate_log = dbutils.widgets.get("populate_log").lower() == "true"

if not table_names_raw or not catalog_name or not schema_name:
    raise ValueError("table_names, catalog_name, and schema_name are all required")

print(f"Bootstrapping KB in {catalog_name}.{schema_name} for: {table_names_raw}")
print(f"Populate metadata_generation_log: {populate_log}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Resolve Table Names

# COMMAND ----------

import sys
sys.path.append("../src")

from dbxmetagen.processing import expand_schema_wildcards, ensure_fully_scoped_table_names

raw_list = [t.strip() for t in table_names_raw.split(",") if t.strip()]
expanded = expand_schema_wildcards(raw_list)
resolved = ensure_fully_scoped_table_names(expanded, catalog_name)

print(f"Resolved {len(resolved)} tables")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bootstrap

# COMMAND ----------

from dbxmetagen.knowledge_base import KnowledgeBaseConfig, KnowledgeBaseBuilder
from dbxmetagen.column_knowledge_base import ColumnKnowledgeBaseConfig, ColumnKnowledgeBaseBuilder

kb_config = KnowledgeBaseConfig(catalog_name=catalog_name, schema_name=schema_name)
kb_builder = KnowledgeBaseBuilder(spark, kb_config)
tbl_count = kb_builder.bootstrap(resolved)
print(f"Table KB: {tbl_count} rows merged")

ckb_config = ColumnKnowledgeBaseConfig(catalog_name=catalog_name, schema_name=schema_name)
ckb_builder = ColumnKnowledgeBaseBuilder(spark, ckb_config)
col_count = ckb_builder.bootstrap(resolved)
print(f"Column KB: {col_count} rows merged")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Populate metadata_generation_log (optional)

# COMMAND ----------

tbl_log_count = 0
col_log_count = 0
if populate_log:
    tbl_log_count = kb_builder.log_bootstrap_to_generation_log(resolved)
    col_log_count = ckb_builder.log_bootstrap_to_generation_log(resolved)
    print(f"Log: {tbl_log_count} table rows + {col_log_count} column rows written to metadata_generation_log")
else:
    print("Skipping metadata_generation_log population (populate_log=false)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"Bootstrap complete -- {tbl_count} tables, {col_count} columns")
if populate_log:
    print(f"Log: {tbl_log_count} table + {col_log_count} column rows")
spark.sql(f"SELECT COUNT(*) AS total FROM {catalog_name}.{schema_name}.table_knowledge_base").display()
spark.sql(f"SELECT COUNT(*) AS total FROM {catalog_name}.{schema_name}.column_knowledge_base").display()
