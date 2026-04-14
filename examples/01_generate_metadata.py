# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1: Generate Metadata
# MAGIC
# MAGIC Runs AI-powered metadata generation on your Unity Catalog tables. This step runs
# MAGIC **all three modes** to produce the richest possible metadata for downstream analytics
# MAGIC and Genie space creation:
# MAGIC
# MAGIC 1. **comment** -- Table and column descriptions using LLM analysis of data samples
# MAGIC 2. **pi** -- PII/PHI/PCI classification for every column
# MAGIC 3. **domain** -- Business domain classification for each table
# MAGIC
# MAGIC Running all modes is what gives Genie spaces their context: descriptions power the
# MAGIC instructions, PII flags inform access guardrails, and domain classification drives
# MAGIC ontology grouping and space auto-splitting.
# MAGIC
# MAGIC **Output:** `metadata_generation_log` table in your output schema.
# MAGIC
# MAGIC **What happens under the hood:** For each table, dbxmetagen samples rows, chunks
# MAGIC columns into groups of 20, builds a prompt with DDL + sample data, calls the LLM,
# MAGIC and persists structured results. The whole process is incremental -- re-running
# MAGIC skips tables that already have metadata for that mode.

# COMMAND ----------

# MAGIC %pip install -qqq https://github.com/databricks-industry-solutions/dbxmetagen/archive/refs/heads/main.zip
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./helpers/common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve Tables

# COMMAND ----------

from dbxmetagen.main import main

tables = resolve_tables(TABLE_NAMES, CATALOG, SCHEMA)
print(f"Resolved {len(tables)} tables")
for t in tables:
    print(f"  {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run All Three Modes
# MAGIC
# MAGIC Each mode appends to the same `metadata_generation_log` table. The `incremental=true`
# MAGIC flag means re-running is safe -- tables already processed for a given mode are skipped.
# MAGIC
# MAGIC If you only need one mode, change the `MODES` list below.

# COMMAND ----------

MODES = ["comment", "pi", "domain"]

for mode in MODES:
    print(f"\n{'='*60}")
    print(f"Running mode: {mode}")
    print(f"{'='*60}")

    main({
        "catalog_name": CATALOG,
        "schema_name": SCHEMA,
        "table_names": ",".join(tables),
        "mode": mode,
        "model": MODEL,
        "apply_ddl": "false",
        "table_names_source": "parameter",
        "incremental": "true",
        "volume_name": VOLUME,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Output

# COMMAND ----------

show_table("metadata_generation_log")
