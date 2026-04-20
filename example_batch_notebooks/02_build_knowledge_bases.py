# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2: Build Knowledge Bases
# MAGIC
# MAGIC Transforms `metadata_generation_log` into structured knowledge base tables that
# MAGIC power everything downstream (graph, ontology, embeddings, Genie context).
# MAGIC
# MAGIC **Outputs:**
# MAGIC - `table_knowledge_base` -- table-level: comments, domain, PII flags
# MAGIC - `column_knowledge_base` -- column-level: comments, classifications, data types
# MAGIC - `schema_knowledge_base` -- schema-level aggregations
# MAGIC - `extended_metadata` -- constraints, lineage, additional derived metadata
# MAGIC
# MAGIC **Prerequisites:** Run `01_generate_metadata` first.

# COMMAND ----------

# MAGIC %pip install -qqq https://github.com/databricks-industry-solutions/dbxmetagen/archive/refs/heads/main.zip
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./helpers/common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Knowledge Base
# MAGIC
# MAGIC Aggregates metadata from the generation log into one row per table with the AI-generated
# MAGIC description, domain classification, and PII summary.

# COMMAND ----------

from dbxmetagen import build_knowledge_base

build_knowledge_base(spark, CATALOG, SCHEMA)
show_table("table_knowledge_base")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column Knowledge Base
# MAGIC
# MAGIC One row per column with its AI description, data type, PII classification, and domain.

# COMMAND ----------

from dbxmetagen import build_column_knowledge_base

build_column_knowledge_base(spark, CATALOG, SCHEMA)
show_table("column_knowledge_base")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Knowledge Base

# COMMAND ----------

from dbxmetagen import build_schema_knowledge_base

try:
    build_schema_knowledge_base(spark, CATALOG, SCHEMA)
    show_table("schema_knowledge_base")
except Exception as e:
    print(f"Schema KB skipped (serverless .cache() limitation): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extended Metadata
# MAGIC
# MAGIC Pulls constraints, lineage, and additional catalog metadata via `DESCRIBE EXTENDED`.

# COMMAND ----------

from dbxmetagen import extract_extended_metadata

extract_extended_metadata(spark, CATALOG, SCHEMA)
print("Extended metadata extraction complete")
