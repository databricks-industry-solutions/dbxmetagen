# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2: Build Knowledge Bases
# MAGIC
# MAGIC Transforms `metadata_generation_log` into structured knowledge base tables:
# MAGIC - `table_knowledge_base` (table-level: comments, domain, PII flags)
# MAGIC - `column_knowledge_base` (column-level: comments, classifications, data types)
# MAGIC - `schema_knowledge_base` (schema-level aggregations)
# MAGIC - `extended_metadata` (constraints, lineage, additional derived metadata)
# MAGIC
# MAGIC **Prerequisites:** Run `01_generate_metadata` first.

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
dbutils.widgets.text("install_source", os.getenv("METAGEN_INSTALL_SOURCE", "git+https://github.com/databricks-industry-solutions/dbxmetagen.git@main"))

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

spark = SparkSession.builder.getOrCreate()
print(f"Building knowledge bases in {catalog_name}.{schema_name}")

# COMMAND ----------

# DBTITLE 1,Build Table Knowledge Base
from dbxmetagen import build_knowledge_base

result = build_knowledge_base(spark, catalog_name, schema_name)
print(f"Table KB: {result}")
display(spark.table(f"{catalog_name}.{schema_name}.table_knowledge_base").limit(5))

# COMMAND ----------

# DBTITLE 1,Build Column Knowledge Base
from dbxmetagen import build_column_knowledge_base

build_column_knowledge_base(spark, catalog_name, schema_name)
count = spark.table(f"{catalog_name}.{schema_name}.column_knowledge_base").count()
print(f"Column KB: {count} rows")

# COMMAND ----------

# DBTITLE 1,Build Schema Knowledge Base
from dbxmetagen import build_schema_knowledge_base

try:
    build_schema_knowledge_base(spark, catalog_name, schema_name)
    count = spark.table(f"{catalog_name}.{schema_name}.schema_knowledge_base").count()
    print(f"Schema KB: {count} rows")
except Exception as e:
    print(f"Schema KB skipped (serverless .cache() limitation): {e}")

# COMMAND ----------

# DBTITLE 1,Extract Extended Metadata
from dbxmetagen import extract_extended_metadata

extract_extended_metadata(spark, catalog_name, schema_name)
print("Extended metadata extraction complete")
