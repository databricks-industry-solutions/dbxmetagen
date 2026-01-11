# Databricks notebook source
# MAGIC %md
# MAGIC # Build Table Knowledge Base
# MAGIC 
# MAGIC This notebook transforms the row-based `metadata_generation_log` into a table-centric 
# MAGIC `table_knowledge_base` with one row per table containing aggregated metadata from
# MAGIC comment, domain, and PI classification runs.
# MAGIC
# MAGIC ## Best Practices (DBR 17.4+ / Serverless)
# MAGIC - Uses Liquid Clustering instead of partitioning
# MAGIC - Incremental MERGE with COALESCE for partial updates
# MAGIC - Parameterized SQL queries

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------
# MAGIC %pip install -qqqq -r ../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

if not catalog_name or not schema_name:
    raise ValueError("Both catalog_name and schema_name are required")

print(f"Building knowledge base in {catalog_name}.{schema_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run ETL Pipeline

# COMMAND ----------

import sys
sys.path.append("../")

from src.dbxmetagen.knowledge_base import (
    KnowledgeBaseConfig,
    KnowledgeBaseBuilder,
    build_knowledge_base
)

# Execute the ETL pipeline
result = build_knowledge_base(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name
)

print(f"Knowledge base build complete")
print(f"Staged records: {result['staged_count']}")
print(f"Total records in knowledge base: {result['total_records']}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## View Results

# COMMAND ----------

# Show sample of the data
print("Sample records from knowledge base:")
spark.sql(f"""
SELECT table_name, domain, subdomain, has_pii, has_phi, 
       LEFT(comment, 100) as comment_preview,
       updated_at
FROM {catalog_name}.{schema_name}.table_knowledge_base
ORDER BY updated_at DESC
LIMIT 10
""").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

# Show statistics
stats = spark.sql(f"""
SELECT 
    COUNT(*) as total_tables,
    SUM(CASE WHEN has_pii THEN 1 ELSE 0 END) as tables_with_pii,
    SUM(CASE WHEN has_phi THEN 1 ELSE 0 END) as tables_with_phi,
    COUNT(DISTINCT domain) as unique_domains,
    SUM(CASE WHEN comment IS NOT NULL THEN 1 ELSE 0 END) as tables_with_comments,
    SUM(CASE WHEN domain IS NOT NULL THEN 1 ELSE 0 END) as tables_with_domain
FROM {catalog_name}.{schema_name}.table_knowledge_base
""").collect()[0]

print(f"Total tables: {stats['total_tables']}")
print(f"Tables with PII: {stats['tables_with_pii']}")
print(f"Tables with PHI: {stats['tables_with_phi']}")
print(f"Unique domains: {stats['unique_domains']}")
print(f"Tables with comments: {stats['tables_with_comments']}")
print(f"Tables with domain classification: {stats['tables_with_domain']}")
