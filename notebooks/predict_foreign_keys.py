# Databricks notebook source
# MAGIC %md
# MAGIC # Predict Foreign Keys
# MAGIC
# MAGIC Uses column embedding similarity, table similarity filtering, sample value
# MAGIC comparison, rule-based scoring, and AI judgment to predict FK relationships.

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("column_similarity_threshold", "0.85", "Column Similarity Threshold")
dbutils.widgets.text("table_similarity_threshold", "0.9", "Table Similarity Threshold (max; exclude above)")
dbutils.widgets.text("confidence_threshold", "0.7", "Confidence Threshold")
dbutils.widgets.text("sample_size", "5", "Sample Size")
dbutils.widgets.text("model_endpoint", "databricks-gpt-oss-120b", "Model Endpoint")
dbutils.widgets.text("apply_ddl", "false", "Apply DDL")
dbutils.widgets.text("dry_run", "false", "Dry Run (count only, no AI calls)")
dbutils.widgets.text("incremental", "true", "Incremental (true/false)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
column_similarity_threshold = float(dbutils.widgets.get("column_similarity_threshold"))
table_similarity_threshold = float(dbutils.widgets.get("table_similarity_threshold"))
confidence_threshold = float(dbutils.widgets.get("confidence_threshold"))
sample_size = int(dbutils.widgets.get("sample_size"))
model_endpoint = dbutils.widgets.get("model_endpoint")
apply_ddl = dbutils.widgets.get("apply_ddl").lower() == "true"
dry_run = dbutils.widgets.get("dry_run").lower() == "true"
incremental = dbutils.widgets.get("incremental").lower() == "true"

if not catalog_name or not schema_name:
    raise ValueError("Both catalog_name and schema_name are required")

print(f"Predicting foreign keys in {catalog_name}.{schema_name}")
print(f"Column similarity threshold: {column_similarity_threshold}")
print(f"Table similarity threshold (exclude above): {table_similarity_threshold}")
print(f"Confidence threshold: {confidence_threshold}")
print(f"Apply DDL: {apply_ddl}")
print(f"Dry run: {dry_run}")
print(f"Incremental: {incremental}")

# COMMAND ----------

import sys
sys.path.append("../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.fk_prediction import predict_foreign_keys

result = predict_foreign_keys(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    column_similarity_threshold=column_similarity_threshold,
    table_similarity_threshold=table_similarity_threshold,
    confidence_threshold=confidence_threshold,
    sample_size=sample_size,
    model_endpoint=model_endpoint,
    apply_ddl=apply_ddl,
    dry_run=dry_run,
    incremental=incremental,
)

print(f"FK prediction complete")
if result.get("dry_run"):
    print(f"  DRY RUN - no AI calls made")
    print(f"  Candidates found: {result.get('candidates', 0)}")
    print(f"  Rows that would be sent to AI_QUERY: {result.get('ai_query_rows', 0)}")
else:
    print(f"  Candidates evaluated: {result.get('candidates', 0)}")
    print(f"  Rows sent to AI_QUERY: {result.get('ai_query_rows', 0)}")
    print(f"  Predictions written: {result.get('predictions', 0)}")
    print(f"  Graph edges created: {result.get('edges', 0)}")
    print(f"  DDL applied: {result.get('ddl_applied', 0)}")

