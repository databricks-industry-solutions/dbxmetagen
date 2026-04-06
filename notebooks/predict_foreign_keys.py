# Databricks notebook source
# MAGIC %md
# MAGIC # Predict Foreign Keys
# MAGIC
# MAGIC Block-aware join-key discovery: embedding similarity with duplicate-table
# MAGIC suppression, declared/query/name/ontology signals, budgeted AI_QUERY.

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("column_similarity_threshold", "0.85", "Column Similarity Threshold")
dbutils.widgets.text("table_similarity_threshold", "0.9", "Deprecated (ignored); kept for job compat")
dbutils.widgets.text("duplicate_table_similarity_threshold", "0.97", "Suppress same-block near-duplicate tables")
dbutils.widgets.text("cross_block_column_similarity_min", "0.92", "Min col similarity for cross-schema embedding pairs")
dbutils.widgets.text("cross_block_strict", "true", "Cross-block stricter floor (true/false)")
dbutils.widgets.text("ontology_cross_block", "false", "Allow ontology pairs across schemas (true/false)")
dbutils.widgets.text("skip_ai_for_declared_fk", "false", "Skip AI_QUERY for declared FKs (true/false)")
dbutils.widgets.text("skip_ai_query_min_observations", "0", "Min query-history hits to skip AI (0=off)")
dbutils.widgets.text("confidence_threshold", "0.7", "Confidence Threshold")
dbutils.widgets.text("sample_size", "5", "Sample Size")
dbutils.widgets.text("apply_ddl", "false", "Apply DDL")
dbutils.widgets.text("dry_run", "false", "Dry Run (count only, no AI calls)")
dbutils.widgets.text("incremental", "true", "Incremental embedding path only (true/false)")
dbutils.widgets.text("max_ai_candidates", "200", "Max rows sent to AI_QUERY")
dbutils.widgets.text("rule_score_min_for_ai", "0.50", "Min rule score to qualify for AI judge")
dbutils.widgets.text("max_candidates_per_table_pair", "5", "Max candidates per table pair (name/ontology)")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
column_similarity_threshold = float(dbutils.widgets.get("column_similarity_threshold"))
table_similarity_threshold = float(dbutils.widgets.get("table_similarity_threshold"))
duplicate_table_similarity_threshold = float(dbutils.widgets.get("duplicate_table_similarity_threshold"))
cross_block_column_similarity_min = float(dbutils.widgets.get("cross_block_column_similarity_min"))
cross_block_strict = dbutils.widgets.get("cross_block_strict").lower() == "true"
ontology_cross_block = dbutils.widgets.get("ontology_cross_block").lower() == "true"
skip_ai_for_declared_fk = dbutils.widgets.get("skip_ai_for_declared_fk").lower() == "true"
skip_ai_query_min_observations = int(dbutils.widgets.get("skip_ai_query_min_observations"))
confidence_threshold = float(dbutils.widgets.get("confidence_threshold"))
sample_size = int(dbutils.widgets.get("sample_size"))
apply_ddl = dbutils.widgets.get("apply_ddl").lower() == "true"
dry_run = dbutils.widgets.get("dry_run").lower() == "true"
incremental = dbutils.widgets.get("incremental").lower() == "true"
max_ai_candidates = int(dbutils.widgets.get("max_ai_candidates"))
rule_score_min_for_ai = float(dbutils.widgets.get("rule_score_min_for_ai"))
max_candidates_per_table_pair = int(dbutils.widgets.get("max_candidates_per_table_pair"))

if not catalog_name or not schema_name:
    raise ValueError("Both catalog_name and schema_name are required")

print(f"Predicting foreign keys in {catalog_name}.{schema_name}")
print(f"Column similarity threshold: {column_similarity_threshold}")
print(f"Duplicate table similarity threshold: {duplicate_table_similarity_threshold}")
print(f"Cross-block column similarity min: {cross_block_column_similarity_min}")
print(f"Confidence threshold: {confidence_threshold}")
print(f"Apply DDL: {apply_ddl}")
print(f"Dry run: {dry_run}")
print(f"Incremental (embedding path): {incremental}")
print(f"Max AI candidates: {max_ai_candidates}")
print(f"Rule score min for AI: {rule_score_min_for_ai}")
print(f"Max candidates per table pair: {max_candidates_per_table_pair}")

# COMMAND ----------

import sys
sys.path.append("../src")

from dbxmetagen.fk_prediction import predict_foreign_keys

result = predict_foreign_keys(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    column_similarity_threshold=column_similarity_threshold,
    table_similarity_threshold=table_similarity_threshold,
    duplicate_table_similarity_threshold=duplicate_table_similarity_threshold,
    cross_block_column_similarity_min=cross_block_column_similarity_min,
    cross_block_strict=cross_block_strict,
    ontology_cross_block=ontology_cross_block,
    skip_ai_for_declared_fk=skip_ai_for_declared_fk,
    skip_ai_query_min_observations=skip_ai_query_min_observations,
    confidence_threshold=confidence_threshold,
    sample_size=sample_size,
    apply_ddl=apply_ddl,
    dry_run=dry_run,
    incremental=incremental,
    max_ai_candidates=max_ai_candidates,
    rule_score_min_for_ai=rule_score_min_for_ai,
    max_candidates_per_table_pair=max_candidates_per_table_pair,
)

print("FK prediction complete")
if result.get("dry_run"):
    print("  DRY RUN - no AI calls made")
    print(f"  Candidates found: {result.get('candidates', 0)}")
    print(f"  Rows that would be sent to AI_QUERY: {result.get('ai_query_rows', 0)}")
else:
    print(f"  Candidates evaluated: {result.get('candidates', 0)}")
    print(f"  Rows sent to AI_QUERY: {result.get('ai_query_rows', 0)}")
    print(f"  Predictions written: {result.get('predictions', 0)}")
    print(f"  Graph edges created: {result.get('edges', 0)}")
    print(f"  DDL applied: {result.get('ddl_applied', 0)}")
