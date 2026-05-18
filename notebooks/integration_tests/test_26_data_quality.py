# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 26: Data Quality Scoring
# MAGIC
# MAGIC Validates the data quality scoring pipeline: reads profiling snapshots and
# MAGIC column stats, computes per-table quality scores, and writes to
# MAGIC `data_quality_scores`.  No LLM calls -- pure computation.

# COMMAND ----------

import sys, json
from datetime import datetime
from pyspark.sql import Row

sys.path.append("../../src")

dbutils.widgets.text("catalog_name", "dev_integration_tests", "Catalog Name")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")

catalog_name = dbutils.widgets.get("catalog_name")
test_schema = dbutils.widgets.get("test_schema")
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
dq_test_schema = f"dq_test_{ts}"
fq = lambda t: f"{catalog_name}.{dq_test_schema}.{t}"

print(f"Data quality test: {catalog_name}.{dq_test_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: seed profiling + KB tables

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{dq_test_schema}")
now = datetime.now()

# profiling_snapshots: one good table, one with quality issues
spark.createDataFrame([
    Row(snapshot_id="snap-1", table_name=fq("clean_table"),
        snapshot_time=now, row_count=1000, last_modified=now),
    Row(snapshot_id="snap-2", table_name=fq("dirty_table"),
        snapshot_time=now, row_count=100, last_modified=now),
]).write.mode("overwrite").saveAsTable(fq("profiling_snapshots"))

# column_profiling_stats: clean table has good stats, dirty table has high nulls
spark.createDataFrame([
    Row(snapshot_id="snap-1", column_name="id", null_rate=0.0,
        distinct_count=1000, cardinality_ratio=1.0,
        distinct_count_change_pct=0.0, empty_string_rate=0.0,
        entropy=10.0, min_length=1, max_length=10),
    Row(snapshot_id="snap-1", column_name="name", null_rate=0.02,
        distinct_count=950, cardinality_ratio=0.95,
        distinct_count_change_pct=0.0, empty_string_rate=0.01,
        entropy=8.5, min_length=3, max_length=50),
    Row(snapshot_id="snap-2", column_name="id", null_rate=0.5,
        distinct_count=50, cardinality_ratio=0.5,
        distinct_count_change_pct=0.0, empty_string_rate=0.0,
        entropy=5.0, min_length=1, max_length=5),
    Row(snapshot_id="snap-2", column_name="value", null_rate=0.8,
        distinct_count=10, cardinality_ratio=0.1,
        distinct_count_change_pct=0.0, empty_string_rate=0.3,
        entropy=2.0, min_length=0, max_length=3),
]).write.mode("overwrite").saveAsTable(fq("column_profiling_stats"))

# table_knowledge_base
spark.createDataFrame([
    Row(table_name=fq("clean_table"), comment="Well-documented clean table",
        domain="analytics", subdomain="core", has_pii=False, has_phi=False, updated_at=now),
    Row(table_name=fq("dirty_table"), comment="",
        domain=None, subdomain=None, has_pii=None, has_phi=None, updated_at=now),
]).write.mode("overwrite").saveAsTable(fq("table_knowledge_base"))

print("Seeded profiling + KB tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run data quality scoring

# COMMAND ----------

from dbxmetagen.data_quality import compute_data_quality

result = compute_data_quality(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=dq_test_schema,
    incremental=False,
)

print(f"Data quality result: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assertions

# COMMAND ----------

scores_df = spark.sql(f"SELECT * FROM {fq('data_quality_scores')}")
score_count = scores_df.count()
assert score_count >= 2, f"Expected scores for 2 tables, got {score_count}"

scores = {r.table_name: r for r in scores_df.collect()}

clean_fqn = fq("clean_table")
dirty_fqn = fq("dirty_table")

assert clean_fqn in scores, f"Missing score for {clean_fqn}"
assert dirty_fqn in scores, f"Missing score for {dirty_fqn}"

clean_score = scores[clean_fqn].overall_score
dirty_score = scores[dirty_fqn].overall_score

print(f"Clean table score: {clean_score}")
print(f"Dirty table score: {dirty_score}")

assert 0 <= clean_score <= 100, f"Clean score out of range: {clean_score}"
assert 0 <= dirty_score <= 100, f"Dirty score out of range: {dirty_score}"
assert clean_score > dirty_score, (
    f"Clean table ({clean_score}) should score higher than dirty table ({dirty_score})"
)

# Incremental re-run should be a no-op
result2 = compute_data_quality(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=dq_test_schema,
    incremental=True,
)
print(f"Incremental re-run result: {result2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{dq_test_schema} CASCADE")
print(f"[CLEANUP] Dropped test schema: {dq_test_schema}")

# COMMAND ----------

print("=" * 60)
print("ALL DATA QUALITY INTEGRATION TESTS PASSED")
print("=" * 60)
dbutils.notebook.exit(json.dumps({"passed": True, "error": None}))
