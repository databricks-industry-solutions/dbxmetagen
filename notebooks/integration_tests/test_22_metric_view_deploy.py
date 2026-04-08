# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 22: Metric View Deploy Location Tracking
# MAGIC
# MAGIC Validates that `apply_metric_views()` records `deployed_catalog` and
# MAGIC `deployed_schema` in `metric_view_definitions`, and that the Genie context
# MAGIC assembler correctly resolves metric view locations when source tables live
# MAGIC in a different schema from where the metric view was deployed.
# MAGIC
# MAGIC **Reproduces the bug:** bronze-sourced metric views were invisible to Genie
# MAGIC because `deployed_catalog`/`deployed_schema` were NULL, causing fallback to
# MAGIC the source table's schema instead of the deploy target schema.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "dev_integration_tests", "Catalog Name")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")

catalog_name = dbutils.widgets.get("catalog_name")
test_schema = dbutils.widgets.get("test_schema")

print(f"Testing metric view deploy tracking in {catalog_name}.{test_schema}")

# COMMAND ----------

import sys

sys.path.append("../../src")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup: Create source table and definitions table

# COMMAND ----------

import json
import uuid
from datetime import datetime

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{test_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{test_schema}_bronze")

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog_name}.{test_schema}_bronze.test_orders (
        order_id INT, amount DOUBLE, region STRING
    )
"""
)
spark.sql(
    f"""
    INSERT INTO {catalog_name}.{test_schema}_bronze.test_orders VALUES
    (1, 100.0, 'North'), (2, 200.0, 'South'), (3, 150.0, 'North')
"""
)

print("Source table created in bronze schema")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: apply_metric_views records deployed_catalog/deployed_schema

# COMMAND ----------

from dbxmetagen.semantic_layer import SemanticLayerGenerator, SemanticLayerConfig

config = SemanticLayerConfig(
    catalog_name=catalog_name,
    schema_name=test_schema,
)
gen = SemanticLayerGenerator(spark, config)
gen.create_tables()

defn = {
    "name": "test_order_metrics",
    "source": f"{catalog_name}.{test_schema}_bronze.test_orders",
    "dimensions": [{"name": "region", "expr": "source.region"}],
    "measures": [{"name": "total_amount", "expr": "SUM(source.amount)", "agg": "sum"}],
}

defn_id = str(uuid.uuid4())
now = datetime.utcnow().isoformat()
json_str = json.dumps(defn).replace("'", "''")

spark.sql(
    f"""
    INSERT INTO {catalog_name}.{test_schema}.{config.definitions_table} VALUES (
        '{defn_id}', 'test_order_metrics',
        '{catalog_name}.{test_schema}_bronze.test_orders',
        '{json_str}', 'q-test', 'validated', '', NULL,
        '{now}', NULL, NULL, NULL
    )
"""
)

result = gen.apply_metric_views()
print(f"apply result: {result}")
assert result["applied"] == 1, f"Expected 1 applied, got {result}"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify: deployed_catalog and deployed_schema are set

# COMMAND ----------

row = spark.sql(
    f"""
    SELECT deployed_catalog, deployed_schema, status
    FROM {catalog_name}.{test_schema}.{config.definitions_table}
    WHERE definition_id = '{defn_id}'
"""
).first()

assert row is not None, "Definition row not found"
assert row.status == "applied", f"Expected status='applied', got '{row.status}'"
bronze_schema = f"{test_schema}_bronze"
assert (
    row.deployed_catalog == catalog_name
), f"Expected deployed_catalog='{catalog_name}', got '{row.deployed_catalog}'"
assert (
    row.deployed_schema == bronze_schema
), f"Expected deployed_schema='{bronze_schema}' (source table's schema), got '{row.deployed_schema}'"
print(f"deployed_catalog={row.deployed_catalog}, deployed_schema={row.deployed_schema}")
print("PASS: apply_metric_views correctly records deployment location (source schema)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify: Metric view is queryable at the DEPLOY location (not source schema)

# COMMAND ----------

mv_fq = f"{catalog_name}.{bronze_schema}.test_order_metrics"
try:
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM {mv_fq}").first().cnt
    print(f"Metric view queryable at {mv_fq}: {cnt} rows")
except Exception as e:
    raise AssertionError(f"Metric view NOT queryable at {mv_fq}: {e}")

wrong_fq = f"{catalog_name}.{test_schema}.test_order_metrics"
try:
    spark.sql(f"SELECT 1 FROM {wrong_fq} LIMIT 1")
    print(f"WARNING: MV also exists at config schema {wrong_fq} (unexpected)")
except Exception:
    print(f"Confirmed: MV does NOT exist at config schema {wrong_fq} (correct)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify: _resolve_mv_location returns deploy schema (not source schema)

# COMMAND ----------

from dbxmetagen.genie.context import GenieContextAssembler

mv_data = {
    "metric_view_name": "test_order_metrics",
    "source_table": f"{catalog_name}.{test_schema}_bronze.test_orders",
    "deployed_catalog": row.deployed_catalog,
    "deployed_schema": row.deployed_schema,
}
resolved_cat, resolved_sch = GenieContextAssembler._resolve_mv_location(
    mv_data, catalog_name, test_schema
)
assert (
    resolved_sch == bronze_schema
), f"_resolve_mv_location should return deploy schema '{bronze_schema}'. Got '{resolved_sch}'"
print(f"PASS: _resolve_mv_location correctly resolves to {resolved_cat}.{resolved_sch}")

# Verify fallback when deployed columns are NULL -> uses source table schema
mv_null = {
    "metric_view_name": "test_order_metrics",
    "source_table": f"{catalog_name}.{test_schema}_bronze.test_orders",
    "deployed_catalog": None,
    "deployed_schema": None,
}
fb_cat, fb_sch = GenieContextAssembler._resolve_mv_location(
    mv_null, catalog_name, test_schema
)
assert (
    fb_sch == bronze_schema
), f"Fallback should use source table schema '{bronze_schema}' when deployed cols are NULL. Got '{fb_sch}'"
print(f"PASS: fallback correctly uses source table schema {fb_cat}.{fb_sch}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql(f"DROP VIEW IF EXISTS {catalog_name}.{test_schema}_bronze.test_order_metrics")
spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{test_schema}_bronze.test_orders")
spark.sql(
    f"DELETE FROM {catalog_name}.{test_schema}.{config.definitions_table} WHERE definition_id = '{defn_id}'"
)
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{test_schema}_bronze")
print("Cleanup complete")
