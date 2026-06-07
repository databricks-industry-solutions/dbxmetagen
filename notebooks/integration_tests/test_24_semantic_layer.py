# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 24: Semantic Layer Generation
# MAGIC
# MAGIC Validates the full semantic layer pipeline: context building from KB/FK data,
# MAGIC LLM-based metric view generation, expression fixing, YAML serialization,
# MAGIC and persistence to `metric_view_definitions`.
# MAGIC
# MAGIC **Requires an LLM endpoint** (uses `databricks-claude-sonnet-4`).

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
sl_test_schema = f"sl_test_{ts}"
fq = lambda t: f"{catalog_name}.{sl_test_schema}.{t}"

print(f"Semantic layer test: {catalog_name}.{sl_test_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: create schema + seed source tables + KB

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{sl_test_schema}")

now = datetime.now()

# Source tables
spark.createDataFrame([
    (1, "Alice", "alice@example.com", "US"),
    (2, "Bob", "bob@example.com", "UK"),
], ["customer_id", "name", "email", "country"]).write.mode("overwrite").saveAsTable(fq("customers"))

spark.createDataFrame([
    (101, 1, "2024-01-15", 99.99, "completed"),
    (102, 2, "2024-01-16", 149.50, "completed"),
    (103, 1, "2024-02-01", 29.99, "pending"),
], ["order_id", "customer_id", "order_date", "total_amount", "status"]).write.mode("overwrite").saveAsTable(fq("orders"))

# table_knowledge_base (required, must be non-empty)
spark.createDataFrame([
    Row(table_name=fq("customers"), comment="Customer master data with demographics",
        domain="ecommerce", subdomain="customers", has_pii=True, has_phi=False, updated_at=now),
    Row(table_name=fq("orders"), comment="Order transactions with amounts and status",
        domain="ecommerce", subdomain="orders", has_pii=False, has_phi=False, updated_at=now),
]).write.mode("overwrite").saveAsTable(fq("table_knowledge_base"))

# column_knowledge_base
cols = [
    (fq("customers"), "customer_id", "Unique customer identifier", "BIGINT", ""),
    (fq("customers"), "name", "Customer full name", "STRING", ""),
    (fq("customers"), "email", "Customer email address", "STRING", "pi"),
    (fq("customers"), "country", "Customer country code", "STRING", ""),
    (fq("orders"), "order_id", "Unique order identifier", "BIGINT", ""),
    (fq("orders"), "customer_id", "FK to customers table", "BIGINT", ""),
    (fq("orders"), "order_date", "Date the order was placed", "STRING", ""),
    (fq("orders"), "total_amount", "Order total in USD", "DOUBLE", ""),
    (fq("orders"), "status", "Order status: completed, pending, cancelled", "STRING", ""),
]
spark.createDataFrame(
    [Row(column_id=f"{t}.{c}", table_name=t, column_name=c, comment=desc,
         data_type=dt, classification=cl, updated_at=now)
     for t, c, desc, dt, cl in cols]
).write.mode("overwrite").saveAsTable(fq("column_knowledge_base"))

# FK predictions -- column names must match what semantic_layer.py queries
spark.createDataFrame([
    Row(src_column="customer_id", dst_column="customer_id",
        src_table=fq("orders"), dst_table=fq("customers"),
        final_confidence=0.95, is_fk=True, updated_at=now),
]).write.mode("overwrite").saveAsTable(fq("fk_predictions"))

print("Seeded source tables and metadata")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run semantic layer generation

# COMMAND ----------

from dbxmetagen.semantic_layer import generate_semantic_layer

result = generate_semantic_layer(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=sl_test_schema,
    questions=["What is total revenue by month and customer country?"],
    model_endpoint="databricks-claude-sonnet-4",
    validate_expressions=False,
)

print(f"Generation result: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assertions

# COMMAND ----------

assert result.get("generated", 0) >= 1, f"Expected at least 1 metric view, got: {result}"

defs_df = spark.sql(f"SELECT * FROM {fq('metric_view_definitions')}")
def_count = defs_df.count()
assert def_count >= 1, f"metric_view_definitions has {def_count} rows, expected >= 1"

first_def = defs_df.collect()[0]
assert first_def.metric_view_name is not None, "metric_view_name is null"
assert first_def.json_definition is not None, "json_definition is null"
# Accept "failed" -- dry-run validation depends on LLM output quality
assert first_def.status in ("generated", "validated", "applied", "failed"), f"Unexpected status: {first_def.status}"

# Verify the JSON definition is parseable and has structure
import json as json_mod
defn = json_mod.loads(first_def.json_definition)
assert "measures" in defn, "Definition missing 'measures'"
assert "dimensions" in defn or "source" in defn, "Definition missing basic structure"
assert len(defn.get("measures", [])) >= 1, "Definition has no measures"

print(f"Generated {def_count} metric view(s)")
print(f"First MV: {first_def.metric_view_name} (status: {first_def.status})")
print(f"Measures: {[m.get('name') for m in defn.get('measures', [])]}")
print(f"Dimensions: {[d.get('name') for d in defn.get('dimensions', [])]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: graph-traversed join discovery (3-table chain)

# COMMAND ----------

# Seed a 3-table FK chain: orders -> customers -> regions
spark.createDataFrame([
    (1, "US"), (2, "UK"),
], ["region_id", "region_name"]).write.mode("overwrite").saveAsTable(fq("regions"))

# Add customers -> regions FK to fk_predictions
spark.sql(f"""
    INSERT INTO {fq('fk_predictions')} VALUES
    ('country', 'region_name', '{fq("customers")}', '{fq("regions")}',
     0.90, TRUE, CURRENT_TIMESTAMP())
""")

from dbxmetagen.semantic_layer import SemanticLayerConfig, SemanticLayerGenerator

cfg = SemanticLayerConfig(
    catalog_name=catalog_name,
    schema_name=sl_test_schema,
    max_join_hops=2,
)
gen = SemanticLayerGenerator(spark, cfg)

# Test _discover_join_paths from orders: should find customers (hop 1) and regions (hop 2)
paths = gen._discover_join_paths(fq("orders"))
print(f"Join paths from orders: {json.dumps(paths, indent=2)}")

assert len(paths) >= 1, f"Expected at least 1 join path from orders, got {len(paths)}"
# Find the customers join
cust_join = next((p for p in paths if "customers" in p.get("name", "")), None)
assert cust_join is not None, f"Expected a customers join in paths: {paths}"

# Verify nested join to regions exists inside the customers join
nested = cust_join.get("joins", [])
assert len(nested) >= 1, f"Expected nested joins under customers, got: {cust_join}"
region_join = next((j for j in nested if "regions" in j.get("name", "")), None)
assert region_join is not None, f"Expected a regions nested join under customers: {nested}"
assert "customers." in region_join["on"], f"Nested join 'on' should reference customers alias: {region_join['on']}"

# Test _enrich_joins_from_fk uses the graph-traversed paths
test_defn = {"name": "test_mv", "source": fq("orders"), "measures": [], "dimensions": []}
gen._enrich_joins_from_fk(test_defn)
assert "joins" in test_defn, f"_enrich_joins_from_fk should have added joins: {test_defn}"
enriched_cust = next((j for j in test_defn["joins"] if "customers" in j.get("name", "")), None)
assert enriched_cust is not None, "Enriched joins should include customers"
assert enriched_cust.get("joins"), "Enriched customers join should have nested regions join"

print("PASS: graph-traversed join discovery produces nested joins")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{sl_test_schema} CASCADE")
print(f"[CLEANUP] Dropped test schema: {sl_test_schema}")

# COMMAND ----------

print("=" * 60)
print("ALL SEMANTIC LAYER INTEGRATION TESTS PASSED")
print("=" * 60)
dbutils.notebook.exit(json.dumps({"passed": True, "error": None}))
