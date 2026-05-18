# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 23: FK Prediction
# MAGIC
# MAGIC Validates the FK prediction pipeline: candidate generation, heuristic scoring,
# MAGIC graph edge writes, and prediction table output.  Uses `max_ai_candidates=0`
# MAGIC to avoid LLM calls while still exercising the full write path.

# COMMAND ----------

import sys, json
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import *

sys.path.append("../../src")

dbutils.widgets.text("catalog_name", "dev_integration_tests", "Catalog Name")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")

catalog_name = dbutils.widgets.get("catalog_name")
test_schema = dbutils.widgets.get("test_schema")
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
fk_test_schema = f"fk_test_{ts}"

print(f"FK prediction test: {catalog_name}.{fk_test_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: create schema + seed tables

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{fk_test_schema}")

# Real tables with obvious FK relationships -- let saveAsTable create schema
spark.createDataFrame([
    (1, "Alice", "alice@example.com"),
    (2, "Bob", "bob@example.com"),
    (3, "Carol", "carol@example.com"),
], ["customer_id", "name", "email"]).write.mode("overwrite").saveAsTable(
    f"{catalog_name}.{fk_test_schema}.customers"
)

spark.createDataFrame([
    (101, 1, "2024-01-15", 99.99),
    (102, 2, "2024-01-16", 149.50),
    (103, 1, "2024-02-01", 29.99),
    (104, 3, "2024-02-10", 250.00),
], ["order_id", "customer_id", "order_date", "total_amount"]).write.mode("overwrite").saveAsTable(
    f"{catalog_name}.{fk_test_schema}.orders"
)

spark.createDataFrame([
    (1001, 101, "Widget A", 2),
    (1002, 101, "Widget B", 1),
    (1003, 102, "Widget A", 3),
    (1004, 103, "Widget C", 1),
], ["item_id", "order_id", "product_name", "quantity"]).write.mode("overwrite").saveAsTable(
    f"{catalog_name}.{fk_test_schema}.order_items"
)

print("Seeded 3 source tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed prerequisite metadata tables

# COMMAND ----------

now = datetime.now()
fq = lambda t: f"{catalog_name}.{fk_test_schema}.{t}"
tables = ["customers", "orders", "order_items"]
all_cols = {
    "customers": [("customer_id", "INT"), ("name", "STRING"), ("email", "STRING")],
    "orders": [("order_id", "INT"), ("customer_id", "INT"), ("order_date", "DATE"), ("total_amount", "DOUBLE")],
    "order_items": [("item_id", "INT"), ("order_id", "INT"), ("product_name", "STRING"), ("quantity", "INT")],
}

# graph_nodes: table + column nodes
node_rows = []
for tbl in tables:
    tbl_fqn = f"{catalog_name}.{fk_test_schema}.{tbl}"
    node_rows.append(Row(id=tbl_fqn, parent_id=f"{catalog_name}.{fk_test_schema}",
                         node_type="table", label=tbl, data_type=None, updated_at=now))
    for col, dtype in all_cols[tbl]:
        node_rows.append(Row(id=f"{tbl_fqn}.{col}", parent_id=tbl_fqn,
                             node_type="column", label=col, data_type=dtype, updated_at=now))

spark.createDataFrame(node_rows).write.mode("overwrite").saveAsTable(fq("graph_nodes"))

# graph_edges: empty with canonical schema (merge_edges requires all EDGE_SCHEMA columns)
from dbxmetagen.knowledge_graph import _ensure_edges_table
_ensure_edges_table(spark, fq("graph_edges"))

# column_knowledge_base: minimal
ckb_rows = []
for tbl in tables:
    tbl_fqn = f"{catalog_name}.{fk_test_schema}.{tbl}"
    for col, dtype in all_cols[tbl]:
        ckb_rows.append(Row(column_id=f"{tbl_fqn}.{col}", table_name=tbl_fqn,
                            column_name=col, comment=f"The {col} column", updated_at=now))
spark.createDataFrame(ckb_rows).write.mode("overwrite").saveAsTable(fq("column_knowledge_base"))

# table_knowledge_base
tkb_rows = [Row(table_name=f"{catalog_name}.{fk_test_schema}.{t}",
                comment=f"Table {t}", domain="ecommerce", subdomain="orders",
                has_pii=False, has_phi=False, updated_at=now) for t in tables]
spark.createDataFrame(tkb_rows).write.mode("overwrite").saveAsTable(fq("table_knowledge_base"))

# Do NOT pre-create ontology tables -- FK prediction guards against missing
# tables via try/except. A dummy-schema table would pass the existence check
# but fail on column resolution.

print("Seeded metadata prerequisite tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run FK prediction (no AI)

# COMMAND ----------

from dbxmetagen.fk_prediction import predict_foreign_keys

result = predict_foreign_keys(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=fk_test_schema,
    max_ai_candidates=0,
    dry_run=False,
    incremental=False,
    confidence_threshold=0.3,
    table_names=[f"{catalog_name}.{fk_test_schema}.{t}" for t in tables],
)

print(f"FK prediction result: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assertions

# COMMAND ----------

pred_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {fq('fk_predictions')}").collect()[0].cnt
print(f"FK predictions written: {pred_count}")
assert pred_count > 0, (
    f"FK prediction produced 0 predictions despite obvious FK relationships "
    f"(orders.customer_id -> customers.customer_id). Result: {result}"
)

preds_df = spark.sql(f"SELECT src_column, dst_column, final_confidence FROM {fq('fk_predictions')} ORDER BY final_confidence DESC")
preds_df.show(truncate=False)

# The test data has matching column names across tables (customer_id, order_id);
# the heuristic scorer must find at least one of these.
name_match = spark.sql(f"""
    SELECT * FROM {fq('fk_predictions')}
    WHERE (src_column LIKE '%customer_id%' AND dst_column LIKE '%customer_id%')
       OR (src_column LIKE '%order_id%' AND dst_column LIKE '%order_id%')
""").count()
print(f"Name-matched FK candidates: {name_match}")
assert name_match > 0, "Expected at least one name-matched FK pair (customer_id or order_id)"

# FK predictions must write graph edges with source_system='fk_predictions'
fk_edges = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {fq('graph_edges')}
    WHERE source_system = 'fk_predictions'
""").collect()[0].cnt
print(f"FK graph edges written: {fk_edges}")
assert fk_edges > 0, "FK prediction should write at least one graph edge"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{fk_test_schema} CASCADE")
print(f"[CLEANUP] Dropped test schema: {fk_test_schema}")

# COMMAND ----------

print("=" * 60)
print("ALL FK PREDICTION INTEGRATION TESTS PASSED")
print("=" * 60)
dbutils.notebook.exit(json.dumps({"passed": True, "error": None}))
