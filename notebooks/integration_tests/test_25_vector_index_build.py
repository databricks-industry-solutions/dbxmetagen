# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 25: Vector Index Document Population
# MAGIC
# MAGIC Validates the `metadata_documents` Delta table build: table docs, column docs,
# MAGIC and metric-view 3-tier decomposition (summary, measures, schema).
# MAGIC
# MAGIC Tests only the document population step (`build_documents_table`), not VS
# MAGIC endpoint creation or index sync, to avoid long endpoint provisioning waits.

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
vi_test_schema = f"vi_test_{ts}"
fq = lambda t: f"{catalog_name}.{vi_test_schema}.{t}"

print(f"Vector index doc build test: {catalog_name}.{vi_test_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: seed prerequisite tables

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{vi_test_schema}")
now = datetime.now()

# table_knowledge_base -- must include table_short_name, catalog, schema (used by vector_index table_docs SQL)
spark.createDataFrame([
    Row(table_name=fq("orders"), table_short_name="orders", catalog=catalog_name, schema=vi_test_schema,
        comment="Order transactions", domain="ecommerce", subdomain="orders",
        has_pii=False, has_phi=False, updated_at=now),
    Row(table_name=fq("customers"), table_short_name="customers", catalog=catalog_name, schema=vi_test_schema,
        comment="Customer master data", domain="ecommerce", subdomain="customers",
        has_pii=True, has_phi=False, updated_at=now),
]).write.mode("overwrite").saveAsTable(fq("table_knowledge_base"))

# column_knowledge_base -- must include data_type, classification, classification_type, catalog, schema
spark.createDataFrame([
    Row(column_id=f"{fq('orders')}.order_id", table_name=fq("orders"),
        catalog=catalog_name, schema=vi_test_schema,
        column_name="order_id", comment="Unique order identifier",
        data_type="INT", classification="", classification_type="", updated_at=now),
    Row(column_id=f"{fq('orders')}.total_amount", table_name=fq("orders"),
        catalog=catalog_name, schema=vi_test_schema,
        column_name="total_amount", comment="Order total in USD",
        data_type="DOUBLE", classification="", classification_type="", updated_at=now),
    Row(column_id=f"{fq('customers')}.customer_id", table_name=fq("customers"),
        catalog=catalog_name, schema=vi_test_schema,
        column_name="customer_id", comment="Unique customer identifier",
        data_type="INT", classification="", classification_type="", updated_at=now),
]).write.mode("overwrite").saveAsTable(fq("column_knowledge_base"))

# graph_nodes (needed by document builder)
spark.createDataFrame([
    Row(id=fq("orders"), parent_id=f"{catalog_name}.{vi_test_schema}",
        node_type="table", label="orders", data_type=None, updated_at=now),
    Row(id=f"{fq('orders')}.order_id", parent_id=fq("orders"),
        node_type="column", label="order_id", data_type="INT", updated_at=now),
    Row(id=fq("customers"), parent_id=f"{catalog_name}.{vi_test_schema}",
        node_type="table", label="customers", data_type=None, updated_at=now),
]).write.mode("overwrite").saveAsTable(fq("graph_nodes"))

# Metric view definitions (to test 3-tier decomposition)
import json as json_mod
mv_defn = json_mod.dumps({
    "source": fq("orders"),
    "comment": "Revenue metrics",
    "measures": [
        {"name": "total_revenue", "expr": "SUM(total_amount)", "comment": "Sum of order amounts",
         "format": {"type": "currency", "currency_code": "USD"}},
        {"name": "order_count", "expr": "COUNT(*)", "comment": "Number of orders",
         "format": {"type": "number"}},
    ],
    "dimensions": [
        {"name": "order_month", "expr": "DATE_TRUNC('MONTH', order_date)", "comment": "Order month"},
    ],
})
spark.createDataFrame([
    Row(definition_id="test-mv-1", metric_view_name="revenue_metrics",
        source_table=fq("orders"), json_definition=mv_defn,
        source_questions="What is total revenue?", status="applied",
        validation_errors=None, genie_space_id=None,
        created_at=now, applied_at=None,
        deployed_catalog=catalog_name, deployed_schema=vi_test_schema),
], schema="definition_id STRING, metric_view_name STRING, source_table STRING, json_definition STRING, source_questions STRING, status STRING, validation_errors STRING, genie_space_id STRING, created_at TIMESTAMP, applied_at TIMESTAMP, deployed_catalog STRING, deployed_schema STRING"
).write.mode("overwrite").saveAsTable(fq("metric_view_definitions"))

# The table_docs and column_docs SQL have correlated subqueries to these optional tables.
# Unlike the UNION ALL parts (guarded by tableExists), correlated subqueries fail at
# analysis time if the table doesn't exist. Create empty tables with the required columns.
for ddl in [
    f"""CREATE TABLE IF NOT EXISTS {fq("ontology_entities")} (
        entity_id STRING, entity_type STRING, entity_name STRING,
        entity_role STRING, description STRING, source_tables ARRAY<STRING>,
        confidence FLOAT)""",
    f"""CREATE TABLE IF NOT EXISTS {fq("ontology_relationships")} (
        src_entity_type STRING, relationship_name STRING, dst_entity_type STRING)""",
    f"""CREATE TABLE IF NOT EXISTS {fq("ontology_column_properties")} (
        column_name STRING, property_role STRING, linked_entity_type STRING,
        table_name STRING, owning_entity_id STRING)""",
    f"""CREATE TABLE IF NOT EXISTS {fq("fk_predictions")} (
        src_table STRING, src_column STRING, dst_table STRING, dst_column STRING,
        final_confidence DOUBLE, pk_uniqueness DOUBLE, ai_reasoning STRING,
        is_fk BOOLEAN)""",
    f"""CREATE TABLE IF NOT EXISTS {fq("profiling_snapshots")} (
        table_name STRING, row_count BIGINT, snapshot_time TIMESTAMP)""",
]:
    spark.sql(ddl)

print("Seeded all prerequisite tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run document population only

# COMMAND ----------

from dbxmetagen.vector_index import VectorIndexConfig, VectorIndexBuilder

config = VectorIndexConfig(
    catalog_name=catalog_name,
    schema_name=vi_test_schema,
    endpoint_name="dbxmetagen-test-unused",
)
builder = VectorIndexBuilder(config=config, spark=spark, incremental=False)
doc_count = builder.build_documents_table()

print(f"Documents built: {doc_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assertions

# COMMAND ----------

docs_df = spark.sql(f"SELECT * FROM {fq('metadata_documents')}")
total_docs = docs_df.count()
assert total_docs > 0, "metadata_documents is empty"

doc_types = [r.doc_type for r in docs_df.select("doc_type").distinct().collect()]
print(f"Document types found: {doc_types}")

assert "table" in doc_types, "'table' doc_type missing"
assert "column" in doc_types, "'column' doc_type missing"

# Check 3-tier metric view documents
mv_types = [dt for dt in doc_types if dt.startswith("metric_view_")]
print(f"Metric view doc types: {mv_types}")
assert "metric_view_summary" in doc_types, "'metric_view_summary' doc_type missing"
assert "metric_view_measures" in doc_types, "'metric_view_measures' doc_type missing"
assert "metric_view_schema" in doc_types, "'metric_view_schema' doc_type missing"

# Verify content is non-empty
empty_content = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {fq('metadata_documents')}
    WHERE content IS NULL OR TRIM(content) = ''
""").collect()[0].cnt
assert empty_content == 0, f"{empty_content} documents have empty content"

# Verify doc_id is deterministic (re-run should not duplicate)
builder2 = VectorIndexBuilder(config=config, spark=spark, incremental=False)
builder2.build_documents_table()
total_after_rerun = spark.sql(f"SELECT COUNT(*) AS cnt FROM {fq('metadata_documents')}").collect()[0].cnt
assert total_after_rerun == total_docs, f"Re-run changed doc count: {total_docs} -> {total_after_rerun}"

print(f"All assertions passed: {total_docs} docs, {len(doc_types)} types, idempotent re-run confirmed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{vi_test_schema} CASCADE")
print(f"[CLEANUP] Dropped test schema: {vi_test_schema}")

# COMMAND ----------

print("=" * 60)
print("ALL VECTOR INDEX DOCUMENT BUILD TESTS PASSED")
print("=" * 60)
dbutils.notebook.exit(json.dumps({"passed": True, "error": None}))
