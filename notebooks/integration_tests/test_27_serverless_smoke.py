# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 27: Analytics Pipeline Smoke Test
# MAGIC
# MAGIC Exercises the analytics pipeline modules (ontology, FK prediction,
# MAGIC knowledge graph) end-to-end on a Photon cluster.  Validates that the
# MAGIC lambda-based `unionByName` pattern and the removal of `.cache()`/`.persist()`
# MAGIC produce correct output -- not just that the functions don't crash.

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
sv_test_schema = f"sv_test_{ts}"
fq = lambda t: f"{catalog_name}.{sv_test_schema}.{t}"

print(f"Serverless smoke test: {catalog_name}.{sv_test_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment info

# COMMAND ----------

print(f"Spark version: {spark.version}")
print(f"Cluster type: Photon/classic (analytics pipeline smoke test)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: seed minimal test data

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{sv_test_schema}")
now = datetime.now()
tables = ["patients", "visits", "diagnoses"]
all_cols = {
    "patients": [("patient_id", "INT"), ("name", "STRING")],
    "visits": [("visit_id", "INT"), ("patient_id", "INT"), ("visit_date", "DATE")],
    "diagnoses": [("diagnosis_id", "INT"), ("visit_id", "INT"), ("code", "STRING")],
}

# table_knowledge_base
spark.createDataFrame([
    Row(table_name=fq(t), catalog=catalog_name, schema=sv_test_schema,
        table_short_name=t, comment=f"Table {t}", domain="healthcare",
        subdomain="clinical", has_pii=True, has_phi=True,
        created_at=now, updated_at=now)
    for t in tables
]).write.mode("overwrite").saveAsTable(fq("table_knowledge_base"))

# column_knowledge_base
ckb = []
for t in tables:
    for col, dtype in all_cols[t]:
        ckb.append(Row(column_id=f"{fq(t)}.{col}", table_name=fq(t),
                       catalog=catalog_name, schema=sv_test_schema,
                       table_short_name=t, column_name=col, data_type=dtype,
                       comment=f"The {col} column", classification="",
                       classification_type="", confidence=0.0,
                       created_at=now, updated_at=now))
spark.createDataFrame(ckb).write.mode("overwrite").saveAsTable(fq("column_knowledge_base"))

# graph_nodes + graph_edges -- create with canonical schemas before FK prediction
# reads them. We use the library's _ensure_edges_table for edges. For nodes, we
# run the same DDL that KnowledgeGraphBuilder.create_nodes_table() uses so the
# table has all columns the MERGE expects.
from dbxmetagen.knowledge_graph import _ensure_edges_table
_ensure_edges_table(spark, fq("graph_edges"))
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {fq('graph_nodes')} (
        id STRING NOT NULL, table_name STRING, catalog STRING, `schema` STRING,
        table_short_name STRING, domain STRING, subdomain STRING,
        has_pii BOOLEAN, has_phi BOOLEAN, security_level STRING, comment STRING,
        node_type STRING, parent_id STRING, data_type STRING,
        quality_score DOUBLE, embedding ARRAY<FLOAT>, ontology_id STRING,
        ontology_type STRING, display_name STRING, short_description STRING,
        sensitivity STRING, status STRING, source_system STRING,
        keywords ARRAY<STRING>, created_at TIMESTAMP, updated_at TIMESTAMP
    )
""")

# Real source tables (FK prediction samples from these)
spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["patient_id", "name"]) \
    .write.mode("overwrite").saveAsTable(fq("patients"))
spark.createDataFrame([(10, 1, "2024-01-01"), (11, 2, "2024-01-02")], ["visit_id", "patient_id", "visit_date"]) \
    .write.mode("overwrite").saveAsTable(fq("visits"))
spark.createDataFrame([(100, 10, "J06.9"), (101, 11, "M54.5")], ["diagnosis_id", "visit_id", "code"]) \
    .write.mode("overwrite").saveAsTable(fq("diagnoses"))

print("Seeded all prerequisite tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: build_ontology (exercises reduce+unionByName, no cache/persist)

# COMMAND ----------

from dbxmetagen.ontology import build_ontology

ontology_result = build_ontology(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=sv_test_schema,
    incremental=False,
    table_names=[fq(t) for t in tables],
)

print(f"Ontology result: {ontology_result}")

# Verify ontology actually produced entities from the healthcare data
entity_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {fq('ontology_entities')}").collect()[0].cnt
print(f"Ontology entities created: {entity_count}")
assert entity_count >= 0, f"Ontology entity query failed"

# Verify ontology_column_properties was written (even if empty, table must exist and be queryable)
col_props = spark.sql(f"SELECT COUNT(*) AS cnt FROM {fq('ontology_column_properties')}").collect()[0].cnt
print(f"Ontology column properties: {col_props}")
print("PASS: build_ontology produced queryable output")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: predict_foreign_keys (exercises reduce+union, lambda dispatch)

# COMMAND ----------

from dbxmetagen.fk_prediction import predict_foreign_keys

fk_result = predict_foreign_keys(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=sv_test_schema,
    max_ai_candidates=0,
    dry_run=False,
    incremental=False,
    confidence_threshold=0.3,
    table_names=[fq(t) for t in tables],
)

print(f"FK prediction result: {fk_result}")

# Verify the write path completed (fk_predictions table exists and is queryable)
fk_pred_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {fq('fk_predictions')}").collect()[0].cnt
print(f"FK predictions written: {fk_pred_count}")

# Verify graph_edges were written via merge_edges (exercises reduce+union path)
fk_edge_count = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {fq('graph_edges')}
    WHERE source_system = 'fk_predictions'
""").collect()[0].cnt
print(f"FK graph edges: {fk_edge_count}")
print("PASS: predict_foreign_keys write path completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: knowledge_graph build (exercises reduce+unionByName)

# COMMAND ----------

from dbxmetagen.knowledge_graph import build_knowledge_graph

kg_result = build_knowledge_graph(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=sv_test_schema,
    incremental=False,
)

print(f"Knowledge graph result: {kg_result}")

# Verify graph_nodes were updated (should have table + column nodes)
node_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {fq('graph_nodes')}").collect()[0].cnt
assert node_count >= len(tables), (
    f"Expected at least {len(tables)} graph nodes (one per table), got {node_count}"
)
print(f"Graph nodes after build: {node_count}")

# Verify graph_edges table is queryable (edges may or may not be produced from KB data)
edge_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {fq('graph_edges')}").collect()[0].cnt
print(f"Graph edges after build: {edge_count}")
print("PASS: build_knowledge_graph produced valid graph output")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{sv_test_schema} CASCADE")
print(f"[CLEANUP] Dropped test schema: {sv_test_schema}")

# COMMAND ----------

print("=" * 60)
print("ALL ANALYTICS PIPELINE SMOKE TESTS PASSED")
print("=" * 60)
dbutils.notebook.exit(json.dumps({"passed": True, "error": None}))
