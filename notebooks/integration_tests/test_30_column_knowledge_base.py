# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 30: Column Knowledge Base
# MAGIC
# MAGIC Runs the real `build_column_knowledge_base` (a single MERGE that pivots
# MAGIC `metadata_generation_log` and joins data types from
# MAGIC `system.information_schema.columns`) against fixture tables, then checks:
# MAGIC
# MAGIC 1. one row per (table, column), table-level rows excluded
# MAGIC 2. latest comment and latest PI classification win
# MAGIC 3. `data_type` / `nullable` resolved from information_schema, matched case-insensitively
# MAGIC 4. a column of a table that does not exist still lands, with NULL type
# MAGIC 5. re-running with no new log rows changes nothing
# MAGIC 6. a steward edit survives a re-run unless the log has newer content
# MAGIC 7. `table_names` scoping leaves out-of-scope columns untouched
# MAGIC
# MAGIC Uses its own schema and always drops it.

# COMMAND ----------

import sys
sys.path.append("../../src")  # For git-clone or DAB deployment; pip-installed package works without this

import uuid
from datetime import datetime, timedelta

from dbxmetagen.column_knowledge_base import build_column_knowledge_base

dbutils.widgets.text("catalog_name", "dev_integration_tests", "Catalog Name")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")
catalog_name = dbutils.widgets.get("catalog_name")

schema = f"ckb_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
fq = lambda name: f"{catalog_name}.{schema}.{name}"
print(f"Test schema: {catalog_name}.{schema}")

# COMMAND ----------

LOG_COLS = ("metadata_type", "table", "ddl_type", "column_name", "_created_at",
            "column_content", "classification", "type", "confidence")


def log(rows):
    """Append rows (dicts keyed by LOG_COLS) to the fixture metadata_generation_log."""
    df = spark.createDataFrame(
        [tuple(r.get(c) for c in LOG_COLS) for r in rows],
        "metadata_type STRING, `table` STRING, ddl_type STRING, column_name STRING, "
        "_created_at TIMESTAMP, column_content STRING, classification STRING, "
        "type STRING, confidence DOUBLE",
    )
    df.write.mode("append").saveAsTable(fq("metadata_generation_log"))


def comment(table, column, text, at):
    return {"metadata_type": "comment", "table": table, "ddl_type": "column",
            "column_name": column, "_created_at": at, "column_content": text}


def kb():
    """column_id -> row dict of the current column KB."""
    return {r["column_id"]: r.asDict() for r in spark.table(fq("column_knowledge_base")).collect()}


def cid(table_short, column):
    return f"{catalog_name}.{schema}.{table_short}.{column}".lower()


T1, T2, T3 = datetime(2024, 1, 1), datetime(2024, 2, 1), datetime(2024, 3, 1)

# COMMAND ----------

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}")
    spark.sql(f"CREATE TABLE {fq('orders')} (order_id BIGINT NOT NULL, amount DECIMAL(10,2), note STRING)")
    spark.sql(f"CREATE TABLE {fq('customers')} (customer_id STRING, email STRING)")

    # The log refers to orders in UPPER case: the KB must lowercase it and still
    # join to information_schema, which stores identifiers in lowercase.
    orders_mixed_case = f"{catalog_name}.{schema}.ORDERS"
    log([
        comment(orders_mixed_case, "order_id", "Order id (old)", T1),
        comment(orders_mixed_case, "order_id", "Order id (latest)", T2),
        comment(orders_mixed_case, "amount", "Order amount", T1),
        comment(orders_mixed_case, "note", "Free-text note", T1),
        comment(fq("customers"), "email", "Customer email", T1),
        {"metadata_type": "pi", "table": fq("customers"), "ddl_type": "column", "column_name": "email",
         "_created_at": T1, "classification": "None", "type": "None", "confidence": 0.2},
        {"metadata_type": "pi", "table": fq("customers"), "ddl_type": "column", "column_name": "email",
         "_created_at": T2, "classification": "pi", "type": "pii", "confidence": 0.9},
        comment(fq("ghost_table"), "ghost_col", "Column of a table that does not exist", T1),
        # Table-level row: must not become a column.
        {"metadata_type": "comment", "table": fq("customers"), "ddl_type": "table",
         "column_name": None, "_created_at": T1, "column_content": "Customers table"},
    ])

    # --- 1-4: first build ---------------------------------------------------
    build_column_knowledge_base(spark, catalog_name, schema)
    rows = kb()
    expected_ids = {cid("orders", "order_id"), cid("orders", "amount"), cid("orders", "note"),
                    cid("customers", "email"), cid("ghost_table", "ghost_col")}
    assert set(rows) == expected_ids, f"[1] column ids {sorted(rows)} != {sorted(expected_ids)}"
    print("[1] PASSED: one row per column, table-level row excluded")

    oid = rows[cid("orders", "order_id")]
    assert oid["comment"] == "Order id (latest)", f"[2] latest comment should win, got {oid['comment']!r}"
    email = rows[cid("customers", "email")]
    assert (email["classification"], email["classification_type"]) == ("pi", "pii"), \
        f"[2] latest PI should win, got {email['classification']!r}/{email['classification_type']!r}"
    print("[2] PASSED: latest comment and latest PI classification win")

    assert oid["table_short_name"] == "orders", f"[3] table name not lowercased: {oid['table_short_name']!r}"
    assert (oid["data_type"] or "").upper().startswith("BIGINT"), f"[3] order_id data_type {oid['data_type']!r}"
    assert oid["nullable"] is False, f"[3] order_id is NOT NULL, got nullable={oid['nullable']!r}"
    amount = rows[cid("orders", "amount")]
    assert (amount["data_type"] or "").upper().startswith("DECIMAL"), f"[3] amount data_type {amount['data_type']!r}"
    assert rows[cid("orders", "note")]["nullable"] is True, "[3] note should be nullable"
    print("[3] PASSED: data_type/nullable resolved from information_schema (case-insensitive join)")

    ghost = rows[cid("ghost_table", "ghost_col")]
    assert ghost["comment"] and ghost["data_type"] is None and ghost["nullable"] is None, \
        f"[4] ghost column should land with NULL type, got {ghost}"
    print("[4] PASSED: column of a missing table lands with NULL type")

    # --- 5: idempotent re-run ------------------------------------------------
    build_column_knowledge_base(spark, catalog_name, schema)
    rerun = kb()
    assert rerun == rows, "[5] re-running with no new log rows changed the KB"
    print("[5] PASSED: re-run with no new log rows changes nothing")

    # --- 6: steward edits ----------------------------------------------------
    spark.sql(f"UPDATE {fq('column_knowledge_base')} SET comment = 'Steward text', "
              f"review_updated_at = current_timestamp() WHERE column_id = '{cid('orders', 'order_id')}'")
    log([comment(orders_mixed_case, "order_id", "Older generated text", T3)])  # older than the review
    build_column_knowledge_base(spark, catalog_name, schema)
    assert kb()[cid("orders", "order_id")]["comment"] == "Steward text", \
        "[6] steward edit was overwritten by log content older than the review"
    newer = datetime.now() + timedelta(days=1)
    log([comment(orders_mixed_case, "order_id", "Regenerated after review", newer)])
    build_column_knowledge_base(spark, catalog_name, schema)
    assert kb()[cid("orders", "order_id")]["comment"] == "Regenerated after review", \
        "[6] log content newer than the review should replace the steward edit"
    print("[6] PASSED: steward edit kept until newer content arrives")

    # --- 7: table_names scoping ----------------------------------------------
    log([comment(orders_mixed_case, "note", "Note (out of scope update)", newer),
         comment(fq("customers"), "email", "Customer email (in scope update)", newer)])
    build_column_knowledge_base(spark, catalog_name, schema, table_names=[fq("customers")])
    scoped = kb()
    assert scoped[cid("customers", "email")]["comment"] == "Customer email (in scope update)", \
        "[7] in-scope column was not updated"
    assert scoped[cid("orders", "note")]["comment"] == "Free-text note", \
        "[7] out-of-scope column was updated by a table_names-scoped build"
    print("[7] PASSED: table_names scoping leaves other tables untouched")

    print("\nAll column knowledge base checks passed")
finally:
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema} CASCADE")
    print(f"Dropped {catalog_name}.{schema}")
