# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: customer_context PRODUCTION builder, live (issue #212)
# MAGIC
# MAGIC Companion to `test_28`. Where test_28 runs a **hand-written, known-good** MERGE (an
# MAGIC independent oracle for the technique), THIS test imports and executes the **actual shipped
# MAGIC builder** `api_server._build_customer_context_upsert(...)` against a real warehouse and
# MAGIC asserts a byte-exact round-trip.
# MAGIC
# MAGIC **Why both:** if test_29 FAILS while test_28 PASSES, the divergence is in the production
# MAGIC builder (or its SQL), not the technique — and the SQL printed below can be diffed directly
# MAGIC against test_28's known-good MERGE to localize the change. This closes the drift gap where
# MAGIC test_28's independent copy could pass while the shipped code silently changed.
# MAGIC
# MAGIC Config (env, with fallbacks):
# MAGIC - `DATABRICKS_WAREHOUSE_ID` -- SQL warehouse (else first available)
# MAGIC - `TEST_CATALOG` / `TEST_SCHEMA` -- where the throwaway table is created (default main/default)

# COMMAND ----------

import os
import time
import uuid

# fq() in api_server reads CATALOG/SCHEMA from env AT IMPORT TIME, so set them BEFORE importing.
TEST_CATALOG = os.environ.get("TEST_CATALOG", "main")
TEST_SCHEMA = os.environ.get("TEST_SCHEMA", "default")
os.environ["CATALOG_NAME"] = TEST_CATALOG
os.environ["SCHEMA_NAME"] = TEST_SCHEMA
os.environ.setdefault("WAREHOUSE_ID", os.environ.get("DATABRICKS_WAREHOUSE_ID", ""))

# Put the app dir on the path (git-clone / DAB layout) and import the REAL module.
import sys
_APP_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "apps", "dbxmetagen-app", "app")
if os.path.isdir(_APP_DIR):
    sys.path.insert(0, _APP_DIR)

try:
    import api_server  # noqa: E402  -- the shipped app module
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementParameterListItem
except Exception as exc:  # app deps absent (e.g. lib-only cluster) -> skip, don't fail
    print(f"SKIP: could not import api_server (app deps missing?): {type(exc).__name__}: {exc}")
    api_server = None

# COMMAND ----------

if api_server is not None:
    w = WorkspaceClient()

    def _warehouse_id() -> str:
        wid = os.environ.get("DATABRICKS_WAREHOUSE_ID") or os.environ.get("WAREHOUSE_ID")
        if wid:
            return wid
        whs = list(w.warehouses.list())
        assert whs, "No SQL warehouse available; set DATABRICKS_WAREHOUSE_ID"
        running = [x for x in whs if str(getattr(x.state, "value", x.state)).upper() == "RUNNING"]
        return (running or whs)[0].id

    WAREHOUSE_ID = _warehouse_id()

    def run_sql(stmt, parameters=None, timeout_s=60):
        resp = w.statement_execution.execute_statement(
            statement=stmt, warehouse_id=WAREHOUSE_ID, wait_timeout="30s", parameters=parameters,
        )
        deadline = time.time() + timeout_s
        while resp.status and resp.status.state and resp.status.state.value in ("PENDING", "RUNNING"):
            assert time.time() < deadline, f"timed out: {stmt[:80]}"
            time.sleep(2)
            resp = w.statement_execution.get_statement(resp.statement_id)
        state = resp.status.state.value if resp.status and resp.status.state else "UNKNOWN"
        assert state == "SUCCEEDED", f"{state}: {getattr(resp.status, 'error', None)} :: {stmt[:120]}"
        return (resp.result.data_array if resp.result and resp.result.data_array else [])

    # Redirect the builder at a throwaway table (fq(_CC_TABLE) reads this module global at call time).
    api_server._CC_TABLE = f"mg212_prod_it_{uuid.uuid4().hex[:8]}"
    TABLE = f"`{TEST_CATALOG}`.`{TEST_SCHEMA}`.`{api_server._CC_TABLE}`"
    print(f"warehouse={WAREHOUSE_ID} table={TABLE} builder=api_server._build_customer_context_upsert")

    run_sql(
        f"CREATE OR REPLACE TABLE {TABLE} "
        f"(context_id STRING, scope STRING, scope_type STRING, context_text STRING, "
        f"context_label STRING, priority INT, active BOOLEAN, created_by STRING, "
        f"created_at TIMESTAMP, updated_at TIMESTAMP) USING DELTA"
    )

    # Real runtime timestamp shape (utcnow().isoformat(), microseconds) -- the value the route passes.
    from datetime import datetime
    NOW = datetime.utcnow().isoformat()
    SENT_TEXT = "'tw' = this week, 'nw' = next week; the segment's 1976 benchmark. path C:\\a\\b"
    SENT_LABEL = "it's a label"
    SENT_SCOPE = "cat.sch.*"

    try:
        # === Execute the ACTUAL shipped builder's output ===
        sql, params = api_server._build_customer_context_upsert(
            "prodrow", SENT_SCOPE, "pattern", SENT_TEXT, SENT_LABEL, 7, NOW,
        )
        # Print what the shipped builder produced -> diff target vs test_28's known-good MERGE.
        print("--- SQL emitted by api_server._build_customer_context_upsert ---")
        print(sql.strip())
        print("--- params ---")
        print([(p.name, p.value) for p in params])

        run_sql(sql, parameters=params)

        rows = run_sql(
            f"SELECT context_text, context_label, scope, priority FROM {TABLE} WHERE context_id = :cid",
            parameters=[StatementParameterListItem(name="cid", value="prodrow")],
        )
        stored_text, stored_label, stored_scope, stored_pri = rows[0]

        assert stored_text == SENT_TEXT, (
            f"PRODUCTION builder round-trip NOT byte-exact:\n  sent  = {SENT_TEXT!r} (len {len(SENT_TEXT)})\n"
            f"  stored= {stored_text!r} (len {len(stored_text)})\n"
            "  -> compare the SQL printed above against test_28's known-good MERGE to localize the drift."
        )
        assert stored_label == SENT_LABEL, f"label mismatch: {stored_label!r}"
        assert stored_scope == SENT_SCOPE, f"scope mismatch: {stored_scope!r}"
        assert str(stored_pri) == "7", f"priority mismatch: {stored_pri!r}"
        print(f"PASS: shipped builder round-trips byte-exact (text len {len(stored_text)}, priority CAST ok)")
    finally:
        run_sql(f"DROP TABLE IF EXISTS {TABLE}")
        print("cleaned up")

    print("TEST PASSED: customer_context PRODUCTION builder live round-trip (issue #212)")
