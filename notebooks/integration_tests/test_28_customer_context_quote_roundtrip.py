# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: customer_context quote round-trip (issue #212)
# MAGIC
# MAGIC Reproduces the exact app persistence path -- the **Statement Execution API** on a SQL
# MAGIC warehouse (NOT `spark.sql`) -- and proves operator-supplied context text with quotes,
# MAGIC apostrophes, enumerated-value quote-lists (`'tw'`, `'nw'`) and backslashes round-trips
# MAGIC **byte-exact** when values are bound as parameters (the fix).
# MAGIC
# MAGIC It also runs a **negative control**: the old `''`-doubling escape, inlined into the SQL
# MAGIC text the way the buggy handler did, and asserts the quote is silently dropped. That both
# MAGIC documents the mechanism and fails loudly if anyone reverts the fix to `''` -- or if a
# MAGIC future Databricks release changes `''` handling (at which point we can revisit).
# MAGIC
# MAGIC Config (env, with fallbacks):
# MAGIC - `DATABRICKS_WAREHOUSE_ID` -- SQL warehouse to run against (else first available)
# MAGIC - `TEST_CATALOG` / `TEST_SCHEMA` -- where the throwaway table is created (default: current)

# COMMAND ----------

import os
import time
import uuid
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

w = WorkspaceClient()


def _warehouse_id() -> str:
    wid = os.environ.get("DATABRICKS_WAREHOUSE_ID") or os.environ.get("WAREHOUSE_ID")
    if wid:
        return wid
    # Prefer a RUNNING warehouse, else the first one listed.
    whs = list(w.warehouses.list())
    assert whs, "No SQL warehouse available; set DATABRICKS_WAREHOUSE_ID"
    running = [x for x in whs if str(getattr(x.state, "value", x.state)).upper() == "RUNNING"]
    return (running or whs)[0].id


WAREHOUSE_ID = _warehouse_id()
CATALOG = os.environ.get("TEST_CATALOG", "main")
SCHEMA = os.environ.get("TEST_SCHEMA", "default")
TABLE = f"{CATALOG}.{SCHEMA}.mg212_cc_it_{uuid.uuid4().hex[:8]}"
print(f"warehouse={WAREHOUSE_ID} table={TABLE}")


def run_sql(stmt: str, parameters=None, timeout_s: int = 60):
    """Execute one statement via the Statement Execution API (the app's exact path)."""
    resp = w.statement_execution.execute_statement(
        statement=stmt, warehouse_id=WAREHOUSE_ID, wait_timeout="30s",
        parameters=parameters,
    )
    deadline = time.time() + timeout_s
    while resp.status and resp.status.state and resp.status.state.value in ("PENDING", "RUNNING"):
        assert time.time() < deadline, f"statement timed out: {stmt[:80]}"
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)
    state = resp.status.state.value if resp.status and resp.status.state else "UNKNOWN"
    assert state == "SUCCEEDED", f"statement {state}: {getattr(resp.status, 'error', None)} :: {stmt[:120]}"
    if resp.result and resp.result.data_array:
        return resp.result.data_array
    return []


# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: throwaway table

# COMMAND ----------

run_sql(
    f"CREATE OR REPLACE TABLE {TABLE} "
    f"(context_id STRING, scope STRING, scope_type STRING, context_text STRING, "
    f"context_label STRING, priority INT, active BOOLEAN, created_by STRING, "
    f"created_at TIMESTAMP, updated_at TIMESTAMP) USING DELTA"
)
print("created")

# The text that broke in the field: possessive apostrophe + enumerated quote-list + backslash.
SENT_TEXT = "'tw' = this week, 'nw' = next week; the segment's 1976 benchmark. path C:\\a\\b"
SENT_LABEL = "it's a label"

try:
    # ------------------------------------------------------------------
    # POSITIVE: the fix -- bind every value as a parameter (mirrors
    # api_server._build_customer_context_upsert).
    # ------------------------------------------------------------------
    now = "2026-09-01T00:00:00"
    params = [
        StatementParameterListItem(name="ctx_id", value="pos1"),
        StatementParameterListItem(name="scope", value="cat.sch.*"),
        StatementParameterListItem(name="scope_type", value="pattern"),
        StatementParameterListItem(name="context_text", value=SENT_TEXT),
        StatementParameterListItem(name="context_label", value=SENT_LABEL),
        StatementParameterListItem(name="priority", value="0"),
        StatementParameterListItem(name="now", value=now),
    ]
    run_sql(
        f"MERGE INTO {TABLE} AS tgt USING (SELECT :ctx_id AS context_id) AS src "
        f"ON tgt.context_id = src.context_id "
        f"WHEN MATCHED THEN UPDATE SET scope=:scope, scope_type=:scope_type, "
        f"context_text=:context_text, context_label=:context_label, "
        f"priority=CAST(:priority AS INT), active=TRUE, updated_at=CAST(:now AS TIMESTAMP) "
        f"WHEN NOT MATCHED THEN INSERT (context_id, scope, scope_type, context_text, "
        f"context_label, priority, active, created_by, created_at, updated_at) VALUES "
        f"(:ctx_id, :scope, :scope_type, :context_text, :context_label, "
        f"CAST(:priority AS INT), TRUE, 'app', CAST(:now AS TIMESTAMP), CAST(:now AS TIMESTAMP))",
        parameters=params,
    )
    rows = run_sql(
        f"SELECT context_text, context_label FROM {TABLE} WHERE context_id = :cid",
        parameters=[StatementParameterListItem(name="cid", value="pos1")],
    )
    stored_text, stored_label = rows[0][0], rows[0][1]
    assert stored_text == SENT_TEXT, (
        f"PARAM round-trip NOT byte-exact:\n  sent  = {SENT_TEXT!r} (len {len(SENT_TEXT)})\n"
        f"  stored= {stored_text!r} (len {len(stored_text)})"
    )
    assert stored_label == SENT_LABEL, f"label mismatch: {stored_label!r}"
    print(f"PASS positive: parameterized round-trip byte-exact (len {len(stored_text)})")

    # ------------------------------------------------------------------
    # NEGATIVE CONTROL: the OLD bug -- '' doubling inlined into SQL text.
    # This MUST drop the apostrophes. If it ever stops dropping them, the
    # environment changed and this test should be revisited.
    # ------------------------------------------------------------------
    escaped = SENT_TEXT.replace("'", "''")  # what the buggy handler did
    run_sql(
        f"INSERT INTO {TABLE} (context_id, context_text, priority, active, created_by) "
        f"VALUES ('neg1', '{escaped}', 0, TRUE, 'app')"
    )
    rows = run_sql(
        f"SELECT context_text FROM {TABLE} WHERE context_id = :cid",
        parameters=[StatementParameterListItem(name="cid", value="neg1")],
    )
    neg_stored = rows[0][0]
    assert neg_stored != SENT_TEXT, (
        "NEGATIVE CONTROL FAILED: '' doubling round-tripped byte-exact. Databricks '' handling "
        "may have changed -- revisit whether parameter binding is still required."
    )
    assert len(neg_stored) < len(SENT_TEXT), (
        f"expected '' escaping to DROP quotes (shorter), got len {len(neg_stored)} "
        f">= sent {len(SENT_TEXT)}: {neg_stored!r}"
    )
    dropped = len(SENT_TEXT) - len(neg_stored)
    print(f"PASS negative control: '' doubling dropped {dropped} char(s) -> {neg_stored!r}")

finally:
    run_sql(f"DROP TABLE IF EXISTS {TABLE}")
    print("cleaned up")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result

# COMMAND ----------

print("TEST PASSED: customer_context quote round-trip (issue #212)")
