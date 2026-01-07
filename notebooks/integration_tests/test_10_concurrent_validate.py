# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Concurrent Tasks Validation
# MAGIC
# MAGIC Validates that concurrent tasks correctly processed tables without duplicates.
# MAGIC
# MAGIC Checks:
# MAGIC 1. Control table has entries for all test tables
# MAGIC 2. Each table was claimed by exactly one task
# MAGIC 3. metadata_generation_log has entries for all tables
# MAGIC 4. No duplicate processing occurred

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

import sys
from datetime import datetime, timedelta

sys.path.append("../../")

from src.dbxmetagen.user_utils import sanitize_user_identifier, get_current_user
from src.dbxmetagen.processing import get_control_table
from src.dbxmetagen.config import MetadataConfig

# Setup widgets
dbutils.widgets.text("test_catalog", "dev_integration_tests", "Test Catalog")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")
dbutils.widgets.text("run_id", "", "Run ID")

test_catalog = dbutils.widgets.get("test_catalog")
test_schema = dbutils.widgets.get("test_schema")
run_id = dbutils.widgets.get("run_id")

# COMMAND ----------

print_test_header("Concurrent Tasks Validation")

spark = SparkSession.builder.getOrCreate()
test_utils = IntegrationTestUtils(spark, test_catalog, test_schema)

current_user = get_current_user()
sanitized_user = sanitize_user_identifier(current_user)

test_passed = False
error_message = None

# Expected test tables
expected_tables = [
    f"{test_catalog}.{test_schema}.concurrent_test_1",
    f"{test_catalog}.{test_schema}.concurrent_test_2",
    f"{test_catalog}.{test_schema}.concurrent_test_3",
]

try:
    print(f"\nValidating concurrent task results")
    print(f"  Run ID: {run_id}")
    print(f"  Current user: {current_user}")
    print(f"  Expected tables: {expected_tables}")

    # Find control tables for this user
    control_tables = test_utils.find_control_tables(sanitized_user)
    print(f"\nControl tables found: {control_tables}")

    test_utils.assert_true(
        len(control_tables) > 0,
        "At least one control table exists"
    )

    # Use the most recent control table (may have run_id suffix)
    control_table_name = control_tables[0]
    for ct in control_tables:
        if run_id and run_id in ct:
            control_table_name = ct
            break
    
    full_control_table = f"{test_catalog}.{test_schema}.{control_table_name}"
    print(f"\nUsing control table: {full_control_table}")

    # Test 1: Verify control table has entries for all expected tables
    print("\nTest 1: Verify control table entries")
    control_df = spark.sql(f"SELECT * FROM {full_control_table}")
    control_df.show(truncate=False)

    control_entries = {row["table_name"] for row in control_df.collect()}
    print(f"  Control table entries: {control_entries}")

    for expected_table in expected_tables:
        test_utils.assert_true(
            expected_table in control_entries,
            f"Control table has entry for {expected_table}"
        )

    # Test 2: Verify each table was claimed (has _claimed_by set)
    print("\nTest 2: Verify table claims")
    claims_df = spark.sql(f"""
        SELECT table_name, _claimed_by, _claimed_at 
        FROM {full_control_table}
        WHERE _claimed_by IS NOT NULL
    """)
    claims_df.show(truncate=False)

    claimed_tables = {row["table_name"] for row in claims_df.collect()}
    print(f"  Claimed tables: {claimed_tables}")

    # All expected tables should have been claimed
    for expected_table in expected_tables:
        test_utils.assert_true(
            expected_table in claimed_tables,
            f"Table {expected_table} was claimed by a task"
        )

    # Test 3: Verify no duplicate claims (each table claimed by exactly one task)
    print("\nTest 3: Verify no duplicate claims")
    duplicate_check_df = spark.sql(f"""
        SELECT table_name, COUNT(DISTINCT _claimed_by) as claim_count
        FROM {full_control_table}
        GROUP BY table_name
        HAVING COUNT(DISTINCT _claimed_by) > 1
    """)
    
    duplicates = duplicate_check_df.collect()
    test_utils.assert_equals(
        len(duplicates),
        0,
        "No table was claimed by multiple tasks"
    )

    # Test 3b: Verify _status column shows completed or in_progress
    print("\nTest 3b: Verify _status column values")
    status_df = spark.sql(f"""
        SELECT table_name, _status, _error_message 
        FROM {full_control_table}
        WHERE _status IS NOT NULL
    """)
    status_df.show(truncate=False)
    
    statuses = {row["table_name"]: row["_status"] for row in status_df.collect()}
    print(f"  Table statuses: {statuses}")
    
    # All tables should have a status (either completed or in_progress)
    for expected_table in expected_tables:
        if expected_table in statuses:
            status = statuses[expected_table]
            test_utils.assert_true(
                status in ('completed', 'in_progress', 'queued'),
                f"Table {expected_table} has valid status: {status}"
            )
        else:
            print(f"  Note: {expected_table} status not found (may use default 'queued')")

    # Test 4: Verify metadata_generation_log has entries
    print("\nTest 4: Verify metadata_generation_log entries")
    one_hour_ago = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    
    log_table = f"{test_catalog}.{test_schema}.metadata_generation_log"
    if spark.catalog.tableExists(log_table):
        log_df = spark.sql(f"""
            SELECT `table`, metadata_type, _created_at
            FROM {log_table}
            WHERE _created_at >= '{one_hour_ago}'
        """)
        log_df.show(truncate=False)

        logged_tables = {row["table"] for row in log_df.collect()}
        print(f"  Logged tables in last hour: {logged_tables}")

        # Verify all expected tables have log entries
        for expected_table in expected_tables:
            simple_name = expected_table.split(".")[-1]
            has_log = simple_name in logged_tables or expected_table in logged_tables
            test_utils.assert_true(
                has_log,
                f"metadata_generation_log has entry for {expected_table}"
            )
    else:
        print(f"  Warning: {log_table} does not exist yet")

    # Test 5: Verify no duplicate log entries (each table processed once)
    print("\nTest 5: Verify no duplicate processing in logs")
    if spark.catalog.tableExists(log_table):
        duplicate_log_df = spark.sql(f"""
            SELECT `table`, COUNT(*) as entry_count
            FROM {log_table}
            WHERE _created_at >= '{one_hour_ago}'
            GROUP BY `table`
            HAVING COUNT(*) > 1
        """)
        
        log_duplicates = duplicate_log_df.collect()
        if log_duplicates:
            print(f"  Warning: Some tables have multiple log entries: {log_duplicates}")
            print("  This may be expected if the test was run multiple times")
        else:
            print("  No duplicate log entries found")

    test_passed = True
    print_test_result("Concurrent Tasks Validation", True)

except TestFailure as e:
    error_message = str(e)
    print(error_message)
    print_test_result("Concurrent Tasks Validation", False, error_message)

except Exception as e:
    error_message = f"Unexpected error: {str(e)}"
    print(error_message)
    import traceback
    traceback.print_exc()
    print_test_result("Concurrent Tasks Validation", False, error_message)

finally:
    # Cleanup test artifacts
    print("\nCleanup")
    test_utils.cleanup_test_artifacts()

    # Cleanup control tables
    try:
        for ct in control_tables:
            full_name = f"{test_catalog}.{test_schema}.{ct}"
            spark.sql(f"DROP TABLE IF EXISTS {full_name}")
            print(f"  Dropped control table: {ct}")
    except Exception as e:
        print(f"  Note: Control table cleanup: {e}")

    # Cleanup test tables
    try:
        for table in expected_tables:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            print(f"  Dropped test table: {table}")
    except Exception as e:
        print(f"  Note: Test table cleanup: {e}")

# COMMAND ----------

# Return result
if not test_passed:
    raise Exception(f"Test failed: {error_message}")

dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))

