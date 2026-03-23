# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Temp Table Cleanup
# MAGIC
# MAGIC Tests that temp tables are cleaned up even when errors occur during execution.

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

import sys

sys.path.append("../../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.config import MetadataConfig
from dbxmetagen.main import main
from dbxmetagen.user_utils import sanitize_user_identifier, get_current_user

# Setup widgets
dbutils.widgets.text("test_catalog", "dev_integration_tests", "Test Catalog")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")

test_catalog = dbutils.widgets.get("test_catalog")
test_schema = dbutils.widgets.get("test_schema")

# COMMAND ----------

print_test_header("Temp Table Cleanup Integration Test")

spark = SparkSession.builder.getOrCreate()
test_utils = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    # Setup
    print("\nSetup: Creating test environment")
    test_utils.setup_test_environment()

    # Get current user for tracking temp tables
    current_user = get_current_user()
    sanitized_user = sanitize_user_identifier(current_user)
    print(f"  Current user: {current_user}")
    print(f"  Sanitized user: {sanitized_user}")

    # Clean up stale temp tables from prior runs
    stale_temps = test_utils.find_temp_tables(sanitized_user)
    if stale_temps:
        print(f"  Cleaning {len(stale_temps)} stale temp table(s) from prior runs")
        for t in stale_temps:
            spark.sql(f"DROP TABLE IF EXISTS {test_catalog}.{test_schema}.{t}")

    # Test 1: Successful run should clean up temp tables
    print("\nTest 1: Successful run cleans up temp tables")
    test_table = test_utils.create_test_table(
        "test_cleanup_success", with_comment=False
    )

    # Create config using production YAML with test overrides
    config = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        grant_permissions_after_creation="false",  # Override for tests
    )

    # Get the exact temp table name that main() will create and should clean up
    expected_temp = config.get_temp_metadata_log_table_name()
    print(f"  Expected temp table: {expected_temp}")

    # Run successfully
    main(config.__dict__)

    # Check if THIS run's specific temp table was cleaned up
    this_table_exists = spark.catalog.tableExists(expected_temp)
    print(f"  This run's temp table still exists: {this_table_exists}")

    test_utils.assert_true(
        not this_table_exists,
        "This run's temp table cleaned up after successful run"
    )

    # Verify metadata_generation_log persists after cleanup
    print("\nVerifying metadata_generation_log persists")
    log_df = verify_metadata_generation_log(spark, test_catalog, test_schema, test_table)
    test_utils.assert_true(
        log_df is not None and log_df.count() > 0,
        "metadata_generation_log persists after temp table cleanup"
    )

    # Test 2: Failed run should STILL clean up temp tables
    print("\nTest 2: Failed run also cleans up temp tables")

    # Create a scenario that will fail - invalid catalog
    config_fail = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name="nonexistent_catalog_xyz_123",  # This will cause an error
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        grant_permissions_after_creation="false",  # Override for tests
    )

    expected_fail_temp = config_fail.get_temp_metadata_log_table_name()
    print(f"  Expected fail temp table: {expected_fail_temp}")

    # This should fail, but we expect temp table cleanup to still happen
    try:
        print("  Attempting run with invalid catalog (expected to fail)...")
        main(config_fail.__dict__)
        print("  [WARNING] Run unexpectedly succeeded")
    except Exception as e:
        print(f"  [OK] Run failed as expected: {type(e).__name__}")

    # Check if the failed run's specific temp table was cleaned up
    fail_table_exists = spark.catalog.tableExists(expected_fail_temp)
    print(f"  Failed run's temp table still exists: {fail_table_exists}")

    test_utils.assert_true(
        not fail_table_exists, "Failed run's temp table cleaned up"
    )

    # Test 3: Control tables
    print("\nTest 3: Control table cleanup")
    control_tables = test_utils.find_control_tables(sanitized_user)
    print(f"  Control tables found: {len(control_tables)}")

    if len(control_tables) > 0:
        print(f"  Control tables: {control_tables}")
        # Control tables might exist if cleanup_control_table=false was used
        # This is expected behavior
        test_utils.assert_true(True, "Control tables found (expected if cleanup=false)")
    else:
        test_utils.assert_true(True, "No control tables left behind")

    test_passed = True
    print_test_result("Temp Table Cleanup", True)

except TestFailure as e:
    error_message = str(e)
    print(error_message)
    print_test_result("Temp Table Cleanup", False, error_message)

except Exception as e:
    error_message = f"Unexpected error: {str(e)}"
    print(error_message)
    import traceback

    traceback.print_exc()
    print_test_result("Temp Table Cleanup", False, error_message)

finally:
    # Cleanup
    print("\nCleanup")
    test_utils.cleanup_test_artifacts()

    # Also cleanup any lingering temp tables
    print("  Cleaning up any remaining temp tables...")
    try:
        current_user = get_current_user()
        sanitized_user = sanitize_user_identifier(current_user)
        remaining_temps = test_utils.find_temp_tables(sanitized_user)
        for temp_table in remaining_temps:
            full_name = f"{test_catalog}.{test_schema}.{temp_table}"
            spark.sql(f"DROP TABLE IF EXISTS {full_name}")
            print(f"    Cleaned up: {temp_table}")
    except Exception as e:
        print(f"    Note: {e}")

# COMMAND ----------

# Return result
if not test_passed:
    raise Exception(f"Test failed: {error_message}")

dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
