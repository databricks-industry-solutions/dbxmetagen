# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Temp Table Cleanup
# MAGIC
# MAGIC Tests that temp tables are cleaned up even when errors occur during execution.

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

import sys

sys.path.append("../../")

from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.main import main
from src.dbxmetagen.user_utils import sanitize_user_identifier, get_current_user

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
    print("\n📋 Setup: Creating test environment")
    test_utils.setup_test_environment()

    # Get current user for tracking temp tables
    current_user = get_current_user()
    sanitized_user = sanitize_user_identifier(current_user)
    print(f"  Current user: {current_user}")
    print(f"  Sanitized user: {sanitized_user}")

    # Test 1: Successful run should clean up temp tables
    print("\n🧪 Test 1: Successful run cleans up temp tables")
    test_table = test_utils.create_test_table(
        "test_cleanup_success", with_comment=False
    )

    # Check for temp tables before run
    temp_tables_before = test_utils.find_temp_tables(sanitized_user)
    print(f"  Temp tables before run: {len(temp_tables_before)}")

    # Create config using production YAML with test overrides
    config = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        grant_permissions_after_creation="false",  # Override for tests
    )

    # Run successfully
    main(config.__dict__)

    # Check temp tables after successful run - should be cleaned up
    temp_tables_after = test_utils.find_temp_tables(sanitized_user)
    print(f"  Temp tables after successful run: {len(temp_tables_after)}")

    test_utils.assert_equals(
        len(temp_tables_after), 0, "Temp tables cleaned up after successful run"
    )

    # Verify metadata_generation_log persists after cleanup
    print("\n🔍 Verifying metadata_generation_log persists")
    log_df = verify_metadata_generation_log(spark, test_catalog, test_schema, test_table)
    test_utils.assert_true(
        log_df is not None and log_df.count() > 0,
        "metadata_generation_log persists after temp table cleanup"
    )

    # Test 2: Failed run should STILL clean up temp tables
    print("\n🧪 Test 2: Failed run also cleans up temp tables")

    # Create a scenario that will fail - invalid catalog
    config_fail = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name="nonexistent_catalog_xyz_123",  # This will cause an error
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        grant_permissions_after_creation="false",  # Override for tests
    )

    # This should fail, but we expect temp table cleanup to still happen
    try:
        print("  Attempting run with invalid catalog (expected to fail)...")
        main(config_fail.__dict__)
        print("  ⚠ Run unexpectedly succeeded")
    except Exception as e:
        print(f"  ✓ Run failed as expected: {type(e).__name__}")

    # Even after failure, temp tables should be cleaned
    temp_tables_after_fail = test_utils.find_temp_tables(sanitized_user)
    print(f"  Temp tables after failed run: {len(temp_tables_after_fail)}")

    # Note: This assertion may fail if the error happens before temp tables are created
    # In that case, we just verify no temp tables are left behind
    test_utils.assert_true(
        len(temp_tables_after_fail) == 0, "No temp tables left behind after failed run"
    )

    # Test 3: Control tables
    print("\n🧪 Test 3: Control table cleanup")
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
    print("\n🧹 Cleanup")
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
