# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Control Table Management
# MAGIC
# MAGIC Tests that control tables are created and managed correctly with cleanup options.

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

# Get current user for integration tests (also used in test logic)
current_user = get_current_user()

# COMMAND ----------

print_test_header("Control Table Management Integration Test")

spark = SparkSession.builder.getOrCreate()
test_utils = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    # Setup
    print("\n📋 Setup: Creating test environment")
    test_utils.setup_test_environment()

    # current_user already set above, just use it
    sanitized_user = sanitize_user_identifier(current_user)
    print(f"  Current user: {current_user}")
    print(f"  Sanitized user: {sanitized_user}")

    test_table = test_utils.create_test_table("test_control_table", with_comment=False)

    # Test 1: cleanup_control_table=false leaves control table
    print("\n🧪 Test 1: cleanup_control_table=false preserves control table")

    # Create config using production YAML with test overrides
    config_no_cleanup = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        cleanup_control_table="false",  # Override to false
        grant_permissions_after_creation="false",  # Override for tests
        current_user=current_user,  # Explicit user for integration tests
    )

    main(config_no_cleanup.__dict__)

    # Check for control table
    control_tables = test_utils.find_control_tables(sanitized_user)
    print(f"  Control tables found: {len(control_tables)}")

    test_utils.assert_true(
        len(control_tables) > 0, "Control table exists when cleanup_control_table=false"
    )

    control_table_name = control_tables[0]
    full_control_table = f"{test_catalog}.{test_schema}.{control_table_name}"
    print(f"  Control table: {control_table_name}")

    # Verify control table has entries
    control_count = (
        spark.sql(f"SELECT COUNT(*) as cnt FROM {full_control_table}").first().cnt
    )
    print(f"  Control table entries: {control_count}")

    test_utils.assert_true(control_count > 0, "Control table has entries")

    # Validate control table schema/content
    print("\n🔍 Validating control table schema")
    control_df = spark.sql(f"SELECT * FROM {full_control_table}")
    control_columns = control_df.columns
    print(f"  Control table columns: {control_columns}")

    # Check for expected columns
    expected_cols = ["table_name", "status", "timestamp"]
    for col in expected_cols:
        if col in control_columns:
            print(f"  ✓ Found expected column: {col}")

    # Test 2: cleanup_control_table=true removes control table
    print("\n🧪 Test 2: cleanup_control_table=true removes control table")

    config_cleanup = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        cleanup_control_table="true",  # Override to true
        grant_permissions_after_creation="false",  # Override for tests
        current_user=current_user,  # Explicit user for integration tests
    )

    main(config_cleanup.__dict__)

    # Check control tables again - should be cleaned up
    control_tables_after = test_utils.find_control_tables(sanitized_user)
    print(f"  Control tables after cleanup: {len(control_tables_after)}")

    test_utils.assert_equals(
        len(control_tables_after),
        0,
        "Control table removed when cleanup_control_table=true",
    )

    test_passed = True
    print_test_result("Control Table Management", True)

except TestFailure as e:
    error_message = str(e)
    print(error_message)
    print_test_result("Control Table Management", False, error_message)

except Exception as e:
    error_message = f"Unexpected error: {str(e)}"
    print(error_message)
    import traceback

    traceback.print_exc()
    print_test_result("Control Table Management", False, error_message)

finally:
    # Cleanup
    print("\n🧹 Cleanup")
    test_utils.cleanup_test_artifacts()

    # Cleanup control tables
    try:
        current_user = get_current_user()
        sanitized_user = sanitize_user_identifier(current_user)
        control_tables = test_utils.find_control_tables(sanitized_user)
        for ct in control_tables:
            full_name = f"{test_catalog}.{test_schema}.{ct}"
            spark.sql(f"DROP TABLE IF EXISTS {full_name}")
            print(f"  Cleaned up control table: {ct}")
    except Exception as e:
        print(f"  Note: {e}")

# COMMAND ----------

# Return result
if not test_passed:
    raise Exception(f"Test failed: {error_message}")

dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
