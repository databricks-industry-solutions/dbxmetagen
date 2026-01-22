# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: apply_ddl=false
# MAGIC
# MAGIC Tests that when apply_ddl is set to false, tables are NOT modified but metadata IS generated.

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

# Get current user for integration tests
current_user = get_current_user()

# COMMAND ----------

print_test_header("apply_ddl=false Integration Test")

spark = SparkSession.builder.getOrCreate()
test_utils = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    # Setup
    print("\nSetup: Creating test environment")
    test_utils.setup_test_environment()

    # Create test table WITHOUT comment
    test_table = test_utils.create_test_table(
        "test_apply_ddl_false", with_comment=False
    )

    # Verify table has no comment initially
    initial_comment = test_utils.get_table_comment(test_table)
    test_utils.assert_none(
        initial_comment, f"Table {test_table} has no initial comment"
    )

    # Run metadata generation with apply_ddl=false
    print("\nRunning metadata generation with apply_ddl=false")

    # Create config using production YAML with test overrides
    config = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",  # Keep explicit
        apply_ddl="false",  # Override to false
        grant_permissions_after_creation="false",  # Override for tests
        current_user=current_user,  # Explicit user for integration tests
    )

    # Run main (this may take a minute)
    main(config.__dict__)

    print("\n[OK] Metadata generation completed")

    # Verify table STILL has no comment (apply_ddl was false)
    print("\nVerifying table was NOT modified")
    final_comment = test_utils.get_table_comment(test_table)

    test_utils.assert_none(
        final_comment, f"Table comment is still None after apply_ddl=false"
    )

    # Verify metadata_generation_log has entry
    print("\nVerifying metadata_generation_log entry")
    log_df = verify_metadata_generation_log(
        spark, test_catalog, test_schema, test_table
    )
    test_utils.assert_true(
        log_df is not None and log_df.count() > 0,
        "metadata_generation_log has entry for table",
    )

    # Verify SQL file was generated in volume
    print("\nVerifying SQL file in volume")
    user_sanitized = sanitize_user_identifier(config.current_user)
    sql_exists = verify_sql_file_exists(
        spark, test_catalog, test_schema, "test_volume", user_sanitized, test_table
    )
    test_utils.assert_true(sql_exists, "SQL DDL file generated in volume")

    # Verify processing log exists
    print("\nVerifying processing log")
    processing_log_exists = verify_processing_log_exists(
        spark, test_catalog, test_schema, test_table
    )
    # Note: Processing log may not exist in all configurations
    if processing_log_exists:
        print("  [OK] Processing log found")

    test_passed = True
    print_test_result("apply_ddl=false", True)

except TestFailure as e:
    error_message = str(e)
    print(error_message)
    print_test_result("apply_ddl=false", False, error_message)

except Exception as e:
    error_message = f"Unexpected error: {str(e)}"
    print(error_message)
    import traceback

    traceback.print_exc()
    print_test_result("apply_ddl=false", False, error_message)

finally:
    # Cleanup
    print("\nCleanup")
    test_utils.cleanup_test_artifacts()

# COMMAND ----------

# Return result
if not test_passed:
    raise Exception(f"Test failed: {error_message}")

dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
