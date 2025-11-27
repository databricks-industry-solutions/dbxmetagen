# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Mode Switching
# MAGIC
# MAGIC Tests that different modes (comment, pi, domain) work correctly.

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

print_test_header("Mode Switching Integration Test")

spark = SparkSession.builder.getOrCreate()
test_utils = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    # Setup
    print("\n📋 Setup: Creating test environment")
    test_utils.setup_test_environment()

    test_table = test_utils.create_test_table("test_modes", with_comment=False)

    # Test Mode: comment
    print("\n🧪 Test 1: mode=comment")

    config_comment = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        mode="comment",
        grant_permissions_after_creation="false",  # Override for tests
        current_user=current_user,  # Explicit user for integration tests
    )

    main(config_comment.__dict__)
    print("  ✓ comment mode completed")

    # Verify comment mode produces descriptions
    log_df_comment = verify_metadata_generation_log(
        spark, test_catalog, test_schema, test_table
    )
    test_utils.assert_true(
        log_df_comment is not None and log_df_comment.count() > 0,
        "Comment mode generated metadata log entry",
    )

    # Test Mode: pi
    print("\n🧪 Test 2: mode=pi")

    config_pi = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        mode="pi",
        grant_permissions_after_creation="false",  # Override for tests
        current_user=current_user,  # Explicit user for integration tests
    )

    main(config_pi.__dict__)
    print("  ✓ pi mode completed")

    # Verify PI mode generates different output
    log_df_pi = verify_metadata_generation_log(
        spark, test_catalog, test_schema, test_table
    )
    test_utils.assert_true(
        log_df_pi is not None and log_df_pi.count() > 0,
        "PI mode generated metadata log entry",
    )

    # Test Mode: domain
    print("\n🧪 Test 3: mode=domain")

    config_domain = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        mode="domain",
        grant_permissions_after_creation="false",  # Override for tests
        current_user=current_user,  # Explicit user for integration tests
    )

    main(config_domain.__dict__)
    print("  ✓ domain mode completed")

    # Verify domain mode generates output
    log_df_domain = verify_metadata_generation_log(
        spark, test_catalog, test_schema, test_table
    )
    test_utils.assert_true(
        log_df_domain is not None and log_df_domain.count() > 0,
        "Domain mode generated metadata log entry",
    )

    # Verify SQL files exist for each mode
    print("\n🔍 Verifying SQL files for each mode")
    user_sanitized = sanitize_user_identifier(config_comment.current_user)
    sql_exists = verify_sql_file_exists(
        spark, test_catalog, test_schema, "test_volume", user_sanitized, test_table
    )
    test_utils.assert_true(sql_exists, "SQL files generated for all modes")

    test_passed = True
    print_test_result("Mode Switching", True)

except TestFailure as e:
    error_message = str(e)
    print(error_message)
    print_test_result("Mode Switching", False, error_message)

except Exception as e:
    error_message = f"Unexpected error: {str(e)}"
    print(error_message)
    import traceback

    traceback.print_exc()
    print_test_result("Mode Switching", False, error_message)

finally:
    # Cleanup
    print("\n🧹 Cleanup")
    test_utils.cleanup_test_artifacts()

# COMMAND ----------

# Return result
if not test_passed:
    raise Exception(f"Test failed: {error_message}")

dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
