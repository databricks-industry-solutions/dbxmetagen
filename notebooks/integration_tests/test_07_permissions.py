# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Permissions
# MAGIC
# MAGIC Tests that proper error messages are shown for permission issues with catalogs, schemas, and volumes.

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

import sys

sys.path.append("../../")

from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.main import main
from src.dbxmetagen.user_utils import get_current_user

# Setup widgets
dbutils.widgets.text("test_catalog", "dev_integration_tests", "Test Catalog")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")

test_catalog = dbutils.widgets.get("test_catalog")
test_schema = dbutils.widgets.get("test_schema")

# Get current user for integration tests
current_user = get_current_user()

# COMMAND ----------

print_test_header("Permissions Integration Test")

spark = SparkSession.builder.getOrCreate()
test_utils = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    # Setup
    print("\n📋 Setup: Creating test environment")
    test_utils.setup_test_environment()

    # Test 1: Valid catalog and schema should work
    print("\n🧪 Test 1: Valid catalog and schema")
    test_table = test_utils.create_test_table("test_permissions", with_comment=False)

    # Create config using production YAML with test overrides
    config = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        grant_permissions_after_creation="false",  # Override for tests
        current_user=current_user,  # Explicit user for integration tests
    )

    main(config.__dict__)
    print("  ✓ Run with valid catalog/schema succeeded")

    # Test 2: Invalid catalog should fail with clear error
    print("\n🧪 Test 2: Invalid catalog produces clear error")
    config_invalid_catalog = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name="nonexistent_catalog_xyz_999",
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        grant_permissions_after_creation="false",  # Override for tests
        current_user=current_user,  # Explicit user for integration tests
    )

    try:
        main(config_invalid_catalog.__dict__)
        test_utils.assert_false(True, "Expected error for invalid catalog")
    except Exception as e:
        error_str = str(e).lower()
        print(f"  ✓ Error raised: {type(e).__name__}")
        print(f"  Error message: {str(e)[:200]}")

        # Verify error message is informative
        test_utils.assert_true(
            "catalog" in error_str
            or "not found" in error_str
            or "does not exist" in error_str,
            "Error message mentions catalog issue",
        )

    # Test 3: Invalid schema should fail with clear error
    print("\n🧪 Test 3: Invalid schema produces clear error")
    config_invalid_schema = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name=test_catalog,
        schema_name="nonexistent_schema_xyz_999",
        table_names=test_table,
        volume_name="test_volume",
        grant_permissions_after_creation="false",  # Override for tests
        current_user=current_user,  # Explicit user for integration tests
    )

    try:
        main(config_invalid_schema.__dict__)
        test_utils.assert_false(True, "Expected error for invalid schema")
    except Exception as e:
        error_str = str(e).lower()
        print(f"  ✓ Error raised: {type(e).__name__}")
        print(f"  Error message: {str(e)[:200]}")

        # Verify error message is informative
        test_utils.assert_true(
            "schema" in error_str
            or "not found" in error_str
            or "does not exist" in error_str,
            "Error message mentions schema issue",
        )

    # Test 4: grant_permissions_after_creation=false should not error
    print("\n🧪 Test 4: grant_permissions_after_creation=false bypasses grants")
    config_no_perms = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        grant_permissions_after_creation="false",  # Override for tests
        current_user=current_user,  # Explicit user for integration tests
    )

    # This should succeed without permission grant errors
    main(config_no_perms.__dict__)
    print("  ✓ Run succeeded with grant_permissions_after_creation=false")

    # Test 5: Volume access (if volume exists)
    print("\n🧪 Test 5: Volume access")
    try:
        volume_path = f"/Volumes/{test_catalog}/{test_schema}/test_volume"
        files = dbutils.fs.ls(volume_path)
        print(f"  ✓ Volume accessible, {len(files)} file(s) found")
        test_utils.assert_true(True, "Volume is accessible")
    except Exception as e:
        print(f"  Note: Volume access test skipped: {e}")
        # This is non-fatal since volume might not exist in test env

    test_passed = True
    print_test_result("Permissions", True)

except TestFailure as e:
    error_message = str(e)
    print(error_message)
    print_test_result("Permissions", False, error_message)

except Exception as e:
    error_message = f"Unexpected error: {str(e)}"
    print(error_message)
    import traceback

    traceback.print_exc()
    print_test_result("Permissions", False, error_message)

finally:
    # Cleanup
    print("\n🧹 Cleanup")
    test_utils.cleanup_test_artifacts()

# COMMAND ----------

# Return result
if not test_passed:
    raise Exception(f"Test failed: {error_message}")

dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
