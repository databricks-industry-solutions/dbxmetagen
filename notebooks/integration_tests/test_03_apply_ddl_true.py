# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: apply_ddl=true
# MAGIC
# MAGIC Tests that when apply_ddl is set to true, tables ARE modified with generated comments.

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

print_test_header("apply_ddl=true Integration Test")

spark = SparkSession.builder.getOrCreate()
test_utils = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    # Setup
    print("\nSetup: Creating test environment")
    test_utils.setup_test_environment()

    # Create test table WITHOUT comment
    test_table = test_utils.create_test_table("test_apply_ddl_true", with_comment=False)

    # Verify table has no comment initially
    initial_comment = test_utils.get_table_comment(test_table)
    test_utils.assert_none(
        initial_comment, f"Table {test_table} has no initial comment"
    )

    # Run metadata generation with apply_ddl=true
    print("\nRunning metadata generation with apply_ddl=true")

    # Create config using production YAML with test overrides
    config = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",  # Keep explicit
        apply_ddl="true",  # Override to true
        grant_permissions_after_creation="false",  # Override for tests
        current_user=current_user,  # Explicit user for integration tests
    )

    # Run main (this may take a minute)
    main(config.__dict__)

    print("\n[OK] Metadata generation completed")

    # Verify table NOW has a comment (apply_ddl was true)
    print("\nVerifying table WAS modified")
    final_comment = test_utils.get_table_comment(test_table)

    print(f"  Final comment: {final_comment}")

    test_utils.assert_not_none(
        final_comment, f"Table comment was added after apply_ddl=true"
    )

    test_utils.assert_true(len(final_comment) > 0, "Table comment is not empty")

    # Check column comments were also added
    print("\nVerifying column comments were added")

    # Check all columns and count how many have comments
    test_columns = ["id", "name", "email", "age", "created_date"]
    columns_with_comments = 0

    for col_name in test_columns:
        column_comment = test_utils.get_column_comment(test_table, col_name)
        if column_comment and len(column_comment) > 5:
            columns_with_comments += 1
            print(f"  [OK] Column '{col_name}' comment: {column_comment[:80]}...")
        else:
            print(f"  - Column '{col_name}' has no comment")

    # Assert that at least 3 out of 5 columns have comments
    test_utils.assert_true(
        columns_with_comments >= 3,
        f"At least 3 out of 5 columns have comments (found {columns_with_comments})",
    )

    # Verify metadata_generation_log has entry with applied values
    print("\nVerifying metadata_generation_log entry")
    log_df = verify_metadata_generation_log(
        spark, test_catalog, test_schema, test_table
    )
    test_utils.assert_true(
        log_df is not None and log_df.count() > 0,
        "metadata_generation_log has entry for table",
    )
    if log_df and log_df.count() > 0:
        log_row = log_df.first()
        print(
            f"  [OK] Log entry found, status: {log_row.status if hasattr(log_row, 'status') else 'N/A'}"
        )

    # Verify SQL file was generated in volume
    print("\nVerifying SQL file in volume")
    user_sanitized = sanitize_user_identifier(config.current_user)
    sql_exists = verify_sql_file_exists(
        spark, test_catalog, test_schema, "test_volume", user_sanitized, test_table
    )
    test_utils.assert_true(sql_exists, "SQL DDL file generated in volume")

    test_passed = True
    print_test_result("apply_ddl=true", True)

except TestFailure as e:
    error_message = str(e)
    print(error_message)
    print_test_result("apply_ddl=true", False, error_message)

except Exception as e:
    error_message = f"Unexpected error: {str(e)}"
    print(error_message)
    import traceback

    traceback.print_exc()
    print_test_result("apply_ddl=true", False, error_message)

finally:
    # Cleanup
    print("\nCleanup")
    test_utils.cleanup_test_artifacts()

# COMMAND ----------

# Return result
if not test_passed:
    raise Exception(f"Test failed: {error_message}")

dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
