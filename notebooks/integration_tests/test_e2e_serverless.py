# Databricks notebook source
# MAGIC %md
# MAGIC # E2E Integration Test: Serverless Compute
# MAGIC
# MAGIC Tests dbxmetagen end-to-end on serverless compute across all modes.

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

import sys

sys.path.append("../../")

from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.main import main
from src.dbxmetagen.user_utils import sanitize_user_identifier, get_current_user
from src.dbxmetagen.databricks_utils import get_job_context

# Setup widgets
dbutils.widgets.text("test_catalog", "dev_integration_tests", "Test Catalog")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")

test_catalog = dbutils.widgets.get("test_catalog")
test_schema = dbutils.widgets.get("test_schema")

# Get current user and job context for integration tests
current_user = get_current_user()
job_context = get_job_context(None, dbutils)
job_id = job_context.get("job_id") if job_context else None

# COMMAND ----------

print_test_header("E2E Serverless Compute Test")

spark = SparkSession.builder.getOrCreate()
test_utils = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    # Setup
    print("\nSetup: Creating test environment")
    test_utils.setup_test_environment()

    test_table = test_utils.create_test_table("test_e2e_serverless", with_comment=False)

    # Test Mode: comment
    print("\nTest 1: mode=comment")

    config_comment = MetadataConfig(
        yaml_file_path="../../variables.yml",
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        mode="comment",
        grant_permissions_after_creation="false",
        cleanup_control_table="false",
        control_table="metadata_control_e2e_serverless_{0}",
        current_user=current_user,
        job_id=job_id,
    )

    main(config_comment.__dict__)
    print("  comment mode completed")

    # Enhanced validation for comment mode
    print("  Validating comment mode outputs...")
    log_df_comment = verify_metadata_log_for_mode(
        spark, test_catalog, test_schema, test_table, "comment"
    )

    # Verify control table entry
    control_table_name = (
        f"metadata_control_e2e_serverless_{sanitize_user_identifier(current_user)}"
    )
    verify_control_table_entry(
        spark, test_catalog, test_schema, control_table_name, test_table
    )

    # Test Mode: pi
    print("\nTest 2: mode=pi")

    config_pi = MetadataConfig(
        yaml_file_path="../../variables.yml",
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        mode="pi",
        grant_permissions_after_creation="false",
        cleanup_control_table="false",
        control_table="metadata_control_e2e_serverless_{0}",
        current_user=current_user,
        job_id=job_id,
    )

    main(config_pi.__dict__)
    print("  pi mode completed")

    # Enhanced validation for PI mode
    print("  Validating PI mode outputs...")
    log_df_pi = verify_metadata_log_for_mode(
        spark, test_catalog, test_schema, test_table, "pi"
    )

    # Verify control table still has entry
    verify_control_table_entry(
        spark, test_catalog, test_schema, control_table_name, test_table
    )

    # Test Mode: domain
    print("\nTest 3: mode=domain")

    config_domain = MetadataConfig(
        yaml_file_path="../../variables.yml",
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        mode="domain",
        grant_permissions_after_creation="false",
        cleanup_control_table="false",
        control_table="metadata_control_e2e_serverless_{0}",
        current_user=current_user,
        job_id=job_id,
    )

    main(config_domain.__dict__)
    print("  domain mode completed")

    # Enhanced validation for domain mode
    print("  Validating domain mode outputs...")
    log_df_domain = verify_metadata_log_for_mode(
        spark, test_catalog, test_schema, test_table, "domain"
    )

    # Verify control table still has entry
    verify_control_table_entry(
        spark, test_catalog, test_schema, control_table_name, test_table
    )

    # Verify SQL file content (not just existence)
    print("\nValidating SQL file content...")
    user_sanitized = sanitize_user_identifier(current_user)
    sql_content = verify_sql_file_content(
        spark, test_catalog, test_schema, "test_volume", user_sanitized, test_table
    )

    # Verify mode differentiation - all three modes should have produced different outputs
    print("\nValidating mode differentiation...")
    verify_mode_differentiation(spark, test_catalog, test_schema, test_table)

    test_passed = True
    print_test_result("E2E Serverless", True)

except TestFailure as e:
    error_message = str(e)
    print(error_message)
    print_test_result("E2E Serverless", False, error_message)

except Exception as e:
    error_message = f"Unexpected error: {str(e)}"
    print(error_message)
    import traceback

    traceback.print_exc()
    print_test_result("E2E Serverless", False, error_message)

finally:
    # Cleanup
    print("\nCleanup")
    test_utils.cleanup_test_artifacts()

    # Clean up control table
    try:
        control_table = f"{test_catalog}.{test_schema}.metadata_control_e2e_serverless_{sanitize_user_identifier(current_user)}"
        spark.sql(f"DROP TABLE IF EXISTS {control_table}")
        print(f"  Dropped control table: {control_table}")
    except Exception as e:
        print(f"  Note: Could not drop control table: {e}")

# COMMAND ----------

# Return result
if not test_passed:
    raise Exception(f"Test failed: {error_message}")

dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
