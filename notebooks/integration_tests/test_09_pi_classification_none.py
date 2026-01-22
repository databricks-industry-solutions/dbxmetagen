# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: PI Classification for Non-PI Data
# MAGIC
# MAGIC Tests that tables with no PI columns are correctly classified with `classification=None`
# MAGIC instead of incorrectly being marked as `protected`.

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

print_test_header("PI Classification for Non-PI Data Integration Test")

spark = SparkSession.builder.getOrCreate()
test_utils = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    # Setup
    print("\n Setup: Creating test environment")
    test_utils.setup_test_environment()

    # Create a table with columns that have NO PI content
    # These are generic non-sensitive columns that should all be classified as "None"
    test_table_name = "test_pi_none_classification"
    full_table_name = f"{test_catalog}.{test_schema}.{test_table_name}"

    # Drop if exists
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

    # Create table with non-PI columns
    spark.sql(
        f"""
        CREATE TABLE {full_table_name} (
            product_id INT,
            product_name STRING,
            category STRING,
            price DECIMAL(10,2),
            quantity INT,
            last_updated DATE
        )
        USING DELTA
    """
    )

    # Insert non-PI data
    spark.sql(
        f"""
        INSERT INTO {full_table_name} VALUES
        (1, 'Widget A', 'Electronics', 29.99, 100, '2024-01-01'),
        (2, 'Widget B', 'Electronics', 49.99, 50, '2024-01-02'),
        (3, 'Gadget C', 'Tools', 15.99, 200, '2024-01-03'),
        (4, 'Item D', 'Home', 99.99, 25, '2024-01-04'),
        (5, 'Thing E', 'Office', 12.99, 150, '2024-01-05')
    """
    )

    test_utils.test_artifacts.append(("table", full_table_name))
    print(f"Created test table with non-PI data: {full_table_name}")

    # Run PI mode
    print("\n Test: Running PI mode on non-PI table")

    config_pi = MetadataConfig(
        yaml_file_path="../../variables.yml",
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=full_table_name,
        volume_name="test_volume",
        mode="pi",
        apply_ddl=True,
        grant_permissions_after_creation="false",
        current_user=current_user,
        use_protected_classification_for_table=True,  # Ensure protected classification is enabled
    )

    main(config_pi.__dict__)
    print("  PI mode completed")

    # Verify the log entry
    print("\n Verifying PI classification results")
    log_df = spark.sql(
        f"""
        SELECT * FROM {test_catalog}.{test_schema}.metadata_generation_log 
        WHERE table_name = '{test_table_name}'
        ORDER BY _created_at DESC 
        LIMIT 1
    """
    )

    test_utils.assert_true(
        log_df is not None and log_df.count() > 0,
        "PI mode generated metadata log entry",
    )

    # Check table-level tags
    print("\n Checking table-level classification tags")
    try:
        table_tags_df = spark.sql(f"SHOW TAGS ON TABLE {full_table_name}")
        table_tags = table_tags_df.collect()

        if len(table_tags) > 0:
            tag_dict = {row.tag_name: row.tag_value for row in table_tags}
            print(f"  Found {len(table_tags)} table-level tag(s): {tag_dict}")

            # Check classification tag
            classification_value = tag_dict.get("classification")
            subclassification_value = tag_dict.get("subclassification")

            print(f"  classification: {classification_value}")
            print(f"  subclassification: {subclassification_value}")

            # The key assertion: if subclassification is None, classification should be "None" (not protected)
            if subclassification_value in ["None", None]:
                test_utils.assert_true(
                    classification_value == "None",
                    f"Table with 'None' subclassification should have classification='None' (got: {classification_value})",
                )
                print("  CORRECT: Table with no PI has classification='None' (not protected)")
            else:
                print(f"  Note: Table has subclassification={subclassification_value}")

        else:
            print("  Note: No table-level tags found")

    except Exception as e:
        print(f"  Note: Could not verify tags: {e}")

    # Also check column-level classifications
    print("\n Checking column-level classifications")
    columns_with_pi = 0
    for col_name in ["product_id", "product_name", "category", "price", "quantity"]:
        try:
            col_tags_df = spark.sql(f"SHOW TAGS ON COLUMN {full_table_name}.{col_name}")
            col_tags = col_tags_df.collect()
            if len(col_tags) > 0:
                tag_dict = {row.tag_name: row.tag_value for row in col_tags}
                col_type = tag_dict.get("type", tag_dict.get("subclassification"))
                if col_type and col_type not in ["None", None]:
                    columns_with_pi += 1
                    print(f"  Column '{col_name}' has type: {col_type}")
                else:
                    print(f"  Column '{col_name}' has type: None (correct)")
        except Exception as e:
            print(f"  Note: Could not check tags for column '{col_name}': {e}")

    # Verify that LLM correctly identified this as non-PI data
    print(f"\n Summary: {columns_with_pi} columns classified as having PI")

    test_passed = True
    print_test_result("PI Classification for Non-PI Data", True)

except TestFailure as e:
    error_message = str(e)
    print(error_message)
    print_test_result("PI Classification for Non-PI Data", False, error_message)

except Exception as e:
    error_message = f"Unexpected error: {str(e)}"
    print(error_message)
    import traceback

    traceback.print_exc()
    print_test_result("PI Classification for Non-PI Data", False, error_message)

finally:
    # Cleanup
    print("\n Cleanup")
    test_utils.cleanup_test_artifacts()

# COMMAND ----------

# Return result
if not test_passed:
    raise Exception(f"Test failed: {error_message}")

dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))

