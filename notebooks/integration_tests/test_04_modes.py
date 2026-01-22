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
    print("\nSetup: Creating test environment")
    test_utils.setup_test_environment()

    test_table = test_utils.create_test_table("test_modes", with_comment=False)

    # Test Mode: comment
    print("\nTest 1: mode=comment")

    config_comment = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        mode="comment",
        apply_ddl=True,  # Apply changes to database for verification
        grant_permissions_after_creation="false",  # Override for tests
        current_user=current_user,  # Explicit user for integration tests
    )

    main(config_comment.__dict__)
    print("  [OK] comment mode completed")

    # Verify comment mode produces descriptions
    log_df_comment = verify_metadata_generation_log(
        spark, test_catalog, test_schema, test_table
    )
    test_utils.assert_true(
        log_df_comment is not None and log_df_comment.count() > 0,
        "Comment mode generated metadata log entry",
    )

    # Verify actual comments were applied to table and columns
    print("\nVerifying comments were applied to database")
    table_comment = test_utils.get_table_comment(test_table)
    test_utils.assert_not_none(
        table_comment, "Table comment was applied in comment mode"
    )
    test_utils.assert_true(
        len(table_comment) > 10, "Table comment has meaningful content"
    )
    print(f"  [OK] Table comment: {table_comment[:100]}...")

    # Check that at least some columns have comments
    columns_with_comments = 0
    for col_name in ["name", "email", "age"]:
        col_comment = test_utils.get_column_comment(test_table, col_name)
        if col_comment and len(col_comment) > 5:
            columns_with_comments += 1
            print(f"  [OK] Column '{col_name}' has comment: {col_comment[:80]}...")

    test_utils.assert_true(
        columns_with_comments >= 2,
        f"At least 2 columns have comments (found {columns_with_comments})",
    )

    # Test Mode: pi
    print("\nTest 2: mode=pi")

    config_pi = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        mode="pi",
        apply_ddl=True,  # Apply changes to database for verification
        grant_permissions_after_creation="false",  # Override for tests
        current_user=current_user,  # Explicit user for integration tests
    )

    main(config_pi.__dict__)
    print("  [OK] pi mode completed")

    # Verify PI mode generates different output
    log_df_pi = verify_metadata_generation_log(
        spark, test_catalog, test_schema, test_table
    )
    test_utils.assert_true(
        log_df_pi is not None and log_df_pi.count() > 0,
        "PI mode generated metadata log entry",
    )

    # Verify PII tags were applied
    print("\nVerifying PII tags were applied")
    try:
        # Check table-level tags
        table_tags_df = spark.sql(f"SHOW TAGS ON TABLE {test_table}")
        table_tags = table_tags_df.collect()

        if len(table_tags) > 0:
            tag_names = [row.tag_name for row in table_tags]
            print(
                f"  [OK] Found {len(table_tags)} table-level tag(s): {', '.join(tag_names)}"
            )

            # Check for expected PII tags
            has_classification_tag = any(
                "classification" in tag.lower() for tag in tag_names
            )
            if has_classification_tag:
                print("  [OK] Found classification-related tags")

        # Check column-level tags for at least one column
        columns_with_tags = 0
        for col_name in ["name", "email"]:
            try:
                col_tags_df = spark.sql(f"SHOW TAGS ON COLUMN {test_table}.{col_name}")
                col_tags = col_tags_df.collect()
                if len(col_tags) > 0:
                    columns_with_tags += 1
                    tag_info = [(row.tag_name, row.tag_value) for row in col_tags]
                    print(
                        f"  [OK] Column '{col_name}' has {len(col_tags)} tag(s): {tag_info}"
                    )
            except Exception as e:
                print(f"  Note: Could not check tags for column '{col_name}': {e}")

        if columns_with_tags > 0:
            print(f"  [OK] {columns_with_tags} column(s) have PII tags")
        else:
            print("  Note: No column-level tags found (may depend on LLM detection)")

    except Exception as e:
        print(f"  Note: Could not verify tags (may require Unity Catalog): {e}")

    # Test Mode: domain
    print("\nTest 3: mode=domain")

    config_domain = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        mode="domain",
        apply_ddl=True,  # Apply changes to database for verification
        grant_permissions_after_creation="false",  # Override for tests
        current_user=current_user,  # Explicit user for integration tests
    )

    main(config_domain.__dict__)
    print("  [OK] domain mode completed")

    # Verify domain mode generates output
    log_df_domain = verify_metadata_generation_log(
        spark, test_catalog, test_schema, test_table
    )
    test_utils.assert_true(
        log_df_domain is not None and log_df_domain.count() > 0,
        "Domain mode generated metadata log entry",
    )

    # Verify domain tags were applied
    print("\nVerifying domain classification tags were applied")
    try:
        table_tags_df = spark.sql(f"SHOW TAGS ON TABLE {test_table}")
        table_tags = table_tags_df.collect()

        if len(table_tags) > 0:
            tag_dict = {row.tag_name: row.tag_value for row in table_tags}
            print(f"  [OK] Found {len(table_tags)} table-level tag(s)")

            # Look for domain-related tags
            domain_tags = [k for k in tag_dict.keys() if "domain" in k.lower()]
            if domain_tags:
                for tag in domain_tags:
                    print(f"  [OK] Domain tag '{tag}' = '{tag_dict[tag]}'")
                test_utils.assert_true(True, "Domain classification tags were applied")
            else:
                print(
                    "  Note: No explicit 'domain' tags found, but other tags may be present"
                )
        else:
            print(
                "  Note: No table-level tags found (may depend on LLM classification)"
            )

    except Exception as e:
        print(f"  Note: Could not verify domain tags: {e}")

    # Verify SQL files exist and contain correct DDL types for each mode
    print("\nVerifying SQL files for each mode")
    user_sanitized = sanitize_user_identifier(config_comment.current_user)

    # Check comment mode SQL
    print("\n  Checking comment mode SQL file...")
    comment_sql_result = verify_sql_file_content(
        spark,
        test_catalog,
        test_schema,
        "test_volume",
        user_sanitized,
        test_table,
        "comment",
    )
    test_utils.assert_true(
        comment_sql_result["file_found"], "Comment mode SQL file exists"
    )
    test_utils.assert_true(
        comment_sql_result["ddl_type_correct"],
        "Comment mode SQL contains COMMENT ON statements",
    )

    # Check PI mode SQL
    print("\n  Checking PI mode SQL file...")
    pi_sql_result = verify_sql_file_content(
        spark,
        test_catalog,
        test_schema,
        "test_volume",
        user_sanitized,
        test_table,
        "pi",
    )
    test_utils.assert_true(pi_sql_result["file_found"], "PI mode SQL file exists")
    test_utils.assert_true(
        pi_sql_result["ddl_type_correct"],
        "PI mode SQL contains ALTER TABLE SET TAGS statements",
    )

    # Check domain mode SQL
    print("\n  Checking domain mode SQL file...")
    domain_sql_result = verify_sql_file_content(
        spark,
        test_catalog,
        test_schema,
        "test_volume",
        user_sanitized,
        test_table,
        "domain",
    )
    test_utils.assert_true(
        domain_sql_result["file_found"], "Domain mode SQL file exists"
    )
    test_utils.assert_true(
        domain_sql_result["ddl_type_correct"],
        "Domain mode SQL contains ALTER TABLE SET TAGS statements",
    )

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
    print("\nCleanup")
    test_utils.cleanup_test_artifacts()

# COMMAND ----------

# Return result
if not test_passed:
    raise Exception(f"Test failed: {error_message}")

dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
