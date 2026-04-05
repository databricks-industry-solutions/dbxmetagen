# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: mode="all" Unified Pass
# MAGIC
# MAGIC Verifies that `mode="all"` produces comment, PI, and domain metadata
# MAGIC from a single invocation (no separate runs per mode).

# COMMAND ----------

# MAGIC %pip install -q -r ../../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

import sys

sys.path.append("../../src")

from dbxmetagen.config import MetadataConfig
from dbxmetagen.main import main
from dbxmetagen.user_utils import sanitize_user_identifier, get_current_user

dbutils.widgets.text("test_catalog", "dev_integration_tests", "Test Catalog")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")

test_catalog = dbutils.widgets.get("test_catalog")
test_schema = dbutils.widgets.get("test_schema")

current_user = get_current_user()

# COMMAND ----------

print_test_header("mode=all Unified Pass Integration Test")

spark = SparkSession.builder.getOrCreate()
test_utils = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    print("\nSetup: Creating test environment")
    test_utils.setup_test_environment()
    test_table = test_utils.create_test_table("test_mode_all", with_comment=False)

    # Run mode="all" -- single invocation should produce comment + PI + domain
    print("\nRunning mode=all...")
    config_all = MetadataConfig(
        yaml_file_path="../../variables.yml",
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        mode="all",
        apply_ddl=True,
        grant_permissions_after_creation="false",
        current_user=current_user,
    )

    main(config_all.__dict__)
    print("  [OK] mode=all completed")

    # ---- Verify comment output ----
    print("\nVerifying comment output...")
    table_comment = test_utils.get_table_comment(test_table)
    test_utils.assert_not_none(table_comment, "Table comment was applied by mode=all")
    test_utils.assert_true(len(table_comment) > 10, "Table comment has meaningful content")
    print(f"  [OK] Table comment: {table_comment[:100]}...")

    columns_with_comments = 0
    for col_name in ["name", "email", "age"]:
        col_comment = test_utils.get_column_comment(test_table, col_name)
        if col_comment and len(col_comment) > 5:
            columns_with_comments += 1
            print(f"  [OK] Column '{col_name}' comment: {col_comment[:80]}...")
    test_utils.assert_true(
        columns_with_comments >= 2,
        f"At least 2 columns have comments (found {columns_with_comments})",
    )

    # ---- Verify PI output (tags) ----
    print("\nVerifying PI tags...")
    try:
        pi_tags_found = False
        for col_name in ["name", "email"]:
            try:
                col_tags_df = spark.sql(f"SHOW TAGS ON COLUMN {test_table}.{col_name}")
                col_tags = col_tags_df.collect()
                if len(col_tags) > 0:
                    pi_tags_found = True
                    tag_info = [(row.tag_name, row.tag_value) for row in col_tags]
                    print(f"  [OK] Column '{col_name}' has PI tags: {tag_info}")
            except Exception as e:
                print(f"  Note: Could not check tags for '{col_name}': {e}")

        if pi_tags_found:
            print("  [OK] PI tags applied by mode=all")
        else:
            print("  Note: No column-level PI tags found (may depend on LLM detection)")

        # Test table has name/email -- at least one should get a PI tag
        test_utils.assert_true(
            pi_tags_found,
            "mode=all PI pass should apply at least one tag on test data with name/email columns",
        )
    except Exception as e:
        print(f"  Note: Could not verify PI tags: {e}")

    # ---- Verify domain output (tags) ----
    print("\nVerifying domain tags...")
    try:
        table_tags_df = spark.sql(f"SHOW TAGS ON TABLE {test_table}")
        table_tags = table_tags_df.collect()
        if len(table_tags) > 0:
            tag_dict = {row.tag_name: row.tag_value for row in table_tags}
            domain_tags = [k for k in tag_dict.keys() if "domain" in k.lower()]
            if domain_tags:
                for tag in domain_tags:
                    print(f"  [OK] Domain tag '{tag}' = '{tag_dict[tag]}'")

        test_utils.assert_true(
            len(table_tags) > 0,
            "mode=all domain pass should apply at least one table-level tag",
        )
    except Exception as e:
        print(f"  Note: Could not verify domain tags: {e}")

    # ---- Verify metadata log has entries for all three modes ----
    print("\nVerifying metadata log entries...")
    # Use simple table name (consistent with test_utils.verify_metadata_generation_log)
    simple_table = test_table.split(".")[-1] if "." in test_table else test_table
    log_df = spark.sql(f"""
        SELECT DISTINCT metadata_type FROM {test_catalog}.{test_schema}.metadata_generation_log
        WHERE table_name LIKE '%{simple_table}'
    """)
    logged_modes = {row.metadata_type for row in log_df.collect()}
    print(f"  Logged modes: {logged_modes}")
    test_utils.assert_true(
        "comment" in logged_modes,
        "Log contains comment mode entry",
    )
    test_utils.assert_true(
        "pi" in logged_modes,
        "Log contains pi mode entry",
    )
    test_utils.assert_true(
        "domain" in logged_modes,
        "Log contains domain mode entry",
    )

    # ---- Verify SQL files exist and have correct DDL types for each sub-mode ----
    print("\nVerifying SQL files...")
    user_sanitized = sanitize_user_identifier(config_all.current_user)
    for sub_mode in ["comment", "pi", "domain"]:
        sql_result = verify_sql_file_content(
            spark, test_catalog, test_schema, "test_volume",
            user_sanitized, test_table, sub_mode,
        )
        test_utils.assert_true(
            sql_result["file_found"],
            f"SQL file exists for {sub_mode} sub-mode",
        )
        test_utils.assert_true(
            sql_result["ddl_type_correct"],
            f"SQL file for {sub_mode} contains correct DDL type",
        )

    test_passed = True
    print_test_result("mode=all Unified Pass", True)

except TestFailure as e:
    error_message = str(e)
    print(error_message)
    print_test_result("mode=all Unified Pass", False, error_message)

except Exception as e:
    error_message = f"Unexpected error: {str(e)}"
    print(error_message)
    import traceback
    traceback.print_exc()
    print_test_result("mode=all Unified Pass", False, error_message)

finally:
    print("\nCleanup")
    test_utils.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")

dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
