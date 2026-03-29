# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Reviewed DDL Sync
# MAGIC
# MAGIC Tests the workflow of reviewing and syncing DDL changes:
# MAGIC 1. Generate initial metadata
# MAGIC 2. Export to reviewable format (Excel/TSV)
# MAGIC 3. Manually modify comments/tags
# MAGIC 4. Re-import and apply reviewed DDL
# MAGIC 5. Verify changes are reflected in database

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

import sys
import os
import pandas as pd

sys.path.append("../../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.config import MetadataConfig
from dbxmetagen.main import main
from dbxmetagen.ddl_regenerator import process_metadata_file, load_metadata_file
from dbxmetagen.user_utils import sanitize_user_identifier, get_current_user
from datetime import datetime

# Setup widgets
dbutils.widgets.text("test_catalog", "dev_integration_tests", "Test Catalog")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")

test_catalog = dbutils.widgets.get("test_catalog")
test_schema = dbutils.widgets.get("test_schema")

# Get current user for integration tests
current_user = get_current_user()

# COMMAND ----------

print_test_header("Reviewed DDL Sync Integration Test")

spark = SparkSession.builder.getOrCreate()
test_utils = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    # Setup
    print("\nSetup: Creating test environment")
    test_utils.setup_test_environment()

    test_table = test_utils.create_test_table("test_reviewed_ddl", with_comment=False)

    # Step 1: Generate initial metadata
    print("\nStep 1: Generate initial metadata")

    config_initial = MetadataConfig(
        yaml_file_path="../../variables.yml",
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        mode="comment",
        apply_ddl=True,  # Apply initial metadata
        grant_permissions_after_creation="false",
        current_user=current_user,
        reviewable_output_format="tsv",  # Export for review
    )

    main(config_initial.__dict__)
    print("  [OK] Initial metadata generated and applied")

    # Verify initial comment exists
    initial_comment = test_utils.get_table_comment(test_table)
    test_utils.assert_not_none(initial_comment, "Initial table comment exists")
    print(f"  [OK] Initial table comment: {initial_comment[:100]}...")

    # Step 2: Locate the exported TSV file
    print("\nStep 2: Locate exported TSV file")

    user_sanitized = sanitize_user_identifier(current_user)
    current_date = datetime.now().strftime("%Y%m%d")
    export_dir = f"/Volumes/{test_catalog}/{test_schema}/test_volume/{user_sanitized}/{current_date}/exportable_run_logs"

    # The exported filename uses mode + log_timestamp (not the table name)
    expected_tsv_name = f"review_metadata_{config_initial.mode}_{config_initial.log_timestamp}.tsv"
    simple_table_name = test_table.split(".")[-1]

    try:
        export_files = dbutils.fs.ls(export_dir)
        tsv_file = None

        # First try exact match by expected name
        for file_info in export_files:
            if file_info.name == expected_tsv_name:
                tsv_file = file_info.path
                print(f"  [OK] Found expected TSV file: {file_info.name}")
                break

        # Fallback: any .tsv file in the directory
        if tsv_file is None:
            for file_info in export_files:
                if file_info.name.endswith(".tsv"):
                    tsv_file = file_info.path
                    print(f"  [OK] Found TSV file (fallback): {file_info.name}")
                    break

        test_utils.assert_not_none(tsv_file, "TSV export file exists")

    except Exception as e:
        print(f"  [ERROR] Could not find TSV file: {e}")
        raise TestFailure(f"Could not find TSV export file: {e}")

    # Step 3: Manually modify the TSV file (simulate review)
    print("\nStep 3: Simulate manual review - modify comments")

    # Read the TSV -- UC Volumes are accessible directly at /Volumes/
    tsv_path_local = tsv_file.replace("dbfs:", "")
    df = pd.read_csv(tsv_path_local, sep="\t", dtype=str, keep_default_na=False, na_values=[])

    print(f"  Loaded {len(df)} rows from TSV")
    print(f"  [DIAG] Columns: {list(df.columns)}")
    print(f"  [DIAG] ddl_type values: {df['ddl_type'].tolist() if 'ddl_type' in df.columns else 'NO ddl_type column'}")

    # Modify the table comment
    table_rows = df[df["ddl_type"] == "table"]
    if len(table_rows) > 0:
        # --- CHECKPOINT 1: Show original DDL before modification ---
        orig_table_ddl = table_rows.iloc[0]["ddl"]
        print(f"  [DIAG] CHECKPOINT 1 - Original table DDL:\n    {orig_table_ddl}")
        print(f"  [DIAG] CHECKPOINT 1 - Original column_content: {table_rows.iloc[0].get('column_content', 'N/A')}")
        print(f"  [DIAG] CHECKPOINT 1 - column_name value: {repr(table_rows.iloc[0].get('column_name', 'N/A'))}")

        df.loc[df["ddl_type"] == "table", "column_content"] = (
            "REVIEWED: This is a manually reviewed table comment"
        )
        print("  [OK] Modified table comment")

    # Modify a column comment
    name_column_rows = df[(df["ddl_type"] == "column") & (df["column_name"] == "name")]
    if len(name_column_rows) > 0:
        df.loc[
            (df["ddl_type"] == "column") & (df["column_name"] == "name"),
            "column_content",
        ] = "REVIEWED: This column contains person names"
        print("  [OK] Modified 'name' column comment")

    # --- CHECKPOINT 2: Test replace_comment_in_ddl directly ---
    if len(table_rows) > 0:
        from dbxmetagen.ddl_regenerator import replace_comment_in_ddl
        test_ddl = orig_table_ddl
        test_result = replace_comment_in_ddl(test_ddl, "REVIEWED: This is a manually reviewed table comment")
        print(f"  [DIAG] CHECKPOINT 2 - replace_comment_in_ddl input:\n    {test_ddl}")
        print(f"  [DIAG] CHECKPOINT 2 - replace_comment_in_ddl output:\n    {test_result}")
        if "REVIEWED:" in test_result:
            print("  [DIAG] CHECKPOINT 2 - Regex replacement WORKED")
        elif test_result == test_ddl:
            print("  [DIAG] CHECKPOINT 2 - Regex replacement FAILED (DDL unchanged)")
        else:
            print("  [DIAG] CHECKPOINT 2 - Regex replacement PARTIAL (DDL changed but no REVIEWED:)")

    # Save the reviewed TSV to reviewed_outputs directory
    reviewed_dir = f"/Volumes/{test_catalog}/{test_schema}/test_volume/{user_sanitized}/reviewed_outputs"
    os.makedirs(reviewed_dir, exist_ok=True)

    reviewed_file_name = f"{simple_table_name}_comment.tsv"
    reviewed_file_path = f"{reviewed_dir}/{reviewed_file_name}"

    df.to_csv(reviewed_file_path, sep="\t", index=False)
    print(f"  [OK] Saved reviewed file: {reviewed_file_name}")

    # Step 4: Process the reviewed file to apply changes
    print("\nStep 4: Process reviewed file and apply DDL")

    config_reviewed = MetadataConfig(
        yaml_file_path="../../variables.yml",
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=test_table,
        volume_name="test_volume",
        mode="comment",
        review_apply_ddl=True,  # Apply reviewed DDL
        review_input_file_type="tsv",
        review_output_file_type="sql",
        column_with_reviewed_ddl="column_content",
        current_user=current_user,
    )

    process_metadata_file(config_reviewed, reviewed_file_name)
    print("  [OK] Reviewed DDL processed and applied")

    # --- CHECKPOINT 3: Read the generated SQL file and print its contents ---
    try:
        reviewed_sql_check_dir = f"{export_dir}"
        for file_info in dbutils.fs.ls(reviewed_sql_check_dir):
            if "reviewed" in file_info.name and file_info.name.endswith(".sql"):
                sql_diag_path = file_info.path.replace("dbfs:", "")
                sql_diag_content = open(sql_diag_path, "r").read()
                print(f"  [DIAG] CHECKPOINT 3 - Reviewed SQL file: {file_info.name}")
                print(f"  [DIAG] CHECKPOINT 3 - SQL content (first 800 chars):\n{sql_diag_content[:800]}")
                if "REVIEWED: This is a manually reviewed table comment" in sql_diag_content:
                    print("  [DIAG] CHECKPOINT 3 - Reviewed table text FOUND in SQL file")
                else:
                    print("  [DIAG] CHECKPOINT 3 - Reviewed table text NOT FOUND in SQL file")
                if "REVIEWED: This column contains person names" in sql_diag_content:
                    print("  [DIAG] CHECKPOINT 3 - Reviewed column text FOUND in SQL file")
                else:
                    print("  [DIAG] CHECKPOINT 3 - Reviewed column text NOT FOUND in SQL file")
                break
        else:
            print("  [DIAG] CHECKPOINT 3 - No reviewed SQL file found in directory")
    except Exception as diag_e:
        print(f"  [DIAG] CHECKPOINT 3 - Could not read SQL file: {diag_e}")

    # Step 5: Verify the changes were applied
    print("\nStep 5: Verify reviewed comments were applied")

    # Check table comment changed
    final_table_comment = test_utils.get_table_comment(test_table)
    test_utils.assert_not_none(final_table_comment, "Table comment exists after review")
    test_utils.assert_contains(
        final_table_comment,
        "REVIEWED: This is a manually reviewed table comment",
        "Table comment reflects manual review",
    )
    print(f"  [OK] Table comment updated: {final_table_comment[:100]}...")

    # Check column comment changed
    final_name_comment = test_utils.get_column_comment(test_table, "name")
    test_utils.assert_not_none(final_name_comment, "'name' column comment exists")
    test_utils.assert_contains(
        final_name_comment,
        "REVIEWED: This column contains person names",
        "'name' column comment reflects manual review",
    )
    print(f"  [OK] Column 'name' comment updated: {final_name_comment}")

    # Step 6: Verify reviewed SQL file was generated
    print("\nStep 6: Verify reviewed SQL file generated")

    reviewed_sql_dir = f"{export_dir}"  # Same location as original
    reviewed_sql_files = dbutils.fs.ls(reviewed_sql_dir)

    reviewed_sql_found = False
    for file_info in reviewed_sql_files:
        if "reviewed" in file_info.name and file_info.name.endswith(".sql"):
            print(f"  [OK] Found reviewed SQL file: {file_info.name}")
            reviewed_sql_found = True

            # Read and verify content
            sql_content = open(file_info.path.replace("dbfs:", ""), "r").read()
            test_utils.assert_contains(
                sql_content,
                "REVIEWED: This is a manually reviewed table comment",
                "Reviewed SQL contains updated comments",
            )
            break

    test_utils.assert_true(reviewed_sql_found, "Reviewed SQL file was generated")

    test_passed = True
    print_test_result("Reviewed DDL Sync", True)

except TestFailure as e:
    error_message = str(e)
    print(error_message)
    print_test_result("Reviewed DDL Sync", False, error_message)

except Exception as e:
    error_message = f"Unexpected error: {str(e)}"
    print(error_message)
    import traceback

    traceback.print_exc()
    print_test_result("Reviewed DDL Sync", False, error_message)

finally:
    # Cleanup
    print("\nCleanup")
    test_utils.cleanup_test_artifacts()

# COMMAND ----------

# Return result
if not test_passed:
    raise Exception(f"Test failed: {error_message}")

dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
