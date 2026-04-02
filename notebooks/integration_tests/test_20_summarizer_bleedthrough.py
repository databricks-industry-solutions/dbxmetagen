# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Summarizer Few-Shot Bleedthrough
# MAGIC
# MAGIC Verifies that a table with a schema very similar to the clinical-trials
# MAGIC few-shot example in `TableCommentSummarizer` does NOT produce a comment
# MAGIC that parrots the few-shot assistant response.

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

import sys

sys.path.append("../../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.config import MetadataConfig
from dbxmetagen.main import main
from dbxmetagen.user_utils import sanitize_user_identifier, get_current_user

# Setup widgets
dbutils.widgets.text("test_catalog", "dev_integration_tests", "Test Catalog")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")

test_catalog = dbutils.widgets.get("test_catalog")
test_schema = dbutils.widgets.get("test_schema")

current_user = get_current_user()

# COMMAND ----------

print_test_header("Summarizer Few-Shot Bleedthrough Test")

spark = SparkSession.builder.getOrCreate()
test_utils = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    print("\nSetup: Creating test environment")
    test_utils.setup_test_environment()

    # Create a clinical-trials-like table that closely mirrors the few-shot example
    table_name = "test_clinical_trial_metrics"
    full_table_name = f"{test_catalog}.{test_schema}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

    spark.sql(f"""
        CREATE TABLE {full_table_name} (
            nct_id STRING,
            facility_count INT,
            subject_count INT,
            sae_subject_count INT,
            nsae_subject_count INT,
            registration_date DATE,
            results_reported BOOLEAN,
            days_to_report INT,
            min_age INT,
            max_age INT,
            age_unit STRING,
            outcome_measure_type STRING,
            has_us_facility BOOLEAN
        ) USING DELTA
    """)

    spark.sql(f"""
        INSERT INTO {full_table_name} VALUES
        ('NCT00000001', 12, 340, 5, 42, '2019-03-15', true,  365, 18, 65, 'Years', 'Primary',   true),
        ('NCT00000002', 8,  120, 2, 18, '2020-07-22', false, NULL, 21, 80, 'Years', 'Secondary', true),
        ('NCT00000003', 25, 800, 12, 95, '2018-01-10', true,  210, 18, 99, 'Years', 'Primary',   true),
        ('NCT00000004', 3,  45,  0, 6,  '2021-11-05', false, NULL, 12, 17, 'Years', 'Safety',    false),
        ('NCT00000005', 15, 500, 8, 60, '2017-06-30', true,  540, 18, 75, 'Years', 'Primary',   true),
        ('NCT00000006', 6,  90,  1, 10, '2022-02-14', false, NULL, 65, 99, 'Years', 'Secondary', false),
        ('NCT00000007', 20, 650, 10, 78, '2019-09-01', true,  180, 18, 55, 'Years', 'Primary',   true),
        ('NCT00000008', 4,  60,  0, 8,  '2023-04-18', false, NULL, 0,  17, 'Years', 'Safety',    false),
        ('NCT00000009', 30, 1200, 18, 140, '2016-12-01', true, 730, 18, 99, 'Years', 'Primary',  true),
        ('NCT00000010', 10, 250, 3, 30, '2020-08-20', true,  400, 18, 70, 'Years', 'Secondary', true)
    """)
    test_utils.test_artifacts.append(("table", full_table_name))
    print(f"Created test table: {full_table_name}")

    # Run metadata generation in comment mode with apply_ddl=true
    print("\nRunning metadata generation (comment mode, apply_ddl=true)")
    config = MetadataConfig(
        yaml_file_path="../../variables.yml",
        catalog_name=test_catalog,
        schema_name=test_schema,
        table_names=full_table_name,
        volume_name="test_volume",
        mode="comment",
        apply_ddl=True,
        grant_permissions_after_creation="false",
        current_user=current_user,
    )
    main(config.__dict__)
    print("  [OK] Metadata generation completed")

    # Retrieve the applied table comment
    print("\nVerifying table comment for bleedthrough")
    table_comment = test_utils.get_table_comment(full_table_name)

    test_utils.assert_true(
        table_comment is not None and len(table_comment) > 0,
        "Table comment was generated and applied",
    )

    comment_lower = table_comment.lower()

    # 1. No schema reference bleedthrough from few-shot example
    test_utils.assert_true(
        "dbxmetagen.test_data" not in comment_lower,
        "Comment does not reference the few-shot example schema 'dbxmetagen.test_data'",
    )

    # 2. No verbatim phrases from the few-shot assistant response
    bleedthrough_phrases = [
        "regulatory compliance regarding result reporting timeframes",
        "geographical distribution of trial facilities",
        "demographic inclusion criteria across different studies",
        "monitoring clinical trial safety profiles",
    ]
    for phrase in bleedthrough_phrases:
        test_utils.assert_true(
            phrase not in comment_lower,
            f"Comment does not contain verbatim few-shot phrase: '{phrase}'",
        )

    # 3. Sanity: comment is substantive and relevant to the actual table
    test_utils.assert_true(
        len(table_comment) > 50,
        f"Comment is substantive (length={len(table_comment)} > 50)",
    )

    relevant_terms = ["clinical", "trial", "facility", "nct", "subject", "adverse"]
    has_relevant = any(term in comment_lower for term in relevant_terms)
    test_utils.assert_true(
        has_relevant,
        "Comment mentions at least one term relevant to the actual table content",
    )

    print(f"\n  Generated comment ({len(table_comment)} chars):")
    print(f"  {table_comment[:300]}...")

    test_passed = True
    print_test_result("Summarizer Bleedthrough", True)

except TestFailure as e:
    error_message = str(e)
    print(error_message)
    print_test_result("Summarizer Bleedthrough", False, error_message)

except Exception as e:
    error_message = f"Unexpected error: {str(e)}"
    print(error_message)
    import traceback

    traceback.print_exc()
    print_test_result("Summarizer Bleedthrough", False, error_message)

finally:
    print("\nCleanup")
    test_utils.cleanup_test_artifacts()

# COMMAND ----------

if not test_passed:
    raise Exception(f"Test failed: {error_message}")

dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
