"""
Integration test for apply_ddl flag behavior.

This test verifies that when apply_ddl=false, NO DDL statements are applied
to database tables, across all modes (comment, pi, domain).

REQUIRES: Active Databricks workspace with proper permissions.

Run in Databricks notebook with:
%run ./test_apply_ddl_integration
"""

import sys
import os

# Add parent directories to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from pyspark.sql import SparkSession
from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.processing import generate_metadata

# Get Spark session
spark = SparkSession.builder.getOrCreate()


def setup_test_table(table_name: str):
    """Create a test table with no comments for integration testing."""
    # Drop if exists
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    # Create simple test table
    spark.sql(
        f"""
        CREATE TABLE {table_name} (
            id INT,
            name STRING,
            email STRING
        )
    """
    )

    print(f"[OK] Created test table: {table_name}")


def verify_no_comments(table_name: str):
    """Verify that no comments exist on the table or columns."""
    # Check table comment
    table_info = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
    for row in table_info:
        if row["col_name"] == "Comment" or row["col_name"] == "comment":
            comment = row["data_type"]
            if comment and comment.strip() and comment.lower() != "null":
                raise AssertionError(
                    f"[FAIL] Table has comment when it shouldn't: {comment}"
                )

    # Check column comments
    columns = spark.sql(f"DESCRIBE {table_name}").collect()
    for col in columns:
        if col["comment"]:
            raise AssertionError(
                f"[FAIL] Column '{col['col_name']}' has comment when it shouldn't: {col['comment']}"
            )

    print(f"[OK] Verified: No comments on {table_name}")


def verify_has_comments(table_name: str):
    """Verify that comments DO exist on the table or columns."""
    has_any_comment = False

    # Check table comment
    table_info = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
    for row in table_info:
        if row["col_name"] == "Comment" or row["col_name"] == "comment":
            comment = row["data_type"]
            if comment and comment.strip() and comment.lower() != "null":
                has_any_comment = True
                print(f"[OK] Found table comment: {comment}")

    # Check column comments
    columns = spark.sql(f"DESCRIBE {table_name}").collect()
    for col in columns:
        if col["comment"]:
            has_any_comment = True
            print(f"[OK] Found column comment on '{col['col_name']}': {col['comment']}")

    if not has_any_comment:
        raise AssertionError(
            f"[FAIL] No comments found on {table_name} when they should exist"
        )


def test_apply_ddl_false_comment_mode():
    """Test that apply_ddl=false prevents DDL application in comment mode."""
    print("\n" + "=" * 80)
    print("TEST: apply_ddl=false in COMMENT mode")
    print("=" * 80)

    table_name = "test_catalog.test_schema.test_apply_ddl_false_comment"

    # Setup
    setup_test_table(table_name)

    # Run metadata generation with apply_ddl=FALSE
    config = MetadataConfig(
        skip_yaml_loading=True,
        catalog_name="test_catalog",
        schema_name="test_schema",
        table_names=table_name,
        mode="comment",
        apply_ddl=False,  # The critical flag
        dry_run=False,
        allow_data=True,
    )

    # This should generate metadata but NOT apply DDL
    generate_metadata(config)

    # Verify no comments were applied
    verify_no_comments(table_name)

    print("[PASS] apply_ddl=false prevented DDL application in comment mode")


def test_apply_ddl_true_comment_mode():
    """Test that apply_ddl=true DOES apply DDL in comment mode."""
    print("\n" + "=" * 80)
    print("TEST: apply_ddl=true in COMMENT mode")
    print("=" * 80)

    table_name = "test_catalog.test_schema.test_apply_ddl_true_comment"

    # Setup
    setup_test_table(table_name)

    # Run metadata generation with apply_ddl=TRUE
    config = MetadataConfig(
        skip_yaml_loading=True,
        catalog_name="test_catalog",
        schema_name="test_schema",
        table_names=table_name,
        mode="comment",
        apply_ddl=True,  # Should apply DDL
        dry_run=False,  # Should execute
        allow_data=True,
    )

    # This should generate metadata AND apply DDL
    generate_metadata(config)

    # Verify comments WERE applied
    verify_has_comments(table_name)

    print("[PASS] apply_ddl=true successfully applied DDL in comment mode")


def test_apply_ddl_false_pi_mode():
    """Test that apply_ddl=false prevents DDL application in PI mode."""
    print("\n" + "=" * 80)
    print("TEST: apply_ddl=false in PI mode")
    print("=" * 80)

    table_name = "test_catalog.test_schema.test_apply_ddl_false_pi"

    # Setup
    setup_test_table(table_name)

    # Run metadata generation with apply_ddl=FALSE
    config = MetadataConfig(
        skip_yaml_loading=True,
        catalog_name="test_catalog",
        schema_name="test_schema",
        table_names=table_name,
        mode="pi",
        apply_ddl=False,  # The critical flag
        dry_run=False,
        allow_data=True,
    )

    # This should generate metadata but NOT apply DDL
    generate_metadata(config)

    # Verify no tags were applied
    tags_df = spark.sql(f"SHOW TAGS ON TABLE {table_name}")
    if tags_df.count() > 0:
        raise AssertionError(
            f"[FAIL] Tags found on {table_name} when they shouldn't exist"
        )

    print("[PASS] apply_ddl=false prevented DDL application in PI mode")


def test_apply_ddl_false_domain_mode():
    """Test that apply_ddl=false prevents DDL application in domain mode."""
    print("\n" + "=" * 80)
    print("TEST: apply_ddl=false in DOMAIN mode")
    print("=" * 80)

    table_name = "test_catalog.test_schema.test_apply_ddl_false_domain"

    # Setup
    setup_test_table(table_name)

    # Run metadata generation with apply_ddl=FALSE
    config = MetadataConfig(
        skip_yaml_loading=True,
        catalog_name="test_catalog",
        schema_name="test_schema",
        table_names=table_name,
        mode="domain",
        apply_ddl=False,  # The critical flag
        dry_run=False,
        allow_data=True,
    )

    # This should generate metadata but NOT apply DDL
    generate_metadata(config)

    # Verify no domain tags were applied
    tags_df = spark.sql(f"SHOW TAGS ON TABLE {table_name}")
    domain_tags = [
        row for row in tags_df.collect() if "domain" in row["tag_name"].lower()
    ]
    if len(domain_tags) > 0:
        raise AssertionError(
            f"[FAIL] Domain tags found on {table_name} when they shouldn't exist"
        )

    print("[PASS] apply_ddl=false prevented DDL application in domain mode")


def test_apply_ddl_string_values():
    """Test that string values 'false' and 'true' are properly parsed."""
    print("\n" + "=" * 80)
    print("TEST: String value parsing ('false', 'true', '')")
    print("=" * 80)

    # Test string "false"
    config1 = MetadataConfig(
        skip_yaml_loading=True,
        catalog_name="test_catalog",
        schema_name="test_schema",
        table_names="test.table",
        apply_ddl="false",  # String
        dry_run="false",  # String
    )

    assert config1.apply_ddl is False, "String 'false' should parse to boolean False"
    assert config1.dry_run is False, "String 'false' should parse to boolean False"
    assert isinstance(config1.apply_ddl, bool), "apply_ddl should be boolean"
    assert isinstance(config1.dry_run, bool), "dry_run should be boolean"

    # Test empty string
    config2 = MetadataConfig(
        skip_yaml_loading=True,
        catalog_name="test_catalog",
        schema_name="test_schema",
        table_names="test.table",
        apply_ddl="",  # Empty string
        dry_run="",  # Empty string
    )

    assert config2.apply_ddl is False, "Empty string should parse to boolean False"
    assert config2.dry_run is False, "Empty string should parse to boolean False"

    # Test string "true"
    config3 = MetadataConfig(
        skip_yaml_loading=True,
        catalog_name="test_catalog",
        schema_name="test_schema",
        table_names="test.table",
        apply_ddl="true",  # String
        dry_run="true",  # String
    )

    assert config3.apply_ddl is True, "String 'true' should parse to boolean True"
    assert config3.dry_run is True, "String 'true' should parse to boolean True"

    print("[PASS] All string values correctly parsed to booleans")


def run_all_tests():
    """Run all integration tests."""
    print("\n" + "=" * 80)
    print("RUNNING APPLY_DDL INTEGRATION TESTS")
    print("=" * 80)

    try:
        # Test string parsing (doesn't require tables)
        test_apply_ddl_string_values()

        # Test actual DDL behavior
        test_apply_ddl_false_comment_mode()
        test_apply_ddl_true_comment_mode()
        test_apply_ddl_false_pi_mode()
        test_apply_ddl_false_domain_mode()

        print("\n" + "=" * 80)
        print("[PASS] ALL INTEGRATION TESTS PASSED")
        print("=" * 80)

    except Exception as e:
        print("\n" + "=" * 80)
        print("[FAIL] INTEGRATION TEST FAILED")
        print("=" * 80)
        print(f"Error: {e}")
        raise


# Run tests when executed
if __name__ == "__main__":
    run_all_tests()
