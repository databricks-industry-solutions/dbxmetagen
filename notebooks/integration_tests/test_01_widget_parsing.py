# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Widget Value Parsing
# MAGIC
# MAGIC Tests that widget values are correctly parsed as booleans in real Databricks environment.

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

import sys

sys.path.append("../../")

from src.dbxmetagen.config import MetadataConfig, _parse_bool

# Setup widgets
dbutils.widgets.text("test_catalog", "dev_integration_tests", "Test Catalog")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")
dbutils.widgets.dropdown(
    "apply_ddl_test", "false", ["true", "false"], "Apply DDL Test Value"
)
dbutils.widgets.dropdown(
    "cleanup_test", "true", ["true", "false"], "Cleanup Test Value"
)

# Get values
test_catalog = dbutils.widgets.get("test_catalog")
test_schema = dbutils.widgets.get("test_schema")

# COMMAND ----------

print_test_header("Widget Value Parsing Integration Test")

spark = SparkSession.builder.getOrCreate()
test_utils = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    # Test 1: Widget value "false" should parse to boolean False
    print("\nTest 1: apply_ddl widget with value 'false'")
    apply_ddl_widget = dbutils.widgets.get("apply_ddl_test")
    print(f"  Widget raw value: '{apply_ddl_widget}' (type: {type(apply_ddl_widget)})")

    parsed_value = _parse_bool(apply_ddl_widget)
    print(f"  Parsed value: {parsed_value} (type: {type(parsed_value)})")

    test_utils.assert_equals(apply_ddl_widget, "false", "Widget returns string 'false'")
    test_utils.assert_equals(
        parsed_value, False, "_parse_bool('false') returns boolean False"
    )
    test_utils.assert_true(
        isinstance(parsed_value, bool), "Parsed value is boolean type"
    )

    # Test 2: Widget value "true" should parse to boolean True
    print("\nTest 2: cleanup_control_table widget with value 'true'")
    cleanup_widget = dbutils.widgets.get("cleanup_test")
    print(f"  Widget raw value: '{cleanup_widget}' (type: {type(cleanup_widget)})")

    parsed_cleanup = _parse_bool(cleanup_widget)
    print(f"  Parsed value: {parsed_cleanup} (type: {type(parsed_cleanup)})")

    test_utils.assert_equals(cleanup_widget, "true", "Widget returns string 'true'")
    test_utils.assert_equals(
        parsed_cleanup, True, "_parse_bool('true') returns boolean True"
    )

    # Test 3: Config correctly parses widget values
    print("\nTest 3: MetadataConfig with widget values")
    config = MetadataConfig(
        yaml_file_path="../../variables.yml",  # Production YAML (2 levels up)
        apply_ddl=apply_ddl_widget,  # Widget value overrides YAML
        cleanup_control_table=cleanup_widget,  # Widget value overrides YAML
    )

    print(f"  config.apply_ddl: {config.apply_ddl} (type: {type(config.apply_ddl)})")
    print(
        f"  config.cleanup_control_table: {config.cleanup_control_table} (type: {type(config.cleanup_control_table)})"
    )

    test_utils.assert_equals(config.apply_ddl, False, "Config.apply_ddl is False")
    test_utils.assert_equals(
        config.cleanup_control_table, True, "Config.cleanup_control_table is True"
    )

    # Test 4: Whitespace handling
    print("\nTest 4: Whitespace handling")
    test_utils.assert_equals(
        _parse_bool("  true  "), True, "Whitespace trimmed from '  true  '"
    )
    test_utils.assert_equals(
        _parse_bool("false  "), False, "Whitespace trimmed from 'false  '"
    )

    test_passed = True
    print_test_result("Widget Value Parsing", True)

except TestFailure as e:
    error_message = str(e)
    print(error_message)
    print_test_result("Widget Value Parsing", False, error_message)
    raise

except Exception as e:
    error_message = f"Unexpected error: {str(e)}"
    print(error_message)
    print_test_result("Widget Value Parsing", False, error_message)
    raise

# COMMAND ----------

# Return result for job orchestration
dbutils.notebook.exit(json.dumps({"passed": test_passed, "error": error_message}))
