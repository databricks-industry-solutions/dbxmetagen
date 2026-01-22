# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: Concurrent Tasks Setup
# MAGIC
# MAGIC Creates test tables for concurrent task testing.

# COMMAND ----------

# MAGIC %run ./test_utils

# COMMAND ----------

import sys

sys.path.append("../../")

from src.dbxmetagen.user_utils import sanitize_user_identifier, get_current_user

# Setup widgets
dbutils.widgets.text("test_catalog", "dev_integration_tests", "Test Catalog")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")

test_catalog = dbutils.widgets.get("test_catalog")
test_schema = dbutils.widgets.get("test_schema")

# COMMAND ----------

print_test_header("Concurrent Tasks Setup")

spark = SparkSession.builder.getOrCreate()
test_utils = IntegrationTestUtils(spark, test_catalog, test_schema)

test_passed = False
error_message = None

try:
    # Setup
    print("\nSetup: Creating test environment")
    test_utils.setup_test_environment()

    # Create 3 test tables for concurrent processing
    test_tables = []
    for i in range(1, 4):
        table_name = f"concurrent_test_{i}"
        full_table_name = test_utils.create_test_table(table_name, with_comment=False)
        test_tables.append(full_table_name)
        print(f"  Created test table: {full_table_name}")

    print(f"\nCreated {len(test_tables)} test tables for concurrent processing:")
    for t in test_tables:
        print(f"  - {t}")

    test_passed = True
    print_test_result("Concurrent Tasks Setup", True)

except Exception as e:
    error_message = f"Setup failed: {str(e)}"
    print(error_message)
    import traceback
    traceback.print_exc()
    print_test_result("Concurrent Tasks Setup", False, error_message)

# COMMAND ----------

# Return result
if not test_passed:
    raise Exception(f"Test failed: {error_message}")

# Pass table names to downstream tasks via notebook exit
result = {
    "passed": test_passed,
    "error": error_message,
    "test_tables": test_tables if test_passed else []
}
dbutils.notebook.exit(json.dumps(result))

