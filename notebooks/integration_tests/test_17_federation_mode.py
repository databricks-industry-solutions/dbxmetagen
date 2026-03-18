# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test 17: Federation Mode
# MAGIC
# MAGIC Validates that federation_mode correctly skips Delta-specific operations.
# MAGIC This test uses a local Delta table but verifies that the code paths for
# MAGIC DESCRIBE DETAIL, DESCRIBE EXTENDED, and DDL apply are properly guarded.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("test_schema", "dbxmetagen_tests", "Test Schema")

catalog_name = dbutils.widgets.get("catalog_name")
test_schema = dbutils.widgets.get("test_schema")

print(f"Testing federation mode in {catalog_name}.{test_schema}")

# COMMAND ----------

import sys
sys.path.append("../../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.config import MetadataConfig
from dbxmetagen.processing import get_extended_metadata_for_column

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Config Forces apply_ddl=false

# COMMAND ----------

config = MetadataConfig(
    skip_yaml_loading=True,
    catalog_name=catalog_name,
    schema_name=test_schema,
    table_names=f"{catalog_name}.{test_schema}.test_table",
    mode="comment",
    model="databricks-claude-sonnet-4-6",
    volume_name="test_volume",
    apply_ddl=True,  # Explicitly set to True
    federation_mode=True,  # Federation mode should override
)

assert config.apply_ddl is False, f"Expected apply_ddl=False with federation_mode, got {config.apply_ddl}"
assert config.federation_mode is True
print("PASS: federation_mode forces apply_ddl=False")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: get_extended_metadata_for_column Returns None

# COMMAND ----------

result = get_extended_metadata_for_column(config, "some_table", "some_column")
assert result is None, f"Expected None from get_extended_metadata_for_column in federation mode, got {result}"
print("PASS: get_extended_metadata_for_column returns None in federation mode")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Test: Normal Mode Still Works

# COMMAND ----------

config_normal = MetadataConfig(
    skip_yaml_loading=True,
    catalog_name=catalog_name,
    schema_name=test_schema,
    table_names=f"{catalog_name}.{test_schema}.test_table",
    mode="comment",
    model="databricks-claude-sonnet-4-6",
    volume_name="test_volume",
    apply_ddl=True,
    federation_mode=False,
)

assert config_normal.apply_ddl is True
assert config_normal.federation_mode is False
print("PASS: Normal mode preserves apply_ddl=True")

# COMMAND ----------

print("\nAll federation mode integration tests passed!")
