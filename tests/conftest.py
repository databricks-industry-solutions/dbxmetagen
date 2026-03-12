"""Shared test fixtures for dbxmetagen unit tests."""

import sys
import os
from unittest.mock import MagicMock, patch
import pytest

# Ensure src/ is on the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# Stub heavy Databricks/Spark deps before any dbxmetagen imports trigger them
for _mod_name in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.sql.window", "pyspark.sql.column",
    "pandas",
    "databricks_langchain",
]:
    if _mod_name not in sys.modules:
        sys.modules[_mod_name] = MagicMock()


@pytest.fixture
def mock_spark():
    """Mock SparkSession with common methods stubbed."""
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    spark.createDataFrame.return_value = MagicMock()
    spark.table.return_value = MagicMock()
    spark.read.table.return_value = MagicMock()
    return spark


@pytest.fixture
def mock_spark_session(mock_spark):
    """Patch SparkSession.builder.getOrCreate to return mock_spark."""
    with patch("pyspark.sql.SparkSession") as mock_cls:
        mock_cls.builder.getOrCreate.return_value = mock_spark
        yield mock_spark


@pytest.fixture
def base_config_kwargs():
    """Minimal kwargs to create a MetadataConfig without YAML loading."""
    return {
        "skip_yaml_loading": True,
        "catalog_name": "test_catalog",
        "schema_name": "test_schema",
        "table_names": "test_catalog.test_schema.test_table",
        "mode": "comment",
        "model": "test-model",
        "volume_name": "test_volume",
        "apply_ddl": False,
        "allow_data": True,
        "sample_size": 5,
        "columns_per_call": 5,
        "temperature": 0.1,
        "max_tokens": 4096,
        "max_prompt_length": 100000,
        "ddl_output_format": "tsv",
        "current_user": "test_user@example.com",
        "control_table": True,
        "cleanup_control_table": False,
        "word_limit_per_cell": 50,
        "limit_prompt_based_on_cell_len": True,
        "allow_data_in_comments": True,
        "include_datatype_from_metadata": False,
        "include_possible_data_fields_in_metadata": True,
        "include_existing_table_comment": False,
        "add_metadata": True,
        "reviewable_output_format": "tsv",
        "review_input_file_type": "tsv",
        "review_output_file_type": "tsv",
        "review_apply_ddl": False,
        "grant_permissions_after_creation": False,
        "include_deterministic_pi": False,
        "tag_none_fields": True,
        "federation_mode": False,
    }


@pytest.fixture
def test_config(base_config_kwargs):
    """Create a MetadataConfig with YAML loading skipped."""
    from dbxmetagen.config import MetadataConfig
    return MetadataConfig(**base_config_kwargs)


@pytest.fixture
def sample_table_rows():
    """Sample table row data for testing."""
    return [
        {"col_a": "foo", "col_b": 1, "col_c": "2024-01-01"},
        {"col_a": "bar", "col_b": 2, "col_c": "2024-01-02"},
        {"col_a": "baz", "col_b": 3, "col_c": "2024-01-03"},
    ]
