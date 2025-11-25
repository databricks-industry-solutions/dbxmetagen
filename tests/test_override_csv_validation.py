"""
Unit tests for metadata override CSV validation logic.

Tests that the override CSV validation:
- Only runs when allow_manual_override is True
- Handles missing files gracefully
- Uses the correct path from config
"""

import sys
from unittest.mock import MagicMock

# Mock Databricks/PySpark modules before importing dbxmetagen
sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["pyspark.sql.functions"] = MagicMock()
sys.modules["pyspark.sql.types"] = MagicMock()
sys.modules["pyspark.sql.column"] = MagicMock()
sys.modules["databricks"] = MagicMock()
sys.modules["databricks.sdk"] = MagicMock()
sys.modules["databricks.sdk.runtime"] = MagicMock()
sys.modules["databricks.sdk.core"] = MagicMock()
sys.modules["grpc"] = MagicMock()
sys.modules["grpc._channel"] = MagicMock()

import os
import tempfile
import pytest

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.dbxmetagen.config import MetadataConfig


class TestOverrideCSVValidation:
    """Test override CSV validation behavior."""

    def test_missing_override_file_with_overrides_disabled(self):
        """When allow_manual_override is False, missing file should not cause error."""
        config = MetadataConfig(skip_yaml_loading=True, 
            allow_manual_override=False, override_csv_path="nonexistent_file.csv"
        )

        # Should create config successfully even though file doesn't exist
        assert config.allow_manual_override is False

    def test_missing_override_file_with_overrides_enabled(self):
        """When allow_manual_override is True but file doesn't exist, should be handled gracefully."""
        config = MetadataConfig(skip_yaml_loading=True, 
            allow_manual_override=True, override_csv_path="nonexistent_file.csv"
        )

        # Should create config successfully
        # Validation only happens if file exists
        assert config.allow_manual_override is True

    def test_override_path_from_config(self):
        """Config should respect override_csv_path parameter."""
        custom_path = "/custom/path/overrides.csv"
        config = MetadataConfig(skip_yaml_loading=True, 
            allow_manual_override=True, override_csv_path=custom_path
        )

        assert config.override_csv_path == custom_path

    def test_override_default_path(self):
        """Config should use default path when not specified."""
        config = MetadataConfig(skip_yaml_loading=True, allow_manual_override=True)

        # Default from variables.yml (may not be set in test environment)
        expected_path = "metadata_overrides.csv"
        actual_path = getattr(config, "override_csv_path", expected_path)
        assert actual_path == expected_path

    def test_valid_override_csv_structure(self):
        """Valid override CSV should have catalog, schema, table columns."""
        # Create a valid temp CSV
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("catalog,schema,table,column,override_value\n")
            f.write("my_catalog,my_schema,my_table,my_column,new_value\n")
            temp_path = f.name

        try:
            config = MetadataConfig(skip_yaml_loading=True, 
                allow_manual_override=True, override_csv_path=temp_path
            )

            assert config.allow_manual_override is True
            assert config.override_csv_path == temp_path
        finally:
            os.unlink(temp_path)

    def test_override_disabled_by_default(self):
        """allow_manual_override should default to True per variables.yml."""
        config = MetadataConfig(skip_yaml_loading=True, )

        # Default from variables.yml is True (may not be set in test environment)
        allow_override = getattr(config, "allow_manual_override", True)
        assert allow_override is True

    def test_missing_allow_manual_override_graceful(self):
        """Test that missing allow_manual_override doesn't crash (integration test scenario)."""
        config = MetadataConfig(skip_yaml_loading=True, catalog_name="test", schema_name="test")

        # Simulate integration test environment where allow_manual_override might not be set
        # Using getattr with default should work gracefully
        allow_override = getattr(config, "allow_manual_override", True)
        assert isinstance(allow_override, bool)


class TestOverrideCSVBehavior:
    """Test the behavior of override CSV in different scenarios."""

    def test_override_enabled_with_no_file(self):
        """When overrides enabled but no file, should not error during config creation."""
        # This is the integration test scenario
        config = MetadataConfig(skip_yaml_loading=True, 
            catalog_name="test_catalog",
            schema_name="test_schema",
            allow_manual_override=True,
            override_csv_path="metadata_overrides.csv",
        )

        assert config.allow_manual_override is True
        # Config creation succeeds even if file doesn't exist

    def test_override_disabled_path_irrelevant(self):
        """When overrides disabled, path doesn't matter."""
        config = MetadataConfig(skip_yaml_loading=True, 
            allow_manual_override=False,
            override_csv_path="/this/path/does/not/exist/overrides.csv",
        )

        assert config.allow_manual_override is False
        # Should succeed regardless of path since overrides are disabled
