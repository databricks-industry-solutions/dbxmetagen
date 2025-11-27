"""
Unit tests to ensure all variables from variables.yml are included in yaml_variable_names.

This test prevents issues where YAML variables exist but aren't loaded because
they're missing from the yaml_variable_names list in MetadataConfig.
"""

import sys
import os
from unittest.mock import MagicMock

# Mock Databricks-specific modules BEFORE any imports
sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["pyspark.sql.functions"] = MagicMock()
sys.modules["pyspark.sql.column"] = MagicMock()
sys.modules["pyspark.sql.types"] = MagicMock()
sys.modules["databricks"] = MagicMock()
sys.modules["databricks.sdk"] = MagicMock()
sys.modules["databricks.sdk.runtime"] = MagicMock()

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import yaml
import pytest
from src.dbxmetagen.config import MetadataConfig


class TestYAMLVariableCoverage:
    """Test that all YAML variables are properly loaded."""

    def test_all_variables_yml_params_in_yaml_variable_names(self):
        """Verify all params from variables.yml are in yaml_variable_names list."""
        # Get path to variables.yml (relative to test file)
        test_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(os.path.join(test_dir, ".."))
        variables_yml_path = os.path.join(project_root, "variables.yml")

        # Load variables.yml
        with open(variables_yml_path, "r") as f:
            variables_data = yaml.safe_load(f)

        # Get all variable names from YAML
        yaml_params = set(variables_data["variables"].keys())

        # Get yaml_variable_names from MetadataConfig
        config_yaml_params = set(MetadataConfig.SETUP_PARAMS["yaml_variable_names"])

        # Find missing parameters
        missing_params = yaml_params - config_yaml_params

        # Some params are intentionally not loaded (job-level or bundle-level only)
        # These are okay to skip
        acceptable_missing = {
            "bundle_target",  # Bundle-level variable
            "create_test_data",  # Deployment-time flag
            "current_working_directory",  # Runtime-determined
            "deploying_user",  # Bundle-level variable
            "job_table_names",  # Job-level default (table_names used instead)
        }

        # Check for unexpected missing params
        unexpected_missing = missing_params - acceptable_missing

        assert (
            len(unexpected_missing) == 0
        ), f"Variables in variables.yml but missing from yaml_variable_names: {unexpected_missing}"

    def test_all_variables_advanced_yml_params_in_yaml_advanced_variable_names(self):
        """Verify all params from variables.advanced.yml are in yaml_advanced_variable_names list."""
        # Get path to variables.advanced.yml
        test_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(os.path.join(test_dir, ".."))
        variables_advanced_yml_path = os.path.join(
            project_root, "variables.advanced.yml"
        )

        # Load variables.advanced.yml
        with open(variables_advanced_yml_path, "r") as f:
            variables_data = yaml.safe_load(f)

        # Get all variable names from YAML
        yaml_params = set(variables_data["variables"].keys())

        # Get yaml_advanced_variable_names from MetadataConfig
        config_yaml_params = set(
            MetadataConfig.SETUP_PARAMS["yaml_advanced_variable_names"]
        )

        # Find missing parameters
        missing_params = yaml_params - config_yaml_params

        # Some params might be intentionally not loaded
        acceptable_missing = {
            "presidio_score_threshold",  # Advanced feature flag
        }

        # Check for unexpected missing params
        unexpected_missing = missing_params - acceptable_missing

        assert (
            len(unexpected_missing) == 0
        ), f"Variables in variables.advanced.yml but missing from yaml_advanced_variable_names: {unexpected_missing}"

    def test_critical_params_present(self):
        """Ensure critical parameters are present in yaml_variable_names."""
        config_yaml_params = set(MetadataConfig.SETUP_PARAMS["yaml_variable_names"])

        # Critical params that must be loadable
        critical_params = {
            "catalog_name",
            "schema_name",
            "mode",  # Fixed in this PR
            "model",
            "apply_ddl",
            "volume_name",
            "control_table",
            "grant_permissions_after_creation",
            "allow_data",
            "sample_size",
            "columns_per_call",
            "review_apply_ddl",  # For review workflow
        }

        missing_critical = critical_params - config_yaml_params

        assert (
            len(missing_critical) == 0
        ), f"Critical parameters missing from yaml_variable_names: {missing_critical}"

    def test_mode_parameter_loads_correctly(self):
        """Regression test: Ensure 'mode' parameter is loaded from YAML."""
        # This was the bug that broke integration tests
        config_yaml_params = set(MetadataConfig.SETUP_PARAMS["yaml_variable_names"])
        assert (
            "mode" in config_yaml_params
        ), "'mode' parameter must be in yaml_variable_names"

    def test_config_with_yaml_override_doesnt_break(self):
        """Test that passing yaml_file_path as kwarg works correctly."""
        # This test validates the fix for yaml_file_path ordering
        try:
            config = MetadataConfig(
                skip_yaml_loading=True,  # Skip for unit test
                yaml_file_path="../../custom_path.yml",  # Should not cause error
                catalog_name="test_catalog",
                schema_name="test_schema",
                mode="comment",
            )
            # If we get here, yaml_file_path was handled correctly
            assert config.yaml_file_path == "../../custom_path.yml"
            assert config.mode == "comment"
        except Exception as e:
            pytest.fail(
                f"MetadataConfig should accept yaml_file_path as kwarg without error: {e}"
            )
