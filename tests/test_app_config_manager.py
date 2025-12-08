"""Unit tests for ConfigManager in app/core/config.py"""

import pytest
import sys
import os
from unittest.mock import Mock, MagicMock, patch, mock_open

# Mock streamlit before importing app modules
sys.modules["streamlit"] = MagicMock()
sys.modules["streamlit.runtime"] = MagicMock()
sys.modules["streamlit.runtime.scriptrunner"] = MagicMock()
sys.modules["databricks"] = MagicMock()
sys.modules["databricks.sdk"] = MagicMock()
sys.modules["databricks.sdk.service"] = MagicMock()
sys.modules["databricks.sdk.service.jobs"] = MagicMock()
sys.modules["databricks.sdk.service.compute"] = MagicMock()

# Add app directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))


class TestDatabricksClientManager:
    """Test DatabricksClientManager utility methods"""

    def setup_method(self):
        """Import after mocking"""
        from core.config import DatabricksClientManager

        self.DatabricksClientManager = DatabricksClientManager

    def test_normalize_host_url_adds_https(self):
        """Test that normalize_host_url adds https:// when missing"""
        result = self.DatabricksClientManager._normalize_host_url(
            "workspace.databricks.com"
        )
        assert result == "https://workspace.databricks.com"

    def test_normalize_host_url_preserves_https(self):
        """Test that normalize_host_url preserves existing https://"""
        result = self.DatabricksClientManager._normalize_host_url(
            "https://workspace.databricks.com"
        )
        assert result == "https://workspace.databricks.com"

    def test_normalize_host_url_strips_whitespace(self):
        """Test that normalize_host_url strips whitespace"""
        result = self.DatabricksClientManager._normalize_host_url(
            "  workspace.databricks.com  "
        )
        assert result == "https://workspace.databricks.com"

    def test_is_valid_host_url_accepts_valid_urls(self):
        """Test that is_valid_host_url accepts properly formatted URLs"""
        assert self.DatabricksClientManager._is_valid_host_url(
            "https://workspace.databricks.com"
        )
        assert self.DatabricksClientManager._is_valid_host_url("http://localhost.com")

    def test_is_valid_host_url_rejects_invalid_urls(self):
        """Test that is_valid_host_url rejects invalid URLs"""
        assert not self.DatabricksClientManager._is_valid_host_url("notaurl")
        assert not self.DatabricksClientManager._is_valid_host_url("https://")
        assert not self.DatabricksClientManager._is_valid_host_url("")
        assert not self.DatabricksClientManager._is_valid_host_url("https://nodot")

    @patch.dict(os.environ, {}, clear=True)
    def test_validate_environment_missing_host(self):
        """Test that validate_environment returns None when host is missing"""
        result = self.DatabricksClientManager._validate_environment()
        assert result is None

    @patch.dict(os.environ, {"DATABRICKS_HOST": "workspace.databricks.com"})
    def test_validate_environment_returns_normalized_host(self):
        """Test that validate_environment returns normalized host URL"""
        result = self.DatabricksClientManager._validate_environment()
        assert result == "https://workspace.databricks.com"


class TestConfigManagerYAMLLoading:
    """Test ConfigManager YAML loading methods"""

    def setup_method(self):
        """Import after mocking"""
        from core.config import ConfigManager

        self.ConfigManager = ConfigManager

    def test_extract_variables_from_databricks_yaml(self):
        """Test extracting default values from Databricks variables.yml structure"""
        raw_config = {
            "variables": {
                "catalog_name": {"default": "my_catalog"},
                "schema_name": {"default": "my_schema"},
                "mode": {"default": "comment"},
            }
        }

        manager = self.ConfigManager()
        result = manager._extract_variables(raw_config)

        assert result["catalog_name"] == "my_catalog"
        assert result["schema_name"] == "my_schema"
        assert result["mode"] == "comment"

    def test_extract_variables_missing_variables_key(self):
        """Test that extract_variables raises error when variables key is missing"""
        raw_config = {"not_variables": {}}

        manager = self.ConfigManager()
        with pytest.raises(ValueError, match="missing 'variables' key"):
            manager._extract_variables(raw_config)

    def test_extract_variables_handles_non_dict_values(self):
        """Test that extract_variables handles direct values (not in default key)"""
        raw_config = {
            "variables": {
                "catalog_name": {"default": "my_catalog"},
                "simple_value": "direct_value",
            }
        }

        manager = self.ConfigManager()
        result = manager._extract_variables(raw_config)

        assert result["catalog_name"] == "my_catalog"
        assert result["simple_value"] == "direct_value"

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="variables:\n  catalog_name:\n    default: test_catalog",
    )
    @patch("os.path.exists", return_value=True)
    def test_load_yaml_file_success(self, mock_exists, mock_file):
        """Test successful YAML file loading"""
        manager = self.ConfigManager()
        result = manager._load_yaml_file("variables.yml")

        assert result is not None
        assert "variables" in result

    @patch("os.path.exists", return_value=False)
    def test_load_yaml_file_missing_optional(self, mock_exists):
        """Test loading missing optional YAML file returns None"""
        manager = self.ConfigManager()
        result = manager._load_yaml_file("optional.yml", required=False)

        assert result is None

    @patch("os.path.exists", return_value=False)
    def test_load_yaml_file_missing_required(self, mock_exists):
        """Test loading missing required YAML file raises error"""
        manager = self.ConfigManager()

        with pytest.raises(FileNotFoundError):
            manager._load_yaml_file("required.yml", required=True)

    @patch.dict(os.environ, {"DATABRICKS_HOST": "override.databricks.com"})
    def test_apply_host_override_from_env(self):
        """Test that environment variable overrides config host"""
        manager = self.ConfigManager()
        config = {"host": "original.databricks.com"}

        result = manager._apply_host_override(config)

        assert result["host"] == "override.databricks.com"

    @patch.dict(os.environ, {}, clear=True)
    def test_apply_host_override_no_env(self):
        """Test that host is preserved when no environment override"""
        manager = self.ConfigManager()
        config = {"host": "original.databricks.com"}

        result = manager._apply_host_override(config)

        assert result["host"] == "original.databricks.com"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
