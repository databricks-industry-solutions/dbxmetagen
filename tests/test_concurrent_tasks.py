"""Unit tests for concurrent task support features (run_id, task_id, claim_table)."""

import pytest
import sys
import os
from unittest.mock import Mock, MagicMock, patch
import json

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.dbxmetagen.config import MetadataConfig


class TestRunIdConfig:
    """Test run_id configuration handling."""

    def test_run_id_uses_provided_value(self):
        """Test that run_id uses the value passed via kwargs."""
        config = MetadataConfig(skip_yaml_loading=True, run_id="12345")
        assert config.run_id == "12345"

    def test_run_id_fallback_to_uuid(self):
        """Test that run_id generates UUID when not provided."""
        config = MetadataConfig(skip_yaml_loading=True)
        assert config.run_id is not None
        # UUID format: 8-4-4-4-12 hex chars with hyphens
        assert len(config.run_id) == 36
        assert config.run_id.count("-") == 4

    def test_run_id_empty_string_generates_uuid(self):
        """Test that empty string run_id generates UUID fallback."""
        config = MetadataConfig(skip_yaml_loading=True, run_id="")
        assert config.run_id is not None
        assert len(config.run_id) == 36

    def test_task_id_attribute_exists(self):
        """Test that task_id attribute is initialized."""
        config = MetadataConfig(skip_yaml_loading=True)
        assert hasattr(config, "task_id")

    def test_task_id_uses_provided_value(self):
        """Test that task_id uses the value passed via kwargs."""
        config = MetadataConfig(skip_yaml_loading=True, task_id="task_abc_123")
        assert config.task_id == "task_abc_123"


class TestGetTaskId:
    """Test get_task_id function from databricks_utils."""

    def setup_method(self):
        """Import the function under test."""
        from src.dbxmetagen.databricks_utils import get_task_id
        self.get_task_id = get_task_id

    def test_get_task_id_from_context_taskRunId(self):
        """Test that get_task_id extracts taskRunId from Databricks context."""
        mock_dbutils = MagicMock()
        context = {
            "tags": {
                "taskRunId": "12345678",
                "jobId": "9999"
            }
        }
        mock_dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson.return_value = json.dumps(context)
        
        result = self.get_task_id(mock_dbutils)
        assert result == "12345678"

    def test_get_task_id_fallback_to_parent_run_id(self):
        """Test that get_task_id falls back to multitaskParentRunId."""
        mock_dbutils = MagicMock()
        context = {
            "tags": {
                "multitaskParentRunId": "parent_run_456"
            }
        }
        mock_dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson.return_value = json.dumps(context)
        
        result = self.get_task_id(mock_dbutils)
        assert result == "parent_run_456_interactive"

    def test_get_task_id_generates_uuid_when_no_context(self):
        """Test that get_task_id generates UUID when no dbutils provided."""
        result = self.get_task_id(None)
        assert result is not None
        assert len(result) == 36  # UUID format

    def test_get_task_id_handles_exception(self):
        """Test that get_task_id handles exceptions gracefully."""
        mock_dbutils = MagicMock()
        mock_dbutils.notebook.entry_point.getDbutils.side_effect = Exception("No context")
        
        result = self.get_task_id(mock_dbutils)
        # Should return a UUID fallback
        assert result is not None
        assert len(result) == 36


class TestGetControlTable:
    """Test get_control_table function with run_id support."""

    def setup_method(self):
        """Import the function under test."""
        from src.dbxmetagen.processing import get_control_table
        self.get_control_table = get_control_table

    @patch("src.dbxmetagen.processing.get_current_user")
    def test_get_control_table_without_cleanup(self, mock_user):
        """Test control table name without cleanup flag."""
        mock_user.return_value = "test_user@example.com"
        
        config = MetadataConfig(
            skip_yaml_loading=True,
            control_table="metadata_control_{}",
            cleanup_control_table=False,
            job_id="123",
            run_id="456"
        )
        
        result = self.get_control_table(config)
        assert result == "metadata_control_test_user_example_com"
        assert "123" not in result
        assert "456" not in result

    @patch("src.dbxmetagen.processing.get_current_user")
    def test_get_control_table_with_cleanup_and_run_id(self, mock_user):
        """Test control table name includes run_id when cleanup is true."""
        mock_user.return_value = "test_user@example.com"
        
        config = MetadataConfig(
            skip_yaml_loading=True,
            control_table="metadata_control_{}",
            cleanup_control_table=True,
            job_id="123",
            run_id="456"
        )
        
        result = self.get_control_table(config)
        assert "123" in result
        assert "456" in result
        assert result == "metadata_control_test_user_example_com_123_456"

    @patch("src.dbxmetagen.processing.get_current_user")
    def test_get_control_table_with_cleanup_only_run_id(self, mock_user):
        """Test control table name with only run_id (no job_id)."""
        mock_user.return_value = "test_user@example.com"
        
        config = MetadataConfig(
            skip_yaml_loading=True,
            control_table="metadata_control_{}",
            cleanup_control_table=True,
            job_id=None,
            run_id="456"
        )
        
        result = self.get_control_table(config)
        assert "456" in result
        assert result == "metadata_control_test_user_example_com_456"

    @patch("src.dbxmetagen.processing.get_current_user")
    def test_get_control_table_cleanup_string_true(self, mock_user):
        """Test control table handles string 'true' for cleanup flag."""
        mock_user.return_value = "test_user"
        
        config = MetadataConfig(
            skip_yaml_loading=True,
            control_table="metadata_control_{}",
            cleanup_control_table="true",  # String value
            job_id="123",
            run_id="456"
        )
        
        result = self.get_control_table(config)
        # Note: cleanup_control_table is parsed to boolean in config
        # So this test verifies the parsing chain works
        assert "123" in result or "456" in result


class TestClaimTable:
    """Test claim_table function for concurrent task safety."""

    def setup_method(self):
        """Import the function under test."""
        from src.dbxmetagen.processing import claim_table
        self.claim_table = claim_table

    @patch("src.dbxmetagen.processing.SparkSession")
    @patch("src.dbxmetagen.processing.get_control_table")
    def test_claim_table_returns_true_without_task_id(self, mock_get_control, mock_spark):
        """Test that claim_table returns True when no task_id (non-concurrent mode)."""
        config = MagicMock()
        config.task_id = None
        config.catalog_name = "test_catalog"
        config.schema_name = "test_schema"
        
        result = self.claim_table("catalog.schema.table", config)
        assert result is True
        # Should not execute any SQL when task_id is None
        mock_spark.builder.getOrCreate().sql.assert_not_called()

    @patch("src.dbxmetagen.processing.SparkSession")
    @patch("src.dbxmetagen.processing.get_control_table")
    def test_claim_table_success(self, mock_get_control, mock_spark):
        """Test successful table claim."""
        mock_get_control.return_value = "control_table"
        mock_spark_instance = mock_spark.builder.getOrCreate.return_value
        
        # Simulate successful claim - after UPDATE, this task owns the claim
        mock_spark_instance.sql.return_value.collect.return_value = [
            {"_claimed_by": "task_123"}
        ]
        
        config = MagicMock()
        config.task_id = "task_123"
        config.catalog_name = "test_catalog"
        config.schema_name = "test_schema"
        
        result = self.claim_table("catalog.schema.table", config)
        assert result is True

    @patch("src.dbxmetagen.processing.SparkSession")
    @patch("src.dbxmetagen.processing.get_control_table")
    def test_claim_table_already_claimed(self, mock_get_control, mock_spark):
        """Test claim fails when another task already claimed the table."""
        mock_get_control.return_value = "control_table"
        mock_spark_instance = mock_spark.builder.getOrCreate.return_value
        
        # Simulate table already claimed by different task
        mock_spark_instance.sql.return_value.collect.return_value = [
            {"_claimed_by": "other_task_456"}
        ]
        
        config = MagicMock()
        config.task_id = "task_123"
        config.catalog_name = "test_catalog"
        config.schema_name = "test_schema"
        
        result = self.claim_table("catalog.schema.table", config)
        assert result is False


class TestWidgetExtraction:
    """Test run_id widget extraction in databricks_utils."""

    def setup_method(self):
        """Import functions under test."""
        from src.dbxmetagen.databricks_utils import get_widgets, setup_widgets
        self.get_widgets = get_widgets
        self.setup_widgets = setup_widgets

    def test_setup_widgets_creates_run_id_widget(self):
        """Test that setup_widgets creates run_id widget."""
        mock_dbutils = MagicMock()
        
        self.setup_widgets(mock_dbutils)
        
        # Verify run_id widget was created
        calls = [str(call) for call in mock_dbutils.widgets.text.call_args_list]
        assert any("run_id" in call for call in calls)

    def test_get_widgets_includes_run_id(self):
        """Test that get_widgets extracts run_id."""
        mock_dbutils = MagicMock()
        mock_dbutils.widgets.get.side_effect = lambda name: {
            "cleanup_control_table": "false",
            "mode": "comment",
            "env": "dev",
            "catalog_name": "test_catalog",
            "schema_name": "test_schema",
            "host": "",
            "table_names": "catalog.schema.table",
            "current_user": "test@example.com",
            "apply_ddl": "false",
            "columns_per_call": "5",
            "sample_size": "10",
            "run_id": "run_12345"
        }.get(name, "")
        
        result = self.get_widgets(mock_dbutils)
        assert result.get("run_id") == "run_12345"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

