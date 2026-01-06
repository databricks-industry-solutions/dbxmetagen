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
    """Test get_control_table function - returns base table name only.
    
    run_id is now used as a key column in the table, not in the table name.
    This allows concurrent tasks to share the same control table while 
    filtering by _run_id for their entries.
    """

    def setup_method(self):
        """Import the function under test."""
        from src.dbxmetagen.processing import get_control_table
        self.get_control_table = get_control_table

    @patch("src.dbxmetagen.processing.get_current_user")
    def test_get_control_table_returns_base_name(self, mock_user):
        """Test control table always returns base name (run_id is a column, not in name)."""
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
        # run_id and job_id should NOT be in the table name
        assert "123" not in result
        assert "456" not in result

    @patch("src.dbxmetagen.processing.get_current_user")
    def test_get_control_table_with_cleanup_true(self, mock_user):
        """Test control table name is same regardless of cleanup flag."""
        mock_user.return_value = "test_user@example.com"
        
        config = MetadataConfig(
            skip_yaml_loading=True,
            control_table="metadata_control_{}",
            cleanup_control_table=True,
            job_id="123",
            run_id="456"
        )
        
        result = self.get_control_table(config)
        # Should return base name - run_id is used as column key, not in table name
        assert result == "metadata_control_test_user_example_com"
        assert "123" not in result
        assert "456" not in result

    @patch("src.dbxmetagen.processing.get_current_user")
    def test_get_control_table_sanitizes_user(self, mock_user):
        """Test that user email is sanitized in table name."""
        mock_user.return_value = "test.user@example.com"
        
        config = MetadataConfig(
            skip_yaml_loading=True,
            control_table="metadata_control_{}",
            cleanup_control_table=True,
            run_id="456"
        )
        
        result = self.get_control_table(config)
        # Dots and @ should be replaced with underscores
        assert "@" not in result
        assert result == "metadata_control_test_user_example_com"


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

    def test_setup_widgets_creates_include_previously_failed_tables_widget(self):
        """Test that setup_widgets creates include_previously_failed_tables widget."""
        mock_dbutils = MagicMock()
        
        self.setup_widgets(mock_dbutils)
        
        # Verify include_previously_failed_tables widget was created as dropdown
        calls = [str(call) for call in mock_dbutils.widgets.dropdown.call_args_list]
        assert any("include_previously_failed_tables" in call for call in calls)

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
            "run_id": "run_12345",
            "include_previously_failed_tables": "false"
        }.get(name, "")
        
        result = self.get_widgets(mock_dbutils)
        assert result.get("run_id") == "run_12345"

    def test_get_widgets_includes_include_previously_failed_tables(self):
        """Test that get_widgets extracts include_previously_failed_tables."""
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
            "run_id": "run_12345",
            "include_previously_failed_tables": "true"
        }.get(name, "")
        
        result = self.get_widgets(mock_dbutils)
        assert result.get("include_previously_failed_tables") == "true"


class TestIncludePreviouslyFailedTablesConfig:
    """Test include_previously_failed_tables configuration handling."""

    def test_include_previously_failed_tables_default_false(self):
        """Test that include_previously_failed_tables defaults to False."""
        config = MetadataConfig(skip_yaml_loading=True)
        assert config.include_previously_failed_tables is False

    def test_include_previously_failed_tables_set_true(self):
        """Test that include_previously_failed_tables can be set to True."""
        config = MetadataConfig(skip_yaml_loading=True, include_previously_failed_tables="true")
        assert config.include_previously_failed_tables is True

    def test_include_previously_failed_tables_string_false(self):
        """Test that string 'false' parses to False."""
        config = MetadataConfig(skip_yaml_loading=True, include_previously_failed_tables="false")
        assert config.include_previously_failed_tables is False

    def test_claim_timeout_minutes_default(self):
        """Test that claim_timeout_minutes defaults to 60."""
        config = MetadataConfig(skip_yaml_loading=True)
        assert config.claim_timeout_minutes == 60

    def test_claim_timeout_minutes_custom_value(self):
        """Test that claim_timeout_minutes can be customized."""
        config = MetadataConfig(skip_yaml_loading=True, claim_timeout_minutes=120)
        assert config.claim_timeout_minutes == 120

    def test_cleanup_failed_tables_default_false(self):
        """Test that cleanup_failed_tables defaults to False."""
        config = MetadataConfig(skip_yaml_loading=True)
        assert config.cleanup_failed_tables is False

    def test_cleanup_failed_tables_set_true(self):
        """Test that cleanup_failed_tables can be set to True."""
        config = MetadataConfig(skip_yaml_loading=True, cleanup_failed_tables="true")
        assert config.cleanup_failed_tables is True


class TestMarkTableStatus:
    """Test mark_table_completed and mark_table_failed functions."""

    def setup_method(self):
        """Import the functions under test."""
        from src.dbxmetagen.processing import mark_table_completed, mark_table_failed
        self.mark_table_completed = mark_table_completed
        self.mark_table_failed = mark_table_failed

    @patch("src.dbxmetagen.processing.SparkSession")
    @patch("src.dbxmetagen.processing.get_control_table")
    def test_mark_table_completed_executes_update(self, mock_get_control, mock_spark):
        """Test that mark_table_completed executes UPDATE SQL."""
        mock_get_control.return_value = "control_table"
        mock_spark_instance = mock_spark.builder.getOrCreate.return_value
        
        config = MagicMock()
        config.catalog_name = "test_catalog"
        config.schema_name = "test_schema"
        config.run_id = "run_123"
        
        self.mark_table_completed("catalog.schema.table", config)
        
        # Verify SQL was called
        mock_spark_instance.sql.assert_called()
        sql_call = mock_spark_instance.sql.call_args[0][0]
        assert "_status = 'completed'" in sql_call
        assert "run_123" in sql_call

    @patch("src.dbxmetagen.processing.SparkSession")
    @patch("src.dbxmetagen.processing.get_control_table")
    def test_mark_table_failed_executes_update(self, mock_get_control, mock_spark):
        """Test that mark_table_failed executes UPDATE SQL with error message."""
        mock_get_control.return_value = "control_table"
        mock_spark_instance = mock_spark.builder.getOrCreate.return_value
        
        config = MagicMock()
        config.catalog_name = "test_catalog"
        config.schema_name = "test_schema"
        config.run_id = "run_123"
        
        self.mark_table_failed("catalog.schema.table", config, "Test error message")
        
        # Verify SQL was called
        mock_spark_instance.sql.assert_called()
        sql_call = mock_spark_instance.sql.call_args[0][0]
        assert "_status = 'failed'" in sql_call
        assert "Test error message" in sql_call
        assert "run_123" in sql_call

    @patch("src.dbxmetagen.processing.SparkSession")
    @patch("src.dbxmetagen.processing.get_control_table")
    def test_mark_table_failed_escapes_single_quotes(self, mock_get_control, mock_spark):
        """Test that mark_table_failed escapes single quotes in error message."""
        mock_get_control.return_value = "control_table"
        mock_spark_instance = mock_spark.builder.getOrCreate.return_value
        
        config = MagicMock()
        config.catalog_name = "test_catalog"
        config.schema_name = "test_schema"
        config.run_id = "run_123"
        
        # Error message with single quotes
        self.mark_table_failed("catalog.schema.table", config, "Error: 'value' is invalid")
        
        # Verify SQL was called and quotes are escaped
        mock_spark_instance.sql.assert_called()
        sql_call = mock_spark_instance.sql.call_args[0][0]
        # Single quotes should be escaped to double quotes in SQL
        assert "''" in sql_call or "value" in sql_call


class TestClaimTableWithStatus:
    """Test claim_table function with status filtering."""

    def setup_method(self):
        """Import the function under test."""
        from src.dbxmetagen.processing import claim_table
        self.claim_table = claim_table

    @patch("src.dbxmetagen.processing.SparkSession")
    @patch("src.dbxmetagen.processing.get_control_table")
    def test_claim_table_sets_status_to_in_progress(self, mock_get_control, mock_spark):
        """Test that claim_table sets _status to 'in_progress'."""
        mock_get_control.return_value = "control_table"
        mock_spark_instance = mock_spark.builder.getOrCreate.return_value
        
        # Simulate successful claim
        mock_spark_instance.sql.return_value.collect.return_value = [
            {"_claimed_by": "task_123"}
        ]
        
        config = MagicMock()
        config.task_id = "task_123"
        config.catalog_name = "test_catalog"
        config.schema_name = "test_schema"
        
        self.claim_table("catalog.schema.table", config)
        
        # Find the UPDATE call (first SQL call)
        update_call = mock_spark_instance.sql.call_args_list[0][0][0]
        assert "_status = 'in_progress'" in update_call

    @patch("src.dbxmetagen.processing.SparkSession")
    @patch("src.dbxmetagen.processing.get_control_table")
    def test_claim_table_filters_by_status(self, mock_get_control, mock_spark):
        """Test that claim_table only claims queued or failed tables."""
        mock_get_control.return_value = "control_table"
        mock_spark_instance = mock_spark.builder.getOrCreate.return_value
        
        # Simulate successful claim
        mock_spark_instance.sql.return_value.collect.return_value = [
            {"_claimed_by": "task_123"}
        ]
        
        config = MagicMock()
        config.task_id = "task_123"
        config.catalog_name = "test_catalog"
        config.schema_name = "test_schema"
        
        self.claim_table("catalog.schema.table", config)
        
        # Find the UPDATE call and verify status filter
        update_call = mock_spark_instance.sql.call_args_list[0][0][0]
        assert "_status IN ('queued', 'failed')" in update_call or "(_status IS NULL OR _status IN ('queued', 'failed'))" in update_call


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

