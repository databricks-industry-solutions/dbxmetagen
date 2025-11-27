"""Unit tests for boolean parsing in configuration."""

import pytest
import sys
import os
from unittest.mock import Mock, MagicMock

# Mock pyspark and databricks modules BEFORE any imports
sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["pyspark.sql.functions"] = MagicMock()
sys.modules["pyspark.sql.types"] = MagicMock()
sys.modules["pyspark.sql.column"] = MagicMock()
sys.modules["databricks"] = MagicMock()
sys.modules["databricks.sdk"] = MagicMock()
sys.modules["databricks.sdk.core"] = MagicMock()
sys.modules["grpc"] = MagicMock()
sys.modules["grpc._channel"] = MagicMock()

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.dbxmetagen.config import _parse_bool, MetadataConfig


class TestParseBool:
    """Test the _parse_bool helper function."""

    def test_parse_bool_with_bool_inputs(self):
        """Test that actual booleans are returned as-is."""
        assert _parse_bool(True) is True
        assert _parse_bool(False) is False

    def test_parse_bool_with_string_true_values(self):
        """Test that various string representations of true are parsed correctly."""
        assert _parse_bool("true") is True
        assert _parse_bool("True") is True
        assert _parse_bool("TRUE") is True
        assert _parse_bool("1") is True
        assert _parse_bool("yes") is True
        assert _parse_bool("Yes") is True
        assert _parse_bool("YES") is True

    def test_parse_bool_with_string_false_values(self):
        """Test that various string representations of false are parsed correctly."""
        assert _parse_bool("false") is False
        assert _parse_bool("False") is False
        assert _parse_bool("FALSE") is False
        assert _parse_bool("0") is False
        assert _parse_bool("no") is False
        assert _parse_bool("No") is False
        assert _parse_bool("NO") is False
        assert _parse_bool("") is False
        assert _parse_bool("anything_else") is False

    def test_parse_bool_with_whitespace(self):
        """Test that whitespace IS trimmed before parsing."""
        # Whitespace should be stripped, allowing " true ", "false  ", etc.
        assert _parse_bool("  true  ") is True
        assert _parse_bool("  false  ") is False
        assert _parse_bool("  1  ") is True
        assert _parse_bool(" ") is False  # Empty after strip

    def test_parse_bool_with_none(self):
        """Test that None returns False."""
        assert _parse_bool(None) is False

    def test_parse_bool_with_integers(self):
        """Test that integers are converted to bool properly."""
        assert _parse_bool(1) is True
        assert _parse_bool(0) is False
        assert _parse_bool(42) is True
        assert _parse_bool(-1) is True

    def test_parse_bool_with_unexpected_types(self):
        """Test that unexpected input types raise TypeError with helpful message."""
        # Lists should raise TypeError
        with pytest.raises(TypeError, match="Cannot convert list to bool"):
            _parse_bool([])

        with pytest.raises(TypeError, match="Cannot convert list to bool"):
            _parse_bool([1, 2])

        # Dicts should raise TypeError
        with pytest.raises(TypeError, match="Cannot convert dict to bool"):
            _parse_bool({})

        with pytest.raises(TypeError, match="Cannot convert dict to bool"):
            _parse_bool({"key": "value"})

        # Float should raise TypeError
        with pytest.raises(TypeError, match="Cannot convert float to bool"):
            _parse_bool(3.14)

    def test_parse_bool_critical_bug_fix(self):
        """Test that the critical bug (string 'false' being truthy) is fixed."""
        # This is THE bug we're fixing: in Python, bool("false") == True
        # But _parse_bool("false") should be False
        assert bool("false") is True  # Demonstrate the bug in raw Python
        assert _parse_bool("false") is False  # Our fix works


class TestConfigBooleanParsing:
    """Test that MetadataConfig properly parses boolean fields."""

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("false", False),
            ("true", True),
            (False, False),
            (True, True),
            ("", False),
            ("0", False),
            ("1", True),
        ],
    )
    def test_config_apply_ddl_parsing(self, value, expected):
        """Test that apply_ddl is parsed correctly with various input types."""
        config = MetadataConfig(skip_yaml_loading=True, apply_ddl=value)
        assert config.apply_ddl is expected
        assert isinstance(config.apply_ddl, bool)

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("false", False),
            ("true", True),
            (False, False),
            (True, True),
        ],
    )
    def test_config_cleanup_control_table_parsing(self, value, expected):
        """Test that cleanup_control_table is parsed correctly."""
        config = MetadataConfig(skip_yaml_loading=True, cleanup_control_table=value)
        assert config.cleanup_control_table is expected
        assert isinstance(config.cleanup_control_table, bool)

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("false", False),
            ("true", True),
            (False, False),
            (True, True),
        ],
    )
    def test_config_review_apply_ddl_parsing(self, value, expected):
        """Test that review_apply_ddl is parsed correctly."""
        config = MetadataConfig(skip_yaml_loading=True, review_apply_ddl=value)
        assert config.review_apply_ddl is expected
        assert isinstance(config.review_apply_ddl, bool)

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("false", False),
            ("true", True),
            (False, False),
            (True, True),
        ],
    )
    def test_config_grant_permissions_parsing(self, value, expected):
        """Test that grant_permissions_after_creation is parsed correctly."""
        config = MetadataConfig(
            skip_yaml_loading=True, grant_permissions_after_creation=value
        )
        assert config.grant_permissions_after_creation is expected
        assert isinstance(config.grant_permissions_after_creation, bool)

    def test_config_default_apply_ddl_is_false(self):
        """Test that apply_ddl defaults to False when not specified."""
        config = MetadataConfig(
            skip_yaml_loading=True,
        )
        assert isinstance(config.apply_ddl, bool)
        assert config.apply_ddl is False

    def test_config_default_grant_permissions_is_true(self):
        """Test that grant_permissions_after_creation defaults to True."""
        config = MetadataConfig(
            skip_yaml_loading=True,
        )
        assert isinstance(config.grant_permissions_after_creation, bool)
        assert config.grant_permissions_after_creation is True

    def test_multiple_boolean_fields_simultaneously(self):
        """Test setting multiple boolean fields at once."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            apply_ddl="true",
            cleanup_control_table="false",
            grant_permissions_after_creation="true",
        )
        assert config.apply_ddl is True
        assert config.cleanup_control_table is False
        assert config.grant_permissions_after_creation is True

    def test_config_allow_data_interactions(self):
        """Test that allow_data=False cascades to related fields."""
        config = MetadataConfig(skip_yaml_loading=True, allow_data="false")
        assert config.allow_data is False
        # When allow_data is False, these should also be False
        assert config.allow_data_in_comments is False
        assert config.sample_size == 0
        assert config.include_possible_data_fields_in_metadata is False

    def test_config_allow_data_true_preserves_other_fields(self):
        """Test that allow_data=True doesn't override other explicit settings."""
        config = MetadataConfig(
            skip_yaml_loading=True, allow_data="true", allow_data_in_comments="false"
        )
        assert config.allow_data is True
        # This should respect the explicit setting
        assert config.allow_data_in_comments is False


class TestBackwardCompatibility:
    """Test that the new boolean parsing doesn't break existing code."""

    def test_apply_ddl_truthiness(self):
        """Test that parsed booleans work correctly in if statements."""
        config_false = MetadataConfig(skip_yaml_loading=True, apply_ddl="false")
        config_true = MetadataConfig(skip_yaml_loading=True, apply_ddl="true")

        # These should work as expected in conditionals
        if config_false.apply_ddl:
            pytest.fail("apply_ddl='false' should not be truthy")

        if not config_true.apply_ddl:
            pytest.fail("apply_ddl='true' should be truthy")

    def test_cleanup_control_table_equality(self):
        """Test that cleanup_control_table can be compared with booleans."""
        config = MetadataConfig(skip_yaml_loading=True, cleanup_control_table="true")

        # Should work with both boolean comparison and string comparison for backward compat
        assert config.cleanup_control_table == True
        assert config.cleanup_control_table is True

    def test_missing_advanced_variables_handled_gracefully(self):
        """Test that config works without advanced variables file (variables.advanced.yml).

        This ensures integration tests and minimal deployments work when advanced
        features like benchmarking aren't configured.
        """
        config = MetadataConfig(
            skip_yaml_loading=True, catalog_name="test", schema_name="test"
        )

        # Should handle missing enable_benchmarking gracefully
        # This simulates integration test environment without variables.advanced.yml
        benchmarking_enabled = getattr(config, "enable_benchmarking", False)
        assert benchmarking_enabled is False

        # Verify accessing other optional advanced variables doesn't crash
        benchmark_table = getattr(config, "benchmark_table_name", None)
        assert benchmark_table is None or isinstance(benchmark_table, str)

    def test_missing_variables_yml_raises_error(self):
        """Test that missing variables.yml raises clear error message.

        This validates that the required variables.yml file must exist and load properly.
        Without it, users should get a clear error rather than cryptic AttributeErrors.
        """
        # Create a config that would normally try to load variables.yml from wrong location
        # We expect this to raise FileNotFoundError with helpful message
        # NOTE: Don't skip YAML loading for this test - we want to test the error
        with pytest.raises(FileNotFoundError) as exc_info:
            # Use a clearly non-existent path (don't skip YAML loading)
            config = MetadataConfig(yaml_file_path="/nonexistent/path/variables.yml")

        # Verify error message is helpful
        error_msg = str(exc_info.value)
        assert "Required configuration file not found" in error_msg
        assert "integration tests" in error_msg.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
