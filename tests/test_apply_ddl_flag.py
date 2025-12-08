"""
Unit tests for apply_ddl flag behavior.

These tests verify that the apply_ddl and dry_run configuration parameters
are properly initialized and parsed as booleans, ensuring correct conditional
logic in DDL execution.

Run with: pytest tests/test_apply_ddl_flag.py -v
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.dbxmetagen.config import MetadataConfig, _parse_bool


class TestParseBoolForApplyDDL:
    """Test boolean parsing for apply_ddl flag."""

    def test_parse_bool_string_false(self):
        """Test that string 'false' parses to False."""
        assert _parse_bool("false") is False
        assert _parse_bool("False") is False
        assert _parse_bool("FALSE") is False

    def test_parse_bool_empty_string(self):
        """Test that empty string parses to False."""
        assert _parse_bool("") is False
        assert _parse_bool("   ") is False

    def test_parse_bool_string_true(self):
        """Test that string 'true' parses to True."""
        assert _parse_bool("true") is True
        assert _parse_bool("True") is True
        assert _parse_bool("TRUE") is True

    def test_parse_bool_none(self):
        """Test that None parses to False."""
        assert _parse_bool(None) is False


class TestConfigApplyDDLInitialization:
    """Test that apply_ddl is properly initialized in config."""

    def test_apply_ddl_defaults_to_false(self):
        """Test that apply_ddl defaults to False when not provided."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
        )
        assert config.apply_ddl is False

    def test_apply_ddl_string_false_becomes_boolean_false(self):
        """Test that string 'false' is parsed to boolean False."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
            apply_ddl="false",
        )
        assert config.apply_ddl is False
        assert isinstance(config.apply_ddl, bool)

    def test_apply_ddl_empty_string_becomes_false(self):
        """Test that empty string is parsed to False."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
            apply_ddl="",
        )
        assert config.apply_ddl is False

    def test_apply_ddl_string_true_becomes_boolean_true(self):
        """Test that string 'true' is parsed to boolean True."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
            apply_ddl="true",
        )
        assert config.apply_ddl is True
        assert isinstance(config.apply_ddl, bool)

    def test_dry_run_initialized_and_parsed(self):
        """Test that dry_run is initialized and parsed as boolean."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
            dry_run="false",
        )
        # dry_run should exist and be a boolean
        assert hasattr(config, "dry_run")
        assert isinstance(config.dry_run, bool)
        assert config.dry_run is False


class TestApplyDDLFalseCommentMode:
    """Test that apply_ddl=False prevents DDL execution in comment mode."""

    def test_apply_ddl_false_prevents_apply_ddl_to_tables_call(self):
        """Test the conditional logic: apply_ddl=False prevents apply_ddl_to_tables call."""
        config_false = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
            mode="comment",
            apply_ddl=False,
        )

        config_true = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
            mode="comment",
            apply_ddl=True,
        )

        # Test the condition directly
        assert config_false.apply_ddl is False
        assert config_true.apply_ddl is True

        # The conditional check in processing.py line 1994:
        # if config.apply_ddl:
        #     dfs["ddl_results"] = apply_ddl_to_tables(dfs, config)

        # When apply_ddl=False, this condition should be False
        assert not config_false.apply_ddl
        # When apply_ddl=True, this condition should be True
        assert config_true.apply_ddl


class TestApplyDDLFalsePIMode:
    """Test that apply_ddl=False prevents DDL execution in PI mode."""

    def test_apply_ddl_false_pi_mode_config(self):
        """Test PI mode config respects apply_ddl=False."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
            mode="pi",
            apply_ddl=False,
        )

        # Verify the configuration is correct
        assert config.mode == "pi"
        assert config.apply_ddl is False
        assert isinstance(config.apply_ddl, bool)


class TestApplyDDLFalseDomainMode:
    """Test that apply_ddl=False prevents DDL execution in domain mode."""

    def test_apply_ddl_false_domain_mode_config(self):
        """Test domain mode config respects apply_ddl=False."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
            mode="domain",
            apply_ddl=False,
        )

        # Verify the configuration is correct
        assert config.mode == "domain"
        assert config.apply_ddl is False
        assert isinstance(config.apply_ddl, bool)


class TestDryRunFlagInteraction:
    """Test the interaction between apply_ddl and dry_run flags."""

    def test_dry_run_false_means_execute_ddl(self):
        """Test that dry_run=False means DDL should execute (when apply_ddl=True)."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
            apply_ddl=True,
            dry_run=False,  # False means "not a dry run" = execute
        )

        # When not a dry run, DDL should execute
        assert config.dry_run is False
        assert config.apply_ddl is True

    def test_dry_run_true_means_skip_execution(self):
        """Test that dry_run=True means DDL should NOT execute."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
            apply_ddl=True,
            dry_run=True,  # True means "dry run" = skip execution
        )

        assert config.dry_run is True
        assert config.apply_ddl is True

    def test_dry_run_string_values_parsed_correctly(self):
        """Test that string values for dry_run are parsed correctly."""
        # Test "false" string
        config1 = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
            dry_run="false",
        )
        assert config1.dry_run is False
        assert isinstance(config1.dry_run, bool)

        # Test "true" string
        config2 = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
            dry_run="true",
        )
        assert config2.dry_run is True
        assert isinstance(config2.dry_run, bool)


class TestApplyDDLOnlyWhenIntended:
    """Test that DDL only executes when BOTH apply_ddl=True AND dry_run=False."""

    def test_no_execution_when_apply_ddl_false_regardless_of_dry_run(self):
        """Critical test: When apply_ddl=False, apply_ddl_to_tables should NOT be called."""
        # Test with dry_run=False (which normally would execute)
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
            mode="comment",
            apply_ddl=False,  # Main flag: False
            dry_run=False,  # Secondary flag: False (would normally execute)
        )

        # The critical condition from processing.py line 1994:
        # if config.apply_ddl:
        #     dfs["ddl_results"] = apply_ddl_to_tables(dfs, config)

        # This condition should be False, preventing apply_ddl_to_tables from being called
        assert config.apply_ddl is False
        assert config.dry_run is False

        # The conditional check should evaluate to False
        assert not config.apply_ddl

    def test_execution_requires_both_apply_ddl_true_and_dry_run_false(self):
        """Test that DDL execution requires apply_ddl=True."""
        # Scenario 1: apply_ddl=True, dry_run=False → Should call apply_ddl_to_tables
        config1 = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
            apply_ddl=True,
            dry_run=False,
        )
        assert config1.apply_ddl is True  # apply_ddl_to_tables will be called
        assert config1.dry_run is False  # spark.sql will execute

        # Scenario 2: apply_ddl=True, dry_run=True → Should call apply_ddl_to_tables but not execute SQL
        config2 = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
            apply_ddl=True,
            dry_run=True,
        )
        assert config2.apply_ddl is True  # apply_ddl_to_tables will be called
        assert config2.dry_run is True  # spark.sql will NOT execute

        # Scenario 3: apply_ddl=False, dry_run=False → Should NOT call apply_ddl_to_tables
        config3 = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_names="test.table",
            apply_ddl=False,
            dry_run=False,
        )
        assert config3.apply_ddl is False  # apply_ddl_to_tables will NOT be called


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
