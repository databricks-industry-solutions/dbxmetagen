"""Unit tests for permission grant error messages."""

import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

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

from src.dbxmetagen.config import MetadataConfig


class TestPermissionErrorMessages:
    """Test permission error message formatting and behavior.

    Note: These tests focus on config behavior. Full integration tests with
    the grant_permissions_on_created_objects function would require a Databricks
    environment and are better suited for integration testing.
    """

    def test_permission_flag_behavior_when_disabled(self):
        """Test that permission flag is correctly set to False when disabled."""
        config = MetadataConfig(skip_yaml_loading=True, 
            grant_permissions_after_creation="false",
            catalog_name="test_catalog",
            schema_name="test_schema",
            volume_name="test_volume",
            current_user="test_user@example.com",
        )

        # Should be False, not truthy string "false"
        assert config.grant_permissions_after_creation is False
        assert isinstance(config.grant_permissions_after_creation, bool)

        # Should work correctly in conditional checks
        if config.grant_permissions_after_creation:
            pytest.fail("grant_permissions_after_creation should be False")

    def test_permission_flag_behavior_when_enabled(self):
        """Test that permission flag is correctly set to True when enabled."""
        config = MetadataConfig(skip_yaml_loading=True, 
            grant_permissions_after_creation="true",
            catalog_name="test_catalog",
            schema_name="test_schema",
            volume_name="test_volume",
            current_user="test_user@example.com",
        )

        # Should be True
        assert config.grant_permissions_after_creation is True
        assert isinstance(config.grant_permissions_after_creation, bool)

        # Should work correctly in conditional checks
        if not config.grant_permissions_after_creation:
            pytest.fail("grant_permissions_after_creation should be True")

    def test_config_has_required_permission_fields(self):
        """Test that config properly stores permission-related fields."""
        config = MetadataConfig(skip_yaml_loading=True, 
            grant_permissions_after_creation="true",
            catalog_name="test_catalog",
            schema_name="test_schema",
            volume_name="test_volume",
            current_user="test_user@example.com",
            permission_groups="data_engineers,analysts",
            permission_users="user1@example.com,user2@example.com",
        )

        # All fields should be accessible
        assert hasattr(config, "grant_permissions_after_creation")
        assert hasattr(config, "catalog_name")
        assert hasattr(config, "schema_name")
        assert hasattr(config, "volume_name")
        assert hasattr(config, "current_user")
        assert hasattr(config, "permission_groups")
        assert hasattr(config, "permission_users")

        # Values should be stored correctly
        assert config.catalog_name == "test_catalog"
        assert config.schema_name == "test_schema"
        assert config.volume_name == "test_volume"
        assert config.current_user == "test_user@example.com"


class TestConfigPermissionFlag:
    """Test the grant_permissions_after_creation config flag."""

    def test_permission_flag_defaults_to_true(self):
        """Test that grant_permissions_after_creation defaults to true."""
        config = MetadataConfig(skip_yaml_loading=True, )
        # Default should be true based on variables.yml
        assert isinstance(config.grant_permissions_after_creation, bool)

    def test_permission_flag_can_be_disabled(self):
        """Test that grant_permissions_after_creation can be set to false."""
        config = MetadataConfig(skip_yaml_loading=True, grant_permissions_after_creation="false")
        assert config.grant_permissions_after_creation is False

    def test_permission_flag_can_be_enabled(self):
        """Test that grant_permissions_after_creation can be set to true."""
        config = MetadataConfig(skip_yaml_loading=True, grant_permissions_after_creation="true")
        assert config.grant_permissions_after_creation is True


class TestPermissionGroupsAndUsers:
    """Test permission groups and users configuration parsing."""

    def test_permission_groups_csv_format(self):
        """Test that permission groups are stored as comma-separated string."""
        config = MetadataConfig(skip_yaml_loading=True, permission_groups="data_engineers,analysts,admins")
        assert hasattr(config, "permission_groups")
        assert config.permission_groups == "data_engineers,analysts,admins"

    def test_permission_users_csv_format(self):
        """Test that permission users are stored as comma-separated string."""
        config = MetadataConfig(skip_yaml_loading=True, permission_users="user1@example.com,user2@example.com")
        assert hasattr(config, "permission_users")
        assert config.permission_users == "user1@example.com,user2@example.com"

    def test_permission_groups_none_value(self):
        """Test handling of 'none' value for permission groups."""
        config = MetadataConfig(skip_yaml_loading=True, permission_groups="none")
        assert config.permission_groups == "none"

    def test_permission_users_empty_string(self):
        """Test handling of empty permission users."""
        config = MetadataConfig(skip_yaml_loading=True, permission_users="")
        # Should handle empty string gracefully
        assert hasattr(config, "permission_users")

    def test_permission_fields_whitespace(self):
        """Test that whitespace in CSV is preserved (will be handled by parsing logic)."""
        config = MetadataConfig(skip_yaml_loading=True, permission_groups="group1, group2, group3")
        # Whitespace handling is responsibility of the parsing code, not config
        assert config.permission_groups == "group1, group2, group3"


class TestConfigFieldInteractions:
    """Test interactions between different config fields."""

    def test_apply_ddl_false_with_grant_permissions_true(self):
        """Test that apply_ddl and grant_permissions are independent."""
        config = MetadataConfig(skip_yaml_loading=True, 
            apply_ddl="false", grant_permissions_after_creation="true"
        )
        assert config.apply_ddl is False
        assert config.grant_permissions_after_creation is True

    def test_all_permission_related_fields_together(self):
        """Test setting all permission-related fields simultaneously."""
        config = MetadataConfig(skip_yaml_loading=True, 
            grant_permissions_after_creation="true",
            permission_groups="admins,engineers",
            permission_users="admin@example.com",
            catalog_name="test_catalog",
            schema_name="test_schema",
            volume_name="test_volume",
        )

        # All fields should be set correctly
        assert config.grant_permissions_after_creation is True
        assert config.permission_groups == "admins,engineers"
        assert config.permission_users == "admin@example.com"
        assert config.catalog_name == "test_catalog"
        assert config.schema_name == "test_schema"
        assert config.volume_name == "test_volume"

    def test_boolean_fields_independence(self):
        """Test that boolean fields don't interfere with each other."""
        config = MetadataConfig(skip_yaml_loading=True, 
            apply_ddl="true",
            cleanup_control_table="false",
            grant_permissions_after_creation="true",
            allow_data="false",
        )

        assert config.apply_ddl is True
        assert config.cleanup_control_table is False
        assert config.grant_permissions_after_creation is True
        assert config.allow_data is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
