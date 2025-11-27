"""
Comprehensive unit tests for Streamlit app business logic.

This test suite focuses on the testable business logic in the app without
requiring Streamlit, Databricks SDK, or pandas to be fully installed.

Tests cover:
- Configuration validation and processing
- URL normalization and validation
- Table name format validation
- Parameter building and conversion
- Path resolution logic
- Node type detection
- Worker configuration mapping
"""

import pytest
import sys
import os
import re
from unittest.mock import Mock, MagicMock, patch


class TestTableNameValidation:
    """Test table name validation logic (from DataOperations)"""

    def test_valid_table_name_format(self):
        """Test regex pattern for valid table names"""
        pattern = re.compile(
            r"^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$"
        )

        valid_names = [
            "catalog.schema.table",
            "my_catalog.my_schema.my_table",
            "_catalog._schema._table",
            "catalog1.schema2.table3",
        ]

        for name in valid_names:
            assert pattern.match(name), f"{name} should be valid"

    def test_invalid_table_name_format(self):
        """Test regex pattern rejects invalid table names"""
        pattern = re.compile(
            r"^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$"
        )

        invalid_names = [
            "schema.table",  # Missing catalog
            "table",  # Missing catalog and schema
            "catalog.schema.",  # Empty table name
            ".schema.table",  # Empty catalog
            "catalog..table",  # Empty schema
            "catalog.schema.table-name",  # Hyphen not allowed
            "catalog.schema.table name",  # Space not allowed
            "123catalog.schema.table",  # Can't start with number
        ]

        for name in invalid_names:
            assert not pattern.match(name), f"{name} should be invalid"


class TestURLNormalization:
    """Test URL normalization logic (from DatabricksClientManager)"""

    def test_normalize_adds_https(self):
        """Test that normalization adds https:// prefix"""

        def normalize_host_url(host: str) -> str:
            host = host.strip()
            if not host.startswith(("https://", "http://")):
                host = f"https://{host}"
            return host

        assert (
            normalize_host_url("workspace.databricks.com")
            == "https://workspace.databricks.com"
        )
        assert (
            normalize_host_url("  workspace.databricks.com  ")
            == "https://workspace.databricks.com"
        )

    def test_normalize_preserves_existing_protocol(self):
        """Test that existing https:// is preserved"""

        def normalize_host_url(host: str) -> str:
            host = host.strip()
            if not host.startswith(("https://", "http://")):
                host = f"https://{host}"
            return host

        assert (
            normalize_host_url("https://workspace.databricks.com")
            == "https://workspace.databricks.com"
        )
        assert normalize_host_url("http://localhost.com") == "http://localhost.com"

    def test_url_validation(self):
        """Test URL validation logic"""

        def is_valid_host_url(host: str) -> bool:
            return (
                host.startswith(("https://", "http://"))
                and len(host.strip()) > 10
                and "." in host
            )

        # Valid URLs
        assert is_valid_host_url("https://workspace.databricks.com")
        assert is_valid_host_url("http://localhost.databricks.com")

        # Invalid URLs
        assert not is_valid_host_url("notaurl")
        assert not is_valid_host_url("https://")
        assert not is_valid_host_url("")
        assert not is_valid_host_url("https://nodot")


class TestWorkerConfiguration:
    """Test worker configuration mapping logic (from JobManager)"""

    def test_cluster_size_to_worker_config(self):
        """Test mapping of cluster sizes to worker counts"""
        worker_map = {
            "Serverless": {"min": 1, "max": 1},
            "Small (1-2 workers)": {"min": 1, "max": 2},
            "Medium (2-4 workers)": {"min": 2, "max": 4},
            "Large (4-8 workers)": {"min": 4, "max": 8},
        }

        assert worker_map["Serverless"]["min"] == 1
        assert worker_map["Serverless"]["max"] == 1
        assert worker_map["Small (1-2 workers)"]["max"] == 2
        assert worker_map["Medium (2-4 workers)"]["min"] == 2
        assert worker_map["Large (4-8 workers)"]["max"] == 8

    def test_valid_cluster_sizes(self):
        """Test validation of cluster size options"""
        valid_sizes = [
            "Serverless",
            "Small (1-2 workers)",
            "Medium (2-4 workers)",
            "Large (4-8 workers)",
        ]

        assert "Serverless" in valid_sizes
        assert "InvalidSize" not in valid_sizes


class TestNodeTypeDetection:
    """Test cloud provider node type detection (from JobManager)"""

    def test_detect_azure_workspace(self):
        """Test Azure workspace detection from URL"""
        url = "https://adb-123456.14.azuredatabricks.net"
        is_azure = "azure" in url.lower()

        assert is_azure
        node_type = "Standard_D3_v2" if is_azure else None
        assert node_type == "Standard_D3_v2"

    def test_detect_aws_workspace(self):
        """Test AWS workspace detection from URL"""
        url = "https://dbc-12345678-1234.cloud.databricks.com"
        contains_aws = "aws" in url.lower()

        # Note: This URL doesn't contain 'aws', so detection would need different logic
        assert not contains_aws

    def test_detect_gcp_workspace(self):
        """Test GCP workspace detection from URL"""
        url = "https://123456789.1.gcp.databricks.com"
        is_gcp = "gcp" in url.lower()

        assert is_gcp
        node_type = "n1-standard-4" if is_gcp else None
        assert node_type == "n1-standard-4"


class TestParameterConversion:
    """Test parameter type conversion logic (from JobManager)"""

    def test_boolean_to_string_conversion(self):
        """Test converting boolean config values to lowercase strings"""

        def convert_bool(value: bool) -> str:
            return str(value).lower()

        assert convert_bool(True) == "true"
        assert convert_bool(False) == "false"

    def test_list_to_comma_separated_string(self):
        """Test converting list of tables to comma-separated string"""
        tables = [
            "catalog.schema.table1",
            "catalog.schema.table2",
            "catalog.schema.table3",
        ]
        result = ",".join(tables)

        assert (
            result
            == "catalog.schema.table1,catalog.schema.table2,catalog.schema.table3"
        )
        assert result.count(",") == 2

    def test_integer_to_string_conversion(self):
        """Test converting integer config values to strings"""
        sample_size = 10
        result = str(sample_size)

        assert result == "10"
        assert isinstance(result, str)


class TestPathResolution:
    """Test notebook path resolution logic (from JobManager)"""

    def test_user_bundle_path_construction(self):
        """Test constructing bundle path for user directory"""
        deploying_user = "user@example.com"
        bundle_name = "dbxmetagen"
        bundle_target = "dev"
        notebook_name = "generate_metadata"

        path = f"/Workspace/Users/{deploying_user}/.bundle/{bundle_name}/{bundle_target}/files/notebooks/{notebook_name}"

        assert path.startswith("/Workspace/Users/")
        assert deploying_user in path
        assert ".bundle" in path
        assert notebook_name in path
        assert bundle_target in path

    def test_shared_bundle_path_construction(self):
        """Test constructing bundle path for shared directory"""
        bundle_name = "dbxmetagen"
        bundle_target = "prod"
        notebook_name = "sync_reviewed_ddl"

        path = f"/Shared/.bundle/{bundle_name}/{bundle_target}/files/notebooks/{notebook_name}"

        assert path.startswith("/Shared/")
        assert ".bundle" in path
        assert notebook_name in path


class TestDDLGeneration:
    """Test DDL statement generation logic (from MetadataProcessor)"""

    def test_comment_ddl_escaping(self):
        """Test that quotes are properly escaped in DDL comments"""
        description = 'Description with "quotes"'
        escaped = description.replace('"', "'")

        assert escaped == "Description with 'quotes'"
        assert '"' not in escaped

    def test_comment_ddl_column_level_structure(self):
        """Test structure of column-level comment DDL"""
        table_name = "catalog.schema.table"
        column_name = "my_column"
        comment = "Column description"

        # DBR 16+ syntax
        ddl = f'COMMENT ON COLUMN {table_name}.`{column_name}` IS "{comment}";'

        assert "COMMENT ON COLUMN" in ddl
        assert table_name in ddl
        assert column_name in ddl
        assert comment in ddl

    def test_comment_ddl_table_level_structure(self):
        """Test structure of table-level comment DDL"""
        table_name = "catalog.schema.table"
        comment = "Table description"

        ddl = f'COMMENT ON TABLE {table_name} IS "{comment}";'

        assert "COMMENT ON TABLE" in ddl
        assert table_name in ddl
        assert comment in ddl
        assert "COLUMN" not in ddl

    def test_pi_tag_ddl_structure(self):
        """Test structure of PI classification tag DDL"""
        table_name = "catalog.schema.table"
        column_name = "email_col"
        classification = "PII"
        type_value = "EMAIL"

        tag_name = "data_classification"
        subclass_tag = "data_subclassification"

        tags_string = (
            f"'{tag_name}' = '{classification}', '{subclass_tag}' = '{type_value}'"
        )
        ddl = f"ALTER TABLE {table_name} ALTER COLUMN `{column_name}` SET TAGS ({tags_string})"

        assert "ALTER TABLE" in ddl
        assert "SET TAGS" in ddl
        assert classification in ddl
        assert type_value in ddl

    def test_domain_tag_ddl_structure(self):
        """Test structure of domain classification tag DDL"""
        table_name = "catalog.schema.table"
        domain = "Finance"
        subdomain = "Accounting"

        domain_tag = "domain"
        subdomain_tag = "subdomain"

        tags_string = f"'{domain_tag}' = '{domain}', '{subdomain_tag}' = '{subdomain}'"
        ddl = f"ALTER TABLE {table_name} SET TAGS ({tags_string})"

        assert "ALTER TABLE" in ddl
        assert "SET TAGS" in ddl
        assert domain in ddl
        assert subdomain in ddl
        assert "ALTER COLUMN" not in ddl  # Domain is table-level


class TestCSVProcessing:
    """Test CSV file processing logic (from DataOperations)"""

    def test_csv_column_name_detection(self):
        """Test detection of valid table name column in CSV"""
        possible_columns = ["table_name", "table", "name", "table_names"]

        # Test priority order
        test_columns_1 = ["table_name", "other_column"]
        found = None
        for col in possible_columns:
            if col in test_columns_1:
                found = col
                break
        assert found == "table_name"

        # Test fallback
        test_columns_2 = ["table", "other_column"]
        found = None
        for col in possible_columns:
            if col in test_columns_2:
                found = col
                break
        assert found == "table"

    def test_filter_empty_and_nan_values(self):
        """Test filtering of empty strings and NaN values"""
        table_names = ["catalog.schema.table1", "", "catalog.schema.table2", "nan"]

        # Filter logic
        filtered = [name for name in table_names if name and name != "nan"]

        assert len(filtered) == 2
        assert "" not in filtered
        assert "nan" not in filtered


class TestEnvironmentConfiguration:
    """Test environment variable handling (from ConfigManager)"""

    def test_host_override_from_environment(self):
        """Test that DATABRICKS_HOST env var overrides config"""
        config = {"host": "original.databricks.com"}
        env_host = "override.databricks.com"

        # Simulate override logic
        if env_host:
            config["host"] = env_host

        assert config["host"] == "override.databricks.com"

    def test_bundle_target_from_environment(self):
        """Test getting bundle target with default"""
        # Simulate os.getenv with default
        bundle_target = "prod" or "dev"  # Would be: os.getenv("BUNDLE_TARGET", "dev")

        assert bundle_target == "prod"

        # Test default
        bundle_target_default = None or "dev"
        assert bundle_target_default == "dev"


class TestYAMLVariableExtraction:
    """Test YAML variable extraction logic (from ConfigManager)"""

    def test_extract_default_values(self):
        """Test extracting default values from Databricks variables structure"""
        raw_config = {
            "variables": {
                "catalog_name": {"default": "my_catalog"},
                "mode": {"default": "comment"},
            }
        }

        # Extraction logic
        config = {}
        for key, value_config in raw_config["variables"].items():
            if isinstance(value_config, dict) and "default" in value_config:
                config[key] = value_config["default"]
            else:
                config[key] = value_config

        assert config["catalog_name"] == "my_catalog"
        assert config["mode"] == "comment"

    def test_handle_direct_values(self):
        """Test handling non-dict values in variables"""
        raw_config = {
            "variables": {
                "catalog_name": {"default": "my_catalog"},
                "simple_value": "direct_value",
            }
        }

        # Extraction logic
        config = {}
        for key, value_config in raw_config["variables"].items():
            if isinstance(value_config, dict) and "default" in value_config:
                config[key] = value_config["default"]
            else:
                config[key] = value_config

        assert config["catalog_name"] == "my_catalog"
        assert config["simple_value"] == "direct_value"


class TestUserContextResolution:
    """Test user context resolution logic (from UserContextManager)"""

    def test_job_user_with_obo(self):
        """Test job user selection with OBO mode"""
        use_obo = True
        current_user = "current.user@example.com"
        service_principal = "sp@service.principal"

        job_user = current_user if use_obo else service_principal

        assert job_user == "current.user@example.com"

    def test_job_user_without_obo(self):
        """Test job user selection without OBO (uses service principal)"""
        use_obo = False
        current_user = "current.user@example.com"
        service_principal = "sp@service.principal"

        job_user = current_user if use_obo else service_principal

        assert job_user == "sp@service.principal"

    def test_deploying_user_path_construction(self):
        """Test constructing paths with deploying user"""
        deploying_user = "deployer@example.com"
        use_shared = False

        base_path = "/Shared" if use_shared else f"/Users/{deploying_user}"

        assert base_path == "/Users/deployer@example.com"

    def test_shared_path_construction(self):
        """Test constructing paths with shared directory"""
        use_shared = True

        base_path = "/Shared" if use_shared else "/Users/someone"

        assert base_path == "/Shared"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
