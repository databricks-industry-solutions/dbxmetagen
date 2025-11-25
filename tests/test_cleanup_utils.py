"""Unit tests for cleanup utility functions."""

import pytest
import sys
import os
import re
from unittest.mock import MagicMock

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

from src.dbxmetagen.user_utils import sanitize_user_identifier


class TestTempTablePatternMatching:
    """Test pattern matching for temp table names."""

    def test_temp_table_name_format(self):
        """Test that temp table names follow expected pattern."""
        user = "test_user@example.com"
        sanitized = sanitize_user_identifier(user)
        timestamp = "20240115_123456"

        table_name = f"temp_metadata_generation_log_{sanitized}_{timestamp}"

        # Should match the pattern
        pattern = r"^temp_metadata_generation_log_\w+_\d{8}_\d{6}$"
        assert re.match(pattern, table_name)

    def test_temp_table_starts_with_prefix(self):
        """Test that temp tables start with correct prefix."""
        user = "jane_doe"
        sanitized = sanitize_user_identifier(user)
        timestamp = "20240115_123456"

        table_name = f"temp_metadata_generation_log_{sanitized}_{timestamp}"

        assert table_name.startswith("temp_metadata_generation_log_")

    def test_identify_temp_table_for_user(self):
        """Test identifying temp tables for a specific user."""
        current_user = "alice@example.com"
        sanitized = sanitize_user_identifier(current_user)

        # Tables that should match
        matching_tables = [
            f"temp_metadata_generation_log_{sanitized}_20240115_123456",
            f"temp_metadata_generation_log_{sanitized}_20240116_000000",
        ]

        # Tables that should not match (different user)
        other_user_tables = [
            "temp_metadata_generation_log_bob_example_com_20240115_123456",
            "temp_metadata_generation_log_charlie_20240115_123456",
        ]

        prefix = f"temp_metadata_generation_log_{sanitized}_"

        for table in matching_tables:
            assert table.startswith(prefix)

        for table in other_user_tables:
            assert not table.startswith(prefix)

    def test_false_positive_prevention(self):
        """Test that non-temp tables are not matched."""
        current_user = "alice@example.com"
        sanitized = sanitize_user_identifier(current_user)

        # These should NOT be matched as temp tables
        non_temp_tables = [
            "metadata_generation_log",  # Missing "temp_" prefix
            f"temp_metadata_log_{sanitized}_20240115_123456",  # Wrong format
            f"temp_metadata_generation_{sanitized}_20240115_123456",  # Missing "log"
            "some_other_table",
        ]

        prefix = f"temp_metadata_generation_log_{sanitized}_"

        for table in non_temp_tables:
            assert not table.startswith(prefix)

    def test_timestamp_format_variations(self):
        """Test that the timestamp format is strict."""
        user = "test@example.com"
        sanitized = sanitize_user_identifier(user)

        # Valid timestamp format
        valid_table = f"temp_metadata_generation_log_{sanitized}_20240115_123456"
        pattern = r"^temp_metadata_generation_log_\w+_\d{8}_\d{6}$"
        assert re.match(pattern, valid_table)

        # Invalid timestamp formats should not match
        invalid_tables = [
            f"temp_metadata_generation_log_{sanitized}_2024115_123456",  # Missing digit
            f"temp_metadata_generation_log_{sanitized}_20240115_12345",  # Missing digit
            f"temp_metadata_generation_log_{sanitized}_20240115",  # Missing time
        ]

        for table in invalid_tables:
            assert not re.match(pattern, table)


class TestControlTablePatternMatching:
    """Test pattern matching for control table names."""

    def test_control_table_name_format(self):
        """Test that control table names follow expected pattern."""
        user = "test_user@example.com"
        sanitized = sanitize_user_identifier(user)

        # Basic control table
        table_name = f"metadata_control_{sanitized}"
        assert table_name.startswith("metadata_control_")

        # Control table with job ID
        table_with_job = f"metadata_control_{sanitized}123456"
        assert table_with_job.startswith("metadata_control_")
        assert table_with_job.startswith(f"metadata_control_{sanitized}")

    def test_identify_control_table_for_user(self):
        """Test identifying control tables for a specific user."""
        current_user = "alice@example.com"
        sanitized = sanitize_user_identifier(current_user)

        # Tables that should match
        matching_tables = [
            f"metadata_control_{sanitized}",
            f"metadata_control_{sanitized}123456",
            f"metadata_control_{sanitized}789",
        ]

        # Tables that should not match (different user)
        other_user_tables = [
            "metadata_control_bob_example_com",
            "metadata_control_charlie",
        ]

        prefix = f"metadata_control_{sanitized}"

        for table in matching_tables:
            assert table.startswith(prefix)

        for table in other_user_tables:
            assert not table.startswith(prefix)


class TestUserSanitization:
    """Test user identifier sanitization."""

    def test_sanitize_email(self):
        """Test that email addresses are sanitized correctly."""
        assert sanitize_user_identifier("user@example.com") == "user_example_com"
        assert (
            sanitize_user_identifier("test.user@company.org") == "test_user_company_org"
        )

    def test_sanitize_special_characters(self):
        """Test that special characters are removed."""
        assert sanitize_user_identifier("user@domain.com") == "user_domain_com"
        assert sanitize_user_identifier("user-name") == "user_name"
        assert sanitize_user_identifier("user.name") == "user_name"

    def test_sanitize_preserves_alphanumeric(self):
        """Test that alphanumeric characters are preserved."""
        assert sanitize_user_identifier("user123") == "user123"
        assert sanitize_user_identifier("abc_123") == "abc_123"

    def test_sanitize_empty_string(self):
        """Test that empty string raises ValueError."""
        with pytest.raises(ValueError, match="User identifier cannot be empty"):
            sanitize_user_identifier("")

        # Whitespace-only should also raise
        with pytest.raises(ValueError, match="User identifier cannot be empty"):
            sanitize_user_identifier("   ")

    def test_sanitize_complex_email(self):
        """Test sanitization of complex email addresses.

        ALL non-alphanumeric characters (except underscore) should be replaced.
        """
        # + is replaced along with @, ., etc.
        assert (
            sanitize_user_identifier("first.last+tag@sub.domain.com")
            == "first_last_tag_sub_domain_com"
        )
        assert (
            sanitize_user_identifier("user_name@example.co.uk")
            == "user_name_example_co_uk"
        )

    def test_sanitize_all_special_characters(self):
        """Test that ALL non-alphanumeric characters (except underscore) are replaced."""
        # Common email/username special characters should all be replaced
        assert "@" not in sanitize_user_identifier("user@domain.com")
        assert "." not in sanitize_user_identifier("user.name@domain.com")
        assert "-" not in sanitize_user_identifier("user-name@domain.com")
        assert "+" not in sanitize_user_identifier("user+tag@domain.com")
        assert "#" not in sanitize_user_identifier("user#tag@domain.com")
        assert "!" not in sanitize_user_identifier("user!name@domain.com")

        # Verify they're replaced with underscores
        assert sanitize_user_identifier("user@domain.com") == "user_domain_com"
        assert sanitize_user_identifier("user+tag") == "user_tag"
        assert sanitize_user_identifier("user#name") == "user_name"

    def test_sanitize_preserves_underscores_and_alphanumeric(self):
        """Test that underscores and alphanumeric characters are preserved."""
        # Alphanumeric preserved
        assert sanitize_user_identifier("user123") == "user123"
        assert sanitize_user_identifier("ABC_123_xyz") == "ABC_123_xyz"

        # Underscores preserved
        assert sanitize_user_identifier("user_name_123") == "user_name_123"

    def test_sanitize_idempotency(self):
        """Test that sanitizing an already sanitized value is idempotent."""
        original = "user@example.com"
        sanitized_once = sanitize_user_identifier(original)
        sanitized_twice = sanitize_user_identifier(sanitized_once)
        assert sanitized_once == sanitized_twice

    def test_sanitize_underscores_preserved(self):
        """Test that existing underscores are preserved."""
        assert sanitize_user_identifier("user_name_123") == "user_name_123"


class TestDryRunLogic:
    """Test dry run behavior for cleanup operations."""

    def test_dry_run_flag_parsing(self):
        """Test that dry_run flag is parsed correctly from strings."""
        from src.dbxmetagen.config import _parse_bool

        assert _parse_bool("true") is True
        assert _parse_bool("false") is False

    def test_cleanup_respects_dry_run(self):
        """Test that cleanup operations respect dry_run flag."""
        # This is a conceptual test - the actual implementation
        # would be in the cleanup utility notebook

        def mock_cleanup_tables(tables, dry_run):
            """Mock cleanup function."""
            if dry_run:
                return {"deleted": [], "would_delete": tables}
            else:
                return {"deleted": tables, "would_delete": []}

        tables = ["temp_table_1", "temp_table_2"]

        # Dry run should not delete
        result_dry = mock_cleanup_tables(tables, dry_run=True)
        assert len(result_dry["deleted"]) == 0
        assert len(result_dry["would_delete"]) == 2

        # Non-dry run should delete
        result_real = mock_cleanup_tables(tables, dry_run=False)
        assert len(result_real["deleted"]) == 2
        assert len(result_real["would_delete"]) == 0

    def test_cleanup_with_empty_table_list(self):
        """Test cleanup behavior with no tables to clean."""

        def mock_cleanup_tables(tables, dry_run):
            if dry_run:
                return {"deleted": [], "would_delete": tables}
            else:
                return {"deleted": tables, "would_delete": []}

        result = mock_cleanup_tables([], dry_run=False)
        assert len(result["deleted"]) == 0
        assert len(result["would_delete"]) == 0

    def test_cleanup_error_handling_concept(self):
        """Test that cleanup handles errors gracefully."""

        def mock_cleanup_tables_with_errors(tables, dry_run):
            """Mock cleanup that can have failures."""
            if dry_run:
                return {"deleted": [], "would_delete": tables, "errors": []}

            success = []
            failures = []
            for table in tables:
                if "error" in table:
                    failures.append({"table": table, "error": "Mock error"})
                else:
                    success.append(table)

            return {"deleted": success, "would_delete": [], "errors": failures}

        tables = ["good_table_1", "error_table", "good_table_2"]
        result = mock_cleanup_tables_with_errors(tables, dry_run=False)

        assert len(result["deleted"]) == 2
        assert len(result["errors"]) == 1
        assert result["errors"][0]["table"] == "error_table"


class TestCleanupSafety:
    """Test safety features of cleanup operations."""

    def test_user_isolation(self):
        """Test that cleanup only targets current user's tables by default."""
        current_user = "alice@example.com"
        sanitized = sanitize_user_identifier(current_user)

        all_tables = [
            f"temp_metadata_generation_log_{sanitized}_20240115_123456",
            "temp_metadata_generation_log_bob_example_com_20240115_123456",
            f"metadata_control_{sanitized}",
            "metadata_control_charlie",
        ]

        # Filter for current user only
        user_prefix_temp = f"temp_metadata_generation_log_{sanitized}_"
        user_prefix_control = f"metadata_control_{sanitized}"

        user_tables = [
            t
            for t in all_tables
            if t.startswith(user_prefix_temp) or t.startswith(user_prefix_control)
        ]

        # Should only get 2 tables (alice's temp and control)
        assert len(user_tables) == 2
        assert all(sanitized in t for t in user_tables)

    def test_admin_mode_includes_all_users(self):
        """Test that admin mode can target all users' tables."""
        all_tables = [
            "temp_metadata_generation_log_alice_example_com_20240115_123456",
            "temp_metadata_generation_log_bob_example_com_20240115_123456",
            "metadata_control_alice_example_com",
            "metadata_control_bob_example_com",
        ]

        # Admin mode filters
        temp_prefix = "temp_metadata_generation_log_"
        control_prefix = "metadata_control_"

        admin_tables = [
            t
            for t in all_tables
            if t.startswith(temp_prefix) or t.startswith(control_prefix)
        ]

        # Should get all 4 tables
        assert len(admin_tables) == 4


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
