"""
Unit tests for DDL regenerator functions (reviewed DDL sync workflow).

These tests verify the process_metadata_file function and related helpers
that handle the workflow of reviewing and syncing DDL changes.

NOTE: Due to mlflow/databricks-sdk import conflicts, these tests may fail
when run with the full test suite. Run them separately:

    pytest tests/test_ddl_regenerator.py -v

Or run all tests except this file:

    pytest tests/ --ignore=tests/test_ddl_regenerator.py -v
"""

import pytest
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.ddl_regenerator import (
    get_comment_from_ddl,
    get_pii_tags_from_ddl,
    replace_comment_in_ddl,
    replace_pii_tags_in_ddl,
    update_ddl_row,
    load_metadata_file,
    get_output_file_name,
    check_file_type,
)


class TestGetCommentFromDDL:
    """Test extracting comments from DDL statements."""

    def test_extract_comment_with_double_quotes(self):
        """Test extracting comment with double quotes."""
        ddl = 'COMMENT ON TABLE test_table IS "This is a test comment";'
        result = get_comment_from_ddl(ddl)
        assert result == "This is a test comment"

    def test_extract_comment_with_single_quotes(self):
        """Test extracting comment with single quotes."""
        ddl = "COMMENT ON TABLE test_table IS 'This is a test comment';"
        result = get_comment_from_ddl(ddl)
        assert result == "This is a test comment"

    def test_extract_comment_from_column_ddl(self):
        """Test extracting comment from COMMENT ON COLUMN statement."""
        ddl = 'COMMENT ON COLUMN test_table.column_name IS "Column description";'
        result = get_comment_from_ddl(ddl)
        # This function looks for "COMMENT ON TABLE" specifically, so it won't match column comments
        assert result == ""

    def test_no_comment_in_ddl(self):
        """Test DDL without comment returns empty string."""
        ddl = "ALTER TABLE test_table ADD COLUMN new_col STRING;"
        result = get_comment_from_ddl(ddl)
        assert result == ""

    def test_empty_comment(self):
        """Test DDL with empty comment."""
        ddl = 'COMMENT ON TABLE test_table IS "";'
        result = get_comment_from_ddl(ddl)
        assert result == ""


class TestGetPIITagsFromDDL:
    """Test extracting PII tags from DDL statements."""

    def test_extract_tags_from_table_ddl(self):
        """Test extracting tags from table-level ALTER TABLE statement."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test",
            schema_name="test",
            table_names="test.table",
        )
        ddl = "ALTER TABLE test_table SET TAGS ('data_classification' = 'PII', 'data_subclassification' = 'email');"

        classification, subclass = get_pii_tags_from_ddl(ddl, config)
        assert classification == "PII"
        assert subclass == "email"

    def test_extract_tags_from_column_ddl(self):
        """Test extracting tags from column-level ALTER COLUMN statement."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test",
            schema_name="test",
            table_names="test.table",
        )
        ddl = "ALTER TABLE test_table ALTER COLUMN email SET TAGS ('data_classification' = 'PII', 'data_subclassification' = 'email');"

        classification, subclass = get_pii_tags_from_ddl(ddl, config)
        assert classification == "PII"
        assert subclass == "email"

    def test_no_tags_in_ddl(self):
        """Test DDL without tags returns empty strings."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test",
            schema_name="test",
            table_names="test.table",
        )
        ddl = "ALTER TABLE test_table ADD COLUMN new_col STRING;"

        classification, subclass = get_pii_tags_from_ddl(ddl, config)
        assert classification == ""
        assert subclass == ""


class TestReplaceCommentInDDL:
    """Test replacing comments in DDL statements."""

    def test_replace_existing_comment(self):
        """Test replacing an existing comment in DDL."""
        original_ddl = 'COMMENT ON TABLE test_table IS "Old comment";'
        new_comment = "New updated comment"

        result = replace_comment_in_ddl(original_ddl, new_comment)
        assert "New updated comment" in result
        assert "Old comment" not in result

    def test_replace_comment_preserves_structure(self):
        """Test that replacing comment preserves DDL structure."""
        original_ddl = 'COMMENT ON COLUMN test_table.email IS "Email address";'
        new_comment = "Updated email description"

        result = replace_comment_in_ddl(original_ddl, new_comment)
        assert "COMMENT ON COLUMN test_table.email IS" in result
        assert "Updated email description" in result


class TestReplacePIITagsInDDL:
    """Test replacing PII tags in DDL statements."""

    def test_replace_tags_in_table_ddl(self):
        """Test replacing tags in table-level DDL."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test",
            schema_name="test",
            table_names="test.table",
        )
        original_ddl = "ALTER TABLE test_table SET TAGS ('data_classification' = 'PUBLIC', 'data_subclassification' = 'general');"

        result = replace_pii_tags_in_ddl(original_ddl, "PII", "email", config)
        assert "'data_classification' = 'PII'" in result
        assert "'data_subclassification' = 'email'" in result
        assert "PUBLIC" not in result
        assert "general" not in result

    def test_replace_tags_in_column_ddl(self):
        """Test replacing tags in column-level DDL."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test",
            schema_name="test",
            table_names="test.table",
        )
        original_ddl = "ALTER TABLE test_table ALTER COLUMN email SET TAGS ('data_classification' = 'PUBLIC', 'data_subclassification' = 'general');"

        result = replace_pii_tags_in_ddl(original_ddl, "PII", "ssn", config)
        assert "'data_classification' = 'PII'" in result
        assert "'data_subclassification' = 'ssn'" in result


class TestUpdateDDLRow:
    """Test update_ddl_row function for different modes and scenarios."""

    def test_pi_mode_update_from_classification(self):
        """Test PI mode update when classification/type columns are reviewed."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test",
            schema_name="test",
            table_names="test.table",
            mode="pi",
        )

        row = pd.Series(
            {
                "classification": "PII",
                "type": "email",
                "ddl": "ALTER TABLE test_table ALTER COLUMN email SET TAGS ('data_classification' = 'PUBLIC', 'data_subclassification' = 'general');",
            }
        )

        new_classification, new_type, new_ddl = update_ddl_row(
            "pi", "classification", row, config
        )

        assert new_classification == "PII"
        assert new_type == "email"
        assert "'data_classification' = 'PII'" in new_ddl
        assert "'data_subclassification' = 'email'" in new_ddl

    def test_pi_mode_update_from_ddl(self):
        """Test PI mode update when DDL column is reviewed."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test",
            schema_name="test",
            table_names="test.table",
            mode="pi",
        )

        row = pd.Series(
            {
                "classification": "PUBLIC",
                "type": "general",
                "ddl": "ALTER TABLE test_table ALTER COLUMN email SET TAGS ('data_classification' = 'PII', 'data_subclassification' = 'ssn');",
            }
        )

        new_classification, new_type, new_ddl = update_ddl_row("pi", "ddl", row, config)

        # Should extract from DDL
        assert new_classification == "PII"
        assert new_type == "ssn"
        assert new_ddl == row["ddl"]

    def test_comment_mode_update_from_column_content(self):
        """Test comment mode update when column_content is reviewed."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test",
            schema_name="test",
            table_names="test.table",
            mode="comment",
        )

        row = pd.Series(
            {
                "column_content": "Updated description",
                "ddl": 'COMMENT ON COLUMN test_table.email IS "Old description";',
            }
        )

        new_content, new_ddl = update_ddl_row("comment", "column_content", row, config)

        assert new_content == "Updated description"
        assert "Updated description" in new_ddl

    def test_comment_mode_update_from_ddl(self):
        """Test comment mode update when DDL column is reviewed."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test",
            schema_name="test",
            table_names="test.table",
            mode="comment",
        )

        row = pd.Series(
            {
                "column_content": "Old description",
                "ddl": 'COMMENT ON TABLE test_table IS "Reviewed table description";',
            }
        )

        new_content, new_ddl = update_ddl_row("comment", "ddl", row, config)

        # Should extract from DDL
        assert new_content == "Reviewed table description"
        assert new_ddl == row["ddl"]

    def test_unknown_mode_raises_error(self):
        """Test that unknown mode raises ValueError."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test",
            schema_name="test",
            table_names="test.table",
        )

        row = pd.Series({"column_content": "test", "ddl": "test"})

        with pytest.raises(ValueError, match="Unknown mode"):
            update_ddl_row("unknown_mode", "column_content", row, config)

    def test_unknown_reviewed_column_raises_error(self):
        """Test that unknown reviewed column raises ValueError."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test",
            schema_name="test",
            table_names="test.table",
        )

        row = pd.Series({"column_content": "test", "ddl": "test"})

        with pytest.raises(ValueError, match="Unknown reviewed column"):
            update_ddl_row("comment", "unknown_column", row, config)


class TestLoadMetadataFile:
    """Test loading metadata from files."""

    def test_load_tsv_file(self, tmp_path):
        """Test loading TSV file."""
        # Create a temporary TSV file
        tsv_file = tmp_path / "test.tsv"
        test_data = "column1\tcolumn2\tcolumn3\nvalue1\tvalue2\tvalue3\n"
        tsv_file.write_text(test_data)

        result = load_metadata_file(str(tsv_file), "tsv")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert list(result.columns) == ["column1", "column2", "column3"]
        assert result.iloc[0]["column1"] == "value1"

    def test_load_excel_file(self, tmp_path):
        """Test loading Excel file."""
        # Create a temporary Excel file
        excel_file = tmp_path / "test.xlsx"
        df = pd.DataFrame({"col1": ["a", "b"], "col2": ["c", "d"]})
        df.to_excel(excel_file, index=False)

        result = load_metadata_file(str(excel_file), "excel")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "col1" in result.columns
        assert "col2" in result.columns

    def test_file_not_found_raises_error(self):
        """Test that missing file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_metadata_file("/nonexistent/file.tsv", "tsv")

    def test_unsupported_file_type_raises_error(self, tmp_path):
        """Test that unsupported file type raises ValueError."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test")

        with pytest.raises(ValueError, match="Unsupported file type"):
            load_metadata_file(str(test_file), "txt")


class TestGetOutputFileName:
    """Test output file name generation."""

    def test_generate_sql_output_name(self):
        """Test generating SQL output file name."""
        input_file = "/path/to/input_file.tsv"
        result = get_output_file_name(input_file, ".sql")
        assert result == "input_file_reviewed.sql"

    def test_generate_tsv_output_name(self):
        """Test generating TSV output file name."""
        input_file = "/path/to/metadata.xlsx"
        result = get_output_file_name(input_file, ".tsv")
        assert result == "metadata_reviewed.tsv"

    def test_generate_excel_output_name(self):
        """Test generating Excel output file name."""
        input_file = "data.tsv"
        result = get_output_file_name(input_file, ".xlsx")
        assert result == "data_reviewed.xlsx"


class TestCheckFileType:
    """Test file type validation."""

    def test_check_tsv_file_type(self):
        """Test checking TSV file type."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test",
            schema_name="test",
            table_names="test.table",
            review_input_file_type="tsv",
        )

        # Should not raise an exception for .tsv file
        try:
            check_file_type("test_file.tsv", config)
        except ValueError:
            pytest.fail("check_file_type raised ValueError for valid .tsv file")

    def test_check_excel_file_type(self):
        """Test checking Excel file type."""
        config = MetadataConfig(
            skip_yaml_loading=True,
            catalog_name="test",
            schema_name="test",
            table_names="test.table",
            review_input_file_type="excel",
        )

        # Should not raise an exception for .xlsx file
        try:
            check_file_type("test_file.xlsx", config)
        except ValueError:
            pytest.fail("check_file_type raised ValueError for valid .xlsx file")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
