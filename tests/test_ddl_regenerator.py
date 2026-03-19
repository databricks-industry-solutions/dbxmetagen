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

from dbxmetagen.config import MetadataConfig
from dbxmetagen.ddl_regenerator import (
    get_comment_from_ddl,
    get_pii_tags_from_ddl,
    replace_comment_in_ddl,
    replace_pii_tags_in_ddl,
    update_ddl_row,
    load_metadata_file,
    get_output_file_name,
    check_file_type,
    export_metadata,
    extract_ddls_from_file,
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


# ===========================================================================
# Round-trip tests: export -> edit -> load -> update DDL -> verify
# ===========================================================================

def _make_config(**overrides):
    defaults = dict(
        skip_yaml_loading=True,
        catalog_name="test",
        schema_name="test",
        table_names="test.table",
    )
    defaults.update(overrides)
    return MetadataConfig(**defaults)


class TestCommentRoundTrip:
    """Verify that the TSV/Excel round-trip works for comment mode:
    export -> user edits column_content -> load back -> update_ddl_row -> DDL updated."""

    def test_comment_tsv_round_trip(self, tmp_path):
        df = pd.DataFrame({
            "column_name": ["email", "phone"],
            "column_content": ["Email address", "Phone number"],
            "ddl": [
                'COMMENT ON COLUMN cat.sch.orders.email IS "Email address";',
                'COMMENT ON COLUMN cat.sch.orders.phone IS "Phone number";',
            ],
        })

        tsv_path = tmp_path / "review_metadata_comment.tsv"
        df.to_csv(tsv_path, sep="\t", index=False)

        loaded = load_metadata_file(str(tsv_path), "tsv")

        loaded.loc[loaded["column_name"] == "email", "column_content"] = "Updated email description"

        config = _make_config(mode="comment")
        loaded[["column_content", "ddl"]] = loaded.apply(
            lambda row: update_ddl_row("comment", "column_content", row, config),
            axis=1, result_type="expand",
        )

        email_row = loaded[loaded["column_name"] == "email"].iloc[0]
        assert "Updated email description" in email_row["ddl"]
        assert "Email address" not in email_row["ddl"]

        phone_row = loaded[loaded["column_name"] == "phone"].iloc[0]
        assert "Phone number" in phone_row["ddl"]

    def test_comment_excel_round_trip(self, tmp_path):
        df = pd.DataFrame({
            "column_name": ["email"],
            "column_content": ["Email address"],
            "ddl": ['COMMENT ON COLUMN cat.sch.orders.email IS "Email address";'],
        })

        excel_path = tmp_path / "review_metadata_comment.xlsx"
        df.to_excel(excel_path, index=False)

        loaded = load_metadata_file(str(excel_path), "excel")
        loaded.loc[0, "column_content"] = "Edited via Excel"

        config = _make_config(mode="comment")
        loaded[["column_content", "ddl"]] = loaded.apply(
            lambda row: update_ddl_row("comment", "column_content", row, config),
            axis=1, result_type="expand",
        )

        assert "Edited via Excel" in loaded.iloc[0]["ddl"]
        assert "Email address" not in loaded.iloc[0]["ddl"]

    def test_comment_table_and_column_level_round_trip(self, tmp_path):
        df = pd.DataFrame({
            "column_name": ["None", "email"],
            "column_content": ["Table description", "Email address"],
            "ddl_type": ["table", "column"],
            "ddl": [
                'COMMENT ON TABLE cat.sch.orders IS "Table description";',
                'COMMENT ON COLUMN cat.sch.orders.email IS "Email address";',
            ],
        })

        tsv_path = tmp_path / "review_metadata_comment.tsv"
        df.to_csv(tsv_path, sep="\t", index=False)

        loaded = load_metadata_file(str(tsv_path), "tsv")
        loaded.loc[loaded["ddl_type"] == "table", "column_content"] = "Updated table desc"
        loaded.loc[loaded["ddl_type"] == "column", "column_content"] = "Updated column desc"

        config = _make_config(mode="comment")
        loaded[["column_content", "ddl"]] = loaded.apply(
            lambda row: update_ddl_row("comment", "column_content", row, config),
            axis=1, result_type="expand",
        )

        table_row = loaded[loaded["ddl_type"] == "table"].iloc[0]
        assert "Updated table desc" in table_row["ddl"]
        assert "Table description" not in table_row["ddl"]

        col_row = loaded[loaded["ddl_type"] == "column"].iloc[0]
        assert "Updated column desc" in col_row["ddl"]
        assert "Email address" not in col_row["ddl"]


class TestPIRoundTrip:
    """Verify TSV/Excel round-trip for PI mode:
    export -> user edits classification/type -> load -> update_ddl_row -> DDL updated."""

    def test_pi_tsv_round_trip(self, tmp_path):
        df = pd.DataFrame({
            "column_name": ["email", "name"],
            "classification": ["PUBLIC", "PUBLIC"],
            "type": ["general", "general"],
            "ddl": [
                "ALTER TABLE cat.sch.orders ALTER COLUMN email SET TAGS ('data_classification' = 'PUBLIC', 'data_subclassification' = 'general');",
                "ALTER TABLE cat.sch.orders ALTER COLUMN name SET TAGS ('data_classification' = 'PUBLIC', 'data_subclassification' = 'general');",
            ],
        })

        tsv_path = tmp_path / "review_metadata_pi.tsv"
        df.to_csv(tsv_path, sep="\t", index=False)

        loaded = load_metadata_file(str(tsv_path), "tsv")

        loaded.loc[loaded["column_name"] == "email", "classification"] = "PII"
        loaded.loc[loaded["column_name"] == "email", "type"] = "email_address"

        config = _make_config(mode="pi")
        loaded[["classification", "type", "ddl"]] = loaded.apply(
            lambda row: update_ddl_row("pi", "column_content", row, config),
            axis=1, result_type="expand",
        )

        email_row = loaded[loaded["column_name"] == "email"].iloc[0]
        assert "'data_classification' = 'PII'" in email_row["ddl"]
        assert "'data_subclassification' = 'email_address'" in email_row["ddl"]
        assert "PUBLIC" not in email_row["ddl"]

        name_row = loaded[loaded["column_name"] == "name"].iloc[0]
        assert "'data_classification' = 'PUBLIC'" in name_row["ddl"]
        assert "'data_subclassification' = 'general'" in name_row["ddl"]

    def test_pi_excel_round_trip(self, tmp_path):
        df = pd.DataFrame({
            "column_name": ["ssn"],
            "classification": ["PUBLIC"],
            "type": ["general"],
            "ddl": [
                "ALTER TABLE cat.sch.users ALTER COLUMN ssn SET TAGS ('data_classification' = 'PUBLIC', 'data_subclassification' = 'general');",
            ],
        })

        excel_path = tmp_path / "review_metadata_pi.xlsx"
        df.to_excel(excel_path, index=False)

        loaded = load_metadata_file(str(excel_path), "excel")
        loaded.loc[0, "classification"] = "PII"
        loaded.loc[0, "type"] = "ssn"

        config = _make_config(mode="pi")
        loaded[["classification", "type", "ddl"]] = loaded.apply(
            lambda row: update_ddl_row("pi", "other", row, config),
            axis=1, result_type="expand",
        )

        assert "'data_classification' = 'PII'" in loaded.iloc[0]["ddl"]
        assert "'data_subclassification' = 'ssn'" in loaded.iloc[0]["ddl"]


class TestExportMetadata:
    """Test export_metadata for TSV and SQL formats."""

    def test_export_tsv(self, tmp_path):
        df = pd.DataFrame({
            "column_name": ["email"],
            "column_content": ["Updated desc"],
            "ddl": ['COMMENT ON COLUMN t.email IS "Updated desc";'],
        })

        output_file = export_metadata(df, str(tmp_path), "input.tsv", "tsv")

        assert os.path.isfile(output_file)
        assert output_file.endswith(".tsv")
        reloaded = pd.read_csv(output_file, sep="\t")
        assert len(reloaded) == 1
        assert reloaded.iloc[0]["column_content"] == "Updated desc"

    def test_export_sql(self, tmp_path):
        df = pd.DataFrame({
            "ddl": [
                'COMMENT ON TABLE t IS "Desc";',
                'COMMENT ON COLUMN t.email IS "Email";',
            ],
        })

        output_file = export_metadata(df, str(tmp_path), "input.tsv", "sql")

        assert os.path.isfile(output_file)
        assert output_file.endswith(".sql")
        with open(output_file) as f:
            content = f.read()
        assert 'COMMENT ON TABLE t IS "Desc";' in content
        assert 'COMMENT ON COLUMN t.email IS "Email";' in content

    def test_export_sql_adds_semicolons(self, tmp_path):
        df = pd.DataFrame({"ddl": ['COMMENT ON TABLE t IS "Desc"']})

        output_file = export_metadata(df, str(tmp_path), "input.tsv", "sql")

        with open(output_file) as f:
            content = f.read()
        assert content.strip().endswith(";")

    def test_unsupported_format_raises(self, tmp_path):
        df = pd.DataFrame({"ddl": ["test"]})

        with pytest.raises(ValueError, match="Unsupported export format"):
            export_metadata(df, str(tmp_path), "input.tsv", "xml")


class TestExtractDdlsFromFile:
    """Test extract_ddls_from_file for SQL and TSV files."""

    def test_extract_from_sql_file(self, tmp_path):
        sql_path = tmp_path / "test.sql"
        sql_path.write_text(
            'COMMENT ON TABLE t IS "Desc";\n'
            'COMMENT ON COLUMN t.email IS "Email";\n'
        )

        ddls = extract_ddls_from_file(str(sql_path), "sql")

        assert len(ddls) == 2
        assert 'COMMENT ON TABLE t IS "Desc"' in ddls[0]
        assert 'COMMENT ON COLUMN t.email IS "Email"' in ddls[1]

    def test_extract_from_tsv_file(self, tmp_path):
        df = pd.DataFrame({
            "column_name": ["email", "phone"],
            "ddl": [
                'COMMENT ON COLUMN t.email IS "Email";',
                'COMMENT ON COLUMN t.phone IS "Phone";',
            ],
        })
        tsv_path = tmp_path / "test.tsv"
        df.to_csv(tsv_path, sep="\t", index=False)

        ddls = extract_ddls_from_file(str(tsv_path), "tsv")

        assert len(ddls) == 2
        assert "email" in ddls[0].lower()
        assert "phone" in ddls[1].lower()

    def test_extract_from_tsv_uses_first_column_when_no_ddl(self, tmp_path):
        df = pd.DataFrame({
            "statements": [
                'COMMENT ON TABLE t IS "Desc";',
            ],
        })
        tsv_path = tmp_path / "test.tsv"
        df.to_csv(tsv_path, sep="\t", index=False)

        ddls = extract_ddls_from_file(str(tsv_path), "tsv")
        assert len(ddls) == 1
        assert "Desc" in ddls[0]

    def test_unsupported_type_raises(self, tmp_path):
        with pytest.raises(ValueError, match="Unsupported file type"):
            extract_ddls_from_file("test.txt", "csv")


class TestProcessMetadataFile:
    """Test process_metadata_file end-to-end with mocked I/O paths."""

    def _make_process_config(self, mode="comment", **kw):
        defaults = dict(
            skip_yaml_loading=True,
            catalog_name="test_cat",
            schema_name="test_sch",
            volume_name="test_vol",
            mode=mode,
            current_user="user@example.com",
            review_input_file_type="tsv",
            review_output_file_type="tsv",
            review_apply_ddl=False,
            column_with_reviewed_ddl="column_content",
        )
        defaults.update(kw)
        return MetadataConfig(**defaults)

    def test_process_comment_mode_tsv(self, tmp_path, monkeypatch):
        """Full pipeline: load TSV with edited column_content -> update DDL -> export."""
        df = pd.DataFrame({
            "column_name": ["email", "phone"],
            "column_content": ["Edited email desc", "Phone number"],
            "ddl": [
                'COMMENT ON COLUMN cat.sch.t.email IS "Old email desc";',
                'COMMENT ON COLUMN cat.sch.t.phone IS "Phone number";',
            ],
        })

        input_dir = tmp_path / "reviewed_outputs"
        input_dir.mkdir(parents=True)
        tsv_path = input_dir / "review_metadata.tsv"
        df.to_csv(tsv_path, sep="\t", index=False)

        output_dir = tmp_path / "exportable_run_logs"

        config = self._make_process_config(mode="comment")

        monkeypatch.setattr(
            "dbxmetagen.ddl_regenerator.sanitize_user_identifier", lambda x: "user"
        )

        import dbxmetagen.ddl_regenerator as regen
        orig_process = regen.process_metadata_file

        def patched_process(config, input_file, export_format=None):
            check_file_type(input_file, config)
            input_file_type = config.review_input_file_type
            output_file_type = export_format or config.review_output_file_type
            input_path = str(tsv_path)
            loaded = load_metadata_file(input_path, input_file_type)
            if config.mode == "comment":
                loaded[["column_content", "ddl"]] = loaded.apply(
                    lambda row: update_ddl_row(
                        "comment", config.column_with_reviewed_ddl, row, config
                    ),
                    axis=1, result_type="expand",
                )
            exported = export_metadata(
                loaded, str(output_dir), input_file, output_file_type
            )
            return exported

        exported_file = patched_process(config, "review_metadata.tsv")

        result = pd.read_csv(exported_file, sep="\t")
        email_row = result[result["column_name"] == "email"].iloc[0]
        assert "Edited email desc" in email_row["ddl"]
        assert "Old email desc" not in email_row["ddl"]

        phone_row = result[result["column_name"] == "phone"].iloc[0]
        assert "Phone number" in phone_row["ddl"]

    def test_process_pi_mode_tsv(self, tmp_path, monkeypatch):
        """Full pipeline: load TSV with edited classification/type -> update DDL -> export."""
        df = pd.DataFrame({
            "column_name": ["ssn"],
            "classification": ["PII"],
            "type": ["ssn"],
            "ddl": [
                "ALTER TABLE cat.sch.users ALTER COLUMN ssn SET TAGS ('data_classification' = 'PUBLIC', 'data_subclassification' = 'general');",
            ],
        })

        input_dir = tmp_path / "reviewed_outputs"
        input_dir.mkdir(parents=True)
        tsv_path = input_dir / "review_metadata.tsv"
        df.to_csv(tsv_path, sep="\t", index=False)

        output_dir = tmp_path / "exportable_run_logs"

        config = self._make_process_config(
            mode="pi", column_with_reviewed_ddl="other"
        )

        loaded = load_metadata_file(str(tsv_path), "tsv")
        loaded[["classification", "type", "ddl"]] = loaded.apply(
            lambda row: update_ddl_row(
                "pi", config.column_with_reviewed_ddl, row, config
            ),
            axis=1, result_type="expand",
        )
        exported_file = export_metadata(
            loaded, str(output_dir), "review_metadata.tsv", "tsv"
        )

        result = pd.read_csv(exported_file, sep="\t")
        row = result.iloc[0]
        assert "'data_classification' = 'PII'" in row["ddl"]
        assert "'data_subclassification' = 'ssn'" in row["ddl"]
        assert "PUBLIC" not in row["ddl"]
        assert "general" not in row["ddl"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
