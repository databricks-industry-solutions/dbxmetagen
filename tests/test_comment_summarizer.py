"""Tests for TableCommentSummarizer prompt structure and response handling."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from unittest.mock import MagicMock, patch

from dbxmetagen.config import MetadataConfig


@pytest.fixture
def test_config():
    return MetadataConfig(
        skip_yaml_loading=True,
        catalog_name="test_catalog",
        schema_name="test_schema",
        table_names="test_catalog.test_schema.my_table",
        mode="comment",
        model="test-model",
        volume_name="test_volume",
        apply_ddl=False,
        allow_data=True,
        sample_size=5,
        columns_per_call=5,
        temperature=0.1,
        max_tokens=4096,
        max_prompt_length=100000,
        ddl_output_format="tsv",
        current_user="test_user@example.com",
        control_table=True,
        cleanup_control_table=False,
        word_limit_per_cell=50,
        limit_prompt_based_on_cell_len=True,
        allow_data_in_comments=True,
        include_datatype_from_metadata=False,
        include_possible_data_fields_in_metadata=True,
        include_existing_table_comment=False,
        add_metadata=True,
        reviewable_output_format="tsv",
        review_input_file_type="tsv",
        review_output_file_type="tsv",
        review_apply_ddl=False,
        grant_permissions_after_creation=False,
        include_deterministic_pi=False,
        tag_none_fields=True,
        federation_mode=False,
    )


def _make_mock_df(comments_text, row_count=5):
    """Build a mock PySpark DataFrame that chains through the summarizer's calls."""
    df = MagicMock()
    filtered = MagicMock()
    limited = MagicMock()

    df.filter.return_value = filtered
    filtered.count.return_value = row_count
    filtered.limit.return_value = limited

    row = MagicMock()
    row.__getitem__ = lambda self, key: comments_text
    limited.select.return_value.collect.return_value = [row]

    return df


def _make_mock_completion(response_text="This is a summary."):
    """Build a mock ChatCompletion object."""
    completion = MagicMock()
    completion.choices = [MagicMock()]
    completion.choices[0].message.content = response_text
    return completion


def _import_summarizer():
    """Lazy import to avoid triggering pandas/numpy during collection."""
    from dbxmetagen.comment_summarizer import TableCommentSummarizer
    return TableCommentSummarizer


class TestPromptRolesAlternateCorrectly:
    """Regression test for the user/assistant role bug."""

    @patch("dbxmetagen.comment_summarizer.ChatClientFactory")
    def test_role_sequence(self, mock_factory, test_config):
        mock_client = MagicMock()
        mock_factory.create_client.return_value = mock_client
        mock_client.create_completion.return_value = _make_mock_completion()

        TableCommentSummarizer = _import_summarizer()
        df = _make_mock_df("Column A stores IDs. Column B stores names.")
        summarizer = TableCommentSummarizer(test_config, df)
        summarizer.summarize_comments("test_catalog.test_schema.my_table")

        messages = mock_client.create_completion.call_args.kwargs["messages"]
        roles = [m["role"] for m in messages]
        assert roles == [
            "system", "user", "assistant", "user", "assistant", "user"
        ], f"Expected alternating roles, got: {roles}"


class TestActualCommentsInFinalUserMessage:

    @patch("dbxmetagen.comment_summarizer.ChatClientFactory")
    def test_final_message_contains_input_comments(self, mock_factory, test_config):
        mock_client = MagicMock()
        mock_factory.create_client.return_value = mock_client
        mock_client.create_completion.return_value = _make_mock_completion()

        TableCommentSummarizer = _import_summarizer()
        input_text = "The widget_id column is a unique identifier for each widget."
        df = _make_mock_df(input_text)
        summarizer = TableCommentSummarizer(test_config, df)
        summarizer.summarize_comments("test_catalog.test_schema.my_table")

        messages = mock_client.create_completion.call_args.kwargs["messages"]
        final_msg = messages[-1]
        assert final_msg["role"] == "user"
        assert input_text in final_msg["content"]


class TestFewshotContentNotInFinalMessage:

    @patch("dbxmetagen.comment_summarizer.ChatClientFactory")
    def test_no_fewshot_leakage(self, mock_factory, test_config):
        mock_client = MagicMock()
        mock_factory.create_client.return_value = mock_client
        mock_client.create_completion.return_value = _make_mock_completion()

        TableCommentSummarizer = _import_summarizer()
        df = _make_mock_df("Some unrelated column descriptions.")
        summarizer = TableCommentSummarizer(test_config, df)
        summarizer.summarize_comments("test_catalog.test_schema.my_table")

        messages = mock_client.create_completion.call_args.kwargs["messages"]
        final_content = messages[-1]["content"]
        assert "retail transaction" not in final_content.lower()
        assert "clinical trial" not in final_content.lower()
        assert "dbxmetagen.test_data" not in final_content.lower()


class TestReturnsLlmResponseContent:

    @patch("dbxmetagen.comment_summarizer.ChatClientFactory")
    def test_returns_completion_text(self, mock_factory, test_config):
        mock_client = MagicMock()
        mock_factory.create_client.return_value = mock_client
        expected = "This table stores patient records with demographics."
        mock_client.create_completion.return_value = _make_mock_completion(expected)

        TableCommentSummarizer = _import_summarizer()
        df = _make_mock_df("Some column descriptions.")
        summarizer = TableCommentSummarizer(test_config, df)
        result = summarizer.summarize_comments("test_catalog.test_schema.my_table")
        assert result == expected


class TestColumnLimitNoteAppended:

    @patch("dbxmetagen.comment_summarizer.ChatClientFactory")
    def test_limitation_note_when_over_25(self, mock_factory, test_config):
        mock_client = MagicMock()
        mock_factory.create_client.return_value = mock_client
        mock_client.create_completion.return_value = _make_mock_completion()

        TableCommentSummarizer = _import_summarizer()
        df = _make_mock_df("Base comments.", row_count=40)
        summarizer = TableCommentSummarizer(test_config, df)
        summarizer.summarize_comments("test_catalog.test_schema.my_table")

        messages = mock_client.create_completion.call_args.kwargs["messages"]
        final_content = messages[-1]["content"]
        assert "40 columns total" in final_content
        assert "first 25 columns" in final_content

    @patch("dbxmetagen.comment_summarizer.ChatClientFactory")
    def test_no_limitation_note_when_under_25(self, mock_factory, test_config):
        mock_client = MagicMock()
        mock_factory.create_client.return_value = mock_client
        mock_client.create_completion.return_value = _make_mock_completion()

        TableCommentSummarizer = _import_summarizer()
        df = _make_mock_df("Base comments.", row_count=10)
        summarizer = TableCommentSummarizer(test_config, df)
        summarizer.summarize_comments("test_catalog.test_schema.my_table")

        messages = mock_client.create_completion.call_args.kwargs["messages"]
        final_content = messages[-1]["content"]
        assert "columns total" not in final_content


class TestSingleRowSkipsSummarizer:

    def test_single_row_returns_df_as_is(self, test_config):
        from conftest import install_processing_stubs, uninstall_processing_stubs

        saved = install_processing_stubs()
        try:
            import dbxmetagen.processing as proc

            table_df = MagicMock()
            table_df.count.return_value = 1

            result = proc.summarize_table_content(
                table_df, test_config, "test_catalog.test_schema.my_table"
            )
            assert result is table_df
        finally:
            uninstall_processing_stubs(saved)
