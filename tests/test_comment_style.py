"""Tests for comment_style parameter: config validation, prompt guidelines, and summarizer targets."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from unittest.mock import MagicMock, patch

from dbxmetagen.config import MetadataConfig
from dbxmetagen.prompts import COMMENT_STYLE_GUIDELINES
from dbxmetagen.comment_summarizer import SUMMARY_WORD_TARGETS


def _make_config(**overrides):
    defaults = dict(
        skip_yaml_loading=True,
        catalog_name="test_catalog",
        schema_name="test_schema",
        table_names="test_catalog.test_schema.t",
        mode="comment",
        model="test-model",
        volume_name="v",
        apply_ddl=False,
        allow_data=True,
        sample_size=5,
        columns_per_call=5,
        temperature=0.1,
        max_tokens=4096,
        max_prompt_length=100000,
        ddl_output_format="tsv",
        current_user="test@example.com",
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
    defaults.update(overrides)
    cfg = MetadataConfig(**defaults)
    if not hasattr(cfg, "acro_content"):
        cfg.acro_content = {}
    return cfg


class TestConfigValidation:

    def test_default_is_standard(self):
        cfg = _make_config()
        assert cfg.comment_style == "standard"

    @pytest.mark.parametrize("style", ["concise", "standard", "detailed"])
    def test_valid_styles_accepted(self, style):
        cfg = _make_config(comment_style=style)
        assert cfg.comment_style == style

    def test_invalid_style_raises(self):
        with pytest.raises(ValueError, match="comment_style must be one of"):
            _make_config(comment_style="verbose")


class TestCommentPromptGuidelines:

    def _get_system_content(self, config):
        from dbxmetagen.prompts import CommentPrompt
        prompt = object.__new__(CommentPrompt)
        prompt.config = config
        prompt.prompt_content = {"table_name": "cat.sch.t", "column_contents": {}}
        tmpl = prompt.create_prompt_template()
        return tmpl["comment"][0]["content"]

    @pytest.mark.parametrize("style", ["concise", "standard", "detailed"])
    def test_guideline_matches_style(self, style):
        cfg = _make_config(comment_style=style)
        system_text = self._get_system_content(cfg)
        expected = COMMENT_STYLE_GUIDELINES[style]["data"]
        assert expected in system_text


class TestCommentNoDataPromptGuidelines:

    def _get_system_content(self, config):
        from dbxmetagen.prompts import CommentNoDataPrompt
        prompt = object.__new__(CommentNoDataPrompt)
        prompt.config = config
        prompt.prompt_content = {"table_name": "cat.sch.t", "column_contents": {}}
        tmpl = prompt.create_prompt_template()
        return tmpl["comment"][0]["content"]

    @pytest.mark.parametrize("style", ["concise", "standard", "detailed"])
    def test_guideline_matches_style(self, style):
        cfg = _make_config(comment_style=style)
        system_text = self._get_system_content(cfg)
        expected = COMMENT_STYLE_GUIDELINES[style]["nodata"]
        assert expected in system_text


class TestSummarizerWordTargets:

    def _make_mock_df(self, text, row_count=5):
        df = MagicMock()
        filtered = MagicMock()
        limited = MagicMock()
        df.filter.return_value = filtered
        filtered.count.return_value = row_count
        filtered.limit.return_value = limited
        row = MagicMock()
        row.__getitem__ = lambda self, key: text
        limited.select.return_value.collect.return_value = [row]
        return df

    @patch("dbxmetagen.comment_summarizer.ChatClientFactory")
    @pytest.mark.parametrize("style", ["concise", "standard", "detailed"])
    def test_summarizer_word_target(self, mock_factory, style):
        mock_client = MagicMock()
        mock_factory.create_client.return_value = mock_client
        completion = MagicMock()
        completion.choices = [MagicMock()]
        completion.choices[0].message.content = "Summary."
        mock_client.create_completion.return_value = completion

        from dbxmetagen.comment_summarizer import TableCommentSummarizer
        cfg = _make_config(comment_style=style)
        df = self._make_mock_df("Column A stores IDs.")
        summarizer = TableCommentSummarizer(cfg, df)
        summarizer.summarize_comments("cat.sch.t")

        messages = mock_client.create_completion.call_args.kwargs["messages"]
        system_text = messages[0]["content"]
        expected = SUMMARY_WORD_TARGETS[style]
        assert expected in system_text
