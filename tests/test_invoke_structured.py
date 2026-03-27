"""Tests for invoke_structured fallback and helpers.

Covers:
- _extract_and_validate_json: JSON extraction + Pydantic validation
- _append_json_instruction: non-mutating message augmentation
- invoke_structured: happy path (with_structured_output) and fallback path
- Mock integration tests for all key Pydantic response models
"""

import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from unittest.mock import patch, MagicMock
from pydantic import BaseModel, Field, ValidationError
from typing import Optional, List

from dbxmetagen.chat_client import (
    _extract_and_validate_json,
    _append_json_instruction,
    invoke_structured,
)


# ---------------------------------------------------------------------------
# Test models
# ---------------------------------------------------------------------------


class SimpleModel(BaseModel):
    name: str
    score: float = Field(ge=0.0, le=1.0)


class NestedModel(BaseModel):
    classifications: List[SimpleModel]


# ---------------------------------------------------------------------------
# _extract_and_validate_json
# ---------------------------------------------------------------------------


class TestExtractAndValidateJson:

    def test_valid_json(self):
        result = _extract_and_validate_json('{"name": "test", "score": 0.8}', SimpleModel)
        assert result.name == "test"
        assert result.score == 0.8

    def test_json_with_markdown_fences(self):
        text = '```json\n{"name": "fenced", "score": 0.5}\n```'
        result = _extract_and_validate_json(text, SimpleModel)
        assert result.name == "fenced"

    def test_json_with_surrounding_prose(self):
        text = 'Here is my answer:\n{"name": "embedded", "score": 0.9}\nHope this helps!'
        result = _extract_and_validate_json(text, SimpleModel)
        assert result.name == "embedded"

    def test_nested_json(self):
        text = '{"classifications": [{"name": "a", "score": 0.1}, {"name": "b", "score": 0.2}]}'
        result = _extract_and_validate_json(text, NestedModel)
        assert len(result.classifications) == 2

    def test_malformed_json_raises(self):
        with pytest.raises(ValueError, match="Could not extract valid JSON"):
            _extract_and_validate_json("{broken json", SimpleModel)

    def test_schema_mismatch_raises(self):
        with pytest.raises(ValueError, match="did not match.*schema"):
            _extract_and_validate_json('{"name": "x", "score": 5.0}', SimpleModel)

    def test_no_json_at_all_raises(self):
        with pytest.raises(ValueError, match="Could not extract valid JSON"):
            _extract_and_validate_json("no json here at all", SimpleModel)


# ---------------------------------------------------------------------------
# _append_json_instruction
# ---------------------------------------------------------------------------


class TestAppendJsonInstruction:

    def test_appends_schema(self):
        msgs = [{"role": "user", "content": "Classify this table"}]
        result = _append_json_instruction(msgs, SimpleModel)
        assert "JSON" in result[-1]["content"]
        assert "schema" in result[-1]["content"].lower()

    def test_does_not_mutate_original(self):
        msgs = [{"role": "user", "content": "hello"}]
        original_content = msgs[0]["content"]
        _append_json_instruction(msgs, SimpleModel)
        assert msgs[0]["content"] == original_content

    def test_skips_if_json_already_present(self):
        msgs = [{"role": "user", "content": "Return JSON please"}]
        result = _append_json_instruction(msgs, SimpleModel)
        assert result is msgs

    def test_empty_messages(self):
        result = _append_json_instruction([], SimpleModel)
        assert result == []


# ---------------------------------------------------------------------------
# invoke_structured: mock-based tests
# ---------------------------------------------------------------------------


def _mock_ai_message(text):
    """Create a mock AIMessage-like object."""
    msg = MagicMock()
    msg.content = text
    return msg


class TestInvokeStructuredHappyPath:
    """with_structured_output succeeds -- should return directly."""

    @patch("dbxmetagen.chat_client.ChatDatabricks")
    def test_returns_structured_output(self, MockChat):
        expected = SimpleModel(name="happy", score=0.7)
        mock_chain = MagicMock()
        mock_chain.invoke.return_value = expected
        MockChat.return_value.with_structured_output.return_value = mock_chain

        result = invoke_structured(
            "databricks-claude-sonnet-4-6",
            [{"role": "user", "content": "test"}],
            SimpleModel,
        )
        assert result == expected
        MockChat.return_value.with_structured_output.assert_called_once_with(SimpleModel)


class TestInvokeStructuredFallback:
    """with_structured_output raises -- should fall back to JSON parsing."""

    @patch("dbxmetagen.chat_client.ChatDatabricks")
    def test_fallback_parses_json(self, MockChat):
        mock_llm = MockChat.return_value
        mock_llm.with_structured_output.return_value.invoke.side_effect = Exception("tool calling not supported")
        mock_llm.invoke.return_value = _mock_ai_message('{"name": "fallback", "score": 0.6}')

        result = invoke_structured(
            "databricks-gpt-oss-120b",
            [{"role": "user", "content": "test"}],
            SimpleModel,
        )
        assert result.name == "fallback"
        assert result.score == 0.6

    @patch("dbxmetagen.chat_client.ChatDatabricks")
    def test_fallback_handles_list_content(self, MockChat):
        """AIMessage.content can be a list of content blocks instead of a string."""
        mock_llm = MockChat.return_value
        mock_llm.with_structured_output.return_value.invoke.side_effect = Exception("fail")
        msg = MagicMock()
        msg.content = [{"type": "text", "text": '{"name": "from_list", "score": 0.4}'}]
        mock_llm.invoke.return_value = msg

        result = invoke_structured(
            "databricks-gpt-oss-120b",
            [{"role": "user", "content": "test"}],
            SimpleModel,
        )
        assert result.name == "from_list"
        assert result.score == 0.4

    @patch("dbxmetagen.chat_client.ChatDatabricks")
    def test_fallback_bad_json_raises(self, MockChat):
        mock_llm = MockChat.return_value
        mock_llm.with_structured_output.return_value.invoke.side_effect = Exception("fail")
        mock_llm.invoke.return_value = _mock_ai_message("not json at all")

        with pytest.raises(ValueError, match="Could not extract valid JSON"):
            invoke_structured(
                "databricks-gpt-oss-120b",
                [{"role": "user", "content": "test"}],
                SimpleModel,
            )

    @patch("dbxmetagen.chat_client.ChatDatabricks")
    def test_fallback_schema_mismatch_raises(self, MockChat):
        mock_llm = MockChat.return_value
        mock_llm.with_structured_output.return_value.invoke.side_effect = Exception("fail")
        mock_llm.invoke.return_value = _mock_ai_message('{"name": "x", "score": 99}')

        with pytest.raises(ValueError, match="did not match.*schema"):
            invoke_structured(
                "databricks-gpt-oss-120b",
                [{"role": "user", "content": "test"}],
                SimpleModel,
            )


# ---------------------------------------------------------------------------
# Integration tests: real Pydantic models from the codebase
# ---------------------------------------------------------------------------


from dbxmetagen.domain_classifier import DomainResult, SubdomainResult, TableClassification
from dbxmetagen.ontology import (
    EntityClassificationResult,
    BatchTableClassificationResult,
    TableClassificationItem,
)


_DOMAIN_RESULT_JSON = json.dumps({
    "domain": "clinical",
    "confidence": 0.85,
    "second_choice_domain": "research",
    "recommended_domain": None,
    "reasoning": "Table contains clinical patient data",
})

_SUBDOMAIN_RESULT_JSON = json.dumps({
    "subdomain": "patient_records",
    "confidence": 0.9,
    "recommended_subdomain": None,
    "reasoning": "Contains patient demographic records",
})

_ENTITY_RESULT_JSON = json.dumps({
    "entity_type": "Patient",
    "secondary_entity_type": None,
    "confidence": 0.95,
    "recommended_entity": None,
    "reasoning": "Table contains patient demographics",
})

_BATCH_TABLE_RESULT_JSON = json.dumps({
    "classifications": [
        {
            "table_name": "patients",
            "entity_type": "Patient",
            "secondary_entity_type": None,
            "confidence": 0.9,
            "recommended_entity": None,
            "reasoning": "Patient demographics",
        }
    ]
})


class TestIntegrationDomainResult:

    @patch("dbxmetagen.chat_client.ChatDatabricks")
    def test_happy_path(self, MockChat):
        expected = DomainResult.model_validate(json.loads(_DOMAIN_RESULT_JSON))
        mock_chain = MagicMock()
        mock_chain.invoke.return_value = expected
        MockChat.return_value.with_structured_output.return_value = mock_chain

        result = invoke_structured("ep", [{"role": "user", "content": "x"}], DomainResult)
        assert result.domain == "clinical"

    @patch("dbxmetagen.chat_client.ChatDatabricks")
    def test_fallback_path(self, MockChat):
        mock_llm = MockChat.return_value
        mock_llm.with_structured_output.return_value.invoke.side_effect = Exception("no tools")
        mock_llm.invoke.return_value = _mock_ai_message(_DOMAIN_RESULT_JSON)

        result = invoke_structured("ep", [{"role": "user", "content": "x"}], DomainResult)
        assert result.domain == "clinical"
        assert result.confidence == 0.85


class TestIntegrationSubdomainResult:

    @patch("dbxmetagen.chat_client.ChatDatabricks")
    def test_fallback_path(self, MockChat):
        mock_llm = MockChat.return_value
        mock_llm.with_structured_output.return_value.invoke.side_effect = Exception("no tools")
        mock_llm.invoke.return_value = _mock_ai_message(_SUBDOMAIN_RESULT_JSON)

        result = invoke_structured("ep", [{"role": "user", "content": "x"}], SubdomainResult)
        assert result.subdomain == "patient_records"


class TestIntegrationEntityClassificationResult:

    @patch("dbxmetagen.chat_client.ChatDatabricks")
    def test_fallback_path(self, MockChat):
        mock_llm = MockChat.return_value
        mock_llm.with_structured_output.return_value.invoke.side_effect = Exception("no tools")
        mock_llm.invoke.return_value = _mock_ai_message(_ENTITY_RESULT_JSON)

        result = invoke_structured("ep", [{"role": "user", "content": "x"}], EntityClassificationResult)
        assert result.entity_type == "Patient"
        assert result.confidence == 0.95


class TestIntegrationBatchTableClassificationResult:

    @patch("dbxmetagen.chat_client.ChatDatabricks")
    def test_fallback_path(self, MockChat):
        mock_llm = MockChat.return_value
        mock_llm.with_structured_output.return_value.invoke.side_effect = Exception("no tools")
        mock_llm.invoke.return_value = _mock_ai_message(_BATCH_TABLE_RESULT_JSON)

        result = invoke_structured("ep", [{"role": "user", "content": "x"}], BatchTableClassificationResult)
        assert len(result.classifications) == 1
        assert result.classifications[0].entity_type == "Patient"
