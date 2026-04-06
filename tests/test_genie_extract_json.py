"""Unit tests for genie/agent.py::_extract_json -- LLM output parsing."""

import sys
from unittest.mock import MagicMock

# Stub heavy deps so genie.agent can import
_STUBS = [
    "langchain_core", "langchain_core._api", "langchain_core.messages",
    "langchain_core.tools", "langchain_core.language_models",
    "langchain_community", "langchain_community.chat_models",
    "langchain_community.chat_models.databricks",
    "langgraph", "langgraph.prebuilt",
    "databricks", "databricks.sdk", "databricks.sdk.service",
    "databricks.sdk.service.sql",
    "mlflow",
]
for mod in _STUBS:
    if mod not in sys.modules:
        sys.modules[mod] = MagicMock()

sys.path.insert(0, "src")
from dbxmetagen.genie.agent import _extract_json  # noqa: E402


class TestExtractJsonClean:
    def test_plain_json(self):
        assert _extract_json('{"key": "value"}') == {"key": "value"}

    def test_json_fenced(self):
        text = 'Here is the output:\n```json\n{"a": 1}\n```\nDone.'
        assert _extract_json(text) == {"a": 1}

    def test_fence_without_json_tag(self):
        text = '```\n{"b": 2}\n```'
        assert _extract_json(text) == {"b": 2}


class TestExtractJsonTrailing:
    def test_trailing_text_ignored(self):
        text = '{"key": 1} some extra text here'
        result = _extract_json(text)
        assert result == {"key": 1}

    def test_leading_text_before_json(self):
        text = 'The answer is: {"result": true}'
        result = _extract_json(text)
        assert result == {"result": True}


class TestExtractJsonEdgeCases:
    def test_nested_braces_in_strings(self):
        text = '{"msg": "use {curly} braces", "n": 1}'
        result = _extract_json(text)
        assert result["msg"] == "use {curly} braces"
        assert result["n"] == 1

    def test_escaped_quotes(self):
        text = r'{"text": "he said \"hello\"", "ok": true}'
        result = _extract_json(text)
        assert result["ok"] is True

    def test_nested_objects(self):
        text = '{"outer": {"inner": {"deep": 42}}}'
        result = _extract_json(text)
        assert result["outer"]["inner"]["deep"] == 42


class TestExtractJsonFailures:
    def test_no_brace_returns_none(self):
        assert _extract_json("no json here") is None

    def test_empty_string_returns_none(self):
        assert _extract_json("") is None

    def test_invalid_json_returns_none(self):
        assert _extract_json("{invalid json content}") is None

    def test_incomplete_json_returns_none(self):
        assert _extract_json('{"key": "value"') is None
