"""Tests for ChatClientFactory.create_client dispatch and guardrails."""

from types import SimpleNamespace

import pytest

from dbxmetagen.chat_client import ChatClientFactory


def test_custom_chat_spec_raises_clear_not_implemented():
    """`custom_chat_spec` is a stub; the factory must fail fast with an actionable
    message instead of the bare NotImplementedError buried in the client __init__."""
    config = SimpleNamespace(
        chat_completion_type="custom_chat_spec",
        custom_endpoint_url="https://example.com/v1",
        custom_endpoint_secret_scope="scope",
        custom_endpoint_secret_key="key",
    )
    with pytest.raises(NotImplementedError) as excinfo:
        ChatClientFactory.create_client(config)
    msg = str(excinfo.value)
    assert "custom_chat_spec" in msg
    # Points the user at the supported alternatives.
    assert "openai_spec" in msg
    assert "databricks" in msg


def test_unknown_chat_type_raises_value_error():
    config = SimpleNamespace(chat_completion_type="not_a_real_type")
    with pytest.raises(ValueError):
        ChatClientFactory.create_client(config)
