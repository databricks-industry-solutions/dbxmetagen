"""Tests for domain_classifier module."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from unittest.mock import MagicMock, patch
from dbxmetagen.domain_classifier import (
    load_domain_config,
    generate_domain_prompt_section,
    create_system_prompt,
    TableClassification,
)


class TestLoadDomainConfig:

    def test_returns_dict_with_valid_path(self, tmp_path):
        config_file = tmp_path / "domain_config.yaml"
        config_file.write_text("domains:\n  finance:\n    description: Financial data\n    keywords: [revenue, cost]\n")
        config = load_domain_config(str(config_file))
        assert isinstance(config, dict)
        assert "domains" in config

    def test_returns_defaults_with_missing_path(self):
        config = load_domain_config("/nonexistent/path.yaml")
        assert isinstance(config, dict)

    def test_returns_none_with_empty_file(self, tmp_path):
        config_file = tmp_path / "empty.yaml"
        config_file.write_text("")
        config = load_domain_config(str(config_file))
        # Empty YAML returns None; caller handles default
        assert config is None or isinstance(config, dict)


class TestGenerateDomainPromptSection:

    def test_generates_string_with_domains(self):
        config = {
            "domains": {
                "Finance": {"description": "Financial data", "keywords": ["revenue", "cost"]},
                "HR": {"description": "Human resources", "keywords": ["employee", "salary"]},
            }
        }
        result = generate_domain_prompt_section(config)
        assert isinstance(result, str)
        assert "Finance" in result
        assert "HR" in result

    def test_handles_empty_domains(self):
        config = {"domains": {}}
        result = generate_domain_prompt_section(config)
        assert isinstance(result, str)


class TestCreateSystemPrompt:

    def test_returns_nonempty_string(self):
        config = {"domains": {"Test": {"description": "Test domain", "keywords": ["test"]}}}
        result = create_system_prompt(config)
        assert isinstance(result, str)
        assert len(result) > 0


class TestTableClassification:

    def test_valid_classification(self):
        tc = TableClassification(
            catalog="test_catalog",
            schema="test_schema",
            table="revenue_table",
            domain="Finance",
            subdomain="Revenue",
            confidence=0.9,
            reasoning="Contains revenue data",
        )
        assert tc.domain == "Finance"
        assert tc.confidence == 0.9
        assert tc.catalog == "test_catalog"
