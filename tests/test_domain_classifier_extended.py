"""Extended tests for domain_classifier -- keyword prefilter, tokenize, enforce, user message, error result."""

import pytest
from dbxmetagen.domain_classifier import (
    _tokenize,
    keyword_prefilter,
    _enforce_value,
    _build_user_message,
    _error_result,
)


# ── _tokenize ─────────────────────────────────────────────────────────


class TestTokenize:

    def test_splits_on_underscore(self):
        assert "order" in _tokenize("order_line_item")
        assert "line" in _tokenize("order_line_item")
        assert "item" in _tokenize("order_line_item")

    def test_lowercases(self):
        tokens = _tokenize("MyTable")
        assert all(t == t.lower() for t in tokens)

    def test_strips_empty(self):
        assert "" not in _tokenize("_leading_")

    def test_splits_on_dots(self):
        tokens = _tokenize("catalog.schema.table")
        assert "catalog" in tokens
        assert "schema" in tokens
        assert "table" in tokens

    def test_splits_on_spaces(self):
        tokens = _tokenize("hello world")
        assert "hello" in tokens
        assert "world" in tokens


# ── keyword_prefilter ─────────────────────────────────────────────────


@pytest.fixture
def domain_config():
    return {
        "domains": {
            "finance": {
                "description": "Financial data",
                "keywords": ["revenue", "billing", "ledger", "payment", "accounting", "transaction", "cost", "budget", "invoice"],
                "subdomains": {
                    "accounting": {"keywords": ["ledger", "journal", "gl", "account", "transaction", "balance"]},
                },
            },
            "customer": {
                "description": "Customer data",
                "keywords": ["customer", "sales", "campaign", "support", "lead", "account", "contact", "order", "crm"],
                "subdomains": {
                    "sales": {"keywords": ["opportunity", "deal", "quote", "pipeline", "order", "invoice"]},
                },
            },
            "governance": {
                "description": "Data governance",
                "keywords": ["data_lineage", "data_catalog", "data_governance", "data_steward", "retention_policy"],
                "subdomains": {},
            },
        }
    }


class TestKeywordPrefilter:

    def test_scores_finance_for_revenue_table(self, domain_config):
        result = keyword_prefilter("revenue_report", {}, domain_config, top_n=3)
        assert result[0] == "finance"

    def test_scores_customer_for_order_table(self, domain_config):
        result = keyword_prefilter("customer_order", {}, domain_config, top_n=3)
        assert "customer" in result[:2]

    def test_governance_does_not_match_generic_names(self, domain_config):
        result = keyword_prefilter("account", {}, domain_config, top_n=3)
        assert "governance" not in result[:2]

    def test_uses_column_contents(self, domain_config):
        metadata = {"column_contents": {"revenue": "decimal", "transaction_id": "string"}}
        result = keyword_prefilter("generic_table", metadata, domain_config, top_n=3)
        assert result[0] == "finance"

    def test_uses_table_tags(self, domain_config):
        metadata = {"table_tags": "customer support data"}
        result = keyword_prefilter("generic_table", metadata, domain_config, top_n=3)
        assert "customer" in result[:2]

    def test_returns_all_when_no_matches(self, domain_config):
        result = keyword_prefilter("xyz_abc", {}, domain_config, top_n=5)
        assert len(result) == 3  # all domains returned

    def test_empty_domains(self):
        result = keyword_prefilter("table", {}, {"domains": {}}, top_n=3)
        assert result == []

    def test_top_n_limits(self, domain_config):
        result = keyword_prefilter("revenue_report", {}, domain_config, top_n=1)
        assert len(result) == 1


# ── _enforce_value ────────────────────────────────────────────────────


class TestEnforceValue:

    def test_exact_match(self):
        val, exact = _enforce_value("Finance", ["Finance", "HR", "IT"])
        assert val == "Finance"
        assert exact is True

    def test_case_insensitive(self):
        val, exact = _enforce_value("finance", ["Finance", "HR", "IT"])
        assert val == "Finance"
        assert exact is True

    def test_fuzzy_substring(self):
        val, exact = _enforce_value("finance_dept", ["Finance", "HR"])
        assert val == "Finance"
        assert exact is False

    def test_fallback(self):
        val, exact = _enforce_value("xyz", ["Finance", "HR"])
        assert val == "unknown"
        assert exact is False

    def test_custom_fallback(self):
        val, _ = _enforce_value("xyz", ["A", "B"], fallback="other")
        assert val == "other"


# ── _build_user_message ───────────────────────────────────────────────


class TestBuildUserMessage:

    def test_includes_table_name(self):
        msg = _build_user_message("cat.sch.orders", {})
        assert "cat.sch.orders" in msg

    def test_includes_column_contents(self):
        meta = {"column_contents": {"order_id": "bigint", "total": "decimal"}}
        msg = _build_user_message("t", meta)
        assert "order_id" in msg
        assert "total" in msg

    def test_includes_tags_when_present(self):
        meta = {"table_tags": "pii=true, domain=finance"}
        msg = _build_user_message("t", meta)
        assert "pii=true" in msg

    def test_includes_constraints_when_present(self):
        meta = {"table_constraints": "PRIMARY KEY (id)"}
        msg = _build_user_message("t", meta)
        assert "PRIMARY KEY" in msg

    def test_includes_comments_when_present(self):
        meta = {"table_comments": "Revenue tracking table"}
        msg = _build_user_message("t", meta)
        assert "Revenue tracking" in msg


# ── _error_result ─────────────────────────────────────────────────────


class TestErrorResult:

    def test_structure(self):
        result = _error_result("cat.sch.orders", Exception("timeout"))
        assert result["catalog"] == "cat"
        assert result["schema"] == "sch"
        assert result["table"] == "orders"
        assert result["domain"] == "unknown"
        assert result["confidence"] == 0.0
        assert "timeout" in result["reasoning"]

    def test_preserves_error_message(self):
        result = _error_result("a.b.c", ValueError("bad value"))
        assert "bad value" in result["reasoning"]
