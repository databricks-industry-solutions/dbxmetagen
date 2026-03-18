"""Extended tests for domain_classifier -- enforce, user message, error result."""

import pytest
from dbxmetagen.domain_classifier import (
    _enforce_value,
    _build_user_message,
    _error_result,
)


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
