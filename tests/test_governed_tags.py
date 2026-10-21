"""Unit tests for governed UC-Domains tag publishing (src/dbxmetagen/governed_tags.py).

Pure logic + the injected-SQL-runner apply path (with a fake runner). No Spark.
"""

import types

import pytest

from dbxmetagen.governed_tags import (
    normalize_governed_domain_value,
    resolve_fqn,
    build_governed_tag_ddl,
    gate,
    build_statements,
    apply_statements,
    gate_and_build,
    _is_capability_error,
)


def _cfg(**kw):
    """Minimal config stand-in (plain namespace); governed_tags only uses getattr."""
    base = dict(enable_governed_tags=True, governed_domain_tag_name="company_domain",
                governed_domain_separator="/", governed_tag_max_len=256)
    base.update(kw)
    return types.SimpleNamespace(**base)


class TestNormalizeValue:
    def test_domain_and_subdomain(self):
        assert normalize_governed_domain_value("banking", "retail_banking") == "banking/retail_banking"

    def test_domain_only(self):
        assert normalize_governed_domain_value("banking", "") == "banking"
        assert normalize_governed_domain_value("banking", None) == "banking"
        assert normalize_governed_domain_value("banking", "None") == "banking"

    def test_custom_separator(self):
        assert normalize_governed_domain_value("Finance", "Tax", sep="|") == "Finance|Tax"

    def test_empty_domain(self):
        assert normalize_governed_domain_value("", "x") == ""
        assert normalize_governed_domain_value(None, "x") == ""
        assert normalize_governed_domain_value("None", "x") == ""

    def test_truncation(self):
        v = normalize_governed_domain_value("d" * 200, "s" * 200, max_len=256)
        assert len(v) == 256

    def test_strips_whitespace(self):
        assert normalize_governed_domain_value("  banking ", " retail ") == "banking/retail"


class TestResolveFqn:
    def test_prequalified(self):
        assert resolve_fqn({"table_name": "cat.sch.tbl"}) == "cat.sch.tbl"

    def test_parts(self):
        assert resolve_fqn({"catalog": "cat", "schema": "sch", "table_short_name": "tbl"}) == "cat.sch.tbl"

    def test_parts_with_short_from_table_name(self):
        assert resolve_fqn({"catalog": "cat", "schema": "sch", "table_name": "tbl"}) == "cat.sch.tbl"


class TestBuildDdl:
    def test_basic(self):
        assert build_governed_tag_ddl("cat.sch.tbl", "banking/retail", "company_domain") == \
            "ALTER TABLE cat.sch.tbl SET TAGS ('company_domain' = 'banking/retail')"

    def test_quote_escape(self):
        ddl = build_governed_tag_ddl("cat.sch.tbl", "a'b", "k'ey")
        assert "'a''b'" in ddl and "'k''ey'" in ddl


class TestGate:
    def test_disabled(self):
        ok, reason = gate(_cfg(enable_governed_tags=False))
        assert ok is False and "disabled" in reason

    def test_no_key(self):
        ok, reason = gate(_cfg(governed_domain_tag_name=""))
        assert ok is False and "governed_domain_tag_name" in reason

    def test_ok(self):
        ok, reason = gate(_cfg())
        assert ok is True and reason is None


class TestBuildStatements:
    def test_builds_and_skips(self):
        rows = [
            {"table_name": "cat.sch.a", "domain": "banking", "subdomain": "retail"},
            {"table_name": "cat.sch.b", "domain": "", "subdomain": ""},        # skipped (no domain)
            {"table_name": "cat.sch.c", "domain": "capital_markets", "subdomain": None},
        ]
        to_apply, skipped = build_statements(_cfg(), rows)
        assert [t["fqn"] for t in to_apply] == ["cat.sch.a", "cat.sch.c"]
        assert to_apply[0]["value"] == "banking/retail"
        assert to_apply[1]["value"] == "capital_markets"
        assert [s["fqn"] for s in skipped] == ["cat.sch.b"]


class TestGateAndBuild:
    def test_disabled_is_noop(self):
        to_apply, skipped, reason = gate_and_build(_cfg(enable_governed_tags=False),
                                                   [{"table_name": "cat.sch.a", "domain": "x"}])
        assert to_apply == [] and reason and "disabled" in reason

    def test_capability_absent_is_noop(self):
        to_apply, skipped, reason = gate_and_build(
            _cfg(), [{"table_name": "cat.sch.a", "domain": "x"}],
            capability={"present": False, "reason": "no governed tag"})
        assert to_apply == [] and reason == "no governed tag"

    def test_enabled_builds(self):
        to_apply, skipped, reason = gate_and_build(_cfg(), [{"table_name": "cat.sch.a", "domain": "x", "subdomain": "y"}])
        assert reason is None and len(to_apply) == 1 and to_apply[0]["value"] == "x/y"


class _FakeRunner:
    """Records statements; raises a chosen error for statements whose fqn is in fail_map."""
    def __init__(self, fail_map=None):
        self.ran = []
        self.fail_map = fail_map or {}

    def __call__(self, sql):
        self.ran.append(sql)
        for fqn, exc in self.fail_map.items():
            if fqn in sql:
                raise exc
        return []


class TestApplyStatements:
    def _items(self):
        return [
            {"fqn": "cat.sch.a", "value": "x/y", "ddl": "ALTER TABLE cat.sch.a SET TAGS ('k' = 'x/y')"},
            {"fqn": "cat.sch.b", "value": "z", "ddl": "ALTER TABLE cat.sch.b SET TAGS ('k' = 'z')"},
        ]

    def test_all_apply(self):
        r = _FakeRunner()
        out = apply_statements(r, self._items())
        assert len(out["applied"]) == 2 and not out["skipped"] and not out["errors"]

    def test_capability_error_is_skipped_not_raised(self):
        r = _FakeRunner({"cat.sch.a": Exception("PERMISSION_DENIED: cannot set tag 'k'")})
        out = apply_statements(r, self._items())
        assert [s["fqn"] for s in out["skipped"]] == ["cat.sch.a"]
        assert out["skipped"][0]["hint"]
        assert [a["fqn"] for a in out["applied"]] == ["cat.sch.b"]

    def test_capability_missing_flag(self):
        r = _FakeRunner({"cat.sch.a": Exception("governed tag policy violation"),
                         "cat.sch.b": Exception("governed tag policy violation")})
        out = apply_statements(r, self._items())
        assert out["capability_missing"] is True and not out["applied"]

    def test_other_error_recorded_not_raised(self):
        r = _FakeRunner({"cat.sch.a": Exception("TABLE_OR_VIEW_NOT_FOUND")})
        out = apply_statements(r, self._items())
        assert [e["fqn"] for e in out["errors"]] == ["cat.sch.a"]
        assert len(out["applied"]) == 1


class TestCapabilityErrorClassifier:
    @pytest.mark.parametrize("msg,expected", [
        ("PERMISSION_DENIED: cannot set tag 'x'", True),
        ("governed tag not allowed", True),
        ("tag policy violation", True),
        ("TABLE_OR_VIEW_NOT_FOUND", False),
        ("syntax error", False),
    ])
    def test_classifier(self, msg, expected):
        assert _is_capability_error(msg) is expected
