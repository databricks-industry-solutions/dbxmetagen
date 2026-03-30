"""Tests for Genie join reliability: MV extraction, force-merge, validation.

These tests exercise pure-Python functions extracted from the agent modules.
They don't require Databricks, Spark, LangChain, or any external services.
"""

import json
import uuid
import sys
import types
import pytest
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Stub heavy dependencies so we can import the functions under test.
# The agent modules import langchain/databricks at module level.
# ---------------------------------------------------------------------------
_STUB_MODS = [
    "langchain_core", "langchain_core._api", "langchain_core.messages",
    "langchain_core.tools", "langchain_core.language_models",
    "langchain_community", "langchain_community.chat_models",
    "langchain_community.chat_models.databricks",
    "langgraph", "langgraph.prebuilt",
]
for mod_name in _STUB_MODS:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

# Now safe to import
sys.path.insert(0, "apps/dbxmetagen-app/app")
from agent.genie_agent import _merge_prebuilt_join_specs, _validate_output  # noqa: E402
from agent.genie_builder import GenieContextAssembler  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _join(left: str, right: str, sql: str = "a.id = b.id") -> dict:
    return {
        "id": uuid.uuid4().hex[:32],
        "left": {"identifier": left},
        "right": {"identifier": right},
        "sql": [sql],
    }


def _raw_with_joins(joins: list, tables: list | None = None) -> dict:
    """Build a minimal agent-output dict with join_specs and data_sources."""
    return {
        "data_sources": {
            "tables": [{"identifier": t} for t in (tables or [])],
            "metric_views": [],
        },
        "instructions": {
            "join_specs": joins,
            "example_sql": [{"question": f"q{i}", "sql": f"SELECT {i}"} for i in range(8)],
            "sql_snippets": {
                "measures": [{"alias": f"m{i}", "sql": [f"SUM(c{i})"]} for i in range(5)],
                "filters": [{"display_name": f"f{i}", "sql": [f"c{i} > 0"]} for i in range(5)],
                "expressions": [{"alias": f"e{i}", "sql": [f"c{i} + 1"]} for i in range(3)],
            },
            "text": "Instructions here.",
        },
        "sample_questions": [f"question {i}?" for i in range(8)],
    }


# ---------------------------------------------------------------------------
# _merge_prebuilt_join_specs
# ---------------------------------------------------------------------------
class TestMergePrebuiltJoinSpecs:
    def test_empty_prebuilt_is_noop(self):
        raw = _raw_with_joins([_join("cat.sch.orders", "cat.sch.customers")])
        result = _merge_prebuilt_join_specs(raw, [])
        assert len(result["instructions"]["join_specs"]) == 1

    def test_prebuilt_survives_when_agent_omits(self):
        raw = _raw_with_joins([])  # agent produced no joins
        prebuilt = [_join("cat.sch.orders", "cat.sch.customers")]
        result = _merge_prebuilt_join_specs(raw, prebuilt)
        assert len(result["instructions"]["join_specs"]) == 1
        assert result["instructions"]["join_specs"][0]["left"]["identifier"] == "cat.sch.orders"

    def test_prebuilt_wins_for_same_pair(self):
        prebuilt = [_join("cat.sch.orders", "cat.sch.customers", "orders.cust_id = customers.id")]
        agent = [_join("cat.sch.orders", "cat.sch.customers", "orders.id = customers.id")]
        raw = _raw_with_joins(agent)
        result = _merge_prebuilt_join_specs(raw, prebuilt)
        joins = result["instructions"]["join_specs"]
        assert len(joins) == 1
        assert "cust_id" in joins[0]["sql"][0]  # prebuilt version kept

    def test_agent_adds_new_pairs(self):
        prebuilt = [_join("cat.sch.orders", "cat.sch.customers")]
        agent = [_join("cat.sch.orders", "cat.sch.products")]
        raw = _raw_with_joins(agent)
        result = _merge_prebuilt_join_specs(raw, prebuilt)
        assert len(result["instructions"]["join_specs"]) == 2

    def test_short_name_dedup_handles_fq_vs_short(self):
        """Agent uses short names, prebuilt uses FQ — same pair should dedup."""
        prebuilt = [_join("catalog.schema.orders", "catalog.schema.customers")]
        agent = [_join("orders", "customers")]  # short names
        raw = _raw_with_joins(agent)
        result = _merge_prebuilt_join_specs(raw, prebuilt)
        assert len(result["instructions"]["join_specs"]) == 1

    def test_both_orientations_dedup(self):
        """(A, B) in prebuilt should block (B, A) from agent."""
        prebuilt = [_join("cat.sch.orders", "cat.sch.customers")]
        agent = [_join("cat.sch.customers", "cat.sch.orders")]  # reversed
        raw = _raw_with_joins(agent)
        result = _merge_prebuilt_join_specs(raw, prebuilt)
        assert len(result["instructions"]["join_specs"]) == 1


# ---------------------------------------------------------------------------
# _validate_output — join warnings
# ---------------------------------------------------------------------------
class TestValidateOutputJoins:
    def test_no_warning_single_table(self):
        raw = _raw_with_joins([], tables=["cat.sch.orders"])
        warnings = _validate_output(raw)
        assert not any("join" in w.lower() for w in warnings)

    def test_warns_no_joins_multiple_tables(self):
        raw = _raw_with_joins([], tables=["cat.sch.orders", "cat.sch.customers", "cat.sch.products"])
        warnings = _validate_output(raw)
        join_warnings = [w for w in warnings if "join" in w.lower()]
        assert len(join_warnings) == 1
        assert "no join_specs" in join_warnings[0]

    def test_warns_thin_joins(self):
        raw = _raw_with_joins(
            [_join("cat.sch.orders", "cat.sch.customers")],
            tables=["cat.sch.orders", "cat.sch.customers", "cat.sch.products", "cat.sch.regions"],
        )
        warnings = _validate_output(raw)
        join_warnings = [w for w in warnings if "join" in w.lower()]
        assert len(join_warnings) == 1
        assert "need 3" in join_warnings[0]

    def test_no_warning_sufficient_joins(self):
        raw = _raw_with_joins(
            [_join("a", "b"), _join("b", "c")],
            tables=["a", "b", "c"],
        )
        warnings = _validate_output(raw)
        assert not any("join" in w.lower() for w in warnings)

    def test_metric_views_dont_inflate_count(self):
        """Metric views are self-contained — shouldn't require joins."""
        raw = _raw_with_joins([], tables=["cat.sch.orders"])
        raw["data_sources"]["metric_views"] = [
            {"identifier": "cat.sch.mv1"}, {"identifier": "cat.sch.mv2"},
        ]
        warnings = _validate_output(raw)
        assert not any("join" in w.lower() for w in warnings)


# ---------------------------------------------------------------------------
# _extract_mv_join_specs
# ---------------------------------------------------------------------------
class TestExtractMVJoinSpecs:
    def _asm(self):
        """Create a minimal assembler (we only test the extraction method)."""
        return object.__new__(GenieContextAssembler)

    def test_extracts_join_from_mv(self):
        mv = {
            "source_table": "cat.sch.orders",
            "json_definition": json.dumps({
                "joins": [{"name": "customers", "source": "cat.sch.customers",
                           "on": "source.cust_id = customers.id"}]
            }),
        }
        specs = self._asm()._extract_mv_join_specs([mv], set(), {"orders", "customers"})
        assert len(specs) == 1
        assert "orders.cust_id" in specs[0]["sql"][0]
        assert specs[0]["right"]["identifier"] == "cat.sch.customers"

    def test_skips_target_not_in_selected(self):
        mv = {
            "source_table": "cat.sch.orders",
            "json_definition": json.dumps({
                "joins": [{"name": "returns", "source": "cat.sch.returns",
                           "on": "source.id = returns.order_id"}]
            }),
        }
        specs = self._asm()._extract_mv_join_specs([mv], set(), {"orders", "customers"})
        assert len(specs) == 0

    def test_dedupes_against_existing(self):
        mv = {
            "source_table": "cat.sch.orders",
            "json_definition": json.dumps({
                "joins": [{"name": "customers", "source": "cat.sch.customers",
                           "on": "source.id = customers.id"}]
            }),
        }
        existing = {tuple(sorted(["orders", "customers"]))}
        specs = self._asm()._extract_mv_join_specs([mv], existing, {"orders", "customers"})
        assert len(specs) == 0

    def test_handles_string_json_definition(self):
        mv = {
            "source_table": "cat.sch.orders",
            "json_definition": '{"joins": [{"name": "c", "source": "cat.sch.c", "on": "source.id = c.id"}]}',
        }
        specs = self._asm()._extract_mv_join_specs([mv], set(), {"orders", "c"})
        assert len(specs) == 1

    def test_handles_no_joins_in_definition(self):
        mv = {
            "source_table": "cat.sch.orders",
            "json_definition": json.dumps({"measures": [{"name": "total", "expr": "SUM(amount)"}]}),
        }
        specs = self._asm()._extract_mv_join_specs([mv], set(), {"orders"})
        assert len(specs) == 0

    def test_alias_resolution_when_alias_differs(self):
        mv = {
            "source_table": "cat.sch.orders",
            "json_definition": json.dumps({
                "joins": [{"name": "cust", "source": "cat.sch.customers",
                           "on": "source.cust_id = cust.id"}]
            }),
        }
        specs = self._asm()._extract_mv_join_specs([mv], set(), {"orders", "customers"})
        assert len(specs) == 1
        # "cust." replaced with "customers.", "source." replaced with "orders."
        assert specs[0]["sql"][0] == "orders.cust_id = customers.id"

    def test_handles_malformed_json(self):
        mv = {
            "source_table": "cat.sch.orders",
            "json_definition": "not valid json {{{",
        }
        specs = self._asm()._extract_mv_join_specs([mv], set(), {"orders"})
        assert len(specs) == 0

    def test_handles_none_json_definition(self):
        mv = {"source_table": "cat.sch.orders", "json_definition": None}
        specs = self._asm()._extract_mv_join_specs([mv], set(), {"orders"})
        assert len(specs) == 0
