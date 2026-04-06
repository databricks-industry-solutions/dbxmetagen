"""Tests for Genie join reliability: MV extraction, force-merge, validation.

These tests exercise pure-Python functions extracted from the agent modules.
They don't require Databricks, Spark, LangChain, or any external services.
"""

import json
import uuid
import sys
import types
import pytest
from unittest.mock import MagicMock, patch

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
    "databricks", "databricks.sdk", "databricks.sdk.service",
    "databricks.sdk.service.sql",
    "mlflow",
]
for mod_name in _STUB_MODS:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

# Now safe to import
sys.path.insert(0, "src")
from dbxmetagen.genie.agent import _merge_prebuilt_join_specs, _validate_output  # noqa: E402
from dbxmetagen.genie.context import GenieContextAssembler  # noqa: E402


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
class TestMergePrebuiltMutation:
    """_merge_prebuilt_join_specs mutates prebuilt dicts with _prebuilt=True."""

    def test_prebuilt_flag_set_on_input_dicts(self):
        prebuilt = [_join("cat.sch.orders", "cat.sch.customers")]
        raw = _raw_with_joins([])
        _merge_prebuilt_join_specs(raw, prebuilt)
        assert prebuilt[0].get("_prebuilt") is True

    def test_raw_keys_preserved_after_merge(self):
        prebuilt = [_join("cat.sch.orders", "cat.sch.customers")]
        raw = _raw_with_joins([_join("cat.sch.a", "cat.sch.b")])
        result = _merge_prebuilt_join_specs(raw, prebuilt)
        assert "data_sources" in result
        assert "instructions" in result
        assert "sample_questions" in result


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


# ---------------------------------------------------------------------------
# assemble() — applied vs unapplied MV contract
# ---------------------------------------------------------------------------
class TestAssembleMetricViewSplit:
    """Applied MVs -> data_sources; unapplied -> sql_snippets only (not duplicate measures)."""

    def _make_assembler(self):
        from unittest.mock import MagicMock, patch
        return GenieContextAssembler(MagicMock(), "wh", "c", "s")

    def test_applied_in_data_sources_unapplied_in_snippets(self):
        asm = self._make_assembler()
        mvs = [
            {
                "metric_view_name": "mv_applied",
                "status": "applied",
                "source_table": "c.s.orders",
                "json_definition": json.dumps({
                    "measures": [{"name": "measure_from_applied", "expr": "SUM(amount)"}],
                }),
            },
            {
                "metric_view_name": "mv_draft",
                "status": "draft",
                "source_table": "c.s.orders",
                "json_definition": json.dumps({
                    "measures": [{"name": "measure_from_draft", "expr": "SUM(qty)"}],
                }),
            },
        ]
        col_meta = [
            {"table_name": "c.s.orders", "column_name": "amount", "data_type": "DECIMAL"},
            {"table_name": "c.s.orders", "column_name": "qty", "data_type": "INT"},
        ]
        with patch.object(asm, "_get_table_metadata", return_value=[{"table_name": "c.s.orders", "comment": ""}]), \
             patch.object(asm, "_get_column_metadata", return_value=col_meta), \
             patch.object(asm, "_get_fk_predictions", return_value=[]), \
             patch.object(asm, "_get_ontology_entities", return_value=[]), \
             patch.object(asm, "_get_entity_relationships", return_value=[]), \
             patch.object(asm, "_get_metric_views_by_name", return_value=(mvs, [])), \
             patch.object(asm, "_sample_categorical_values", return_value={}), \
             patch.object(asm, "_format_context", return_value=""), \
             patch.object(asm, "_get_ontology_join_specs", return_value=[]), \
             patch.object(asm, "_load_genie_reference", return_value=""):
            out = asm.assemble(["c.s.orders"], metric_view_names=["mv_applied", "mv_draft"])
        ds = out["data_sources"]
        assert len(ds["metric_views"]) == 1
        assert ds["metric_views"][0]["identifier"] == "c.s.mv_applied"
        aliases = [m.get("alias") for m in out["sql_snippets"].get("measures", [])]
        assert "measure_from_draft" in aliases
        assert "measure_from_applied" not in aliases

    def test_all_applied_no_mv_yaml_measures_in_snippets(self):
        asm = self._make_assembler()
        mvs = [
            {
                "metric_view_name": "mv_only",
                "status": "applied",
                "source_table": "c.s.orders",
                "json_definition": json.dumps({
                    "measures": [{"name": "only_in_applied", "expr": "SUM(amount)"}],
                }),
            },
        ]
        col_meta = [{"table_name": "c.s.orders", "column_name": "amount", "data_type": "DECIMAL"}]
        with patch.object(asm, "_get_table_metadata", return_value=[{"table_name": "c.s.orders", "comment": ""}]), \
             patch.object(asm, "_get_column_metadata", return_value=col_meta), \
             patch.object(asm, "_get_fk_predictions", return_value=[]), \
             patch.object(asm, "_get_ontology_entities", return_value=[]), \
             patch.object(asm, "_get_entity_relationships", return_value=[]), \
             patch.object(asm, "_get_metric_views_by_name", return_value=(mvs, [])), \
             patch.object(asm, "_sample_categorical_values", return_value={}), \
             patch.object(asm, "_format_context", return_value=""), \
             patch.object(asm, "_get_ontology_join_specs", return_value=[]), \
             patch.object(asm, "_load_genie_reference", return_value=""):
            out = asm.assemble(["c.s.orders"], metric_view_names=["mv_only"])
        aliases = [m.get("alias") for m in out["sql_snippets"].get("measures", [])]
        assert "only_in_applied" not in aliases


# ---------------------------------------------------------------------------
# _build_sql_snippets — non-trivial yaml
# ---------------------------------------------------------------------------
class TestBuildSqlSnippets:
    """Direct coverage of snippet decomposition from MV yaml."""

    def test_measure_and_filter_from_unapplied_style_mv(self):
        asm = object.__new__(GenieContextAssembler)
        mv = {
            "source_table": "c.s.orders",
            "json_definition": json.dumps({
                "measures": [{"name": "total_amt", "expr": "SUM(amount)"}],
                "filter": "order_date >= '2024-01-01'",
            }),
        }
        col_meta = [{"table_name": "c.s.orders", "column_name": "amount", "data_type": "DECIMAL"}]
        sn = asm._build_sql_snippets([mv], {}, col_meta)
        assert any(m.get("alias") == "total_amt" for m in sn.get("measures", []))
        assert len(sn.get("filters", [])) >= 1
