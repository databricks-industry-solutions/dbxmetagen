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

# mlflow is stubbed as a MagicMock, so the agent's @trace decorator would wrap
# run_genie_agent into a MagicMock (uncallable as the real fn). Make mlflow.trace
# a pass-through decorator so the real function is exercised. (conftest does the
# same globally; repeated here because this file stubs mlflow independently and
# may be collected/run on its own.)
sys.modules["mlflow"].trace = lambda *a, **k: (lambda fn: fn)

# Now safe to import
sys.path.insert(0, "src")
from dbxmetagen.genie import agent as agent_mod  # noqa: E402
from dbxmetagen.genie.agent import (  # noqa: E402
    _merge_prebuilt_join_specs, _validate_output, run_genie_agent,
    _sql_skeleton, _dedup_example_sql,
)
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

    def test_empty_selected_skips_all_joins(self):
        """When table_identifiers is empty (MV-only room), no joins should be emitted."""
        mv = {
            "source_table": "federated_cat.analytics_dwh.trainprogram",
            "json_definition": json.dumps({
                "joins": [{"name": "project", "source": "federated_cat.analytics_dwh.project",
                           "on": "source.project_id = project.id"}]
            }),
        }
        specs = self._asm()._extract_mv_join_specs([mv], set(), set())
        assert len(specs) == 0

    def test_none_selected_skips_all_joins(self):
        """None for selected_short_names should also skip all joins."""
        mv = {
            "source_table": "cat.sch.orders",
            "json_definition": json.dumps({
                "joins": [{"name": "c", "source": "cat.sch.c", "on": "source.id = c.id"}]
            }),
        }
        specs = self._asm()._extract_mv_join_specs([mv], set(), None)
        assert len(specs) == 0

    def test_left_side_not_in_selected_skips(self):
        """If the MV's source_table is not in selected tables, skip its joins."""
        mv = {
            "source_table": "other_cat.other_sch.unselected_table",
            "json_definition": json.dumps({
                "joins": [{"name": "customers", "source": "cat.sch.customers",
                           "on": "source.cust_id = customers.id"}]
            }),
        }
        specs = self._asm()._extract_mv_join_specs([mv], set(), {"customers"})
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
        # Applied MV measures are NOT emitted as snippets -- Genie auto-discovers them
        assert "measure_from_applied" not in aliases

    def test_all_applied_no_measures_in_snippets(self):
        """Applied MVs in hybrid rooms should NOT produce measure snippets."""
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
        # Applied MV measures NOT emitted -- Genie auto-discovers them
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


# ---------------------------------------------------------------------------
# _build_applied_mv_snippets
# ---------------------------------------------------------------------------
class TestBuildAppliedMvSnippets:
    def test_no_measures_emitted_filter_extracted(self):
        """Applied MV measures are auto-discovered by Genie -- no snippets needed."""
        asm = object.__new__(GenieContextAssembler)
        mv = {
            "metric_view_name": "mv_sales",
            "json_definition": json.dumps({
                "measures": [{"name": "total_revenue", "expr": "SUM(revenue)", "comment": "Total rev"}],
                "dimensions": [{"name": "region", "expr": "region"}],
                "filter": "status = 'active'",
            }),
        }
        sn = asm._build_applied_mv_snippets([mv])
        assert len(sn["measures"]) == 0
        assert len(sn["filters"]) == 1
        assert "active" in sn["filters"][0]["sql"][0]

    def test_dimension_expressions_skipped_for_applied_mvs(self):
        """Applied MV dimensions are already columns -- no expression snippets needed."""
        asm = object.__new__(GenieContextAssembler)
        mv = {
            "metric_view_name": "mv_time",
            "json_definition": json.dumps({
                "measures": [],
                "dimensions": [{"name": "year", "expr": "YEAR(order_date)", "comment": "Order year"}],
            }),
        }
        sn = asm._build_applied_mv_snippets([mv])
        assert len(sn["expressions"]) == 0

    def test_dimension_same_as_name_skipped(self):
        asm = object.__new__(GenieContextAssembler)
        mv = {
            "metric_view_name": "mv_x",
            "json_definition": json.dumps({
                "measures": [{"name": "cnt", "expr": "COUNT(*)"}],
                "dimensions": [{"name": "region", "expr": "region"}],
            }),
        }
        sn = asm._build_applied_mv_snippets([mv])
        assert len(sn["expressions"]) == 0

    def test_no_measures_even_with_column_key(self):
        """Applied MV measures are never emitted as snippets regardless of key format."""
        asm = object.__new__(GenieContextAssembler)
        mv = {
            "metric_view_name": "mv_fb",
            "json_definition": json.dumps({
                "measures": [{"column": "amount", "display_name": "Amount"}],
                "dimensions": [],
            }),
        }
        sn = asm._build_applied_mv_snippets([mv])
        assert len(sn["measures"]) == 0




class TestBuildJoinSpecsComposite:
    """Phase 3: FK rows with a composite join_condition emit the full multi-column
    predicate (source. rewritten to the real short table name); single-column rows
    keep the simple equality."""

    def _asm(self):
        return GenieContextAssembler(MagicMock(), "wh", "c", "s")

    def test_single_column_spec(self):
        asm = self._asm()
        specs = asm._build_join_specs([{
            "src_table": "c.s.orders", "dst_table": "c.s.customers",
            "src_column": "c.s.orders.cust_id", "dst_column": "c.s.customers.id",
            "is_composite": False, "join_condition": None,
        }])
        assert specs[0]["sql"] == ["orders.cust_id = customers.id"]

    def test_composite_spec_uses_condition(self):
        asm = self._asm()
        specs = asm._build_join_specs([{
            "src_table": "c.s.orders", "dst_table": "c.s.lines",
            "src_column": "c.s.orders.order_id", "dst_column": "c.s.lines.order_id",
            "is_composite": True,
            "join_condition": "source.order_id = lines.order_id AND source.line_no = lines.line_no",
        }])
        assert specs[0]["sql"] == [
            "orders.order_id = lines.order_id AND orders.line_no = lines.line_no"
        ]
        assert specs[0]["left"]["identifier"] == "c.s.orders"
        assert specs[0]["right"]["identifier"] == "c.s.lines"

    def test_composite_does_not_rewrite_source_substring_in_identifier(self):
        # review finding #7: a bare replace("source.", ...) also rewrites a
        # 'source.' fragment inside a larger alias like 'data_source.'. The
        # word-boundary rewrite must leave data_source.* untouched.
        asm = self._asm()
        specs = asm._build_join_specs([{
            "src_table": "c.s.events", "dst_table": "c.s.dim_source",
            "src_column": "c.s.events.sid", "dst_column": "c.s.dim_source.id",
            "is_composite": True,
            "join_condition": "source.sid = data_source.id AND source.k = data_source.k",
        }])
        # only the standalone 'source.' alias is rewritten to 'events.';
        # 'data_source.' is preserved intact.
        assert specs[0]["sql"] == [
            "events.sid = data_source.id AND events.k = data_source.k"
        ]


# ---------------------------------------------------------------------------
# run_genie_agent refinement path -- regression guard for the mv_only bug.
# The refinement branch referenced mv_only in the shared tail before it was
# assigned (it was only defined in the fresh-generation branch), so Improve
# crashed with UnboundLocalError for every existing space. mv_only is now
# hoisted above the branch; this test drives the refinement path end-to-end
# (LLM + SQL validation stubbed) and asserts it completes without that crash.
# ---------------------------------------------------------------------------
import queue as _queue  # noqa: E402


class TestRunGenieAgentRefinement:
    def _run(self, context):
        q = _queue.Queue()
        prior = {
            "description": "prior",
            "instructions": {"text": "prior text", "join_specs": [], "example_sql": []},
            "sample_questions": ["q1"],
        }
        # Feedback that routes to all phases so every mv_only reference executes.
        feedback = "improve joins, example sql, and measures/filters/expressions"
        # Return content that satisfies whichever phase asks, so phases_completed
        # advances and we exercise the shared tail (where mv_only is referenced).
        def _fake_phase(llm, sys_prompt, user_msg, label):
            return {
                "description": "d", "instructions": {"text": "t"},
                "sample_questions": ["q"], "join_specs": [],
                "example_sql": [{"question": "q?", "sql": "SELECT 1"}],
                "sql_snippets": {"measures": [], "filters": [], "expressions": []},
            }
        with patch.object(agent_mod, "ChatDatabricks", return_value=MagicMock()), \
             patch.object(agent_mod, "_llm_phase", side_effect=_fake_phase), \
             patch.object(agent_mod, "_validate_and_strip_sql", side_effect=lambda s, *a, **k: s):
            return run_genie_agent(
                MagicMock(), "wh", context, q,
                refinement_feedback=feedback, prior_result=prior,
            )

    def test_refinement_table_space_no_unbound_mv_only(self):
        # Table-only space (the common case that crashed).
        ctx = {
            "data_sources": {"tables": [{"identifier": "c.s.orders"}], "metric_views": []},
            "join_specs": [], "sql_snippets": {}, "questions": [],
            "context_text": "test context",
        }
        result = self._run(ctx)
        assert isinstance(result, dict)

    def test_refinement_mv_only_space(self):
        # Metric-view-only space -- exercises the mv_only=True branches in the tail.
        ctx = {
            "data_sources": {"tables": [], "metric_views": [{"identifier": "c.s.mv"}]},
            "join_specs": [], "sql_snippets": {}, "questions": [],
            "context_text": "test context",
        }
        result = self._run(ctx)
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# Item 26: SQL-skeleton dedup, invented-join / truncation warnings, MV-only
# fresh-generation path
# ---------------------------------------------------------------------------

class TestSqlSkeleton:
    def test_different_dim_or_measure_is_DISTINCT(self):
        # Softened dedup: different dims/measures are genuinely different analytical
        # questions and must NOT collapse (revenue-by-region vs units-by-product).
        a = "SELECT region, MEASURE(revenue) FROM c.s.mv GROUP BY ALL ORDER BY MEASURE(revenue) DESC"
        b = "SELECT product, MEASURE(units) FROM c.s.mv GROUP BY ALL ORDER BY MEASURE(units) DESC"
        assert _sql_skeleton(a) != _sql_skeleton(b)

    def test_true_duplicate_collapses(self):
        # Same tables, same shape, same dims+measures; differ only in a LIMIT
        # literal value (cosmetic) -> genuine duplicate.
        a = "SELECT region, MEASURE(revenue) FROM c.s.mv GROUP BY ALL ORDER BY MEASURE(revenue) DESC LIMIT 10"
        b = "SELECT region, MEASURE(revenue) FROM c.s.mv GROUP BY ALL ORDER BY MEASURE(revenue) DESC LIMIT 25"
        assert _sql_skeleton(a) == _sql_skeleton(b)

    def test_alias_and_literal_do_not_matter(self):
        # Cosmetic-only differences (AS-alias, literal values) still collapse.
        a = "SELECT region, SUM(x) FROM c.s.orders WHERE amount > 100 GROUP BY region"
        b = "SELECT region, SUM(x) AS total FROM c.s.orders WHERE amount > 999 GROUP BY region"
        assert _sql_skeleton(a) == _sql_skeleton(b)

    def test_different_shape_is_distinct(self):
        base = "SELECT region, MEASURE(a) FROM c.s.mv GROUP BY ALL ORDER BY MEASURE(a) DESC"
        multi = "SELECT region, MEASURE(a), MEASURE(b) FROM c.s.mv GROUP BY ALL"
        filtered = "SELECT region, MEASURE(a) FROM c.s.mv WHERE region = 'x' GROUP BY ALL"
        assert _sql_skeleton(base) != _sql_skeleton(multi)
        assert _sql_skeleton(base) != _sql_skeleton(filtered)

    def test_same_shape_different_table_is_distinct(self):
        a = "SELECT region, MEASURE(x) FROM c.s.mv1 GROUP BY ALL"
        b = "SELECT region, MEASURE(x) FROM c.s.mv2 GROUP BY ALL"
        assert _sql_skeleton(a) != _sql_skeleton(b)

    def test_non_string_does_not_raise(self):
        assert isinstance(_sql_skeleton(None), str)
        assert isinstance(_sql_skeleton(123), str)


class TestDedupExampleSql:
    def test_keeps_first_drops_later_duplicate(self):
        # a and b are TRUE duplicates (same dims+measures, only the LIMIT value
        # differs); c is a distinct question (different dims/measures) and survives.
        a = "SELECT region, MEASURE(revenue) FROM c.s.mv GROUP BY ALL ORDER BY MEASURE(revenue) DESC LIMIT 10"
        b = "SELECT region, MEASURE(revenue) FROM c.s.mv GROUP BY ALL ORDER BY MEASURE(revenue) DESC LIMIT 25"
        c = "SELECT product, MEASURE(units) FROM c.s.mv GROUP BY ALL ORDER BY MEASURE(units) DESC"
        raw = {"instructions": {"example_sql": [
            {"question": "q1", "sql": a},
            {"question": "q2", "sql": b},
            {"question": "q3", "sql": c},
        ]}}
        raw, removed = _dedup_example_sql(raw)
        assert removed == 1
        kept = raw["instructions"]["example_sql"]
        assert [e["question"] for e in kept] == ["q1", "q3"]

    def test_handles_example_question_sqls_key(self):
        a = "SELECT region, SUM(x) FROM c.s.t GROUP BY region"
        raw = {"instructions": {"example_question_sqls": [
            {"question": "q1", "sql": [a]},
            {"question": "q2", "sql": [a]},
        ]}}
        raw, removed = _dedup_example_sql(raw)
        assert removed == 1
        assert len(raw["instructions"]["example_question_sqls"]) == 1

    def test_no_examples_is_noop(self):
        raw = {"instructions": {}}
        raw, removed = _dedup_example_sql(raw)
        assert removed == 0


class TestInventedJoinWarning:
    def test_join_to_unknown_table_warns(self):
        raw = {
            "data_sources": {"tables": [{"identifier": "c.s.orders"}]},
            "instructions": {"join_specs": [
                {"left": {"identifier": "c.s.orders"}, "right": {"identifier": "c.s.ghost"}},
            ]},
        }
        warnings = _validate_output(raw)
        assert any("not in data_sources" in w and "c.s.ghost" in w for w in warnings)

    def test_all_known_tables_no_invented_warning(self):
        raw = {
            "data_sources": {"tables": [{"identifier": "c.s.orders"}, {"identifier": "c.s.customers"}]},
            "instructions": {"join_specs": [
                {"left": {"identifier": "c.s.orders"}, "right": {"identifier": "c.s.customers"}},
            ]},
        }
        warnings = _validate_output(raw)
        assert not any("not in data_sources" in w for w in warnings)


class TestGenieAgentFreshGeneration:
    """Exercise the fresh-generation (non-refinement) path, incl. MV-only Phase 2."""

    def _run(self, context, phase_payload=None):
        q = _queue.Queue()
        payload = phase_payload or {
            "description": "d", "instructions": {"text": "t"},
            "sample_questions": ["What is revenue by region?"],
            "join_specs": [],
            "example_sql": [{"question": "q?", "sql": "SELECT 1"}],
            "sql_snippets": {"measures": [], "filters": [], "expressions": []},
        }

        def _fake_phase(llm, sys_prompt, user_msg, label):
            return payload

        with patch.object(agent_mod, "ChatDatabricks", return_value=MagicMock()), \
             patch.object(agent_mod, "_llm_phase", side_effect=_fake_phase), \
             patch.object(agent_mod, "_validate_and_strip_sql", side_effect=lambda s, *a, **k: s):
            return run_genie_agent(MagicMock(), "wh", context, q)

    def test_mv_only_fresh_generation(self):
        ctx = {
            "data_sources": {"tables": [], "metric_views": [{"identifier": "c.s.mv"}]},
            "join_specs": [], "sql_snippets": {}, "questions": [],
            "context_text": "ctx",
        }
        result = self._run(ctx)
        assert isinstance(result, dict)
        # Phase 3 must be skipped for MV-only; result should still have instructions.
        assert "instructions" in result

    def test_table_fresh_generation_dedups_examples(self):
        # True duplicates: same tables/shape/dims/measures, differ only in a
        # literal + AS-alias (both cosmetic). Softened dedup still drops one.
        dup = "SELECT region, SUM(x) FROM c.s.orders WHERE amount > 100 GROUP BY region"
        payload = {
            "description": "d", "instructions": {"text": "t"},
            "sample_questions": ["Q"], "join_specs": [],
            "example_sql": [
                {"question": "q1", "sql": dup},
                {"question": "q2", "sql": "SELECT region, SUM(x) AS t FROM c.s.orders WHERE amount > 5 GROUP BY region"},
            ],
            "sql_snippets": {"measures": [], "filters": [], "expressions": []},
        }
        ctx = {
            "data_sources": {"tables": [{"identifier": "c.s.orders"}], "metric_views": []},
            "join_specs": [], "sql_snippets": {}, "questions": [],
            "context_text": "ctx",
        }
        result = self._run(ctx, payload)
        inst = result.get("instructions", {})
        examples = inst.get("example_sql") or inst.get("example_question_sqls") or []
        assert len(examples) == 1  # structural duplicate removed

    def test_truncation_warning_emitted(self):
        long_text = "\n".join(f"line {i}" for i in range(30))
        payload = {
            "description": "d", "instructions": {"text": long_text},
            "sample_questions": ["Q"], "join_specs": [],
            "example_sql": [{"question": "q", "sql": "SELECT 1"}],
            "sql_snippets": {"measures": [], "filters": [], "expressions": []},
        }
        ctx = {
            "data_sources": {"tables": [{"identifier": "c.s.orders"}], "metric_views": []},
            "join_specs": [], "sql_snippets": {}, "questions": [],
            "context_text": "ctx",
        }
        q = _queue.Queue()

        def _fake_phase(llm, sys_prompt, user_msg, label):
            return payload

        with patch.object(agent_mod, "ChatDatabricks", return_value=MagicMock()), \
             patch.object(agent_mod, "_llm_phase", side_effect=_fake_phase), \
             patch.object(agent_mod, "_validate_and_strip_sql", side_effect=lambda s, *a, **k: s):
            run_genie_agent(MagicMock(), "wh", ctx, q)

        events = []
        while not q.empty():
            events.append(q.get())
        done = [e for e in events if e.get("stage") == "done"]
        assert done
        assert any("truncated" in w.lower() for w in done[0].get("warnings", []))


# ---------------------------------------------------------------------------
# _sample_categorical_values -- PQ-3 federation safety (prefer cached samples)
# ---------------------------------------------------------------------------
class TestSampleCategoricalValuesCache:
    """PQ-3: must read cached column_profiling_stats.sample_values, NOT run
    SELECT DISTINCT against the (possibly federated) source."""

    def _asm(self):
        asm = object.__new__(GenieContextAssembler)
        asm.ws = MagicMock()
        asm.wh = "wh"
        asm.catalog = "c"
        asm.schema = "s"
        return asm

    def test_reads_cache_no_source_query(self, monkeypatch):
        from dbxmetagen.genie import context as ctx
        calls = []
        def fake_safe_sql(ws, wh, query, *a, **k):
            calls.append(query)
            if "column_profiling_stats" in query:
                return [{"table_name": "c.s.orders", "column_name": "status",
                         "sample_values": json.dumps(["open", "closed", "open", None, "closed"])}]
            return []  # any source query would land here
        monkeypatch.setattr(ctx, "_safe_sql", fake_safe_sql)
        asm = self._asm()
        cols = [{"table_name": "c.s.orders", "column_name": "status", "data_type": "STRING"}]
        out = asm._sample_categorical_values(cols)
        # got the cached values, locally de-duped, nulls dropped
        assert out["c.s.orders"]["status"] == ["open", "closed"]
        # and NO SELECT DISTINCT was issued against the source
        assert not any("SELECT DISTINCT" in q.upper() for q in calls)
        assert any("column_profiling_stats" in q for q in calls)

    def test_federated_no_source_fallback(self, monkeypatch):
        from dbxmetagen.genie import context as ctx
        monkeypatch.setenv("FEDERATION_MODE", "true")
        calls = []
        def fake_safe_sql(ws, wh, query, *a, **k):
            calls.append(query)
            return []  # cache empty
        monkeypatch.setattr(ctx, "_safe_sql", fake_safe_sql)
        asm = self._asm()
        cols = [{"table_name": "fed.s.orders", "column_name": "status", "data_type": "STRING"}]
        out = asm._sample_categorical_values(cols)
        # cache miss + federated -> NO source read at all
        assert out == {}
        assert not any("FROM `fed`" in q or "FROM fed" in q for q in calls if "profiling_stats" not in q)
