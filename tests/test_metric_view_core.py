"""Tests for the shared metric_view_core module.

These pin the behavior of the drift-free helpers extracted from both the library
generator and the app backend (Phase 0a of the semantic-layer consolidation).
The module must import with only stdlib + re (no pyspark), so these tests import
it directly rather than through dbxmetagen.semantic_layer.
"""

import pytest

from dbxmetagen.metric_view_core import (
    _infer_display_name,
    _infer_synonyms,
    _backfill_agent_metadata,
    _drop_broken_measures,
    _drop_placeholder_dimensions,
    _normalize_window_specs,
    _strip_kpi_references,
    _infer_format_specs,
    _fix_percentage_scaling,
    _autofix_expr,
    _normalize_joins,
    _restructure_chained_to_nested,
    _qualify_nested_refs,
    _definition_to_yaml,
)


class TestInferDisplayName:
    def test_snake_case(self):
        assert _infer_display_name("total_revenue") == "Total Revenue"

    def test_kebab_case(self):
        assert _infer_display_name("gross-margin") == "Gross Margin"

    def test_mixed(self):
        assert _infer_display_name("avg_order-value") == "Avg Order Value"

    def test_empty(self):
        assert _infer_display_name("") == ""


class TestInferSynonyms:
    def test_abbreviation_from_initials(self):
        # "monthly recurring revenue" -> MRR
        assert "MRR" in _infer_synonyms("monthly_recurring_revenue", None)

    def test_stopwords_excluded_from_abbr(self):
        # "cost of goods" -> CG (of is a stopword)
        syns = _infer_synonyms("cost_of_goods", None)
        assert "CG" in syns

    def test_single_word_no_abbreviation(self):
        # one word can't form a >=2-char abbreviation from initials
        assert _infer_synonyms("revenue", None) == []

    def test_comment_keywords_added(self):
        syns = _infer_synonyms("mrr", "Recurring subscription income")
        assert any(len(s) > 3 for s in syns)

    def test_capped_at_five(self):
        syns = _infer_synonyms(
            "a_b_c", "alpha beta gamma delta epsilon zeta eta theta"
        )
        assert len(syns) <= 5


class TestBackfillAgentMetadata:
    def test_backfills_missing(self):
        defn = {"measures": [{"name": "total_sales"}], "dimensions": [{"name": "region_code"}]}
        _backfill_agent_metadata(defn)
        assert defn["measures"][0]["display_name"] == "Total Sales"
        assert isinstance(defn["measures"][0]["synonyms"], list)
        assert defn["dimensions"][0]["display_name"] == "Region Code"

    def test_preserves_existing(self):
        defn = {"measures": [{"name": "x", "display_name": "Custom", "synonyms": ["Y"]}], "dimensions": []}
        _backfill_agent_metadata(defn)
        assert defn["measures"][0]["display_name"] == "Custom"
        assert defn["measures"][0]["synonyms"] == ["Y"]


class TestDropBrokenMeasures:
    def test_drops_self_dividing(self):
        defn = {"measures": [{"name": "share", "expr": "SUM(x) / NULLIF(SUM(x), 0)"}]}
        _drop_broken_measures(defn)
        assert defn["measures"] == []

    def test_dedups_identical_exprs(self):
        defn = {"measures": [
            {"name": "a", "expr": "SUM(revenue)"},
            {"name": "b", "expr": "SUM(revenue)"},
        ]}
        _drop_broken_measures(defn)
        assert len(defn["measures"]) == 1

    def test_keeps_distinct_valid(self):
        defn = {"measures": [
            {"name": "a", "expr": "SUM(revenue)"},
            {"name": "b", "expr": "COUNT(1)"},
        ]}
        _drop_broken_measures(defn)
        assert len(defn["measures"]) == 2

    def test_empty_is_noop(self):
        defn = {}
        _drop_broken_measures(defn)
        assert defn == {}


class TestDropPlaceholderDimensions:
    def test_drops_placeholder(self):
        # dim name implies the 'territory' join alias, but expr uses 'source'
        defn = {
            "joins": [{"name": "territory"}],
            "dimensions": [{"name": "territory_code", "expr": "source.prescription_id"}],
        }
        _drop_placeholder_dimensions(defn)
        assert defn["dimensions"] == []

    def test_keeps_matching_alias(self):
        defn = {
            "joins": [{"name": "territory"}],
            "dimensions": [{"name": "territory_code", "expr": "territory.code"}],
        }
        _drop_placeholder_dimensions(defn)
        assert len(defn["dimensions"]) == 1

    def test_no_joins_is_noop(self):
        defn = {"dimensions": [{"name": "anything", "expr": "source.x"}]}
        _drop_placeholder_dimensions(defn)
        assert len(defn["dimensions"]) == 1


class TestUnifiedAutofixExpr:
    """The reconciled _autofix_expr: library ordering + library-only fixers +
    app's correct additions, with the app's buggy bare-interval DATE_TRUNC loop
    deliberately dropped."""

    # --- do no harm: correct expressions must survive unchanged ---
    @pytest.mark.parametrize("expr", [
        "SUM(revenue)",
        "SUM(amount) FILTER (WHERE status = 'fulfilled')",
        "SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END)",
        "SUM(a) / NULLIF(COUNT(*), 0)",
        "SUM(x) * 1.0 / NULLIF(SUM(y), 0)",
        "DATE_TRUNC('MONTH', order_date)",
        "CONCAT(YEAR(source.date), '-Q', QUARTER(source.date))",
        "SUM(source.amount) OVER (PARTITION BY source.category ORDER BY source.date)",
    ])
    def test_good_expr_unchanged(self, expr):
        assert _autofix_expr(expr) == expr

    # --- library-only fixers (app lacked these) ---
    def test_date_part_to_extract(self):
        assert _autofix_expr("DATE_PART(YEAR, order_date)") == "EXTRACT(YEAR FROM order_date)"

    def test_datediff_to_timestampdiff(self):
        assert _autofix_expr("DATEDIFF(DAY, a, b)") == "TIMESTAMPDIFF(DAY, a, b)"

    def test_null_comparison_rewrite(self):
        assert "IS NOT NULL" in _autofix_expr("SUM(CASE WHEN x != NULL THEN 1 END)")

    # --- app's correct additions (library lacked these) ---
    def test_percentile_cont_within_group(self):
        assert _autofix_expr("PERCENTILE_CONT(0.5, revenue)") == \
            "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue)"

    def test_empty_over_stripped(self):
        assert _autofix_expr("SUM(revenue) OVER ()").strip() == "SUM(revenue)"

    # --- the dropped buggy loop: valid extract functions must NOT be clobbered ---
    def test_month_extract_not_clobbered(self):
        # regression guard: the app's old bare-interval loop turned MONTH(col)
        # into DATE_TRUNC('MONTH', col), silently changing semantics. Must not happen.
        assert _autofix_expr("MONTH(order_date)") == "MONTH(order_date)"

    def test_year_extract_not_clobbered(self):
        assert _autofix_expr("YEAR(dt)") == "YEAR(dt)"

    # --- malformed DATE_TRUNC still gets quoted (shared fix, must survive) ---
    def test_unquoted_date_trunc_still_fixed(self):
        assert "DATE_TRUNC('MONTH'" in _autofix_expr("DATE_TRUNC(MONTH, source.order_date)")

    def test_unquoted_literal_quoted(self):
        result = _autofix_expr("COUNT(CASE WHEN x = Active THEN 1 END)")
        assert "= 'Active'" in result


class TestInferFormatSpecs:
    def test_percentage_from_fraction_expr(self):
        defn = {"measures": [{"name": "rate", "expr": "SUM(a) * 1.0 / NULLIF(COUNT(*), 0)"}]}
        _infer_format_specs(defn)
        assert defn["measures"][0]["format"]["type"] == "percentage"

    def test_percentage_from_name(self):
        # Name match uses \brate\b etc. -- matches space-separated display names.
        defn = {"measures": [{"name": "Conversion Rate", "expr": "AVG(x)"}]}
        _infer_format_specs(defn)
        assert defn["measures"][0]["format"]["type"] == "percentage"

    def test_name_match_does_not_fire_on_snake_case(self):
        # KNOWN behavior (both library + app): \brate\b does NOT match
        # "conversion_rate" (underscore is a word char, no boundary). Documented
        # here so a future regex change is a deliberate decision, not accidental.
        defn = {"measures": [{"name": "conversion_rate", "expr": "AVG(x)"}]}
        _infer_format_specs(defn)
        assert defn["measures"][0]["format"]["type"] == "number"

    def test_currency_from_expr(self):
        defn = {"measures": [{"name": "total", "expr": "SUM(revenue)"}]}
        _infer_format_specs(defn)
        assert defn["measures"][0]["format"] == {"type": "currency", "currency_code": "USD"}

    def test_number_fallback(self):
        defn = {"measures": [{"name": "cnt", "expr": "COUNT(1)"}]}
        _infer_format_specs(defn)
        assert defn["measures"][0]["format"]["type"] == "number"

    def test_currency_code_backfilled(self):
        defn = {"measures": [{"name": "x", "expr": "SUM(y)", "format": {"type": "currency"}}]}
        _infer_format_specs(defn)
        assert defn["measures"][0]["format"]["currency_code"] == "USD"

    def test_existing_format_preserved(self):
        defn = {"measures": [{"name": "x", "expr": "SUM(y)", "format": {"type": "number"}}]}
        _infer_format_specs(defn)
        assert defn["measures"][0]["format"]["type"] == "number"


class TestFixPercentageScaling:
    """Metric-view percentage format expects a 0-1 fraction; strip stray 100x
    (both orderings) so the renderer's x100 does not double-scale."""

    def test_strips_leading_100(self):
        defn = {"measures": [{"name": "r", "expr": "100.0 * SUM(a) / NULLIF(COUNT(*), 0)", "format": {"type": "percentage"}}]}
        _fix_percentage_scaling(defn)
        assert defn["measures"][0]["expr"] == "SUM(a) / NULLIF(COUNT(*), 0)"

    def test_strips_trailing_100(self):
        defn = {"measures": [{"name": "r", "expr": "SUM(a) * 100.0 / NULLIF(COUNT(*), 0)", "format": {"type": "percentage"}}]}
        _fix_percentage_scaling(defn)
        assert defn["measures"][0]["expr"] == "SUM(a) / NULLIF(COUNT(*), 0)"

    def test_strips_round_wrapped_leading(self):
        defn = {"measures": [{"name": "r", "expr": "ROUND(100.0 * SUM(a) / NULLIF(COUNT(*), 0), 2)", "format": {"type": "percentage"}}]}
        _fix_percentage_scaling(defn)
        assert defn["measures"][0]["expr"] == "SUM(a) / NULLIF(COUNT(*), 0)"

    def test_leaves_correct_fraction_untouched(self):
        expr = "SUM(a) * 1.0 / NULLIF(COUNT(*), 0)"
        defn = {"measures": [{"name": "r", "expr": expr, "format": {"type": "percentage"}}]}
        _fix_percentage_scaling(defn)
        assert defn["measures"][0]["expr"] == expr

    def test_ignores_non_percentage_measures(self):
        expr = "100.0 * SUM(a) / NULLIF(COUNT(*), 0)"
        defn = {"measures": [{"name": "r", "expr": expr, "format": {"type": "number"}}]}
        _fix_percentage_scaling(defn)
        assert defn["measures"][0]["expr"] == expr

    def test_end_to_end_fraction_contract(self):
        # infer marks it percentage, fix ensures it's a fraction (0-1), not pre-scaled
        defn = {"measures": [{"name": "win_rate", "expr": "SUM(won) * 100.0 / NULLIF(COUNT(*), 0)"}]}
        _infer_format_specs(defn)
        _fix_percentage_scaling(defn)
        assert defn["measures"][0]["format"]["type"] == "percentage"
        assert "100" not in defn["measures"][0]["expr"]


class TestStripKpiReferences:
    """Reconciled (app-canonical superset) KPI-reference stripping."""

    def test_strips_implements_kpi_number(self):
        defn = {"comment": "Total sales. Implements KPI 3."}
        _strip_kpi_references(defn)
        assert defn["comment"] == "Total sales."

    def test_strips_kpi_hash_form(self):
        # app-only superset form: "KPI #3"
        defn = {"comment": "Revenue KPI #3."}
        _strip_kpi_references(defn)
        assert "#3" not in defn["comment"]

    def test_strips_paren_hash_form(self):
        # app-only superset form: "(#3)"
        defn = {"comment": "Gross margin (#3)."}
        _strip_kpi_references(defn)
        assert "#3" not in defn["comment"]

    def test_strips_paren_kpi_colon_form(self):
        defn = {"comment": "Churn rate (KPI: retention)."}
        _strip_kpi_references(defn)
        assert "KPI" not in defn["comment"]

    def test_collapses_double_spaces(self):
        # whitespace-collapse pass is app-canonical behavior
        defn = {"measures": [{"comment": "Net  revenue Supports KPI 1."}], "dimensions": []}
        _strip_kpi_references(defn)
        assert "  " not in defn["measures"][0]["comment"]

    def test_empty_comment_untouched(self):
        defn = {"comment": ""}
        _strip_kpi_references(defn)
        assert defn["comment"] == ""

    def test_measures_and_dimensions(self):
        defn = {
            "measures": [{"comment": "Sum. Implements KPI 2."}],
            "dimensions": [{"comment": "Region. Addresses question 5."}],
        }
        _strip_kpi_references(defn)
        assert "KPI" not in defn["measures"][0]["comment"]
        assert "question" not in defn["dimensions"][0]["comment"].lower()


class TestNormalizeWindowSpecs:
    def test_none_returns_empty(self):
        assert _normalize_window_specs(None) == []

    def test_dict_wrapped_to_list(self):
        out = _normalize_window_specs({"order": "date"})
        assert out == [{"order": "date", "semiadditive": "last"}]

    def test_interval_range_normalized(self):
        out = _normalize_window_specs({"order": "date", "range": "INTERVAL 7 DAYS"})
        assert out[0]["range"] == "trailing 7 day"

    def test_unbounded_rows(self):
        out = _normalize_window_specs({"order": "date", "rows": "UNBOUNDED PRECEDING"})
        assert out[0]["range"] == "unbounded"
        assert "rows" not in out[0]

    def test_spec_without_order_skipped(self):
        assert _normalize_window_specs([{"range": "INTERVAL 1 DAY"}]) == []

    def test_semiadditive_preserved(self):
        out = _normalize_window_specs({"order": "date", "semiadditive": "first"})
        assert out[0]["semiadditive"] == "first"


class TestNormalizeJoins:
    """Library-canonical (recursive) join normalization. The app copy only
    rewrote the top level; the shared version rewrites parent short-name ->
    alias at every nesting level."""

    def test_flat_join_source_ref(self):
        defn = {"source": "c.s.orders", "joins": [
            {"name": "cust", "source": "c.s.customers", "on": "orders.customer_id = cust.id"}
        ]}
        _normalize_joins(defn)
        assert defn["joins"][0]["on"] == "source.customer_id = cust.id"

    def test_nested_join_parent_ref_uses_alias(self):
        # regression guard (Bug #3): nested 'on' referencing parent by TABLE name
        # (customers.geo_id) must be rewritten to the parent ALIAS (cust.geo_id).
        defn = {"source": "c.s.orders", "joins": [
            {"name": "cust", "source": "c.s.customers", "on": "orders.customer_id = cust.id",
             "joins": [{"name": "geo", "source": "c.s.geo", "on": "customers.geo_id = geo.id"}]}
        ]}
        _normalize_joins(defn)
        assert defn["joins"][0]["joins"][0]["on"] == "cust.geo_id = geo.id"

    def test_returns_defn(self):
        defn = {"source": "c.s.t", "joins": [{"name": "a", "source": "c.s.a", "on": "t.x = a.y"}]}
        assert _normalize_joins(defn) is defn

    def test_no_joins_noop(self):
        defn = {"source": "c.s.t"}
        assert _normalize_joins(defn) == {"source": "c.s.t"}


class TestRestructureChainedToNested:
    def test_chained_join_nested_under_parent(self):
        defn = {"source": "c.s.f", "joins": [
            {"name": "a", "source": "c.s.a", "on": "source.a_id = a.id"},
            {"name": "b", "source": "c.s.b", "on": "a.b_id = b.id"},
        ]}
        _restructure_chained_to_nested(defn)
        assert len(defn["joins"]) == 1
        assert defn["joins"][0]["name"] == "a"
        assert defn["joins"][0]["joins"][0]["name"] == "b"

    def test_all_root_joins_unchanged(self):
        defn = {"source": "c.s.f", "joins": [
            {"name": "a", "source": "c.s.a", "on": "source.a_id = a.id"},
            {"name": "b", "source": "c.s.b", "on": "source.b_id = b.id"},
        ]}
        _restructure_chained_to_nested(defn)
        assert len(defn["joins"]) == 2


class TestQualifyNestedRefs:
    def test_nested_alias_gets_dotpath(self):
        defn = {
            "source": "c.s.f",
            "joins": [{"name": "a", "source": "c.s.a", "on": "source.a_id = a.id",
                       "joins": [{"name": "b", "source": "c.s.b", "on": "a.b_id = b.id"}]}],
            "measures": [{"name": "m", "expr": "SUM(b.value)"}],
        }
        _qualify_nested_refs(defn)
        assert defn["measures"][0]["expr"] == "SUM(a.b.value)"

    def test_top_level_alias_unchanged(self):
        defn = {
            "source": "c.s.f",
            "joins": [{"name": "a", "source": "c.s.a", "on": "source.a_id = a.id"}],
            "measures": [{"name": "m", "expr": "SUM(a.value)"}],
        }
        _qualify_nested_refs(defn)
        assert defn["measures"][0]["expr"] == "SUM(a.value)"


class TestDefinitionToYaml:
    """Pure serialization (caller runs the join pipeline first)."""

    def test_basic_serialization(self):
        defn = {"source": "c.s.orders",
                "measures": [{"name": "Total", "expr": "SUM(amount)"}],
                "dimensions": [{"name": "Region", "expr": "region"}]}
        y = _definition_to_yaml(defn)
        assert "version:" in y and "1.1" in y
        assert "source: c.s.orders" in y
        assert "Total" in y and "Region" in y

    def test_missing_source_raises(self):
        with pytest.raises(ValueError):
            _definition_to_yaml({"measures": []})

    def test_materialization_excluded_by_default(self):
        defn = {"source": "c.s.t", "measures": [], "materialization": {"kind": "x"}}
        assert "materialization" not in _definition_to_yaml(defn)
        assert "materialization" in _definition_to_yaml(defn, include_materialization=True)

    def test_currency_code_backfilled_in_yaml(self):
        defn = {"source": "c.s.t", "measures": [
            {"name": "Rev", "expr": "SUM(x)", "format": {"type": "currency"}}]}
        assert "currency_code" in _definition_to_yaml(defn)

    def test_window_json_string_parsed(self):
        import json as _json
        defn = {"source": "c.s.t", "measures": [
            {"name": "M", "expr": "SUM(x)", "window": _json.dumps([{"order": "dt"}])}]}
        y = _definition_to_yaml(defn)
        assert "window" in y
