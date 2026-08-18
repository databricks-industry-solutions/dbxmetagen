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
    _clean_joins_for_yaml,
    _dedup_new_items,
    _measure_semantic_key,
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

    def test_does_not_corrupt_larger_numeric_literal(self):
        # Regression: `* 100.05` previously matched `* 100` and left `.05`, and
        # `* 1000` / `100.5 *` must never be treated as the 100 premultiply.
        for expr in ("SUM(x) * 100.05 / total", "SUM(x) * 1000 / total", "100.5 * SUM(x)"):
            defn = {"measures": [{"name": "r", "expr": expr, "format": {"type": "percentage"}}]}
            _fix_percentage_scaling(defn)
            assert defn["measures"][0]["expr"] == expr, f"corrupted: {expr}"

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


class TestCleanJoinsForYaml:
    """Phase 3: join dicts are whitelisted to spec-valid keys before YAML dump,
    internal keys are stripped, default cardinality is omitted, non-default kept."""

    def test_strips_internal_keys(self):
        joins = [{"name": "cust", "source": "c.s.customers",
                  "on": "source.cid = cust.id",
                  "is_composite": True, "extra_pairs": [{"src": "a", "dst": "b"}],
                  "kind": "join_key", "src_column": "cid"}]
        out = _clean_joins_for_yaml(joins)
        assert out == [{"name": "cust", "source": "c.s.customers", "on": "source.cid = cust.id"}]

    def test_omits_default_cardinality(self):
        joins = [{"name": "c", "source": "c.s.c", "on": "source.x = c.y",
                  "cardinality": "many_to_one"}]
        assert "cardinality" not in _clean_joins_for_yaml(joins)[0]

    def test_keeps_one_to_many_cardinality(self):
        joins = [{"name": "c", "source": "c.s.c", "on": "source.x = c.y",
                  "cardinality": "one_to_many"}]
        assert _clean_joins_for_yaml(joins)[0]["cardinality"] == "one_to_many"

    def test_keeps_using_and_rely(self):
        joins = [{"name": "c", "source": "c.s.c", "using": ["x", "y"], "rely": True}]
        out = _clean_joins_for_yaml(joins)[0]
        assert out["using"] == ["x", "y"] and out["rely"] is True

    def test_recurses_nested_and_strips(self):
        joins = [{"name": "c", "source": "c.s.c", "on": "source.x = c.y",
                  "is_composite": False,
                  "joins": [{"name": "g", "source": "c.s.g", "on": "c.gid = g.id", "kind": "x"}]}]
        out = _clean_joins_for_yaml(joins)[0]
        assert "is_composite" not in out
        assert out["joins"][0] == {"name": "g", "source": "c.s.g", "on": "c.gid = g.id"}

    def test_composite_on_survives_yaml(self):
        # A multi-column composite ON must pass through _definition_to_yaml intact.
        defn = {"source": "c.s.orders", "measures": [{"name": "M", "expr": "SUM(x)"}],
                "joins": [{"name": "lines", "source": "c.s.lines",
                           "on": "source.order_id = lines.order_id AND source.line_no = lines.line_no",
                           "is_composite": True}]}
        y = _definition_to_yaml(defn)
        assert "source.order_id = lines.order_id AND source.line_no = lines.line_no" in y
        assert "is_composite" not in y   # internal key stripped


class TestJoinConditionHelpers:
    """Phase 3 fix: composite conditions are parsed + re-rendered per direction,
    not string-replaced (which broke reverse-direction walks)."""

    def test_parse_child_on_left(self):
        from dbxmetagen.metric_view_core import _parse_join_condition
        pairs = _parse_join_condition(
            "source.order_id = lines.order_id AND source.line_no = lines.line_no", "source")
        assert pairs == [("order_id", "order_id"), ("line_no", "line_no")]

    def test_parse_child_on_right_normalizes(self):
        from dbxmetagen.metric_view_core import _parse_join_condition
        # child qualifier appears on the RHS of a term -> still child-first.
        pairs = _parse_join_condition("lines.a = source.x AND source.y = lines.b", "source")
        assert pairs == [("x", "a"), ("y", "b")]

    def test_parse_returns_empty_when_child_absent(self):
        from dbxmetagen.metric_view_core import _parse_join_condition
        assert _parse_join_condition("a.x = b.y", "source") == []

    def test_render_orientation(self):
        from dbxmetagen.metric_view_core import _render_join_condition
        pairs = [("order_id", "order_id"), ("line_no", "line_no")]
        # child=source, parent=lines (view sourced from the fact)
        assert _render_join_condition(pairs, "source", "lines") == \
            "source.order_id = lines.order_id AND source.line_no = lines.line_no"
        # reversed: child=orders (joined), parent=source (view sourced from parent)
        assert _render_join_condition(pairs, "orders", "source") == \
            "orders.order_id = source.order_id AND orders.line_no = source.line_no"


class TestCleanJoinsDropsUnjoinable:
    def test_drops_join_without_on_or_using(self):
        joins = [{"name": "x", "source": "c.s.x"},
                 {"name": "y", "source": "c.s.y", "on": "source.a = y.b"}]
        out = _clean_joins_for_yaml(joins)
        assert [j["name"] for j in out] == ["y"]

    def test_keeps_using_only_join(self):
        joins = [{"name": "y", "source": "c.s.y", "using": ["a", "b"]}]
        assert len(_clean_joins_for_yaml(joins)) == 1


class TestMeasureSemanticKey:
    def test_none_when_no_aggregate(self):
        assert _measure_semantic_key("o.amount") is None

    def test_whitespace_case_normalized(self):
        assert _measure_semantic_key("SUM(o.amount)") == _measure_semantic_key("sum( o.amount )")

    def test_sum_vs_avg_distinct(self):
        assert _measure_semantic_key("SUM(o.amount)") != _measure_semantic_key("AVG(o.amount)")

    def test_count_vs_count_distinct(self):
        assert _measure_semantic_key("COUNT(o.id)") != _measure_semantic_key("COUNT(DISTINCT o.id)")

    def test_conditional_aggregate_distinct_from_plain(self):
        plain = _measure_semantic_key("SUM(o.amount)")
        cond = _measure_semantic_key("SUM(o.amount) FILTER (WHERE o.status = 'returned')")
        assert plain != cond

    def test_ratios_sharing_numerator_are_distinct(self):
        # Same leading aggregate (SUM(o.revenue)) but different denominators -> the key
        # must reflect ALL aggregates, not just the first, so these stay distinct.
        rev_per_order = _measure_semantic_key("SUM(o.revenue) / NULLIF(SUM(o.orders), 0)")
        rev_per_cust = _measure_semantic_key("SUM(o.revenue) / NULLIF(SUM(o.customers), 0)")
        assert rev_per_order != rev_per_cust

    def test_ratio_reworded_still_collapses(self):
        # Genuinely-identical ratio, only whitespace differs -> same key (still dedups).
        a = _measure_semantic_key("SUM(o.revenue) / NULLIF(SUM(o.orders), 0)")
        b = _measure_semantic_key("SUM( o.revenue )/NULLIF( SUM( o.orders ), 0 )")
        assert a == b

    def test_plain_aggregate_on_case_named_column_not_conditional(self):
        # A column literally named case_amount must NOT be flagged as a CASE conditional
        # (word-boundary match), so it stays distinct from a real FILTER conditional.
        plain = _measure_semantic_key("SUM(o.case_amount)")
        cond = _measure_semantic_key("SUM(o.case_amount) FILTER (WHERE o.status = 'open')")
        assert plain != cond

    def test_filter_predicates_differ_are_distinct(self):
        # Two FILTER aggregates over the same column with different predicates -> distinct.
        a = _measure_semantic_key("SUM(o.amount) FILTER (WHERE o.status = 'open')")
        b = _measure_semantic_key("SUM(o.amount) FILTER (WHERE o.status = 'closed')")
        assert a != b


class TestDedupNewItems:
    def test_exact_expr_duplicate_skipped(self):
        existing = [{"name": "total", "expr": "SUM(o.amount)"}]
        cands = [{"name": "total2", "expr": "SUM( o.amount )"}]
        acc, skip = _dedup_new_items(existing, cands, "measures")
        assert acc == []
        assert skip == ["total2"]

    def test_name_collision_skipped(self):
        existing = [{"name": "total_amount", "expr": "SUM(o.amount)"}]
        cands = [{"name": "total_amount", "expr": "SUM(o.total)"}]
        acc, skip = _dedup_new_items(existing, cands, "measures")
        assert acc == []
        assert "total_amount" in skip

    def test_semantic_duplicate_skipped(self):
        # Different name + reworded, but same (agg, column) -> collapsed.
        existing = [{"name": "revenue", "expr": "SUM(o.amount)"}]
        cands = [{"name": "gross_sales", "expr": "SUM(o.amount)  "}]
        acc, skip = _dedup_new_items(existing, cands, "measures")
        assert acc == []

    def test_distinct_aggregates_kept(self):
        existing = [{"name": "revenue", "expr": "SUM(o.amount)"}]
        cands = [
            {"name": "avg_amount", "expr": "AVG(o.amount)"},
            {"name": "distinct_cust", "expr": "COUNT(DISTINCT o.customer_id)"},
        ]
        acc, skip = _dedup_new_items(existing, cands, "measures")
        assert {a["name"] for a in acc} == {"avg_amount", "distinct_cust"}
        assert skip == []

    def test_conditional_aggregate_over_same_column_kept(self):
        existing = [{"name": "revenue", "expr": "SUM(o.amount)"}]
        cands = [{"name": "returned_rev",
                  "expr": "SUM(o.amount) FILTER (WHERE o.status = 'returned')"}]
        acc, skip = _dedup_new_items(existing, cands, "measures")
        assert [a["name"] for a in acc] == ["returned_rev"]

    def test_ratio_with_shared_numerator_kept(self):
        # Regression: distinct ratio measures sharing a numerator must not collapse.
        existing = [{"name": "rev_per_order",
                     "expr": "SUM(o.revenue) / NULLIF(SUM(o.orders), 0)"}]
        cands = [{"name": "rev_per_customer",
                  "expr": "SUM(o.revenue) / NULLIF(SUM(o.customers), 0)"}]
        acc, skip = _dedup_new_items(existing, cands, "measures")
        assert [a["name"] for a in acc] == ["rev_per_customer"]
        assert skip == []

    def test_case_named_column_conditional_variant_kept(self):
        # Regression: plain SUM(o.case_amount) must not be treated as conditional, so a
        # real FILTER variant over the same column is not falsely dropped.
        existing = [{"name": "case_total", "expr": "SUM(o.case_amount)"}]
        cands = [{"name": "open_case_total",
                  "expr": "SUM(o.case_amount) FILTER (WHERE o.status = 'open')"}]
        acc, skip = _dedup_new_items(existing, cands, "measures")
        assert [a["name"] for a in acc] == ["open_case_total"]
        assert skip == []

    def test_filter_variants_with_different_predicates_kept(self):
        existing = [{"name": "open_rev",
                     "expr": "SUM(o.amount) FILTER (WHERE o.status = 'open')"}]
        cands = [{"name": "closed_rev",
                  "expr": "SUM(o.amount) FILTER (WHERE o.status = 'closed')"}]
        acc, skip = _dedup_new_items(existing, cands, "measures")
        assert [a["name"] for a in acc] == ["closed_rev"]
        assert skip == []

    def test_candidate_vs_candidate_dedup(self):
        acc, skip = _dedup_new_items(
            [], [{"name": "a", "expr": "SUM(o.amount)"}, {"name": "b", "expr": "SUM( o.amount )"}],
            "measures",
        )
        assert len(acc) == 1

    def test_empty_expr_candidate_dropped(self):
        acc, skip = _dedup_new_items([], [{"name": "x", "expr": "  "}], "measures")
        assert acc == []

    def test_dimensions_ignore_semantic_key(self):
        # Same base column, different DATE_TRUNC bucket -> both kept (dims dedup on
        # exact-expr + name only, no aggregate semantic key).
        existing = [{"name": "order_day", "expr": "DATE_TRUNC('DAY', o.order_date)"}]
        cands = [{"name": "order_month", "expr": "DATE_TRUNC('MONTH', o.order_date)"}]
        acc, skip = _dedup_new_items(existing, cands, "dimensions")
        assert [a["name"] for a in acc] == ["order_month"]

    def test_dimensions_exact_duplicate_skipped(self):
        existing = [{"name": "region", "expr": "o.region"}]
        cands = [{"name": "region2", "expr": "o.region"}]
        acc, skip = _dedup_new_items(existing, cands, "dimensions")
        assert acc == []
