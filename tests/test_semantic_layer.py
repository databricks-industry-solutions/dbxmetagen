"""Tests for semantic_layer module -- expression fixers, JSON parsers, column ref extraction."""

import pytest
import yaml
from unittest.mock import MagicMock
from dbxmetagen.semantic_layer import (
    SemanticLayerGenerator,
    SemanticLayerConfig,
    _normalize_window_specs,
    _infer_format_specs,
    _fix_percentage_scaling,
    check_dim_source_pattern,
    _swap_source_and_join,
    profile_schema,
)


@pytest.fixture
def gen():
    """Minimal SemanticLayerGenerator with mocked Spark."""
    spark = MagicMock()
    config = SemanticLayerConfig(catalog_name="cat", schema_name="sch")
    return SemanticLayerGenerator(spark, config)


# ── Expression Fix Helpers ────────────────────────────────────────────


class TestFixUnquotedLiterals:

    def test_quotes_bare_word_after_equals(self):
        assert "='Active'" in SemanticLayerGenerator._fix_unquoted_literals("=Active THEN")

    def test_leaves_sql_keywords_alone(self):
        result = SemanticLayerGenerator._fix_unquoted_literals("=NULL THEN")
        assert "'NULL'" not in result

    def test_leaves_already_quoted(self):
        expr = "= 'Active' THEN"
        assert SemanticLayerGenerator._fix_unquoted_literals(expr) == expr

    def test_handles_not_equals(self):
        result = SemanticLayerGenerator._fix_unquoted_literals("!=Pending)")
        assert "'Pending'" in result

    def test_quotes_multi_word_value(self):
        result = SemanticLayerGenerator._fix_unquoted_literals("= Requirement change THEN")
        assert "= 'Requirement change' THEN" in result

    def test_quotes_single_word_before_and(self):
        result = SemanticLayerGenerator._fix_unquoted_literals("= Active AND")
        assert "= 'Active' AND" in result

    def test_leaves_column_ref(self):
        expr = "= source.CustomField THEN"
        assert SemanticLayerGenerator._fix_unquoted_literals(expr) == expr

    def test_leaves_numeric(self):
        expr = "= 0 THEN"
        assert SemanticLayerGenerator._fix_unquoted_literals(expr) == expr

    def test_leaves_function_call(self):
        expr = "= NULLIF(x, 0) THEN"
        assert "'" not in SemanticLayerGenerator._fix_unquoted_literals(expr).replace("NULLIF", "")

    def test_full_case_when_expression(self):
        expr = "CASE WHEN source.State = Active AND source.Risk IN (1 - High, 2 - Medium) THEN 1 ELSE 0 END"
        result = SemanticLayerGenerator._fix_unquoted_literals(expr)
        assert "= 'Active' AND" in result


class TestFixBareComparison:

    def test_inserts_empty_string_before_paren(self):
        result = SemanticLayerGenerator._fix_bare_comparison("source.x != )")
        assert "!= ''" in result
        assert ")" in result

    def test_inserts_empty_string_before_comma(self):
        result = SemanticLayerGenerator._fix_bare_comparison("source.x != ,")
        assert "!= ''" in result

    def test_inserts_empty_string_before_and(self):
        result = SemanticLayerGenerator._fix_bare_comparison("source.x != AND source.y = 1")
        assert "!= '' AND" in result

    def test_preserves_existing_quoted_value(self):
        expr = "source.x != 'value')"
        assert SemanticLayerGenerator._fix_bare_comparison(expr) == expr

    def test_preserves_numeric_value(self):
        expr = "source.x != 0)"
        assert SemanticLayerGenerator._fix_bare_comparison(expr) == expr

    def test_full_filter_expression(self):
        bad = "COUNT(DISTINCT source.id) FILTER (WHERE source.col IS NOT NULL AND source.col != )"
        result = SemanticLayerGenerator._fix_bare_comparison(bad)
        assert "!= ''" in result
        assert "!= )" not in result


class TestFixCaseQuoting:

    def test_splits_trapped_keywords(self):
        result = SemanticLayerGenerator._fix_case_quoting("'Medium Cost ELSE Standard Cost END'")
        assert "ELSE" in result
        assert "END" in result
        assert "'Medium Cost'" in result
        assert "'Standard Cost'" in result

    def test_leaves_normal_strings(self):
        expr = "'hello world'"
        assert SemanticLayerGenerator._fix_case_quoting(expr) == expr


class TestFixThenElseLiterals:

    def test_quotes_bare_text_after_then(self):
        result = SemanticLayerGenerator._fix_then_else_literals("THEN Active ELSE Inactive END")
        assert "'Active'" in result

    def test_leaves_numbers_alone(self):
        result = SemanticLayerGenerator._fix_then_else_literals("THEN 42 ELSE 0 END")
        assert "'42'" not in result

    def test_leaves_already_quoted(self):
        expr = "THEN 'Active' ELSE 'Inactive' END"
        assert SemanticLayerGenerator._fix_then_else_literals(expr) == expr


class TestFixInClauseLiterals:

    def test_quotes_bare_words(self):
        result = SemanticLayerGenerator._fix_in_clause_literals("IN (Active, Pending, Closed)")
        assert "'Active'" in result
        assert "'Pending'" in result
        assert "'Closed'" in result

    def test_leaves_numbers(self):
        result = SemanticLayerGenerator._fix_in_clause_literals("IN (1, 2, 3)")
        assert "'1'" not in result

    def test_leaves_already_quoted(self):
        expr = "IN ('Active', 'Pending')"
        assert SemanticLayerGenerator._fix_in_clause_literals(expr) == expr

    def test_quotes_multi_word_tokens(self):
        result = SemanticLayerGenerator._fix_in_clause_literals("IN (1 - High, 2 - Medium)")
        assert "'1 - High'" in result
        assert "'2 - Medium'" in result


class TestFixDatePart:

    def test_rewrites_to_extract(self):
        result = SemanticLayerGenerator._fix_date_part("DATE_PART(YEAR, order_date)")
        assert "EXTRACT(YEAR FROM order_date)" in result

    def test_handles_quoted_unit(self):
        result = SemanticLayerGenerator._fix_date_part("DATE_PART('MONTH', created_at)")
        assert "EXTRACT(MONTH FROM created_at)" in result

    def test_leaves_unknown_unit(self):
        expr = "DATE_PART(foo, col)"
        assert SemanticLayerGenerator._fix_date_part(expr) == expr


class TestFixDatediff:

    def test_rewrites_to_timestampdiff(self):
        result = SemanticLayerGenerator._fix_datediff("DATEDIFF(DAY, start_date, end_date)")
        assert "TIMESTAMPDIFF(DAY" in result

    def test_normalizes_plural(self):
        result = SemanticLayerGenerator._fix_datediff("DATEDIFF(DAYS, start_date, end_date)")
        assert "TIMESTAMPDIFF(DAY" in result


class TestAutofixExpr:

    def test_fixes_unquoted_date_trunc_interval(self):
        result = SemanticLayerGenerator._autofix_expr("DATE_TRUNC(MONTH, order_date)")
        assert "DATE_TRUNC('MONTH'" in result

    def test_leaves_already_quoted_interval(self):
        expr = "DATE_TRUNC('MONTH', order_date)"
        assert SemanticLayerGenerator._autofix_expr(expr) == expr

    def test_composes_multiple_fixes(self):
        expr = "CASE WHEN status =Active THEN High Cost ELSE Low Cost END"
        result = SemanticLayerGenerator._autofix_expr(expr)
        assert "'Active'" in result


class TestAutofixDoesNotCorruptGoodExprs:
    """Run the full _autofix_expr chain on correctly-formed expressions and assert
    they come out UNCHANGED.  This is the 'do no harm' safety net."""

    @pytest.mark.parametrize("expr", [
        # Simple aggregation
        "SUM(source.amount)",
        "COUNT(source.WorkItemId)",
        "AVG(source.price)",
        "COUNT(DISTINCT source.customer_id)",
        # Arithmetic
        "SUM(source.revenue) * 100.0 / NULLIF(SUM(source.cost), 0)",
        "SUM(source.amount) - SUM(source.discount)",
        # CASE WHEN with properly quoted strings
        "SUM(CASE WHEN source.State = 'Active' AND source.Risk IN ('1 - High', '2 - Medium') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(CASE WHEN source.State = 'Active' THEN 1 END), 0)",
        "COUNT(CASE WHEN source.Custom_MilestoneStatus = 'Blocked' THEN 1 END)",
        "COUNT(CASE WHEN source.State = 'Closed' THEN 1 END) * 100.0 / NULLIF(COUNT(source.WorkItemId), 0)",
        "COUNT(CASE WHEN source.Custom_RevisedDueDateInfluencedBy = 'Requirement change' THEN 1 END)",
        # CASE WHEN with numeric comparisons
        "SUM(CASE WHEN source.amount > 1000 THEN 1 ELSE 0 END)",
        "CASE WHEN source.score >= 90 THEN 'A' WHEN source.score >= 80 THEN 'B' ELSE 'C' END",
        # Column refs in comparisons
        "SUM(CASE WHEN source.start_date = source.end_date THEN 1 ELSE 0 END)",
        # IS NULL / IS NOT NULL
        "COUNT(CASE WHEN source.Custom_RevisedDueDate IS NOT NULL THEN 1 END) * 100.0 / NULLIF(COUNT(source.WorkItemId), 0)",
        # COALESCE / NULLIF
        "COALESCE(source.name, 'Unknown')",
        "NULLIF(source.total, 0)",
        # CONCAT with properly quoted separators
        "CONCAT(YEAR(source.date), '-Q', QUARTER(source.date))",
        "CONCAT(source.first_name, ' ', source.last_name)",
        # DATE_TRUNC with properly quoted interval
        "DATE_TRUNC('MONTH', source.created_at)",
        "DATE_TRUNC('QUARTER', source.order_date)",
        # Window functions
        "SUM(source.amount) OVER (PARTITION BY source.category ORDER BY source.date)",
        # LIKE with properly quoted pattern
        "COUNT(CASE WHEN source.name LIKE '%Corp%' THEN 1 END)",
        # IN with properly quoted values
        "SUM(CASE WHEN source.status IN ('Active', 'Pending') THEN source.amount ELSE 0 END)",
        # BETWEEN
        "COUNT(CASE WHEN source.score BETWEEN 80 AND 100 THEN 1 END)",
        # Nested functions
        "ROUND(SUM(source.revenue) / NULLIF(COUNT(DISTINCT source.customer_id), 0), 2)",
        # FILTER clause
        "COUNT(*) FILTER (WHERE source.status = 'Active')",
        # Plain column refs (dimensions)
        "source.ProjectName",
        "proj.ProjectName",
        "source.Custom_MilestoneStatus",
        # CURRENT_DATE without parentheses
        "SUM(CASE WHEN source.expected_close_date < CURRENT_DATE AND source.actual_close_date IS NULL THEN 1 ELSE 0 END)",
        # NULL comparison -- correct forms must not be corrupted
        "source.x IS NOT NULL",
        "COALESCE(source.x, NULL)",
        "NULLIF(source.total, 0)",
        "SUM(CASE WHEN source.flag != 'N' THEN 1 ELSE 0 END)",
    ])
    def test_good_expr_unchanged(self, expr):
        assert SemanticLayerGenerator._autofix_expr(expr) == expr


class TestAutofixFixesBadExprs:
    """The full _autofix_expr chain on BROKEN expressions should produce the
    expected corrections."""

    def test_fixes_multi_word_unquoted_literal(self):
        bad = "COUNT(CASE WHEN source.Custom_RevisedDueDateInfluencedBy = Requirement change THEN 1 END)"
        result = SemanticLayerGenerator._autofix_expr(bad)
        assert "= 'Requirement change' THEN" in result

    def test_fixes_multiple_unquoted_comparisons(self):
        bad = "SUM(CASE WHEN source.State = Active AND source.Risk IN (1 - High, 2 - Medium) THEN 1 ELSE 0 END)"
        result = SemanticLayerGenerator._autofix_expr(bad)
        assert "= 'Active' AND" in result
        assert "'1 - High'" in result
        assert "'2 - Medium'" in result

    def test_fixes_unquoted_date_trunc(self):
        bad = "DATE_TRUNC(MONTH, source.order_date)"
        result = SemanticLayerGenerator._autofix_expr(bad)
        assert "DATE_TRUNC('MONTH'" in result

    def test_fixes_single_word_unquoted(self):
        bad = "COUNT(CASE WHEN source.State = Closed THEN 1 END)"
        result = SemanticLayerGenerator._autofix_expr(bad)
        assert "= 'Closed' THEN" in result

    def test_fixes_closed_won_in_case(self):
        bad = "SUM(CASE WHEN source.stage = Closed Won THEN source.amount_usd ELSE 0 END)"
        result = SemanticLayerGenerator._autofix_expr(bad)
        assert "= 'Closed Won' THEN" in result

    def test_fixes_in_clause_with_closed_won_lost(self):
        bad = "SUM(CASE WHEN source.stage IN (Closed Won, Closed Lost) THEN 1 ELSE 0 END)"
        result = SemanticLayerGenerator._autofix_expr(bad)
        assert "'Closed Won'" in result
        assert "'Closed Lost'" in result

    def test_fixes_win_rate_composite_expression(self):
        bad = (
            "SUM(CASE WHEN source.stage = Closed Won THEN 1 ELSE 0 END) * 100.0 "
            "/ NULLIF(SUM(CASE WHEN source.stage IN (Closed Won, Closed Lost) THEN 1 ELSE 0 END), 0)"
        )
        result = SemanticLayerGenerator._autofix_expr(bad)
        assert "= 'Closed Won' THEN" in result
        assert "'Closed Lost'" in result

    def test_fixes_bare_comparison_in_filter(self):
        bad = "COUNT(DISTINCT source.experiment_id) FILTER (WHERE source.eln_number IS NOT NULL AND source.eln_number != )"
        result = SemanticLayerGenerator._autofix_expr(bad)
        assert "!= ''" in result
        assert "!= )" not in result

    def test_fixes_bare_comparison_in_ratio(self):
        bad = (
            "COUNT(DISTINCT source.id) FILTER (WHERE source.x IS NOT NULL AND source.x != ) "
            "* 100.0 / NULLIF(COUNT(DISTINCT source.id), 0)"
        )
        result = SemanticLayerGenerator._autofix_expr(bad)
        assert "!= ''" in result
        assert "!= )" not in result

    def test_fixes_neq_null(self):
        bad = "SUM(CASE WHEN source.reason <> NULL THEN 1 ELSE 0 END)"
        result = SemanticLayerGenerator._autofix_expr(bad)
        assert "IS NOT NULL" in result
        assert "<> NULL" not in result

    def test_fixes_eq_null(self):
        bad = "SUM(CASE WHEN source.reason = NULL THEN 1 ELSE 0 END)"
        result = SemanticLayerGenerator._autofix_expr(bad)
        assert "IS NULL" in result
        assert "= NULL" not in result

    def test_fixes_excl_eq_null(self):
        bad = "SUM(CASE WHEN source.reason != NULL THEN 1 ELSE 0 END)"
        result = SemanticLayerGenerator._autofix_expr(bad)
        assert "IS NOT NULL" in result
        assert "!= NULL" not in result


class TestFixConcatSeparators:

    def test_quotes_bare_separator(self):
        result = SemanticLayerGenerator._fix_concat_separators("CONCAT(a, -, b)")
        assert "'-'" in result

    def test_quotes_dash_Q(self):
        expr = "CONCAT(YEAR(source.CreatedDate), -Q, QUARTER(source.CreatedDate))"
        result = SemanticLayerGenerator._fix_concat_separators(expr)
        assert "'-Q'" in result

    def test_quotes_dash_FY(self):
        result = SemanticLayerGenerator._fix_concat_separators("CONCAT(a, -FY, b)")
        assert "'-FY'" in result

    def test_preserves_numeric(self):
        expr = "CONCAT(a, -1, b)"
        result = SemanticLayerGenerator._fix_concat_separators(expr)
        assert "'-1'" not in result

    def test_preserves_already_quoted(self):
        expr = "CONCAT(a, '-Q', b)"
        result = SemanticLayerGenerator._fix_concat_separators(expr)
        assert result == expr


class TestFixBareWhitespaceSeparator:

    def test_quotes_bare_double_space(self):
        result = SemanticLayerGenerator._fix_bare_whitespace_separator("CONCAT(a,  , b)")
        assert result == "CONCAT(a, ' ', b)"

    def test_quotes_bare_triple_space(self):
        result = SemanticLayerGenerator._fix_bare_whitespace_separator("CONCAT(a,   , b)")
        assert result == "CONCAT(a, ' ', b)"

    def test_preserves_already_quoted(self):
        expr = "CONCAT(contact.first_name, ' ', contact.last_name)"
        assert SemanticLayerGenerator._fix_bare_whitespace_separator(expr) == expr

    def test_preserves_normal_expression(self):
        expr = "SUM(CASE WHEN source.stage = 'Closed Won' THEN 1 ELSE 0 END)"
        assert SemanticLayerGenerator._fix_bare_whitespace_separator(expr) == expr

    def test_full_autofix_chain_fixes_concat_space(self):
        bad = "CONCAT(contact.first_name,  , contact.last_name)"
        result = SemanticLayerGenerator._autofix_expr(bad)
        assert result == "CONCAT(contact.first_name, ' ', contact.last_name)"

    # -- double-quote identifier to backtick --

    def test_fix_dquote_identifier_converts_dotted(self):
        expr = 'source."assay name"'
        assert SemanticLayerGenerator._fix_dquote_identifier(expr) == "source.`assay name`"

    def test_fix_dquote_identifier_multiple(self):
        expr = 'CONCAT(source."project id", source."assay name")'
        result = SemanticLayerGenerator._fix_dquote_identifier(expr)
        assert result == "CONCAT(source.`project id`, source.`assay name`)"

    def test_fix_dquote_identifier_ignores_bare_string_literal(self):
        expr = """CASE WHEN source.status = "Active" THEN 1 END"""
        result = SemanticLayerGenerator._fix_dquote_identifier(expr)
        assert result == expr

    def test_fix_dquote_identifier_ignores_date_format(self):
        expr = 'DATE_FORMAT(source.created_date, "yyyy-MM")'
        result = SemanticLayerGenerator._fix_dquote_identifier(expr)
        assert result == expr

    def test_fix_dquote_via_autofix_chain(self):
        expr = 'SUM(source."total cost")'
        result = SemanticLayerGenerator._autofix_expr(expr)
        assert "source.`total cost`" in result

    # -- INSTR bare arg --

    def test_fix_instr_bare_semicolon(self):
        expr = "INSTR(source.tested_conditions, ;)"
        result = SemanticLayerGenerator._fix_instr_bare_arg(expr)
        assert result == "INSTR(source.tested_conditions, ';')"

    def test_fix_instr_already_quoted(self):
        expr = "INSTR(source.tested_conditions, ';')"
        result = SemanticLayerGenerator._fix_instr_bare_arg(expr)
        assert result == expr

    def test_fix_instr_ignores_column_arg(self):
        expr = "INSTR(source.col1, source.col2)"
        result = SemanticLayerGenerator._fix_instr_bare_arg(expr)
        assert result == expr

    def test_fix_locate_bare_char(self):
        expr = "LOCATE(-, source.phone)"
        result = SemanticLayerGenerator._fix_instr_bare_arg(expr)
        assert "LOCATE('-'" in result


# ── JSON Parsers ──────────────────────────────────────────────────────


class TestParsePlanResponse:

    def test_extracts_views(self, gen):
        response = '```json\n{"views": [{"name": "mv1"}]}\n```'
        result = gen._parse_plan_response(response)
        assert len(result) == 1
        assert result[0]["name"] == "mv1"

    def test_returns_empty_on_garbage(self, gen):
        assert gen._parse_plan_response("not json at all") == []

    def test_returns_empty_on_missing_views(self, gen):
        assert gen._parse_plan_response('{"other": 1}') == []


class TestParseSingleDefinition:

    def test_extracts_object(self, gen):
        response = '```json\n{"name": "mv1", "source": "t"}\n```'
        result = gen._parse_single_definition(response)
        assert result["name"] == "mv1"

    def test_returns_none_on_no_json(self, gen):
        assert gen._parse_single_definition("no json here") is None


class TestParseAiResponse:

    def test_extracts_array(self, gen):
        response = '```json\n[{"name": "a"}, {"name": "b"}]\n```'
        result = gen._parse_ai_response(response)
        assert len(result) == 2

    def test_falls_back_on_malformed(self, gen):
        response = '[{"name": "a"}, GARBAGE {"name": "b"}]'
        result = gen._parse_ai_response(response)
        assert len(result) >= 1

    def test_returns_empty_on_no_array(self, gen):
        assert gen._parse_ai_response("no array here") == []


class TestParseIndividualObjects:

    def test_extracts_from_malformed_array(self, gen):
        text = '[{"a": 1}, bad, {"b": 2}]'
        result = gen._parse_individual_objects(text)
        assert len(result) == 2

    def test_handles_nested_braces(self, gen):
        text = '[{"a": {"nested": 1}}, {"b": 2}]'
        result = gen._parse_individual_objects(text)
        assert len(result) == 2


# ── Column Ref Extraction ─────────────────────────────────────────────


class TestExtractColumnRefs:

    def test_extracts_simple_column(self, gen):
        refs = gen._extract_column_refs("SUM(total_amount)")
        assert "total_amount" in refs
        assert "SUM" not in refs

    def test_strips_string_literals(self, gen):
        refs = gen._extract_column_refs("CASE WHEN status = 'Active' THEN 1 END")
        assert "Active" not in refs
        assert "status" in refs

    def test_handles_dotted_identifiers(self, gen):
        refs = gen._extract_column_refs("orders.order_id = customers.customer_id")
        assert "order_id" in refs
        assert "customer_id" in refs
        assert "orders" not in refs

    def test_filters_sql_keywords(self, gen):
        refs = gen._extract_column_refs("COUNT(DISTINCT customer_id)")
        assert "customer_id" in refs
        assert "COUNT" not in refs
        assert "DISTINCT" not in refs

    def test_backtick_qualified_ref(self, gen):
        refs = gen._extract_column_refs_with_prefix("source.`assay name`")
        assert ("source", "assay name") in refs
        # Should NOT split into bare 'assay' and 'name'
        col_names = [c for _, c in refs]
        assert "assay" not in col_names
        assert "name" not in col_names

    def test_backtick_multiple_refs(self, gen):
        refs = gen._extract_column_refs_with_prefix(
            "CONCAT(source.`project id`, ' - ', source.`assay name`)"
        )
        assert ("source", "project id") in refs
        assert ("source", "assay name") in refs

    def test_backtick_mixed_with_normal(self, gen):
        refs = gen._extract_column_refs_with_prefix(
            "SUM(source.`total cost`) / NULLIF(source.quantity, 0)"
        )
        assert ("source", "total cost") in refs
        assert ("source", "quantity") in refs


# ── YAML / Joins ──────────────────────────────────────────────────────


class TestNormalizeJoins:

    def test_replaces_table_name_with_source(self, gen):
        defn = {
            "source": "cat.sch.orders",
            "joins": [{"name": "customers", "source": "cat.sch.customers", "on": "orders.customer_id = customers.id"}],
        }
        gen._normalize_joins(defn)
        assert "source.customer_id" in defn["joins"][0]["on"]

    def test_noop_without_joins(self, gen):
        defn = {"source": "cat.sch.orders"}
        gen._normalize_joins(defn)
        assert "joins" not in defn


class TestDefinitionToYaml:

    def test_produces_valid_yaml(self, gen):
        defn = {
            "name": "test_mv",
            "source": "cat.sch.orders",
            "comment": "Test metric",
            "dimensions": [{"name": "Order Month", "expr": "DATE_TRUNC('MONTH', order_date)"}],
            "measures": [{"name": "Total Revenue", "expr": "SUM(total_amount)"}],
        }
        result = gen._definition_to_yaml(defn)
        parsed = yaml.safe_load(result)
        assert parsed["source"] == "cat.sch.orders"
        assert len(parsed["dimensions"]) == 1
        assert len(parsed["measures"]) == 1

    def test_window_specs_included_when_present(self, gen):
        defn = {
            "name": "mv",
            "source": "cat.sch.t",
            "dimensions": [],
            "measures": [{"name": "m", "expr": "SUM(x)", "window": {"order": "event_date", "range": "trailing 7 day"}}],
        }
        result = gen._definition_to_yaml(defn)
        parsed = yaml.safe_load(result)
        assert "window" in parsed["measures"][0]
        assert parsed["measures"][0]["window"][0]["order"] == "event_date"

    def test_empty_window_omitted(self, gen):
        defn = {
            "name": "mv",
            "source": "cat.sch.t",
            "dimensions": [],
            "measures": [{"name": "m", "expr": "SUM(x)", "window": []}],
        }
        result = gen._definition_to_yaml(defn)
        parsed = yaml.safe_load(result)
        assert "window" not in parsed["measures"][0]

    def test_format_specs_included(self, gen):
        defn = {
            "name": "mv",
            "source": "cat.sch.t",
            "dimensions": [],
            "measures": [{"name": "m", "expr": "SUM(x)", "format": {"type": "currency", "currency_code": "USD"}}],
        }
        result = gen._definition_to_yaml(defn)
        parsed = yaml.safe_load(result)
        assert parsed["measures"][0]["format"]["type"] == "currency"


# ── Normalize Window Specs ───────────────────────────────────────────


class TestNormalizeWindowSpecs:

    def test_none_returns_empty(self):
        assert _normalize_window_specs(None) == []

    def test_string_returns_empty(self):
        assert _normalize_window_specs("not a dict") == []

    def test_dict_becomes_list(self):
        result = _normalize_window_specs({"order": "event_date"})
        assert isinstance(result, list) and len(result) == 1
        assert result[0]["order"] == "event_date"

    def test_interval_normalization(self):
        result = _normalize_window_specs({"order": "dt", "range": "INTERVAL 7 DAYS"})
        assert result[0]["range"] == "trailing 7 day"

    def test_unbounded_rows(self):
        result = _normalize_window_specs({"order": "dt", "rows": "UNBOUNDED PRECEDING"})
        assert result[0]["range"] == "unbounded"
        assert result[0].get("rows", "") == ""

    def test_skips_entry_without_order(self):
        result = _normalize_window_specs([{"range": "trailing 7 day"}])
        assert result == []

    def test_multiple_specs(self):
        specs = [
            {"order": "event_date", "range": "trailing 7 day"},
            {"order": "report_date", "range": "trailing 30 day"},
        ]
        result = _normalize_window_specs(specs)
        assert len(result) == 2

    def test_semiadditive_default(self):
        result = _normalize_window_specs({"order": "dt"})
        assert result[0]["semiadditive"] == "last"

    def test_semiadditive_preserved(self):
        result = _normalize_window_specs({"order": "dt", "semiadditive": "first"})
        assert result[0]["semiadditive"] == "first"

    def test_order_by_alias(self):
        result = _normalize_window_specs({"order_by": "ts"})
        assert result[0]["order"] == "ts"


# ── Infer Format Specs ───────────────────────────────────────────────


class TestInferFormatSpecs:

    def test_percentage_expr(self):
        defn = {"measures": [{"name": "rate", "expr": "SUM(x) * 100.0 / COUNT(*)"}]}
        _infer_format_specs(defn)
        assert defn["measures"][0]["format"]["type"] == "percentage"

    def test_percentage_name(self):
        defn = {"measures": [{"name": "rate", "expr": "SUM(x)"}]}
        _infer_format_specs(defn)
        assert defn["measures"][0]["format"]["type"] == "percentage"

    def test_currency_expr(self):
        defn = {"measures": [{"name": "total", "expr": "SUM(amount_usd)"}]}
        _infer_format_specs(defn)
        assert defn["measures"][0]["format"]["type"] == "currency"

    def test_default_number(self):
        defn = {"measures": [{"name": "total_count", "expr": "COUNT(*)"}]}
        _infer_format_specs(defn)
        assert defn["measures"][0]["format"]["type"] == "number"

    def test_preserves_existing_format(self):
        defn = {"measures": [{"name": "rate", "expr": "SUM(x) * 100", "format": {"type": "custom"}}]}
        _infer_format_specs(defn)
        assert defn["measures"][0]["format"]["type"] == "custom"

    def test_empty_measures(self):
        defn = {"measures": []}
        _infer_format_specs(defn)
        assert defn["measures"] == []


class TestFixPercentageScaling:

    def test_strips_100_multiply(self):
        defn = {"measures": [{
            "name": "Win Rate",
            "expr": "100.0 * COUNT(DISTINCT CASE WHEN stage = 'Won' THEN id END) / NULLIF(COUNT(*), 0)",
            "format": {"type": "percentage"},
        }]}
        _fix_percentage_scaling(defn)
        assert "100" not in defn["measures"][0]["expr"]
        assert defn["measures"][0]["expr"].startswith("COUNT(DISTINCT")

    def test_strips_round_100_multiply(self):
        defn = {"measures": [{
            "name": "Coverage Pct",
            "expr": "ROUND(100.0 * COUNT(x) / NULLIF(COUNT(y), 0), 2)",
            "format": {"type": "percentage"},
        }]}
        _fix_percentage_scaling(defn)
        assert "100" not in defn["measures"][0]["expr"]
        assert "ROUND" not in defn["measures"][0]["expr"]

    def test_leaves_fraction_alone(self):
        defn = {"measures": [{
            "name": "Rate",
            "expr": "SUM(x) * 1.0 / NULLIF(COUNT(*), 0)",
            "format": {"type": "percentage"},
        }]}
        _fix_percentage_scaling(defn)
        assert defn["measures"][0]["expr"] == "SUM(x) * 1.0 / NULLIF(COUNT(*), 0)"

    def test_leaves_non_percentage_alone(self):
        defn = {"measures": [{
            "name": "Score",
            "expr": "100.0 * SUM(x) / NULLIF(COUNT(*), 0)",
            "format": {"type": "number"},
        }]}
        _fix_percentage_scaling(defn)
        assert "100.0" in defn["measures"][0]["expr"]


# ── Parenthesized String Literal Regression ──────────────────────────


class TestParenthesizedStringLiterals:
    """Regression tests for the parentheses heuristic fix.

    Parenthesized natural language like 'Mild (Grade 1)' must be quoted,
    while actual function calls like COALESCE(x, 0) must be left alone.
    """

    def test_then_else_quotes_parenthesized_string(self):
        expr = "THEN Mild (Grade 1) ELSE Severe (Grade 4) END"
        result = SemanticLayerGenerator._fix_then_else_literals(expr)
        assert "'Mild (Grade 1)'" in result
        assert "'Severe (Grade 4)'" in result

    def test_then_else_leaves_function_call(self):
        expr = "THEN COALESCE(x, 0) ELSE NULL END"
        result = SemanticLayerGenerator._fix_then_else_literals(expr)
        assert "'" not in result.replace("COALESCE", "")

    def test_unquoted_leaves_function_call(self):
        expr = "= NULLIF(x, 0) THEN"
        result = SemanticLayerGenerator._fix_unquoted_literals(expr)
        assert "'" not in result.replace("NULLIF", "")

    def test_unquoted_quotes_multi_word_with_parens(self):
        """_fix_unquoted_literals treats ')' as a delimiter, so parenthesized
        strings in comparison values get partially captured.  This test
        documents the current behavior; _fix_then_else_literals handles
        the THEN/ELSE case correctly."""
        expr = "= Requirement change THEN"
        result = SemanticLayerGenerator._fix_unquoted_literals(expr)
        assert "'Requirement change'" in result


# ── Source Validation ─────────────────────────────────────────────────


class TestCheckDimSourcePattern:
    def test_no_joins_returns_none(self):
        defn = {"source": "cat.sch.dim_customer", "joins": []}
        assert check_dim_source_pattern(defn, []) is None

    def test_dim_prefix_fact_join_detected(self):
        defn = {
            "source": "cat.sch.dim_customer",
            "joins": [{"source": "cat.sch.fct_orders", "on": "source.id = fct_orders.cust_id"}],
        }
        result = check_dim_source_pattern(defn, [])
        assert result is not None
        assert "name_prefix" in [s[0] for s in result["signals"]]

    def test_fk_direction_signal(self):
        defn = {
            "source": "cat.sch.dim_customer",
            "joins": [{"source": "cat.sch.orders", "on": "source.id = orders.cust_id"}],
        }
        fk_rows = [{"src_table": "cat.sch.orders", "dst_table": "cat.sch.dim_customer",
                     "src_column": "cust_id", "dst_column": "id"}]
        result = check_dim_source_pattern(defn, fk_rows)
        assert result is not None
        assert "fk_direction" in [s[0] for s in result["signals"]]

    def test_fk_fanout_signal(self):
        defn = {
            "source": "cat.sch.dim_product",
            "joins": [{"source": "cat.sch.sales", "on": "source.id = sales.prod_id"}],
        }
        fk_rows = [
            {"src_table": "cat.sch.orders", "dst_table": "cat.sch.dim_product",
             "src_column": "prod_id", "dst_column": "id"},
            {"src_table": "cat.sch.sales", "dst_table": "cat.sch.dim_product",
             "src_column": "prod_id", "dst_column": "id"},
        ]
        result = check_dim_source_pattern(defn, fk_rows)
        assert result is not None
        assert "fk_fanout" in [s[0] for s in result["signals"]]

    def test_fact_source_not_flagged(self):
        defn = {
            "source": "cat.sch.fct_orders",
            "joins": [{"source": "cat.sch.dim_customer", "on": "source.cust_id = dim_customer.id"}],
        }
        result = check_dim_source_pattern(defn, [])
        assert result is None

    def test_no_signals_returns_none(self):
        defn = {
            "source": "cat.sch.events",
            "joins": [{"source": "cat.sch.users", "on": "source.uid = users.id"}],
        }
        assert check_dim_source_pattern(defn, []) is None


class TestSwapSourceAndJoin:
    def test_swap_reverses_source_and_join(self):
        defn = {
            "source": "cat.sch.dim_customer",
            "joins": [{"name": "fct_orders", "source": "cat.sch.fct_orders",
                        "on": "source.id = fct_orders.cust_id"}],
            "measures": [{"name": "cnt", "expr": "COUNT(*)"}],
        }
        swapped = _swap_source_and_join(defn, "fct_orders")
        assert swapped["source"] == "cat.sch.fct_orders"
        assert any(j["source"] == "cat.sch.dim_customer" for j in swapped["joins"])

    def test_swap_no_match_returns_unchanged(self):
        defn = {
            "source": "cat.sch.dim_customer",
            "joins": [{"name": "other", "source": "cat.sch.other", "on": "source.id = other.cid"}],
        }
        swapped = _swap_source_and_join(defn, "fct_orders")
        assert swapped["source"] == "cat.sch.dim_customer"


class TestProfileSchema:
    def test_star_schema(self):
        tables = ["cat.sch.fct_orders", "cat.sch.dim_customer", "cat.sch.dim_product"]
        fk_rows = [
            {"src_table": "cat.sch.fct_orders", "src_column": "cust_id",
             "dst_table": "cat.sch.dim_customer", "dst_column": "id", "final_confidence": 0.9},
            {"src_table": "cat.sch.fct_orders", "src_column": "prod_id",
             "dst_table": "cat.sch.dim_product", "dst_column": "id", "final_confidence": 0.9},
        ]
        result = profile_schema(tables, fk_rows)
        assert result["schema_type"] == "STAR"
        assert result["table_count"] == 3
        assert result["fk_count"] == 2
        assert "fct_orders" in result["fact_tables"]
        assert "STAR" in result["profile_text"]

    def test_simple_no_fks(self):
        tables = ["cat.sch.events", "cat.sch.users"]
        result = profile_schema(tables, [])
        assert result["schema_type"] == "SIMPLE"
        assert result["fk_count"] == 0
        assert "SIMPLE" in result["profile_text"]

    def test_data_mart(self):
        tables = ["cat.sch.sales_summary", "cat.sch.revenue_rollup"]
        result = profile_schema(tables, [])
        assert result["schema_type"] == "DATA_MART"
        assert "sales_summary" in result["mart_tables"]
        assert "revenue_rollup" in result["mart_tables"]

    def test_snowflake_multihop(self):
        tables = ["cat.sch.fct_orders", "cat.sch.dim_customer", "cat.sch.dim_region"]
        fk_rows = [
            {"src_table": "cat.sch.fct_orders", "src_column": "cust_id",
             "dst_table": "cat.sch.dim_customer", "dst_column": "id", "final_confidence": 0.9},
            {"src_table": "cat.sch.dim_customer", "src_column": "region_id",
             "dst_table": "cat.sch.dim_region", "dst_column": "id", "final_confidence": 0.9},
        ]
        result = profile_schema(tables, fk_rows)
        assert result["schema_type"] == "SNOWFLAKE"
        assert "multi-hop" in result["profile_text"]

    def test_single_fk_few_tables_is_simple(self):
        tables = ["cat.sch.orders", "cat.sch.users"]
        fk_rows = [
            {"src_table": "cat.sch.orders", "src_column": "user_id",
             "dst_table": "cat.sch.users", "dst_column": "id", "final_confidence": 0.8},
        ]
        result = profile_schema(tables, fk_rows)
        assert result["schema_type"] == "SIMPLE"


# ── KPI Reference Stripping Regex ────────────────────────────────────

import re

_KPI_REF_RE = re.compile(
    r"\.?\s*(?:Implements|Supports|Addresses|Answers|Covers|Partially implements)"
    r"\s+(?:KPI|question|Q)s?\s*(?::\s*[^.]*(?:\.\s*)?|[\d,\s\-and#()]+\.?)"
    r"|\s*\(KPI:\s*[^)]+\)"
    r"|\s*\bKPI\s*#\d+\b(?:\s*\([^)]*\))?\.?"
    r"|\s*\(#\d+\)",
    re.IGNORECASE,
)


class TestKpiRefRegex:
    """Tests for _KPI_REF_RE -- must strip KPI/question references from MV comments."""

    def _strip(self, text):
        result = _KPI_REF_RE.sub("", text)
        result = re.sub(r"  +", " ", result).strip().rstrip(".")
        return (result + ".") if result else ""

    def test_numbered_kpis(self):
        assert self._strip("Tracks revenue growth. Implements KPIs 1, 3, 10.") == "Tracks revenue growth."

    def test_named_kpis_with_colon(self):
        text = "Tracks deal pipeline metrics. Implements KPIs: Enterprise Segment Weighted Pipeline Concentration (#2), New Business Deal Share of Pipeline (#4)."
        assert self._strip(text) == "Tracks deal pipeline metrics."

    def test_inline_kpi_hash(self):
        assert self._strip("Total weighted deal value KPI #6 (Customer Win Rate).") == "Total weighted deal value."

    def test_inline_hash_number(self):
        assert self._strip("Revenue growth metric (#3) by region.") == "Revenue growth metric by region."

    def test_parenthetical_kpi(self):
        assert self._strip("Shows overdue deals (KPI: Pipeline Risk).") == "Shows overdue deals."

    def test_supports_questions(self):
        assert self._strip("Pipeline analysis. Supports questions 1, 2 and 5.") == "Pipeline analysis."

    def test_no_match_preserves_text(self):
        assert self._strip("Clean description with no references.") == "Clean description with no references."

    def test_partially_implements(self):
        assert self._strip("Deal metrics. Partially implements KPI 7.") == "Deal metrics."

    def test_multiple_patterns_in_one(self):
        text = "Revenue growth KPI #2 (Win Rate). Implements KPIs: Overdue Risk (#1)."
        result = self._strip(text)
        assert "KPI" not in result
        assert "#" not in result
