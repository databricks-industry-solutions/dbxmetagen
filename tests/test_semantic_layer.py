"""Tests for semantic_layer module -- expression fixers, JSON parsers, column ref extraction."""

import pytest
from unittest.mock import MagicMock
from dbxmetagen.semantic_layer import SemanticLayerGenerator, SemanticLayerConfig


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
        import yaml
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
