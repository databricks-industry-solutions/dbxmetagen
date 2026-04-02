"""Unit tests for genie_schema.py: SQL fixers, helpers, and build_serialized_space."""

import sys
import pytest

sys.path.insert(0, "src")

from dbxmetagen.genie.schema import (
    _simplify_join_sql,
    _backtick_join_sql,
    _fix_date_func_units,
    _fix_date_trunc_units,
    _fix_case_string_literals,
    _to_str_list,
    _ensure_list,
    _name_from_sql,
    _split_text_into_instructions,
    _dedup_join_specs,
    build_serialized_space,
    JoinSide,
    JoinSpec,
)


# ---------------------------------------------------------------------------
# 1. SQL Helper Functions
# ---------------------------------------------------------------------------

class TestSimplifyJoinSql:
    def test_fq_four_part_reduced(self):
        assert _simplify_join_sql("cat.sch.orders.id = cat.sch.customers.cust_id") == "orders.id = customers.cust_id"

    def test_two_part_untouched(self):
        assert _simplify_join_sql("orders.id = customers.cust_id") == "orders.id = customers.cust_id"

    def test_bare_column_noop(self):
        assert _simplify_join_sql("id = cust_id") == "id = cust_id"

    def test_multiple_refs(self):
        sql = "cat.sch.a.x = cat.sch.b.y AND cat.sch.a.z = cat.sch.c.w"
        assert _simplify_join_sql(sql) == "a.x = b.y AND a.z = c.w"


class TestBacktickJoinSql:
    def test_basic(self):
        assert _backtick_join_sql("orders.id = customers.cust_id") == "`orders`.`id` = `customers`.`cust_id`"

    def test_multiple_refs(self):
        result = _backtick_join_sql("a.x = b.y")
        assert result == "`a`.`x` = `b`.`y`"

    def test_bare_word_noop(self):
        assert _backtick_join_sql("id = cust_id") == "id = cust_id"


class TestFixDateFuncUnits:
    def test_plural_depluralised(self):
        assert "MONTH," in _fix_date_func_units("TIMESTAMPADD(MONTHS, 1, col)")

    def test_quoted_unit_stripped(self):
        result = _fix_date_func_units("TIMESTAMPADD('MONTHS', 1, col)")
        assert "MONTH," in result
        assert "'" not in result.split("(")[1].split(",")[0]

    def test_valid_unit_unchanged(self):
        assert "MONTH," in _fix_date_func_units("TIMESTAMPADD(MONTH, 1, col)")

    def test_timestampdiff(self):
        assert "DAY," in _fix_date_func_units("TIMESTAMPDIFF(DAYS, a, b)")

    def test_no_match_noop(self):
        sql = "SELECT 1"
        assert _fix_date_func_units(sql) == sql


class TestFixDateTruncUnits:
    def test_bare_unit_quoted(self):
        assert _fix_date_trunc_units("DATE_TRUNC(MONTH, col)") == "DATE_TRUNC('MONTH', col)"

    def test_already_quoted_unchanged(self):
        sql = "DATE_TRUNC('MONTH', col)"
        assert _fix_date_trunc_units(sql) == sql

    def test_invalid_unit_unchanged(self):
        sql = "DATE_TRUNC(FOOBAR, col)"
        assert _fix_date_trunc_units(sql) == sql

    def test_case_insensitive(self):
        result = _fix_date_trunc_units("date_trunc(year, col)")
        assert "'YEAR'" in result


class TestFixCaseStringLiterals:
    def test_bare_literal_quoted(self):
        sql = "CASE WHEN x=1 THEN Active ELSE Inactive END"
        result = _fix_case_string_literals(sql)
        assert "'Active'" in result
        assert "'Inactive'" in result

    def test_null_skipped(self):
        sql = "CASE WHEN x=1 THEN NULL ELSE foo END"
        result = _fix_case_string_literals(sql)
        assert "THEN NULL" in result

    def test_true_false_skipped(self):
        sql = "CASE WHEN x=1 THEN TRUE ELSE FALSE END"
        result = _fix_case_string_literals(sql)
        assert "THEN TRUE" in result
        assert "ELSE FALSE" in result

    def test_already_quoted_skipped(self):
        sql = "CASE WHEN x=1 THEN 'Active' ELSE 'Inactive' END"
        assert _fix_case_string_literals(sql) == sql

    def test_no_case_noop(self):
        sql = "SELECT col FROM tbl"
        assert _fix_case_string_literals(sql) == sql


# ---------------------------------------------------------------------------
# 2. Helper Utilities
# ---------------------------------------------------------------------------

class TestToStrList:
    def test_string(self):
        assert _to_str_list("hello") == ["hello"]

    def test_list(self):
        assert _to_str_list(["a", "b"]) == ["a", "b"]

    def test_none(self):
        assert _to_str_list(None) is None

    def test_empty_string(self):
        assert _to_str_list("") is None

    def test_list_filters_falsy(self):
        assert _to_str_list(["a", "", None, "b"]) == ["a", "b"]


class TestEnsureList:
    def test_string(self):
        assert _ensure_list("hello") == ["hello"]

    def test_list(self):
        assert _ensure_list(["a", "b"]) == ["a", "b"]

    def test_none(self):
        assert _ensure_list(None) == []

    def test_empty_string(self):
        assert _ensure_list("") == []


class TestNameFromSql:
    def test_normal(self):
        assert _name_from_sql(["SUM(amount)"]) == "SUM(amount)"

    def test_truncation(self):
        long_sql = ["A" * 100]
        result = _name_from_sql(long_sql)
        assert len(result) == 60
        assert result.endswith("...")

    def test_empty(self):
        assert _name_from_sql([]) == ""

    def test_strips_quotes(self):
        assert _name_from_sql(["`col`"]) == "col"


class TestSplitTextInstructions:
    def test_single_block(self):
        result = _split_text_into_instructions("Just plain text")
        assert len(result) == 1
        assert result[0].content == ["Just plain text"]

    def test_multiple_sections(self):
        text = "## Section A\nContent A\n## Section B\nContent B"
        result = _split_text_into_instructions(text)
        assert len(result) == 2
        assert "Section A" in result[0].content[0]
        assert "Content A" in result[0].content[0]
        assert "Section B" in result[1].content[0]

    def test_empty_input(self):
        assert _split_text_into_instructions("") == []
        assert _split_text_into_instructions(None) == []

    def test_preamble_before_first_header(self):
        text = "Preamble\n## Section\nBody"
        result = _split_text_into_instructions(text)
        assert len(result) == 2
        assert result[0].content == ["Preamble"]
        assert "Section" in result[1].content[0]


class TestDedupJoinSpecs:
    def _js(self, left: str, right: str) -> JoinSpec:
        return JoinSpec(
            left=JoinSide(identifier=left),
            right=JoinSide(identifier=right),
            sql=["a = b"],
        )

    def test_no_dupes(self):
        joins = [self._js("a", "b"), self._js("b", "c")]
        assert len(_dedup_join_specs(joins)) == 2

    def test_forward_dup(self):
        joins = [self._js("a", "b"), self._js("a", "b")]
        assert len(_dedup_join_specs(joins)) == 1

    def test_reversed_dup(self):
        joins = [self._js("a", "b"), self._js("b", "a")]
        assert len(_dedup_join_specs(joins)) == 1


# ---------------------------------------------------------------------------
# 3. build_serialized_space
# ---------------------------------------------------------------------------

def _minimal_raw(**overrides) -> dict:
    """Build a minimal valid raw dict for build_serialized_space."""
    base = {
        "data_sources": {"tables": [{"identifier": "cat.sch.orders"}], "metric_views": []},
        "instructions": {
            "join_specs": [],
            "example_sql": [],
            "sql_snippets": {"measures": [], "filters": [], "expressions": []},
            "text": "",
        },
        "sample_questions": [],
    }
    base.update(overrides)
    return base


class TestBuildJoins:
    def test_fq_simplified_and_backticked(self):
        raw = _minimal_raw()
        raw["instructions"]["join_specs"] = [{
            "left": {"identifier": "cat.sch.orders"},
            "right": {"identifier": "cat.sch.customers"},
            "sql": ["cat.sch.orders.id = cat.sch.customers.order_id"],
        }]
        result = build_serialized_space(raw)
        js = result["instructions"]["join_specs"][0]
        assert "`orders`.`id`" in js["sql"][0]
        assert "`customers`.`order_id`" in js["sql"][0]

    def test_compound_and_split(self):
        raw = _minimal_raw()
        raw["instructions"]["join_specs"] = [{
            "left": {"identifier": "cat.sch.a"},
            "right": {"identifier": "cat.sch.b"},
            "sql": ["a.x = b.x AND a.y = b.y"],
        }]
        result = build_serialized_space(raw)
        js = result["instructions"]["join_specs"][0]
        conditions = [s for s in js["sql"] if not s.startswith("--")]
        assert len(conditions) == 2

    def test_alias_populated(self):
        raw = _minimal_raw()
        raw["instructions"]["join_specs"] = [{
            "left": {"identifier": "cat.sch.orders"},
            "right": {"identifier": "cat.sch.customers"},
            "sql": ["orders.id = customers.id"],
        }]
        result = build_serialized_space(raw)
        js = result["instructions"]["join_specs"][0]
        assert js["left"]["alias"] == "orders"
        assert js["right"]["alias"] == "customers"

    def test_rt_directive_appended(self):
        raw = _minimal_raw()
        raw["instructions"]["join_specs"] = [{
            "left": {"identifier": "cat.sch.a"},
            "right": {"identifier": "cat.sch.b"},
            "sql": ["a.id = b.id"],
        }]
        result = build_serialized_space(raw)
        js = result["instructions"]["join_specs"][0]
        assert js["sql"][-1] == "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--"

    def test_instruction_set(self):
        raw = _minimal_raw()
        raw["instructions"]["join_specs"] = [{
            "left": {"identifier": "cat.sch.orders"},
            "right": {"identifier": "cat.sch.items"},
            "sql": ["orders.id = items.order_id"],
        }]
        result = build_serialized_space(raw)
        js = result["instructions"]["join_specs"][0]
        assert js["instruction"] == ["to join orders to items"]

    def test_duplicate_joins_deduped(self):
        raw = _minimal_raw()
        raw["instructions"]["join_specs"] = [
            {"left": {"identifier": "cat.sch.a"}, "right": {"identifier": "cat.sch.b"}, "sql": ["a.id = b.id"]},
            {"left": {"identifier": "cat.sch.a"}, "right": {"identifier": "cat.sch.b"}, "sql": ["a.id = b.id"]},
        ]
        result = build_serialized_space(raw)
        assert len(result["instructions"]["join_specs"]) == 1

    def test_join_condition_fallback(self):
        raw = _minimal_raw()
        raw["instructions"]["join_specs"] = [{
            "left": {"identifier": "cat.sch.a"},
            "right": {"identifier": "cat.sch.b"},
            "join_condition": "a.id = b.id",
            "sql": [],
        }]
        result = build_serialized_space(raw)
        assert len(result["instructions"]["join_specs"]) == 1

    def test_left_table_right_table_fallback(self):
        raw = _minimal_raw()
        raw["instructions"]["join_specs"] = [{
            "left_table": "cat.sch.a",
            "right_table": "cat.sch.b",
            "sql": ["a.id = b.id"],
        }]
        result = build_serialized_space(raw)
        assert len(result["instructions"]["join_specs"]) == 1
        assert result["instructions"]["join_specs"][0]["left"]["identifier"] == "cat.sch.a"


class TestBuildExamples:
    def test_parameters_wired(self):
        raw = _minimal_raw()
        raw["instructions"]["example_sql"] = [{
            "question": "How many orders?",
            "sql": "SELECT COUNT(*) FROM orders",
            "parameters": [{"name": "region", "type_hint": "STRING", "default_value": {"values": ["US"]}}],
            "usage_guidance": "Filter by region",
        }]
        result = build_serialized_space(raw)
        ex = result["instructions"]["example_question_sqls"][0]
        assert ex["parameters"] == [{"name": "region", "type_hint": "STRING", "default_value": {"values": ["US"]}}]
        assert ex["usage_guidance"] == ["Filter by region"]

    def test_no_parameters_excluded(self):
        raw = _minimal_raw()
        raw["instructions"]["example_sql"] = [{
            "question": "How many orders?",
            "sql": "SELECT COUNT(*) FROM orders",
        }]
        result = build_serialized_space(raw)
        ex = result["instructions"]["example_question_sqls"][0]
        assert "parameters" not in ex
        assert "usage_guidance" not in ex

    def test_empty_question_or_sql_skipped(self):
        raw = _minimal_raw()
        raw["instructions"]["example_sql"] = [
            {"question": "", "sql": "SELECT 1"},
            {"question": "q", "sql": ""},
        ]
        result = build_serialized_space(raw)
        assert len(result["instructions"]["example_question_sqls"]) == 0


class TestBuildSnippets:
    def test_measure_instruction_and_synonyms(self):
        raw = _minimal_raw()
        raw["instructions"]["sql_snippets"] = {
            "measures": [{"alias": "total_rev", "sql": ["SUM(revenue)"], "instruction": "total revenue", "synonyms": ["rev", "income"]}],
            "filters": [], "expressions": [],
        }
        result = build_serialized_space(raw)
        m = result["instructions"]["sql_snippets"]["measures"][0]
        assert m["instruction"] == ["total revenue"]
        assert m["synonyms"] == ["rev", "income"]

    def test_filter_instruction_and_synonyms(self):
        raw = _minimal_raw()
        raw["instructions"]["sql_snippets"] = {
            "measures": [],
            "filters": [{"display_name": "Active", "sql": ["status = 'active'"], "instruction": "active only", "synonyms": ["live"]}],
            "expressions": [],
        }
        result = build_serialized_space(raw)
        f = result["instructions"]["sql_snippets"]["filters"][0]
        assert f["instruction"] == ["active only"]
        assert f["synonyms"] == ["live"]

    def test_expression_instruction_and_synonyms(self):
        raw = _minimal_raw()
        raw["instructions"]["sql_snippets"] = {
            "measures": [], "filters": [],
            "expressions": [{"alias": "full_name", "sql": ["CONCAT(first, last)"], "instruction": "full name", "synonyms": ["name"]}],
        }
        result = build_serialized_space(raw)
        e = result["instructions"]["sql_snippets"]["expressions"][0]
        assert e["instruction"] == ["full name"]
        assert e["synonyms"] == ["name"]

    def test_description_excluded(self):
        raw = _minimal_raw()
        raw["instructions"]["sql_snippets"] = {
            "measures": [{"alias": "x", "sql": ["SUM(x)"], "description": "should not appear"}],
            "filters": [],
            "expressions": [{"alias": "y", "sql": ["y+1"], "description": "also excluded"}],
        }
        result = build_serialized_space(raw)
        m = result["instructions"]["sql_snippets"]["measures"][0]
        e = result["instructions"]["sql_snippets"]["expressions"][0]
        assert "description" not in m
        assert "description" not in e

    def test_empty_sql_skipped(self):
        raw = _minimal_raw()
        raw["instructions"]["sql_snippets"] = {
            "measures": [{"alias": "x", "sql": []}],
            "filters": [{"display_name": "f", "sql": []}],
            "expressions": [{"alias": "e", "sql": []}],
        }
        result = build_serialized_space(raw)
        assert "sql_snippets" not in result["instructions"]

    def test_name_fallback_from_sql(self):
        raw = _minimal_raw()
        raw["instructions"]["sql_snippets"] = {
            "measures": [{"sql": ["SUM(amount)"]}],
            "filters": [{"sql": ["status > 0"]}],
            "expressions": [{"sql": ["col + 1"]}],
        }
        result = build_serialized_space(raw)
        snips = result["instructions"]["sql_snippets"]
        assert snips["measures"][0]["alias"] == "SUM(amount)"
        assert snips["filters"][0]["display_name"] == "status > 0"
        assert snips["expressions"][0]["alias"] == "col + 1"


class TestBuildMisc:
    def test_text_instructions_string(self):
        raw = _minimal_raw()
        raw["instructions"]["text"] = "Use date columns carefully."
        result = build_serialized_space(raw)
        ti = result["instructions"]["text_instructions"]
        assert len(ti) == 1
        assert ti[0]["content"] == ["Use date columns carefully."]

    def test_text_instructions_list_of_dicts(self):
        raw = _minimal_raw()
        raw["instructions"]["text"] = [
            {"content": "Part A"},
            {"content": "Part B"},
        ]
        result = build_serialized_space(raw)
        ti = result["instructions"]["text_instructions"]
        assert len(ti) == 1
        assert "Part A" in ti[0]["content"][0]
        assert "Part B" in ti[0]["content"][0]

    def test_text_instructions_list_of_strings(self):
        raw = _minimal_raw()
        raw["instructions"]["text"] = ["Line 1", "Line 2"]
        result = build_serialized_space(raw)
        ti = result["instructions"]["text_instructions"]
        assert len(ti) == 1
        assert "Line 1" in ti[0]["content"][0]

    def test_sample_questions_strings(self):
        raw = _minimal_raw(sample_questions=["What is revenue?", "Top customers?"])
        result = build_serialized_space(raw)
        qs = result["config"]["sample_questions"]
        assert len(qs) == 2
        assert qs[0]["question"] == ["What is revenue?"] or qs[1]["question"] == ["What is revenue?"]

    def test_sample_questions_dicts(self):
        raw = _minimal_raw(sample_questions=[{"question": "What is revenue?"}])
        result = build_serialized_space(raw)
        qs = result["config"]["sample_questions"]
        assert len(qs) == 1

    def test_no_sample_questions_no_config(self):
        raw = _minimal_raw(sample_questions=[])
        result = build_serialized_space(raw)
        assert "config" not in result

    def test_data_sources_sorted(self):
        raw = _minimal_raw()
        raw["data_sources"]["tables"] = [
            {"identifier": "cat.sch.zebra"},
            {"identifier": "cat.sch.alpha"},
        ]
        result = build_serialized_space(raw)
        ids = [t["identifier"] for t in result["data_sources"]["tables"]]
        assert ids == ["cat.sch.alpha", "cat.sch.zebra"]

    def test_version_is_two(self):
        result = build_serialized_space(_minimal_raw())
        assert result["version"] == 2

    def test_no_none_fields(self):
        raw = _minimal_raw()
        raw["instructions"]["join_specs"] = [{
            "left": {"identifier": "cat.sch.a"},
            "right": {"identifier": "cat.sch.b"},
            "sql": ["a.id = b.id"],
        }]
        raw["instructions"]["example_sql"] = [{"question": "q", "sql": "SELECT 1"}]
        raw["instructions"]["sql_snippets"] = {
            "measures": [{"alias": "m", "sql": ["SUM(x)"]}],
            "filters": [{"display_name": "f", "sql": ["x > 0"]}],
            "expressions": [{"alias": "e", "sql": ["x+1"]}],
        }
        result = build_serialized_space(raw)

        def _check_no_none(obj, path=""):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    assert v is not None, f"None value at {path}.{k}"
                    _check_no_none(v, f"{path}.{k}")
            elif isinstance(obj, list):
                for i, v in enumerate(obj):
                    assert v is not None, f"None value at {path}[{i}]"
                    _check_no_none(v, f"{path}[{i}]")

        _check_no_none(result)

    def test_date_func_fix_applied_in_snippets(self):
        raw = _minimal_raw()
        raw["instructions"]["sql_snippets"] = {
            "measures": [{"alias": "m", "sql": ["TIMESTAMPADD(MONTHS, 1, col)"]}],
            "filters": [], "expressions": [],
        }
        result = build_serialized_space(raw)
        sql = result["instructions"]["sql_snippets"]["measures"][0]["sql"][0]
        assert "MONTH," in sql
        assert "MONTHS" not in sql

    def test_date_trunc_fix_applied_in_snippets(self):
        raw = _minimal_raw()
        raw["instructions"]["sql_snippets"] = {
            "measures": [], "filters": [],
            "expressions": [{"alias": "e", "sql": ["DATE_TRUNC(MONTH, col)"]}],
        }
        result = build_serialized_space(raw)
        sql = result["instructions"]["sql_snippets"]["expressions"][0]["sql"][0]
        assert "'MONTH'" in sql
