"""Unit tests for table_filter.py -- parse_table_names and table_filter_sql."""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dbxmetagen.table_filter import parse_table_names, table_filter_sql


class TestParseTableNames:
    def test_empty_string(self):
        assert parse_table_names("") == []

    def test_none(self):
        assert parse_table_names(None) == []

    def test_single(self):
        assert parse_table_names("cat.sch.tbl") == ["cat.sch.tbl"]

    def test_multiple(self):
        assert parse_table_names("a.b.c, d.e.f") == ["a.b.c", "d.e.f"]

    def test_strips_whitespace(self):
        assert parse_table_names("  a.b.c , d.e.f  ") == ["a.b.c", "d.e.f"]

    def test_drops_empties(self):
        assert parse_table_names("a.b.c,,d.e.f,") == ["a.b.c", "d.e.f"]


class TestTableFilterSqlLiterals:
    def test_empty_list(self):
        assert table_filter_sql([]) == ""

    def test_single_literal(self):
        result = table_filter_sql(["cat.sch.tbl"])
        assert result == "AND (table_name IN ('cat.sch.tbl'))"

    def test_multiple_literals(self):
        result = table_filter_sql(["cat.sch.a", "cat.sch.b"])
        assert "IN (" in result
        assert "'cat.sch.a'" in result
        assert "'cat.sch.b'" in result

    def test_custom_column(self):
        result = table_filter_sql(["cat.sch.tbl"], column="kb.table_name")
        assert result.startswith("AND (kb.table_name IN (")

    def test_escapes_single_quotes(self):
        result = table_filter_sql(["cat.sch.it's"])
        assert "it''s" in result


class TestTableFilterSqlWildcards:
    def test_single_wildcard(self):
        result = table_filter_sql(["cat.sch.*"])
        assert result == "AND (table_name LIKE 'cat.sch.%')"

    def test_wildcard_custom_column(self):
        result = table_filter_sql(["cat.sch.*"], column="kb.table_name")
        assert result == "AND (kb.table_name LIKE 'cat.sch.%')"

    def test_multiple_wildcards(self):
        result = table_filter_sql(["cat.a.*", "cat.b.*"])
        assert "LIKE 'cat.a.%'" in result
        assert "LIKE 'cat.b.%'" in result
        assert " OR " in result


class TestTableFilterSqlMixed:
    def test_literal_and_wildcard(self):
        result = table_filter_sql(["cat.sch.tbl1", "cat.sch2.*"])
        assert "IN ('cat.sch.tbl1')" in result
        assert "LIKE 'cat.sch2.%'" in result
        assert " OR " in result
        assert result.startswith("AND (")
        assert result.endswith(")")

    def test_all_wildcards_no_in_clause(self):
        result = table_filter_sql(["a.b.*", "c.d.*"])
        assert "IN" not in result
        assert "LIKE 'a.b.%'" in result
        assert "LIKE 'c.d.%'" in result
