"""Tests for composite (multi-column) join-key helpers."""

from dbxmetagen.composite_keys import (
    composite_key_pairs,
    composite_uniqueness_sql,
    quote_table_name,
    render_composite_condition,
    uniqueness_ratio,
)


class TestCompositeKeyPairs:
    def test_canonical_erd_form(self):
        cond = "source.guid = user_mst.guid AND source.base_dt = user_mst.base_dt"
        assert composite_key_pairs(cond, "guid", "guid") == [("guid", "guid"), ("base_dt", "base_dt")]

    def test_anchor_comes_first_and_is_not_duplicated(self):
        cond = "source.base_dt = m.snap_dt AND source.guid = m.user_guid"
        assert composite_key_pairs(cond, "guid", "user_guid") == [("guid", "user_guid"), ("base_dt", "snap_dt")]

    def test_child_qualifier_on_the_right_is_oriented(self):
        cond = "m.user_guid = source.guid AND m.snap_dt = source.base_dt"
        assert composite_key_pairs(cond) == [("guid", "user_guid"), ("base_dt", "snap_dt")]

    def test_legacy_key_name_list_pairs_same_named_columns(self):
        # Older rows store only the key names, with no equality predicate.
        assert composite_key_pairs("guid AND base_dt", "guid", "guid") == [("guid", "guid"), ("base_dt", "base_dt")]

    def test_fully_qualified_anchor_columns_use_the_leaf(self):
        pairs = composite_key_pairs("guid AND base_dt", "c.s.txn.guid", "c.s.mst.guid")
        assert pairs == [("guid", "guid"), ("base_dt", "base_dt")]

    def test_backticks_and_parentheses_are_tolerated(self):
        cond = "(source.`guid` = m.`guid`) AND (source.`base_dt` = m.`base_dt`)"
        assert composite_key_pairs(cond) == [("guid", "guid"), ("base_dt", "base_dt")]

    def test_single_pair_is_not_composite(self):
        assert composite_key_pairs("source.id = d.id", "id", "id") == []

    def test_filter_literal_term_makes_it_unparseable(self):
        # A filter is not a key column; refuse rather than invent a pair.
        assert composite_key_pairs("source.id = d.id AND d.active = 1") == []
        assert composite_key_pairs("source.id = d.id AND d.kind = 'x'") == []

    def test_empty_input(self):
        assert composite_key_pairs(None) == []
        assert composite_key_pairs("   ") == []


class TestRendering:
    def test_render_condition(self):
        pairs = [("guid", "guid"), ("base_dt", "base_dt")]
        assert render_composite_condition(pairs, "src", "mst") == "src.guid = mst.guid AND src.base_dt = mst.base_dt"

    def test_quote_table_name(self):
        assert quote_table_name("c.s.t") == "`c`.`s`.`t`"
        assert quote_table_name("`c`.s.`t`") == "`c`.`s`.`t`"  # idempotent
        assert quote_table_name("c.s.we`ird") == "`c`.`s`.`we``ird`"


class TestUniquenessSql:
    def test_exact_count_by_default(self):
        sql = composite_uniqueness_sql("`c`.`s`.`t`", ["guid", "base_dt"])
        assert sql == ("SELECT COUNT(*) AS total, COUNT(DISTINCT `guid`, `base_dt`) AS distinct_keys "
                       "FROM `c`.`s`.`t`")
        assert "TABLESAMPLE" not in sql and "LIMIT" not in sql

    def test_limit_sample_for_federation(self):
        sql = composite_uniqueness_sql("`c`.`s`.`t`", ["a", "b"], limit_rows=1000)
        assert "FROM (SELECT `a`, `b` FROM `c`.`s`.`t` LIMIT 1000)" in sql

    def test_ratio(self):
        assert uniqueness_ratio(1_400_000, 1_400_000) == 1.0
        assert uniqueness_ratio("1400000", "100000") == 100000 / 1400000
        assert uniqueness_ratio(0, 0) is None
        assert uniqueness_ratio(None, 5) is None
        assert uniqueness_ratio(10, 20) == 1.0  # clamped
