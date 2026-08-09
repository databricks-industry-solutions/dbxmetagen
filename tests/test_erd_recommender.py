"""Tests for the pure ERD recommendation engine.

recommend_erd() takes already-fetched metadata rows and returns roles, join
edges, and a coverage-aware sufficiency recommendation -- no Spark/LLM. These
fixtures cover STAR (one fact, N dims), SNOWFLAKE (bridge chains), and SIMPLE
(no FKs) schemas, plus the key behavior that the recommended view count is
coverage-aware rather than a naive fraction of the table count.
"""

import pytest

from dbxmetagen.erd_recommender import (
    recommend_erd,
    recommend_questions_kpis,
    _build_edges,
    _infer_role,
    _recommend_view_count,
    ErdRecommendation,
    GenSufficiency,
    FK_CONFIRMED_MIN,
    MAX_RECOMMENDED_VIEWS,
    QUESTIONS_PER_FACT,
)


def _fk(src_t, src_c, dst_t, dst_c, conf=0.9, is_fk=False, **kw):
    row = {
        "src_table": src_t, "src_column": src_c,
        "dst_table": dst_t, "dst_column": dst_c,
        "final_confidence": conf, "is_fk": is_fk,
    }
    row.update(kw)
    return row


def _num_col(name, null_rate=0.0, unique=False):
    return {"column_name": name, "has_numeric_stats": True,
            "null_rate": null_rate, "is_unique_candidate": unique,
            "cardinality_ratio": 1.0 if unique else 0.3}


def _key_col(name):
    return {"column_name": name, "has_numeric_stats": False,
            "null_rate": 0.0, "is_unique_candidate": True,
            "cardinality_ratio": 1.0}


def _attr_col(name):
    """A descriptive (non-numeric, non-unique) attribute column."""
    return {"column_name": name, "has_numeric_stats": False,
            "null_rate": 0.0, "is_unique_candidate": False,
            "cardinality_ratio": 0.2}


class TestStarSchema:
    """One fact referencing several dimensions."""

    def _rec(self):
        tables = ["c.s.fct_orders", "c.s.dim_customer", "c.s.dim_product"]
        fks = [
            _fk("c.s.fct_orders", "customer_id", "c.s.dim_customer", "id", conf=0.9),
            _fk("c.s.fct_orders", "product_id", "c.s.dim_product", "id", conf=0.9),
        ]
        profiling = {
            "c.s.fct_orders": [_num_col("amount"), _num_col("quantity"), _num_col("discount")],
            "c.s.dim_customer": [_key_col("id"), {"column_name": "name", "has_numeric_stats": False}],
            "c.s.dim_product": [_key_col("id")],
        }
        return recommend_erd(tables, fk_rows=fks, profiling_by_table=profiling)

    def test_schema_type_is_star(self):
        assert self._rec().schema_type == "STAR"

    def test_fact_table_role(self):
        rec = self._rec()
        fct = next(n for n in rec.nodes if n.table.endswith("fct_orders"))
        assert fct.role == "fact"
        assert "amount" in fct.measurable_columns

    def test_dimension_roles(self):
        rec = self._rec()
        dims = [n for n in rec.nodes if n.table.endswith(("dim_customer", "dim_product"))]
        assert all(d.role == "dimension" for d in dims)

    def test_edges_built_from_fks(self):
        rec = self._rec()
        assert len(rec.edges) == 2
        assert all(e.source == "confirmed" for e in rec.edges)  # conf >= 0.85

    def test_dim_grain_detected(self):
        rec = self._rec()
        cust = next(n for n in rec.nodes if n.table.endswith("dim_customer"))
        assert cust.grain == "id"


class TestSnowflakeSchema:
    """A bridge table that is both an FK source and destination."""

    def test_bridge_detected_and_snowflake(self):
        tables = ["c.s.fct_sales", "c.s.dim_store", "c.s.dim_region"]
        fks = [
            _fk("c.s.fct_sales", "store_id", "c.s.dim_store", "id", conf=0.9),
            _fk("c.s.dim_store", "region_id", "c.s.dim_region", "id", conf=0.9),
        ]
        rec = recommend_erd(tables, fk_rows=fks)
        assert rec.schema_type == "SNOWFLAKE"
        store = next(n for n in rec.nodes if n.table.endswith("dim_store"))
        assert store.role == "bridge"


class TestSimpleSchema:
    """No FKs -> single-table views, no fabricated joins."""

    def test_no_fks_is_simple(self):
        tables = ["c.s.events"]
        rec = recommend_erd(tables, fk_rows=[],
                            profiling_by_table={"c.s.events": [_num_col("value")]})
        assert rec.schema_type == "SIMPLE"
        assert rec.edges == []

    def test_no_signal_defaults_to_source(self):
        rec = recommend_erd(["c.s.plain_table"], fk_rows=[])
        assert rec.nodes[0].role == "source"


class TestEdgeConfidence:
    def test_low_confidence_predicted_fk_dropped(self):
        edges = _build_edges([_fk("a", "x", "b", "y", conf=0.2, is_fk=False)])
        assert edges == []

    def test_confirmed_flag_wins_over_low_confidence(self):
        edges = _build_edges([_fk("c.s.a", "x", "c.s.b", "y", conf=0.1, is_fk=True)])
        assert len(edges) == 1
        assert edges[0].source == "confirmed"
        assert edges[0].confidence == 1.0

    def test_mid_confidence_is_predicted(self):
        edges = _build_edges([_fk("c.s.a", "x", "c.s.b", "y",
                                  conf=(FK_CONFIRMED_MIN - 0.1), is_fk=False)])
        assert len(edges) == 1
        assert edges[0].source == "predicted"


class TestSufficiency:
    """Recommended count is coverage-aware, not a naive fraction of tables."""

    def _star_tables(self):
        return ["c.s.fct_orders", "c.s.dim_customer", "c.s.dim_product"]

    def _fks(self):
        return [
            _fk("c.s.fct_orders", "customer_id", "c.s.dim_customer", "id"),
            _fk("c.s.fct_orders", "product_id", "c.s.dim_product", "id"),
        ]

    def test_missing_kpis_raise_recommendation(self):
        no_kpi = recommend_erd(self._star_tables(), fk_rows=self._fks())
        with_kpi = recommend_erd(
            self._star_tables(), fk_rows=self._fks(),
            kpi_coverage={"implemented": [], "missing": ["Revenue", "Margin"], "total": 2},
        )
        assert with_kpi.sufficiency.metric_views_recommended >= \
            no_kpi.sufficiency.metric_views_recommended
        assert with_kpi.sufficiency.missing_kpis == ["Revenue", "Margin"]

    def test_covered_fact_is_not_uncovered(self):
        rec = recommend_erd(
            self._star_tables(), fk_rows=self._fks(),
            existing_defs=[{"source_table": "c.s.fct_orders", "status": "validated"}],
        )
        assert "c.s.fct_orders".lower() not in [t.lower() for t in rec.sufficiency.uncovered_tables]
        assert rec.sufficiency.metric_views_current == 1

    def test_current_views_counts_only_realized(self):
        # created/failed drafts must NOT inflate current_views (which would
        # spuriously shrink the gap); only validated/applied count.
        rec = recommend_erd(
            self._star_tables(), fk_rows=self._fks(),
            existing_defs=[
                {"source_table": "c.s.fct_orders", "status": "created"},
                {"source_table": "c.s.fct_orders", "status": "failed"},
                {"source_table": "c.s.fct_orders", "status": "validated"},
            ],
        )
        assert rec.sufficiency.metric_views_current == 1  # only the validated one

    def test_coverage_aware_differs_from_naive_third(self):
        # 3 tables -> naive num//3 == 1. But an uncovered fact + 2 missing KPIs
        # should push the recommendation above 1.
        rec = recommend_erd(
            self._star_tables(), fk_rows=self._fks(),
            kpi_coverage={"implemented": [], "missing": ["A", "B"], "total": 2},
        )
        naive = len(self._star_tables()) // 3
        assert rec.sufficiency.metric_views_recommended > naive

    def test_gap_never_negative(self):
        rec = recommend_erd(
            self._star_tables(), fk_rows=self._fks(),
            existing_defs=[{"source_table": f"c.s.t{i}", "status": "validated"} for i in range(20)],
        )
        assert rec.sufficiency.metric_views_gap >= 0


class TestOntologyRole:
    def test_ontology_role_influences_classification(self):
        # A plainly-named table with no FKs, but ontology says it's a fact.
        rec = recommend_erd(
            ["c.s.transactions"], fk_rows=[],
            ontology_rows=[{"entity_role": "primary", "source_tables": ["c.s.transactions"]}],
            profiling_by_table={"c.s.transactions": [_num_col("amt"), _num_col("qty"), _num_col("fee")]},
        )
        assert rec.nodes[0].role == "fact"


class TestNonStarAndMart:
    """Tables that aren't fact_/dim_-named: marts, wide denormalized tables."""

    def test_unlabeled_wide_fact_like_table_is_fact_not_dimension(self):
        # No fact naming, but a wide table with many measures + inline attributes
        # and no outbound FKs. Should read as fact/source, NOT dimension.
        cols = ([_num_col(f"m{i}") for i in range(5)]
                + [_attr_col(f"a{i}") for i in range(8)]
                + [_key_col("row_id")])
        rec = recommend_erd(["c.s.sales_wide"], fk_rows=[],
                            profiling_by_table={"c.s.sales_wide": cols})
        assert rec.nodes[0].role in ("fact", "source")
        assert rec.nodes[0].role != "dimension"

    def test_mart_tables_that_join_get_edges(self):
        # Two mart-named tables with an FK between them -> edge exists, they join.
        tables = ["c.s.sales_summary", "c.s.region_rollup"]
        fks = [_fk("c.s.sales_summary", "region_id", "c.s.region_rollup", "region_id", conf=0.9)]
        rec = recommend_erd(tables, fk_rows=fks)
        assert len(rec.edges) == 1

    def test_mart_dominated_no_fks_is_data_mart(self):
        tables = ["c.s.sales_summary", "c.s.revenue_rollup", "c.s.kpi_snapshot"]
        rec = recommend_erd(tables, fk_rows=[])
        assert rec.schema_type == "DATA_MART"

    def test_data_mart_coexists_with_a_fact_name(self):
        # A mart-dominated scope that also contains a fact-named table, no FKs:
        # still DATA_MART (the old logic required zero fact names).
        tables = ["c.s.sales_summary", "c.s.revenue_rollup", "c.s.fct_leftover"]
        rec = recommend_erd(tables, fk_rows=[])
        assert rec.schema_type == "DATA_MART"

    def test_star_still_wins_when_facts_and_fks_present(self):
        # Regression: a real star must NOT be reclassified as DATA_MART just
        # because one table happens to contain a mart keyword.
        tables = ["c.s.fct_orders", "c.s.dim_customer", "c.s.dim_product"]
        fks = [
            _fk("c.s.fct_orders", "customer_id", "c.s.dim_customer", "id"),
            _fk("c.s.fct_orders", "product_id", "c.s.dim_product", "id"),
        ]
        rec = recommend_erd(tables, fk_rows=fks)
        assert rec.schema_type == "STAR"


class TestSerialization:
    def test_to_dict_is_json_serializable(self):
        import json
        rec = recommend_erd(["c.s.fct_orders", "c.s.dim_customer"],
                            fk_rows=[_fk("c.s.fct_orders", "cid", "c.s.dim_customer", "id")])
        assert isinstance(rec, ErdRecommendation)
        json.dumps(rec.to_dict())  # must not raise


class TestQuestionsKpisSufficiency:
    def _star(self):
        return {
            "tables": ["c.s.fct_orders", "c.s.dim_customer", "c.s.dim_product"],
            "fk_rows": [
                _fk("c.s.fct_orders", "customer_id", "c.s.dim_customer", "id"),
                _fk("c.s.fct_orders", "product_id", "c.s.dim_product", "id"),
            ],
        }

    def test_returns_both_generators(self):
        out = recommend_questions_kpis(**self._star())
        assert set(out) == {"questions", "kpis"}
        assert isinstance(out["questions"], GenSufficiency)
        assert out["questions"].kind == "questions"
        assert out["kpis"].kind == "kpis"

    def test_empty_scope_still_recommends_a_few_questions(self):
        out = recommend_questions_kpis(**self._star(), current_questions=0)
        assert out["questions"].recommended >= QUESTIONS_PER_FACT
        assert out["questions"].should_generate_more is True

    def test_enough_questions_stops_recommending(self):
        out = recommend_questions_kpis(**self._star(), current_questions=100)
        assert out["questions"].should_generate_more is False
        assert out["questions"].gap == 0

    def test_domains_raise_question_target(self):
        base = recommend_questions_kpis(**self._star())
        with_domains = recommend_questions_kpis(
            **self._star(),
            ontology_rows=[
                {"entity_type": "Order", "source_tables": ["c.s.fct_orders"]},
                {"entity_type": "Customer", "source_tables": ["c.s.dim_customer"]},
            ],
        )
        assert with_domains["questions"].recommended >= base["questions"].recommended

    def test_missing_kpis_force_more_even_when_count_met(self):
        out = recommend_questions_kpis(
            **self._star(), current_kpis=100,
            kpi_coverage={"implemented": [], "missing": ["Revenue"], "total": 1},
        )
        # count target is met, but an unimplemented KPI still flags "generate more"
        assert out["kpis"].should_generate_more is True

    def test_kpi_target_scales_with_facts(self):
        out = recommend_questions_kpis(**self._star())
        # one fact * KPIS_PER_FACT
        assert out["kpis"].recommended >= 2

    def test_serializable(self):
        import json
        out = recommend_questions_kpis(**self._star())
        json.dumps({k: v.to_dict() for k, v in out.items()})


class TestInferRoleTiebreak:
    """Role ties must resolve by MEANINGFUL signal (FK topology/measures), not by
    dict-insertion order which always picked 'fact' (review finding #5)."""

    def _num(self, name):
        return {"column_name": name, "has_numeric_stats": True}

    def test_dim_named_with_outbound_fks_resolves_to_fact(self):
        # dimension naming (+0.35) TIES fact outbound FKs (+0.35). Outbound FKs
        # are the deciding signal -> fact.
        role, _, _ = _infer_role(
            "c.s.dim_activity", [self._num("a"), self._num("b")],
            outbound_fk_count=2, inbound_fk_count=0, ontology_role=None, is_bridge=False)
        assert role == "fact"

    def test_fact_named_referenced_only_resolves_to_dimension(self):
        # fact naming (+0.35) TIES dimension inbound-only (+0.35). Inbound-only +
        # a grain key -> dimension wins the tie.
        role, _, _ = _infer_role(
            "c.s.fact_lookup", [{"column_name": "id", "is_unique": True, "has_numeric_stats": False}],
            outbound_fk_count=0, inbound_fk_count=3, ontology_role=None, is_bridge=False)
        assert role == "dimension"

    def test_clear_winner_unaffected(self):
        role, _, _ = _infer_role(
            "c.s.fct_orders", [self._num("amount"), self._num("qty"), self._num("disc")],
            outbound_fk_count=3, inbound_fk_count=0, ontology_role=None, is_bridge=False)
        assert role == "fact"

    def test_suffixless_measure_heavy_table_is_fact(self):
        # PQ-5: a table with NO fct_/dim_ prefix but 3+ measures + outbound FKs must
        # still classify as fact purely on structure (naming is only a tie-breaker now).
        role, _, _ = _infer_role(
            "c.s.transactions",  # no prefix
            [self._num("amount"), self._num("quantity"), self._num("discount")],
            outbound_fk_count=2, inbound_fk_count=0, ontology_role=None, is_bridge=False)
        assert role == "fact"

    def test_fk_topology_outweighs_misleading_name(self):
        # PQ-5: a table misleadingly named like a dimension but that references many
        # tables (outbound FKs) + has measures resolves to fact -- data beats naming.
        role, _, _ = _infer_role(
            "c.s.dim_sales_event",  # dim-prefixed but behaves like a fact
            [self._num("revenue"), self._num("units"), self._num("cost")],
            outbound_fk_count=3, inbound_fk_count=0, ontology_role=None, is_bridge=False)
        assert role == "fact"


class TestRecommendViewCountFloor:
    """_recommend_view_count must never recommend FEWER views than already exist,
    even when the existing count exceeds MAX_RECOMMENDED_VIEWS (the old min(max(...))
    could pull the recommendation below current_views and misreport the gap)."""

    def test_never_below_current_when_over_cap(self):
        rec, _ = _recommend_view_count(
            facts=["c.s.fct"], uncovered_tables=[], missing_kpis=[],
            current_views=MAX_RECOMMENDED_VIEWS + 5,
        )
        assert rec >= MAX_RECOMMENDED_VIEWS + 5   # not clamped below what exists

    def test_normal_case_still_capped(self):
        rec, _ = _recommend_view_count(
            facts=["c.s.f1", "c.s.f2"], uncovered_tables=[], missing_kpis=[],
            current_views=0,
        )
        assert rec == 2   # 2 facts x 1 view, under the cap

    def test_fact_base_is_floor(self):
        rec, _ = _recommend_view_count(
            facts=["c.s.f1", "c.s.f2", "c.s.f3"], uncovered_tables=[], missing_kpis=[],
            current_views=1,
        )
        assert rec >= 3   # never below the fact base
