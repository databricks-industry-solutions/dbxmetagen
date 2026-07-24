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
    ErdRecommendation,
    GenSufficiency,
    FK_CONFIRMED_MIN,
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
