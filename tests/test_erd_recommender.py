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
    _grain_column,
    _measurable_columns,
    _is_key_like,
    _is_continuous_numeric,
    _is_integer_numeric,
    _name_hint,
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


def _num_col(name, null_rate=0.0, unique=False, data_type="double"):
    # data_type drives key-vs-measure: continuous (double/decimal) -> measure even
    # when unique; integer -> identifier when unique. Default continuous (a measure).
    return {"column_name": name, "has_numeric_stats": True,
            "null_rate": null_rate, "is_unique_candidate": unique,
            "cardinality_ratio": 1.0 if unique else 0.3,
            "data_type": data_type}


def _int_id_col(name, data_type="bigint"):
    """A unique integer identifier column (surrogate/natural key shape)."""
    return {"column_name": name, "has_numeric_stats": True,
            "null_rate": 0.0, "is_unique_candidate": True,
            "cardinality_ratio": 1.0, "data_type": data_type}


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

    def test_missing_kpis_are_surfaced_not_added_as_views(self):
        # Missing KPIs are a coverage NOTE (add measures to the relevant grain
        # view), NOT extra views -- the recommended count must not change.
        no_kpi = recommend_erd(self._star_tables(), fk_rows=self._fks())
        with_kpi = recommend_erd(
            self._star_tables(), fk_rows=self._fks(),
            kpi_coverage={"implemented": [], "missing": ["Revenue", "Margin"], "total": 2},
        )
        assert with_kpi.sufficiency.metric_views_recommended == \
            no_kpi.sufficiency.metric_views_recommended
        assert with_kpi.sufficiency.missing_kpis == ["Revenue", "Margin"]
        assert any("KPI" in r for r in with_kpi.sufficiency.reasons)

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

    def test_single_fact_star_recommends_one_grain_view(self):
        # 1 fact + 2 connected dims -> ONE comprehensive grain view (dims join in),
        # NOT one-per-table and NOT inflated by missing KPIs.
        rec = recommend_erd(
            self._star_tables(), fk_rows=self._fks(),
            kpi_coverage={"implemented": [], "missing": ["A", "B"], "total": 2},
        )
        assert rec.sufficiency.metric_views_recommended == 1
        assert any("grain anchor" in r for r in rec.sufficiency.reasons)
        assert not any("fact table(s)" in r for r in rec.sufficiency.reasons)

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


class TestRecommendViewCountGrainTarget:
    """The recommended count is the GRAIN target (anchors + diminishing orphan bump),
    INDEPENDENT of how many views already exist. Flooring it at current_views made the
    number drift as views were applied, so the displayed 'recommended: N' diverged from
    the generation cap actually enforced (regression that produced 3 views for a shown
    'recommended: 1')."""

    def test_not_floored_at_current_views(self):
        # 1 grain anchor, many existing views -> recommend the grain target (1), NOT the
        # existing count. current_views drives the gap in the caller, not the target.
        rec, _ = _recommend_view_count(
            anchors=["c.s.fct"], orphans=[], missing_kpis=[],
            current_views=MAX_RECOMMENDED_VIEWS + 5,
        )
        assert rec == 1

    def test_normal_case_still_capped(self):
        rec, _ = _recommend_view_count(
            anchors=["c.s.f1", "c.s.f2"], orphans=[], missing_kpis=[],
            current_views=0,
        )
        assert rec == 2   # 2 grain anchors -> 2 views

    def test_anchor_base_drives_count(self):
        rec, _ = _recommend_view_count(
            anchors=["c.s.f1", "c.s.f2", "c.s.f3"], orphans=[], missing_kpis=[],
            current_views=1,
        )
        assert rec == 3   # grain target, not inflated or deflated by current_views


class TestGrainAnchorCount:
    """The recommended count consolidates around grain anchors, not table count,
    and missing KPIs do not inflate it."""

    def test_pure_anchor_count_ignores_kpis(self):
        # 4 anchors, 15 missing KPIs -> still 4 views (KPIs are a note).
        rec, reasons = _recommend_view_count(
            anchors=[f"c.s.f{i}" for i in range(4)], orphans=[],
            missing_kpis=[f"kpi{i}" for i in range(15)], current_views=0,
        )
        assert rec == 4
        assert any("grain anchor" in r for r in reasons)
        assert any("KPI" in r and "not a new view" in r for r in reasons)

    def test_disconnected_bonus_is_diminishing(self):
        # 5 anchors + 10 standalone tables -> 5 + floor(sqrt(10))=3 = 8; never
        # approaches the 15-table total.
        rec, reasons = _recommend_view_count(
            anchors=[f"c.s.f{i}" for i in range(5)],
            orphans=[f"c.s.o{i}" for i in range(10)],
            missing_kpis=[], current_views=0,
        )
        assert rec == 8
        assert any("standalone" in r for r in reasons)

    def test_no_clear_anchor_relabels(self):
        rec, reasons = _recommend_view_count(
            anchors=["c.s.t1", "c.s.t2"], orphans=[], missing_kpis=[],
            current_views=0, no_clear_anchor=True,
        )
        assert any("no clear fact/grain" in r for r in reasons)
        assert not any("fact table(s)" in r for r in reasons)


class TestMeasurelessFactIsAnchor:
    """A fact-role table with ZERO numeric measures (an event log of ids/keys) is still a
    grain anchor -- COUNT(*) is its measure. Regression (epic_emr_demo): fact_clinical_event
    (0 measures) was dropped from the anchor count because a sibling fact (fact_encounter)
    had measures, so the relax-to-all branch never fired -> 1 grain anchor instead of 2."""

    def _rec(self):
        tables = ["c.s.fct_events", "c.s.fct_sales", "c.s.dim_customer", "c.s.dim_product"]
        fks = [
            _fk("c.s.fct_events", "customer_id", "c.s.dim_customer", "id", conf=0.9),
            _fk("c.s.fct_events", "product_id", "c.s.dim_product", "id", conf=0.9),
            _fk("c.s.fct_sales", "customer_id", "c.s.dim_customer", "id", conf=0.9),
            _fk("c.s.fct_sales", "product_id", "c.s.dim_product", "id", conf=0.9),
        ]
        profiling = {
            # fct_events: only key/id columns -> 0 measurable columns
            "c.s.fct_events": [_key_col("event_id"), _int_id_col("customer_id"), _int_id_col("product_id")],
            # fct_sales: has a continuous numeric measure
            "c.s.fct_sales": [_num_col("amount"), _int_id_col("customer_id"), _int_id_col("product_id")],
            "c.s.dim_customer": [_key_col("id")],
            "c.s.dim_product": [_key_col("id")],
        }
        return recommend_erd(tables, fk_rows=fks, profiling_by_table=profiling)

    def test_both_facts_counted_as_grain_anchors(self):
        rec = self._rec()
        facts = [n for n in rec.nodes if n.role == "fact"]
        assert {n.table for n in facts} == {"c.s.fct_events", "c.s.fct_sales"}
        # the event fact genuinely has no numeric measures ...
        events = next(n for n in rec.nodes if n.table.endswith("fct_events"))
        assert events.measurable_columns == []
        # ... yet BOTH grains are recommended (2), not just the measure-bearing one.
        assert rec.sufficiency.metric_views_recommended == 2
        assert any("2 grain anchor" in r for r in rec.sufficiency.reasons)


class TestStringTypedFlagsFromExecuteSql:
    """The app feeds recommend_erd from execute_sql, whose SQL-API data_array returns
    EVERY value as a STRING -- so is_unique_candidate/has_numeric_stats/is_fk arrive as
    'true'/'false'. bool('false') is True, which used to mark every integer column a
    unique key -> dropped from measures -> integer-grain fact tables (supplychain_gold
    order_lines/inventory_snapshots with LONG quantities) mislabeled bridge/dimension."""

    def _scol(self, name, data_type="long", unique="false", numeric="true", card="0.02"):
        # a profiling row exactly as execute_sql returns it: ALL values are strings
        return {"column_name": name, "has_numeric_stats": numeric, "is_unique_candidate": unique,
                "cardinality_ratio": card, "null_rate": "0.0", "data_type": data_type}

    def test_string_false_flag_not_treated_as_unique_key(self):
        # a LONG measure whose is_unique_candidate is the STRING "false" must be a measure
        assert _is_key_like(self._scol("quantity")) is False

    def test_integer_measure_fact_detected_with_string_flags(self):
        tables = ["c.s.order_lines", "c.s.orders", "c.s.products"]
        fks = [
            {"src_table": "c.s.order_lines", "src_column": "order_id", "dst_table": "c.s.orders",
             "dst_column": "order_id", "final_confidence": "1.0", "is_fk": "true"},
            {"src_table": "c.s.order_lines", "src_column": "product_id", "dst_table": "c.s.products",
             "dst_column": "sku_id", "final_confidence": "1.0", "is_fk": "true"},
        ]
        prof = {
            "c.s.order_lines": [self._scol("order_id", unique="false", card="0.5"),
                                self._scol("product_id", card="0.1"),
                                self._scol("quantity"), self._scol("unit_price"), self._scol("line_total")],
            "c.s.orders": [self._scol("order_id", unique="true", card="1.0")],
            "c.s.products": [self._scol("sku_id", unique="true", card="1.0")],
        }
        rec = recommend_erd(tables, fk_rows=fks, profiling_by_table=prof)
        ol = next(n for n in rec.nodes if n.table.endswith("order_lines"))
        assert ol.role == "fact"
        assert set(ol.measurable_columns) >= {"quantity", "unit_price", "line_total"}


class TestNeverJoinsVeto:
    """A pair the data probe PROVED does not join (join_matched=0 AND ri_score=0) must be
    dropped from ERD edges even when a stale/ERD-confirmed is_fk=true row exists for it --
    so a false FK (e.g. sku_id=order_id, pre-populated then saved from the ERD designer)
    can't be shown or re-confirmed and then drive wrong metric-view joins."""

    def test_probe_rejected_pair_excluded_even_when_is_fk_true(self):
        fks = [
            # ERD-confirmed row: is_fk true, no probe data -> would normally become an edge
            {"src_table": "c.s.inv", "src_column": "sku_id", "dst_table": "c.s.ol",
             "dst_column": "order_id", "final_confidence": "1.0", "is_fk": "true"},
            # its structural twin: the probe PROVED no join
            {"src_table": "c.s.inv", "src_column": "c.s.inv.sku_id", "dst_table": "c.s.ol",
             "dst_column": "c.s.ol.order_id", "final_confidence": "0.13", "is_fk": "false",
             "join_matched": "0", "ri_score": "0"},
            # a real, joining FK for contrast
            {"src_table": "c.s.ol", "src_column": "order_id", "dst_table": "c.s.o",
             "dst_column": "order_id", "final_confidence": "0.97", "is_fk": "true",
             "join_matched": "7500", "ri_score": "1.0"},
        ]
        pairs = {(e.src.split(".")[-1], e.dst.split(".")[-1]) for e in _build_edges(fks)}
        assert ("inv", "ol") not in pairs   # vetoed: probe proved no join
        assert ("ol", "o") in pairs          # real join kept


class TestGrainKeyAndMeasures:
    """A high-cardinality continuous MEASURE (e.g. instrument_revenue) must never
    be picked as the grain key, and must still count as a measure. Real join keys
    (id/*_id names or FK columns) win the grain slot."""

    def test_measure_named_unique_numeric_is_not_the_grain(self):
        rows = [_num_col("instrument_revenue", unique=True), _key_col("account_id")]
        assert _grain_column(rows) == "account_id"

    def test_measure_named_unique_numeric_still_counts_as_measure(self):
        rows = [_num_col("instrument_revenue", unique=True), _num_col("volume")]
        measures = _measurable_columns(rows)
        assert "instrument_revenue" in measures
        assert "volume" in measures

    def test_fk_column_hint_wins_grain_over_measure(self):
        # Among unique candidates, the actual join key (marked via key_hints, and
        # not an id-like name) beats a unique measure column.
        rows = [_num_col("total_cost", unique=True),
                {"column_name": "provider_ref", "has_numeric_stats": False,
                 "null_rate": 0.0, "is_unique_candidate": True, "cardinality_ratio": 1.0}]
        assert _grain_column(rows, key_hints={"provider_ref"}) == "provider_ref"

    def test_named_key_still_excluded_from_measures(self):
        # A near-unique numeric that DOES look like a key stays out of measures.
        rows = [{"column_name": "order_id", "has_numeric_stats": True,
                 "null_rate": 0.0, "is_unique_candidate": True, "cardinality_ratio": 1.0}]
        assert _measurable_columns(rows) == []

    def test_summary_table_with_fks_classifies_as_anchor_not_dimension(self):
        # procedure_summary-like: mart naming + 2 outbound FKs + measures (one of
        # which, instrument_revenue, is a high-cardinality unique numeric). The
        # grain bug used to flip this to a dimension; it must be a fact/source
        # anchor with the revenue counted as a measure and NOT as the grain.
        tables = ["c.s.procedure_summary", "c.s.account_master", "c.s.contact_master"]
        fks = [
            _fk("c.s.procedure_summary", "account_id", "c.s.account_master", "account_id", conf=0.91),
            _fk("c.s.procedure_summary", "surgeon_id", "c.s.contact_master", "surgeon_id", conf=0.9),
        ]
        profiling = {
            "c.s.procedure_summary": [
                _num_col("instrument_revenue", unique=True),
                _num_col("procedure_volume"),
                _num_col("device_count"),
                _attr_col("approach"),
            ],
            "c.s.account_master": [_key_col("account_id"), _attr_col("name")],
            "c.s.contact_master": [_key_col("surgeon_id"), _attr_col("name")],
        }
        rec = recommend_erd(tables, fk_rows=fks, profiling_by_table=profiling)
        ps = next(n for n in rec.nodes if n.table.endswith("procedure_summary"))
        assert ps.role in ("fact", "source")
        assert "instrument_revenue" in ps.measurable_columns
        assert ps.grain != "instrument_revenue"
        # Only procedure_summary anchors a view; the two master tables join in.
        assert rec.sufficiency.metric_views_recommended == 1
        assert any("grain anchor" in r for r in rec.sufficiency.reasons)


class TestNoClearAnchorFallback:
    def test_all_dimension_schema_does_not_report_fact_tables(self):
        # Two dimension tables (a lookup chain), no measures. The old fallback
        # labeled every table a "fact table"; now it must relabel.
        tables = ["c.s.dim_x", "c.s.dim_y"]
        fks = [_fk("c.s.dim_x", "y_id", "c.s.dim_y", "id", conf=0.9)]
        profiling = {
            "c.s.dim_x": [_key_col("id"), _attr_col("val")],
            "c.s.dim_y": [_key_col("id"), _attr_col("label")],
        }
        rec = recommend_erd(tables, fk_rows=fks, profiling_by_table=profiling)
        assert not any("fact table(s)" in r for r in rec.sufficiency.reasons)
        assert any("no clear fact/grain" in r for r in rec.sufficiency.reasons)


class TestFanoutDetection:
    def test_low_pk_uniqueness_is_fanout(self):
        edges = _build_edges([_fk("c.s.a", "x", "c.s.b", "y", conf=0.9, pk_uniqueness=0.4)])
        assert edges[0].fanout_risk is True
        assert edges[0].cardinality == "one_to_many"

    def test_high_pk_uniqueness_is_safe(self):
        edges = _build_edges([_fk("c.s.a", "x", "c.s.b", "y", conf=0.9, pk_uniqueness=0.95)])
        assert edges[0].fanout_risk is False
        assert edges[0].cardinality == "many_to_one"

    def test_missing_pk_uniqueness_defaults_safe(self):
        edges = _build_edges([_fk("c.s.a", "x", "c.s.b", "y", conf=0.9)])
        assert edges[0].fanout_risk is False

    def test_recommend_erd_surfaces_fanout_warning(self):
        tables = ["c.s.fct_orders", "c.s.dim_customer"]
        fks = [_fk("c.s.fct_orders", "customer_id", "c.s.dim_customer", "id",
                   conf=0.9, pk_uniqueness=0.4)]
        rec = recommend_erd(tables, fk_rows=fks,
                            profiling_by_table={"c.s.fct_orders": [_num_col("amount")]})
        assert len(rec.sufficiency.fanout_warnings) == 1
        assert "fan out" in rec.sufficiency.fanout_warnings[0]


class TestKeyMeasureIsDataDrivenNotNameBased:
    """Key-vs-measure classification must generalize across ALL schemas: it comes
    from FK participation + type/uniqueness, NEVER from column-name vocabulary.
    These tests deliberately use names a naming heuristic would get WRONG."""

    def test_type_helpers(self):
        assert _is_continuous_numeric("double") is True
        assert _is_continuous_numeric("decimal(18,2)") is True
        assert _is_continuous_numeric("float") is True
        assert _is_continuous_numeric("bigint") is False
        assert _is_integer_numeric("bigint") is True
        assert _is_integer_numeric("int") is True
        assert _is_integer_numeric("double") is False
        # Unknown/absent type: neither -- callers fall back conservatively.
        assert _is_continuous_numeric(None) is False
        assert _is_integer_numeric("") is False

    def test_continuous_numeric_is_a_measure_even_with_a_key_like_name(self):
        # Named like an id, but a unique DECIMAL -> a measure, not a key. A
        # name-suffix rule would wrongly call this a key.
        rows = [_num_col("transaction_id", unique=True, data_type="decimal(18,2)")]
        assert "transaction_id" in _measurable_columns(rows)
        assert _grain_column(rows) is None      # a measure is never the grain

    def test_integer_unique_is_an_identifier_even_with_a_measure_like_name(self):
        # Named like a measure ("revenue_key"), but a unique BIGINT -> identifier.
        # A keyword rule ("revenue") would wrongly call this a measure.
        rows = [_int_id_col("revenue_key"), _num_col("net", data_type="double")]
        assert "revenue_key" not in _measurable_columns(rows)
        assert _grain_column(rows) == "revenue_key"
        assert "net" in _measurable_columns(rows)

    def test_fk_participation_beats_type_and_name(self):
        # A continuous-typed column that is actually a join key (key_hints) is a
        # key regardless of its type/name -- FK participation is the primary signal.
        rows = [_num_col("amount", unique=True, data_type="double")]
        assert _grain_column(rows, key_hints={"amount"}) == "amount"
        assert "amount" not in _measurable_columns(rows, {"amount"})

    def test_measure_and_grain_agree_on_every_column(self):
        # The invariant: a column is never both a measure and the grain.
        rows = [_int_id_col("k"), _num_col("m1", data_type="double"),
                _num_col("m2", data_type="decimal(10,2)"), _attr_col("label")]
        measures = set(_measurable_columns(rows))
        grain = _grain_column(rows)
        assert grain not in measures
        assert measures == {"m1", "m2"}
        assert grain == "k"

    def test_unknown_type_uninformative_name_falls_back_to_identifier(self):
        # Thin/federated profiling, no data_type, uninformative name: a unique
        # numeric reads as an ID (conservative default).
        rows = [{"column_name": "x", "has_numeric_stats": True, "null_rate": 0.0,
                 "is_unique_candidate": True, "cardinality_ratio": 1.0}]
        assert _measurable_columns(rows) == []
        assert _grain_column(rows) == "x"


class TestNameIsAMildTiebreakerNotAGate:
    """The name prior is consulted ONLY where the data is silent (unknown-type
    unique numeric) and NEVER overrides FK participation or a clear type."""

    def test_name_hint_is_token_based(self):
        assert _name_hint("account") is None          # NOT "count" (substring)
        assert _name_hint("account_id") == "key"
        assert _name_hint("net_revenue") == "measure"
        assert _name_hint("order_counts") == "measure"  # plural stripped
        assert _name_hint("region") is None
        assert _name_hint("revenue_id") is None        # both signals -> no hint

    def test_name_breaks_tie_only_when_type_unknown(self):
        # Unknown data_type + measure-ish name -> measure; key-ish/blank -> ID.
        measure_named = {"column_name": "gross_amount", "has_numeric_stats": True,
                         "null_rate": 0.0, "is_unique_candidate": True,
                         "cardinality_ratio": 1.0}  # no data_type
        key_named = {"column_name": "member_id", "has_numeric_stats": True,
                     "null_rate": 0.0, "is_unique_candidate": True,
                     "cardinality_ratio": 1.0}      # no data_type
        assert _is_key_like(measure_named) is False   # -> measure
        assert _is_key_like(key_named) is True        # -> identifier

    def test_name_does_not_override_a_clear_type(self):
        # Clear CONTINUOUS type beats a key-ish name; clear INTEGER type beats a
        # measure-ish name. Data wins whenever it speaks.
        cont_key_named = _num_col("member_id", unique=True, data_type="decimal(9,2)")
        int_measure_named = _int_id_col("total_amount", data_type="bigint")
        assert _is_key_like(cont_key_named) is False  # still a measure
        assert _is_key_like(int_measure_named) is True  # still an identifier

    def test_name_does_not_override_fk_participation(self):
        # A measure-ish name that is actually an FK stays a key.
        col = _num_col("sales_amount", unique=True, data_type="double")
        assert _is_key_like(col, key_hints={"sales_amount"}) is True

    def test_grain_name_nudge_orders_equal_candidates(self):
        # Two unknown-type unique integers; the id-named one is preferred as grain
        # (ordering nudge only -- both remain key-like).
        a = {"column_name": "seq", "has_numeric_stats": True, "null_rate": 0.0,
             "is_unique_candidate": True, "cardinality_ratio": 1.0}
        b = {"column_name": "customer_key", "has_numeric_stats": True,
             "null_rate": 0.0, "is_unique_candidate": True, "cardinality_ratio": 1.0}
        assert _grain_column([a, b]) == "customer_key"


class TestNoAnchorRecommendsOneNotTableCount:
    def test_all_dimension_schema_recommends_one_not_table_count(self):
        # No fact/source/bridge and no measures anywhere: must recommend a single
        # starter view, never the raw table count (the reintroduced-bug guard).
        tables = ["c.s.dim_a", "c.s.dim_b", "c.s.dim_c", "c.s.dim_d"]
        fks = [_fk("c.s.dim_a", "b_id", "c.s.dim_b", "id", conf=0.9)]
        profiling = {t: [_key_col("id"), _attr_col("label")] for t in tables}
        rec = recommend_erd(tables, fk_rows=fks, profiling_by_table=profiling)
        assert rec.sufficiency.metric_views_recommended == 1
        assert any("no clear fact/grain" in r for r in rec.sufficiency.reasons)
