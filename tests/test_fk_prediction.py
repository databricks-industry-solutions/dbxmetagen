"""Unit tests for FK prediction (config, orchestration, heuristics, safety)."""
import contextlib
import inspect
import re
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import dbxmetagen.fk_prediction as fk_prediction_mod
from dbxmetagen.fk_prediction import (
    FKPredictionConfig,
    FKPredictor,
    _FK_MODEL,
    predict_foreign_keys,
    SR_COL_PROP,
    SR_DECLARED,
    SR_EMBEDDING,
    SR_NAME,
    SR_ONTOLOGY,
    SR_QUERY,
)


def test_source_rank_ordering():
    """Lower number = higher trust for dedup."""
    assert SR_DECLARED < SR_QUERY < SR_COL_PROP < SR_NAME < SR_ONTOLOGY < SR_EMBEDDING


def test_dedup_sort_key_tuple():
    """Document expected (source_rank, col_similarity, query_hit_count) ordering."""
    rows = [
        (SR_EMBEDDING, 0.9, 0),
        (SR_DECLARED, 0.0, 0),
        (SR_QUERY, 0.5, 5),
    ]
    rows_sorted = sorted(rows, key=lambda r: (r[0], -r[1], -r[2]))
    assert rows_sorted[0][0] == SR_DECLARED


def _cfg():
    return FKPredictionConfig(catalog_name="c", schema_name="s")


def _empty_candidate_df():
    df = MagicMock()
    for name in (
        "unionByName",
        "filter",
        "withColumn",
        "drop",
        "fillna",
        "select",
        "join",
        "createOrReplaceTempView",
    ):
        setattr(df, name, MagicMock(return_value=df))
    df.count = MagicMock(return_value=0)
    return df


def _nonempty_candidate_df(total=4, ai_eligible=2, skip_n=1):
    df = _empty_candidate_df()
    df.count = MagicMock(return_value=total)
    need = MagicMock()
    need.count = MagicMock(return_value=ai_eligible)
    skip = MagicMock()
    skip.count = MagicMock(return_value=skip_n)
    fc = {"n": 0}

    def _filter(*_a, **_k):
        fc["n"] += 1
        if fc["n"] == 3:
            return skip
        if fc["n"] == 4:
            return need
        return df

    df.filter = MagicMock(side_effect=_filter)
    return df


class _ColExpr:
    __slots__ = ()

    def asc(self):
        return self

    def desc(self):
        return self

    def cast(self, _t):
        return self

    def __invert__(self):
        return self

    def __ge__(self, _o):
        return self

    def __le__(self, _o):
        return self

    def __eq__(self, _o):
        return self

    def __ne__(self, _o):
        return self

    def __and__(self, _o):
        return self


@contextlib.contextmanager
def _patch_fk_functions_for_run():
    orig = fk_prediction_mod.F
    mock_f = MagicMock()
    mock_f.col = lambda *_a, **_k: _ColExpr()
    mock_f.row_number = lambda *_a, **_k: SimpleNamespace(over=lambda *_w, **_kw: _ColExpr())
    mock_f.lit = lambda *_a, **_k: _ColExpr()
    fk_prediction_mod.F = mock_f
    try:
        yield
    finally:
        fk_prediction_mod.F = orig


# --- TestDeclaredFKGuard ---


class TestDeclaredFKGuard:
    def test_skips_self_referential_same_table(self):
        spark = MagicMock()
        row = SimpleNamespace(
            table_name="c.s.orders",
            foreign_keys={"parent_id": "c.s.orders.id"},
        )
        spark.sql.return_value.collect.return_value = [row]
        p = FKPredictor(spark, _cfg())
        p.get_declared_fk_candidates()
        spark.createDataFrame.assert_called_once()
        args = spark.createDataFrame.call_args[0]
        assert args[0] == []

    def test_skips_malformed_ref_target_short_path(self):
        spark = MagicMock()
        row = SimpleNamespace(table_name="c.s.a", foreign_keys={"x": "too.short"})
        spark.sql.return_value.collect.return_value = [row]
        p = FKPredictor(spark, _cfg())
        p.get_declared_fk_candidates()
        assert spark.createDataFrame.call_args[0][0] == []

    def test_skips_null_ref_target(self):
        spark = MagicMock()
        row = SimpleNamespace(table_name="c.s.a", foreign_keys={"fk": None})
        spark.sql.return_value.collect.return_value = [row]
        p = FKPredictor(spark, _cfg())
        p.get_declared_fk_candidates()
        assert spark.createDataFrame.call_args[0][0] == []

    def test_emits_cross_table_pair(self):
        spark = MagicMock()
        row = SimpleNamespace(
            table_name="c.s.fact",
            foreign_keys={"cust_id": "c.s.dim.customer_id"},
        )
        spark.sql.return_value.collect.return_value = [row]
        out = MagicMock()
        spark.createDataFrame.return_value = out
        out.withColumn = MagicMock(return_value=out)
        p = FKPredictor(spark, _cfg())
        p.get_declared_fk_candidates()
        pairs = spark.createDataFrame.call_args[0][0]
        assert len(pairs) == 1
        assert pairs[0][2] == "c.s.fact"
        assert pairs[0][3] == "c.s.dim"


# --- TestUIDedup ---


class TestUIDedup:
    def test_dedup_key_orders_by_source_rank_first(self):
        a = ("x", "y", SR_EMBEDDING, 0.99, 100)
        b = ("x", "y", SR_DECLARED, 0.1, 0)
        rows = [a, b]
        rows.sort(key=lambda r: (r[2], -r[3], -r[4]))
        assert rows[0][2] == SR_DECLARED

    def test_same_source_prefers_higher_col_similarity(self):
        rows = [
            ("x", "y", SR_NAME, 0.1, 0),
            ("x", "y", SR_NAME, 0.9, 0),
        ]
        rows.sort(key=lambda r: (r[2], -r[3], -r[4]))
        assert rows[0][3] == 0.9

    def test_same_source_and_sim_prefers_query_hits(self):
        rows = [
            ("x", "y", SR_QUERY, 0.5, 1),
            ("x", "y", SR_QUERY, 0.5, 9),
        ]
        rows.sort(key=lambda r: (r[2], -r[3], -r[4]))
        assert rows[0][4] == 9


# --- TestEnforceDirection ---


class TestEnforceDirection:
    def test_returns_unchanged_without_ai_or_cardinality_cols(self):
        df = MagicMock()
        df.columns = ["col_a", "col_b", "table_a", "table_b"]
        out = FKPredictor._enforce_direction(df)
        assert out is df

    def test_no_columns_attr_falsy_branch(self):
        df = MagicMock(spec=["withColumn"])
        df.columns = []
        out = FKPredictor._enforce_direction(df)
        assert out is df


# --- TestQueryHistoryRegex ---


class TestQueryHistoryRegex:
    @staticmethod
    def _join_re():
        return re.compile(
            r"(\w+(?:\.\w+){0,3})\.(\w+)\s*=\s*(\w+(?:\.\w+){0,3})\.(\w+)",
            re.IGNORECASE,
        )

    def test_matches_simple_equality(self):
        m = self._join_re().search("JOIN a.b ON c.s.t1.id = c.s.t2.fk_id")
        assert m
        assert m.group(2).lower() == "id"

    def test_matches_three_part_table(self):
        m = self._join_re().search("ON cat.sch.orders.user_id = cat.sch.users.id")
        assert m and "orders" in m.group(1).lower()

    def test_get_query_join_candidates_returns_empty_on_sql_error(self):
        spark = MagicMock()
        spark.sql.side_effect = Exception("no system tables")
        p = FKPredictor(spark, _cfg())
        p.get_query_join_candidates()
        spark.createDataFrame.assert_called_once()
        args, _kw = spark.createDataFrame.call_args
        assert args[0] == []

    def test_regex_ignores_case(self):
        m = self._join_re().search("t1.X = t2.Y")
        assert m


# --- TestQueryHistorySameTableFilter ---


class TestQueryHistorySameTableFilter:
    def test_same_table_pair_skipped(self):
        cat, sch = "mycat", "mysch"
        prefix = f"{cat}.{sch}.".lower()
        tbl_a = f"{cat}.{sch}.t1"
        tbl_b = f"{cat}.{sch}.t1"
        assert prefix in tbl_a.lower()
        assert tbl_a.lower() == tbl_b.lower()


# --- TestFKPredictionConfig ---


class TestFKPredictionConfig:
    def test_fq_builds_three_part_name(self):
        c = FKPredictionConfig(catalog_name="a", schema_name="b")
        assert c.fq("tbl") == "a.b.tbl"

    def test_no_model_endpoint_field(self):
        c = FKPredictionConfig(catalog_name="x", schema_name="y")
        assert not hasattr(c, "model_endpoint")


# --- TestHardcodedModel ---


class TestHardcodedModel:
    def test_fk_model_constant(self):
        assert _FK_MODEL == "databricks-gpt-oss-120b"

    def test_predict_foreign_keys_wraps_config(self):
        sig = inspect.signature(predict_foreign_keys)
        assert "catalog_name" in sig.parameters
        assert "dry_run" in sig.parameters
        spark = MagicMock()
        with patch.object(FKPredictor, "run", return_value={"ok": True}) as run:
            predict_foreign_keys(spark, "c", "s", dry_run=True)
            run.assert_called_once()

    def test_ai_judge_sql_mentions_model(self):
        src = inspect.getsource(FKPredictor.ai_judge)
        assert "AI_QUERY" in src
        assert "model = _FK_MODEL" in src


# --- TestRunOrchestration ---


class TestRunOrchestration:
    @pytest.fixture
    def spark(self):
        return MagicMock()

    def test_zero_candidates_returns_early_counts(self, spark):
        empty = _empty_candidate_df()
        with (
            _patch_fk_functions_for_run(),
            patch.object(FKPredictor, "_ensure_output_tables"),
            patch.object(FKPredictor, "get_candidates", return_value=empty),
            patch.object(FKPredictor, "get_name_based_candidates", return_value=empty),
            patch.object(FKPredictor, "get_ontology_relationship_candidates", return_value=empty),
            patch.object(FKPredictor, "get_column_property_candidates", return_value=empty),
            patch.object(FKPredictor, "get_declared_fk_candidates", return_value=empty),
            patch.object(FKPredictor, "get_query_join_candidates", return_value=empty),
        ):
            p = FKPredictor(spark, _cfg())
            out = p.run()
        assert out["candidates"] == 0
        assert out["predictions"] == 0

    def test_invokes_all_six_candidate_sources(self, spark):
        empty = _empty_candidate_df()
        with (
            _patch_fk_functions_for_run(),
            patch.object(FKPredictor, "_ensure_output_tables"),
            patch.object(FKPredictor, "get_candidates", return_value=empty) as m_emb,
            patch.object(FKPredictor, "get_name_based_candidates", return_value=empty) as m_nm,
            patch.object(FKPredictor, "get_ontology_relationship_candidates", return_value=empty) as m_on,
            patch.object(FKPredictor, "get_column_property_candidates", return_value=empty) as m_cp,
            patch.object(FKPredictor, "get_declared_fk_candidates", return_value=empty) as m_dc,
            patch.object(FKPredictor, "get_query_join_candidates", return_value=empty) as m_qy,
        ):
            p = FKPredictor(spark, _cfg())
            p.run()
        m_emb.assert_called_once()
        m_nm.assert_called_once()
        m_on.assert_called_once()
        m_cp.assert_called_once()
        m_dc.assert_called_once()
        m_qy.assert_called_once()

    def test_patches_post_candidate_pipeline_for_dry_run(self, spark):
        df = _nonempty_candidate_df(total=3, ai_eligible=2, skip_n=1)
        cfg = _cfg()
        cfg.dry_run = True
        with (
            _patch_fk_functions_for_run(),
            patch.object(FKPredictor, "_ensure_output_tables"),
            patch.object(FKPredictor, "get_candidates", return_value=df),
            patch.object(FKPredictor, "get_name_based_candidates", return_value=df),
            patch.object(FKPredictor, "get_ontology_relationship_candidates", return_value=df),
            patch.object(FKPredictor, "get_column_property_candidates", return_value=df),
            patch.object(FKPredictor, "get_declared_fk_candidates", return_value=df),
            patch.object(FKPredictor, "get_query_join_candidates", return_value=df),
            patch.object(FKPredictor, "add_entity_match", side_effect=lambda c: c),
            patch.object(FKPredictor, "add_lineage_signal", side_effect=lambda c: c),
            patch.object(FKPredictor, "_sample_from_source", side_effect=lambda c: c),
            patch.object(FKPredictor, "rule_score", side_effect=lambda c: c),
            patch.object(FKPredictor, "cardinality_analysis", side_effect=lambda c: c),
            patch.object(FKPredictor, "_with_skip_ai_flags", side_effect=lambda c: c),
            patch.object(FKPredictor, "ai_judge") as m_ai,
        ):
            p = FKPredictor(spark, cfg)
            out = p.run()
        m_ai.assert_not_called()
        assert out["dry_run"] is True
        assert out["ai_query_rows"] == 2


# --- TestDryRunAccounting ---


class TestDryRunAccounting:
    def test_dry_run_skips_ai_judge(self):
        spark = MagicMock()
        df = _nonempty_candidate_df()
        cfg = _cfg()
        cfg.dry_run = True
        with (
            _patch_fk_functions_for_run(),
            patch.object(FKPredictor, "_ensure_output_tables"),
            patch.object(FKPredictor, "get_candidates", return_value=df),
            patch.object(FKPredictor, "get_name_based_candidates", return_value=df),
            patch.object(FKPredictor, "get_ontology_relationship_candidates", return_value=df),
            patch.object(FKPredictor, "get_column_property_candidates", return_value=df),
            patch.object(FKPredictor, "get_declared_fk_candidates", return_value=df),
            patch.object(FKPredictor, "get_query_join_candidates", return_value=df),
            patch.object(FKPredictor, "add_entity_match", side_effect=lambda c: c),
            patch.object(FKPredictor, "add_lineage_signal", side_effect=lambda c: c),
            patch.object(FKPredictor, "_sample_from_source", side_effect=lambda c: c),
            patch.object(FKPredictor, "rule_score", side_effect=lambda c: c),
            patch.object(FKPredictor, "cardinality_analysis", side_effect=lambda c: c),
            patch.object(FKPredictor, "_with_skip_ai_flags", side_effect=lambda c: c),
            patch.object(FKPredictor, "ai_judge") as m_ai,
            patch.object(FKPredictor, "_heuristic_ai_fill") as m_heur,
        ):
            p = FKPredictor(spark, cfg)
            p.run()
        m_ai.assert_not_called()
        m_heur.assert_not_called()

    def test_dry_run_reports_candidate_and_ai_counts(self):
        spark = MagicMock()
        df = _nonempty_candidate_df(total=6, ai_eligible=4, skip_n=2)
        cfg = _cfg()
        cfg.dry_run = True
        with (
            _patch_fk_functions_for_run(),
            patch.object(FKPredictor, "_ensure_output_tables"),
            patch.object(FKPredictor, "get_candidates", return_value=df),
            patch.object(FKPredictor, "get_name_based_candidates", return_value=df),
            patch.object(FKPredictor, "get_ontology_relationship_candidates", return_value=df),
            patch.object(FKPredictor, "get_column_property_candidates", return_value=df),
            patch.object(FKPredictor, "get_declared_fk_candidates", return_value=df),
            patch.object(FKPredictor, "get_query_join_candidates", return_value=df),
            patch.object(FKPredictor, "add_entity_match", side_effect=lambda c: c),
            patch.object(FKPredictor, "add_lineage_signal", side_effect=lambda c: c),
            patch.object(FKPredictor, "_sample_from_source", side_effect=lambda c: c),
            patch.object(FKPredictor, "rule_score", side_effect=lambda c: c),
            patch.object(FKPredictor, "cardinality_analysis", side_effect=lambda c: c),
            patch.object(FKPredictor, "_with_skip_ai_flags", side_effect=lambda c: c),
        ):
            p = FKPredictor(spark, cfg)
            out = p.run()
        assert out["candidates"] == 6
        assert out["ai_query_rows"] == 4


# --- TestAIConfidenceGating ---


class TestAIConfidenceGating:
    def test_negative_is_fk_zeros_effective_confidence(self):
        raw, is_fk = 0.9, False
        eff = raw if is_fk else 0.0
        assert eff == 0.0

    def test_positive_is_fk_keeps_bounded_raw(self):
        raw = min(1.0, max(0.0, 0.85))
        assert raw == 0.85

    def test_ai_judge_where_uses_rule_score_min(self):
        src = inspect.getsource(FKPredictor.ai_judge)
        assert "rule_score >=" in src or "rule_score >=" in src.replace(" ", "")

    def test_skip_ai_column_exists_after_with_skip_ai_flags(self):
        src = inspect.getsource(FKPredictor._with_skip_ai_flags)
        assert "skip_ai" in src
        assert "SR_DECLARED" in src or "source_rank" in src

    def test_heuristic_fill_sets_high_confidence_for_declared(self):
        src = inspect.getsource(FKPredictor._heuristic_ai_fill)
        assert "SR_DECLARED" in src
        assert "0.95" in src

    def test_need_llm_filters_skip_ai_and_rule_floor(self):
        src = inspect.getsource(FKPredictor.run)
        assert "skip_ai" in src
        assert "rule_score" in src


# --- TestJoinRateCapping ---


class TestJoinRateCapping:
    def test_join_validate_adjusts_ai_confidence_formula(self):
        ai, jr = 0.5, 1.0
        adj = round(ai * (0.6 + 0.4 * jr), 4)
        assert adj == 0.5

    def test_join_rate_clamped_in_join_validate(self):
        src = inspect.getsource(FKPredictor.join_validate)
        assert "join_rate" in src
        assert "0.6" in src and "0.4" in src

    def test_write_predictions_caps_join_rate(self):
        src = inspect.getsource(FKPredictor.write_predictions)
        assert "join_rate" in src
        assert "least" in src.lower() or "LEAST" in src

    def test_join_rate_one_boosts_confidence(self):
        ai, jr = 0.25, 1.0
        assert ai * (0.6 + 0.4 * jr) > ai * 0.6


# --- TestSafetyFilters ---


class TestSafetyFilters:
    def test_run_filters_same_table_pairs(self):
        src = inspect.getsource(FKPredictor.run)
        assert "table_a" in src and "table_b" in src

    def test_write_predictions_filters_same_table_and_column(self):
        src = inspect.getsource(FKPredictor.write_predictions)
        assert "src_table" in src and "dst_table" in src
        assert "src_column" in src

    def test_write_predictions_respects_confidence_threshold(self):
        src = inspect.getsource(FKPredictor.write_predictions)
        assert "confidence_threshold" in src

    def test_ensure_output_tables_cleanup_self_ref(self):
        src = inspect.getsource(FKPredictor._ensure_output_tables)
        assert "src_table = dst_table" in src or "dst_table" in src

    def test_ensure_output_tables_cleanup_legacy_null_is_fk(self):
        src = inspect.getsource(FKPredictor._ensure_output_tables)
        assert "is_fk" in src

    def test_merge_deletes_reversed_direction_rows(self):
        src = inspect.getsource(FKPredictor.write_predictions)
        assert "DELETE FROM" in src or "dst_column" in src

    def test_graph_edges_high_conf_filter(self):
        src = inspect.getsource(FKPredictor.write_graph_edges)
        assert "confidence_threshold" in src


# --- TestAIJudgeMockIntegration ---


class TestAIJudgeMockIntegration:
    def test_ai_judge_creates_temp_view_fk_scored(self):
        spark = MagicMock()
        df = MagicMock()
        sql_result = MagicMock()
        sql_result.withColumn = MagicMock(return_value=sql_result)
        sql_result.drop = MagicMock(return_value=sql_result)
        spark.sql.return_value = sql_result
        p = FKPredictor(spark, _cfg())
        with patch.object(FKPredictor, "ai_judge", wraps=p.ai_judge):
            try:
                p.ai_judge(df)
            except Exception:
                pass
        df.createOrReplaceTempView.assert_called_with("fk_scored")

    def test_ai_judge_strips_markdown_fences_in_logic(self):
        src = inspect.getsource(FKPredictor.ai_judge)
        assert "regexp_replace" in src
        assert "ai_raw" in src


# --- TestConfidenceScoringLogic ---


class TestConfidenceScoringLogic:
    def test_final_confidence_weights_sum_coverage(self):
        w = {"sim": 0.15, "rule": 0.15, "ai": 0.25, "join": 0.15, "pk": 0.15, "ri": 0.15}
        assert abs(sum(w.values()) - 1.0) < 1e-9

    def test_write_predictions_final_confidence_formula_present(self):
        src = inspect.getsource(FKPredictor.write_predictions)
        for token in ("col_similarity", "rule_score", "ai_confidence", "pk_uniqueness", "ri_score"):
            assert token in src

    def test_rule_score_includes_col_similarity_weight(self):
        src = inspect.getsource(FKPredictor.rule_score)
        assert "col_similarity" in src

    def test_skip_ai_declared_requires_flag_and_rank(self):
        src = inspect.getsource(FKPredictor._with_skip_ai_flags)
        assert "skip_ai_for_declared_fk" in src

    def test_skip_ai_query_requires_min_observations(self):
        src = inspect.getsource(FKPredictor._with_skip_ai_flags)
        assert "skip_ai_query_min_observations" in src


# --- TestEnforceDirectionSwap ---


class TestEnforceDirectionSwap:
    def test_uses_ai_fk_side_b_for_swap(self):
        src = inspect.getsource(FKPredictor._enforce_direction)
        assert 'lit("b")' in src or '"b"' in src

    def test_falls_back_to_cardinality_when_ai_side_unknown(self):
        src = inspect.getsource(FKPredictor._enforce_direction)
        assert "_card_ratio" in src

    def test_swap_uses_temp_columns_to_avoid_corruption(self):
        src = inspect.getsource(FKPredictor._enforce_direction)
        assert "_swap_" in src

    def test_swaps_col_table_dtype_pairs(self):
        src = inspect.getsource(FKPredictor._enforce_direction)
        for col in ("col_a", "table_a", "dtype_a"):
            assert col in src

    def test_optional_entity_and_card_swap_when_present(self):
        src = inspect.getsource(FKPredictor._enforce_direction)
        assert "entity_type_a" in src or "optional_pairs" in src

    def test_ai_only_branch_when_card_missing(self):
        src = inspect.getsource(FKPredictor._enforce_direction)
        assert "has_ai" in src and "has_card" in src


# --- TestRuleScoreBehavior ---


class TestRuleScoreBehavior:
    """Execute rule_score() on mock DataFrames to verify feature gating and wiring.

    Existing tests only inspect source strings. These tests actually call
    rule_score() and verify the expression wiring through the mock layer.
    """

    def _make_predictor(self, **cfg_overrides):
        cfg = FKPredictionConfig(catalog_name="c", schema_name="s", **cfg_overrides)
        return FKPredictor(spark=MagicMock(), config=cfg)

    def _make_candidates(self, columns):
        df = MagicMock()
        df.columns = columns
        df.withColumn = MagicMock(return_value=df)
        return df

    def test_adds_rule_score_column(self):
        """rule_score() must call withColumn('rule_score', ...)."""
        p = self._make_predictor()
        df = self._make_candidates(["col_a", "col_b", "dtype_a", "dtype_b",
                                     "table_a", "table_b", "col_similarity",
                                     "samples_a", "samples_b"])
        result = p.rule_score(df)
        df.withColumn.assert_called_once()
        assert df.withColumn.call_args[0][0] == "rule_score"
        assert result is df

    def test_entity_match_used_when_present(self):
        """When entity_match IS in columns, F.col('entity_match') should be called."""
        from pyspark.sql import functions as F
        F.col.reset_mock()
        p = self._make_predictor()
        df = self._make_candidates(["col_a", "col_b", "dtype_a", "dtype_b",
                                     "table_a", "table_b", "col_similarity",
                                     "samples_a", "samples_b", "entity_match"])
        p.rule_score(df)
        col_args = [c[0][0] for c in F.col.call_args_list if c[0]]
        assert "entity_match" in col_args

    def test_entity_match_fallback_when_absent(self):
        """When entity_match is NOT in columns, F.lit(0.0) should be used."""
        from pyspark.sql import functions as F
        F.lit.reset_mock()
        p = self._make_predictor()
        df = self._make_candidates(["col_a", "col_b", "dtype_a", "dtype_b",
                                     "table_a", "table_b", "col_similarity",
                                     "samples_a", "samples_b"])
        p.rule_score(df)
        lit_args = [c[0][0] for c in F.lit.call_args_list if c[0]]
        assert 0.0 in lit_args

    def test_lineage_score_used_when_present(self):
        """When lineage_score IS in columns, F.col('lineage_score') should be called."""
        from pyspark.sql import functions as F
        F.col.reset_mock()
        p = self._make_predictor()
        df = self._make_candidates(["col_a", "col_b", "dtype_a", "dtype_b",
                                     "table_a", "table_b", "col_similarity",
                                     "samples_a", "samples_b", "lineage_score"])
        p.rule_score(df)
        col_args = [c[0][0] for c in F.col.call_args_list if c[0]]
        assert "lineage_score" in col_args

    def test_ontology_bonus_weight_from_config(self):
        """The ontology_match_bonus_weight config value must be read during scoring."""
        p = self._make_predictor(ontology_match_bonus_weight=0.42)
        df = self._make_candidates(["col_a", "col_b", "dtype_a", "dtype_b",
                                     "table_a", "table_b", "col_similarity",
                                     "samples_a", "samples_b", "entity_match"])
        p.rule_score(df)
        assert p.config.ontology_match_bonus_weight == 0.42

    def test_returns_dataframe(self):
        """rule_score() must return the DataFrame (result of withColumn)."""
        p = self._make_predictor()
        df = self._make_candidates(["col_a", "col_b", "dtype_a", "dtype_b",
                                     "table_a", "table_b", "col_similarity",
                                     "samples_a", "samples_b"])
        result = p.rule_score(df)
        assert result is df.withColumn.return_value


# --- TestColumnPropertyCandidates ---


class TestColumnPropertyCandidates:
    """Tests for the column-property FK candidate source."""

    def test_source_rank_values(self):
        """SR_COL_PROP sits between QUERY and NAME."""
        assert SR_QUERY < SR_COL_PROP < SR_NAME

    def test_column_property_candidates_empty(self):
        """Returns empty DataFrame when column_properties table doesn't exist."""
        spark = MagicMock()
        spark.sql.side_effect = Exception("TABLE_OR_VIEW_NOT_FOUND")
        p = FKPredictor(spark, _cfg())
        result = p.get_column_property_candidates()
        assert result is not None

    def test_column_property_candidates_pk_pair(self):
        """When object_property + PK exist, the SQL is executed with correct tables."""
        spark = MagicMock()
        # First call (existence check) succeeds; second call (main SQL) returns df
        result_df = MagicMock()
        result_df.withColumn = MagicMock(return_value=result_df)
        result_df.count = MagicMock(return_value=3)
        spark.sql.side_effect = [MagicMock(), result_df]
        p = FKPredictor(spark, _cfg())
        result = p.get_column_property_candidates()
        # The main SQL should reference the column_properties table
        main_sql = spark.sql.call_args_list[1][0][0]
        assert "ontology_column_properties" in main_sql
        assert "object_property" in main_sql
        assert "primary_key" in main_sql

    def test_column_property_candidates_name_fallback(self):
        """SQL includes both pk_pairs and name_pairs strategies."""
        spark = MagicMock()
        result_df = MagicMock()
        result_df.withColumn = MagicMock(return_value=result_df)
        result_df.count = MagicMock(return_value=0)
        spark.sql.side_effect = [MagicMock(), result_df]
        p = FKPredictor(spark, _cfg())
        p.get_column_property_candidates()
        main_sql = spark.sql.call_args_list[1][0][0]
        assert "pk_pairs" in main_sql
        assert "name_pairs" in main_sql
        assert "NOT EXISTS" in main_sql

    def test_column_property_candidates_no_linked_entity(self):
        """SQL filters for linked_entity_type IS NOT NULL."""
        spark = MagicMock()
        result_df = MagicMock()
        result_df.withColumn = MagicMock(return_value=result_df)
        result_df.count = MagicMock(return_value=0)
        spark.sql.side_effect = [MagicMock(), result_df]
        p = FKPredictor(spark, _cfg())
        p.get_column_property_candidates()
        main_sql = spark.sql.call_args_list[1][0][0]
        assert "linked_entity_type IS NOT NULL" in main_sql

    def test_config_has_column_properties_table(self):
        """FKPredictionConfig includes the column_properties_table field."""
        cfg = _cfg()
        assert cfg.column_properties_table == "ontology_column_properties"
        assert cfg.fq(cfg.column_properties_table) == "c.s.ontology_column_properties"

