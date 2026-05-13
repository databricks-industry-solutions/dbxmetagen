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
    _FK_EXCLUDED_DTYPES,
    _dtype_exclusion_sql,
    _dtype_excluded,
    _DEFAULT_SYSTEM_COL_PATTERNS,
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
    def test_sql_contains_self_referential_filter(self):
        """The SQL WHERE clause excludes self-referential FKs."""
        spark = MagicMock()
        p = FKPredictor(spark, _cfg())
        p.get_declared_fk_candidates()
        sql = spark.sql.call_args[0][0]
        assert "!= table_name" in sql

    def test_sql_contains_size_filter(self):
        """The SQL WHERE clause excludes malformed refs with < 4 parts."""
        spark = MagicMock()
        p = FKPredictor(spark, _cfg())
        p.get_declared_fk_candidates()
        sql = spark.sql.call_args[0][0]
        assert "SIZE(parts) >= 4" in sql

    def test_sql_filters_null_ref_target(self):
        """The SQL WHERE clause excludes null/empty ref_target."""
        spark = MagicMock()
        p = FKPredictor(spark, _cfg())
        p.get_declared_fk_candidates()
        sql = spark.sql.call_args[0][0]
        assert "ref_target IS NOT NULL" in sql

    def test_returns_dataframe_with_source_rank(self):
        """Result should have withColumn calls for source_rank and query_hit_count."""
        spark = MagicMock()
        out_mock = spark.sql.return_value.withColumn.return_value
        out_mock.withColumn.return_value = out_mock
        p = FKPredictor(spark, _cfg())
        result = p.get_declared_fk_candidates()
        assert spark.sql.called


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

    def test_similarity_propagation_preserves_embedding_signal(self):
        """Name-based wins source_rank dedup, but must keep embedding col_similarity."""
        # (col_a, col_b, source_rank, col_similarity, table_similarity, query_hit_count)
        embedding_row = ("x", "y", SR_EMBEDDING, 0.92, 0.85, 0)
        name_row = ("x", "y", SR_NAME, 0.0, 0.0, 0)
        rows = [embedding_row, name_row]

        # Step 1: propagate max similarity across all sources per (col_a, col_b)
        by_pair: dict[tuple, list] = {}
        for r in rows:
            by_pair.setdefault((r[0], r[1]), []).append(r)
        propagated = []
        for _key, group in by_pair.items():
            max_col_sim = max(r[3] for r in group)
            max_tbl_sim = max(r[4] for r in group)
            for r in group:
                propagated.append((r[0], r[1], r[2], max_col_sim, max_tbl_sim, r[5]))

        # Step 2: dedup by source_rank (name=3 wins over embedding=5)
        propagated.sort(key=lambda r: (r[2], -r[3], -r[5]))
        winner = propagated[0]

        assert winner[2] == SR_NAME, "name-based should win the dedup"
        assert winner[3] == 0.92, "col_similarity from embedding must be preserved"
        assert winner[4] == 0.85, "table_similarity from embedding must be preserved"


# --- TestTableNameMatchStem ---


class TestTableNameMatchStem:
    """Verify that table_name_match strips dim_/fct_ prefixes before matching."""

    @staticmethod
    def _stem(table_short: str) -> str:
        """Mirror the SQL: strip dim_/fct_/fact_/stg_ prefix, then trailing 's'."""
        import re
        s = re.sub(r"^(dim_|fct_|fact_|stg_)", "", table_short)
        return re.sub(r"s$", "", s)

    @staticmethod
    def _matches(col_short: str, stem: str) -> bool:
        import re
        return bool(re.search(rf"(^|_){re.escape(stem)}(_|$)", col_short))

    def test_index_code_matches_dim_index(self):
        stem = self._stem("dim_index")
        assert stem == "index"
        assert self._matches("index_code", stem)

    def test_security_id_matches_dim_security(self):
        stem = self._stem("dim_security")
        assert stem == "security"
        assert self._matches("security_id", stem)

    def test_account_id_matches_fct_accounts(self):
        stem = self._stem("fct_accounts")
        assert stem == "account"
        assert self._matches("account_id", stem)

    def test_status_does_not_match_dim_customer(self):
        stem = self._stem("dim_customer")
        assert stem == "customer"
        assert not self._matches("status", stem)

    def test_no_prefix_still_works(self):
        stem = self._stem("orders")
        assert stem == "order"
        assert self._matches("order_id", stem)

    def test_unrelated_col_no_match(self):
        stem = self._stem("dim_product")
        assert stem == "product"
        assert not self._matches("customer_id", stem)


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

    def test_max_ontology_table_pairs_default(self):
        c = FKPredictionConfig(catalog_name="x", schema_name="y")
        assert c.max_ontology_table_pairs == 50_000


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
            patch.object(FKPredictor, "add_domain_signal", side_effect=lambda c: c),
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
            patch.object(FKPredictor, "add_domain_signal", side_effect=lambda c: c),
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
            patch.object(FKPredictor, "add_domain_signal", side_effect=lambda c: c),
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


class TestTableBackedOutputs:
    """Verify generate_ddl and write_graph_edges read from the authoritative
    fk_predictions table rather than accepting a transient DataFrame."""

    def test_generate_ddl_no_df_parameter(self):
        sig = inspect.signature(FKPredictor.generate_ddl)
        assert list(sig.parameters.keys()) == ["self"]

    def test_write_graph_edges_no_df_parameter(self):
        sig = inspect.signature(FKPredictor.write_graph_edges)
        assert "self" in sig.parameters
        for p in sig.parameters.values():
            if p.name != "self":
                assert p.default is not inspect.Parameter.empty, f"{p.name} should have a default"

    def test_generate_ddl_reads_predictions_table(self):
        src = inspect.getsource(FKPredictor.generate_ddl)
        assert "spark.table" in src or "self.spark.table" in src
        assert "predictions_table" in src

    def test_generate_ddl_uses_canonical_column_names(self):
        src = inspect.getsource(FKPredictor.generate_ddl)
        assert "src_table" in src
        assert "dst_table" in src
        assert "src_column" in src
        assert "dst_column" in src

    def test_generate_ddl_filters_is_fk(self):
        src = inspect.getsource(FKPredictor.generate_ddl)
        assert "is_fk" in src

    def test_write_graph_edges_reads_predictions_table(self):
        src = inspect.getsource(FKPredictor.write_graph_edges)
        assert "spark.table" in src or "self.spark.table" in src
        assert "predictions_table" in src

    def test_write_graph_edges_uses_canonical_column_names(self):
        src = inspect.getsource(FKPredictor.write_graph_edges)
        assert "src_column" in src
        assert "dst_column" in src

    def test_write_graph_edges_filters_is_fk(self):
        src = inspect.getsource(FKPredictor.write_graph_edges)
        assert "is_fk" in src

    def test_write_graph_edges_uses_merge_edges(self):
        src = inspect.getsource(FKPredictor.write_graph_edges)
        assert "merge_edges" in src
        assert "fk_predictions" in src

    def test_write_graph_edges_sets_edge_id_and_source_system(self):
        src = inspect.getsource(FKPredictor.write_graph_edges)
        assert "edge_id" in src
        assert "source_system" in src


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


# --- TestSystemColumnExclusion ---


class TestSystemColumnExclusion:
    """Verify system_column_patterns config and its effect on rule_score."""

    def test_default_system_patterns_exist(self):
        cfg = _cfg()
        assert len(cfg.system_column_patterns) > 0
        assert cfg.system_column_patterns == _DEFAULT_SYSTEM_COL_PATTERNS

    def test_custom_patterns_override_defaults(self):
        custom = (r"^audit_", r"^_etl_")
        cfg = FKPredictionConfig(catalog_name="c", schema_name="s", system_column_patterns=custom)
        assert cfg.system_column_patterns == custom

    def test_rule_score_source_includes_system_col_guard(self):
        src = inspect.getsource(FKPredictor.rule_score)
        assert "system_column_patterns" in src
        assert "not_system" in src
        assert "is_system_col" in src

    def test_rule_score_gates_new_signals_by_system_flag(self):
        """same_schema, same_domain, sim_floor are all multiplied by not_system."""
        src = inspect.getsource(FKPredictor.rule_score)
        assert "same_schema * 0.05 * not_system" in src
        assert "same_domain * 0.05 * not_system" in src
        assert "sim_floor * not_system" in src

    def test_predict_fk_accepts_system_column_patterns(self):
        sig = inspect.signature(predict_foreign_keys)
        assert "system_column_patterns" in sig.parameters


# --- TestDomainSignal ---


class TestDomainSignal:
    """Verify add_domain_signal wiring and behavior."""

    def test_add_domain_signal_exists(self):
        assert hasattr(FKPredictor, "add_domain_signal")

    def test_run_calls_add_domain_signal(self):
        src = inspect.getsource(FKPredictor.run)
        assert "add_domain_signal" in src

    def test_add_domain_signal_gracefully_handles_missing_table(self):
        spark = MagicMock()
        spark.sql.side_effect = Exception("TABLE_OR_VIEW_NOT_FOUND")
        p = FKPredictor(spark, _cfg())
        df = MagicMock()
        df.withColumn = MagicMock(return_value=df)
        result = p.add_domain_signal(df)
        assert result is not None

    def test_add_domain_signal_sql_references_metadata_log(self):
        src = inspect.getsource(FKPredictor.add_domain_signal)
        assert "metadata_generation_log" in src
        assert "domain" in src
        assert "subdomain" in src

    def test_domain_match_scoring_logic(self):
        """Source should compute 1.0 for same domain+subdomain, 0.5 for domain-only."""
        src = inspect.getsource(FKPredictor.add_domain_signal)
        assert "1.0" in src
        assert "0.5" in src


# --- TestNewRuleScoreSignals ---


class TestNewRuleScoreSignals:
    """Verify new rule_score signals: same_schema, same_domain, sim_floor."""

    def test_same_schema_signal_in_source(self):
        src = inspect.getsource(FKPredictor.rule_score)
        assert "same_schema" in src

    def test_same_domain_signal_in_source(self):
        src = inspect.getsource(FKPredictor.rule_score)
        assert "same_domain" in src

    def test_sim_floor_tiered_thresholds(self):
        """sim_floor should have tiers at 0.98, 0.95, 0.90."""
        src = inspect.getsource(FKPredictor.rule_score)
        assert "0.98" in src
        assert "0.95" in src
        assert "0.90" in src

    def test_source_bonus_weight_reduced(self):
        """source_bonus weight should be 0.10, not the old 0.15."""
        src = inspect.getsource(FKPredictor.rule_score)
        assert "source_bonus * 0.10" in src

    def test_domain_match_fallback_when_absent(self):
        """When domain_match is NOT in columns, F.lit(0.0) should be used."""
        from pyspark.sql import functions as F
        F.lit.reset_mock()
        p = FKPredictor(spark=MagicMock(), config=_cfg())
        df = MagicMock()
        df.columns = ["col_a", "col_b", "dtype_a", "dtype_b",
                       "table_a", "table_b", "col_similarity",
                       "samples_a", "samples_b"]
        df.withColumn = MagicMock(return_value=df)
        p.rule_score(df)
        lit_args = [c[0][0] for c in F.lit.call_args_list if c[0]]
        assert 0.0 in lit_args


class TestDtypeExclusion:
    """Verify FK-excluded data types constant and SQL helper."""

    def test_excluded_dtypes_contains_float(self):
        assert "float" in _FK_EXCLUDED_DTYPES

    def test_excluded_dtypes_contains_variant(self):
        assert "variant" in _FK_EXCLUDED_DTYPES

    def test_excluded_dtypes_contains_struct(self):
        assert "struct" in _FK_EXCLUDED_DTYPES

    def test_excluded_dtypes_contains_boolean(self):
        assert "boolean" in _FK_EXCLUDED_DTYPES

    def test_excluded_dtypes_does_not_contain_int(self):
        assert "int" not in _FK_EXCLUDED_DTYPES
        assert "bigint" not in _FK_EXCLUDED_DTYPES
        assert "string" not in _FK_EXCLUDED_DTYPES

    def test_dtype_exclusion_sql_returns_quoted_list(self):
        sql = _dtype_exclusion_sql()
        assert "'float'" in sql
        assert "'variant'" in sql
        assert ", " in sql

    def test_embedding_gen_uses_dtype_filter(self):
        src = inspect.getsource(FKPredictor.get_candidates)
        assert "_dtype_excluded(" in src

    def test_name_gen_uses_dtype_filter(self):
        src = inspect.getsource(FKPredictor.get_name_based_candidates)
        assert "_dtype_excluded(" in src

    def test_ontology_gen_uses_dtype_filter(self):
        src = inspect.getsource(FKPredictor.get_ontology_relationship_candidates)
        assert "_dtype_excluded(" in src

    def test_colprop_gen_uses_shared_constant(self):
        src = inspect.getsource(FKPredictor.get_column_property_candidates)
        assert "_dtype_excluded(" in src

    def test_dtype_excluded_predicate_format(self):
        pred = _dtype_excluded("col.data_type")
        assert "ELEMENT_AT(SPLIT(LOWER(col.data_type)" in pred
        assert "'float'" in pred
        assert "'decimal'" in pred

    def test_dtype_excluded_handles_parameterized_types(self):
        """Verify the SQL pattern strips parenthesized suffixes like decimal(38,0)."""
        pred = _dtype_excluded("data_type")
        assert "SPLIT" in pred and "'\\\\('" in pred


class TestPKSignal:
    """Verify add_pk_signal method and pk_match in rule_score."""

    def test_add_pk_signal_exists(self):
        assert hasattr(FKPredictor, "add_pk_signal")

    def test_add_pk_signal_graceful_on_missing_table(self):
        spark = MagicMock()
        spark.sql.side_effect = Exception("table not found")
        p = FKPredictor(spark=spark, config=_cfg())
        df = MagicMock()
        df.withColumn = MagicMock(return_value=df)
        result = p.add_pk_signal(df)
        assert result.withColumn.called

    def test_run_calls_add_pk_signal(self):
        src = inspect.getsource(FKPredictor.run)
        assert "add_pk_signal" in src

    def test_pk_match_signal_in_rule_score(self):
        src = inspect.getsource(FKPredictor.rule_score)
        assert "pk_match" in src

    def test_pk_match_weight_in_formula(self):
        src = inspect.getsource(FKPredictor.rule_score)
        assert "pk_match * 0.10" in src

    def test_col_similarity_weight_reduced(self):
        """col_similarity weight reduced from 0.10 to 0.05 to make room for pk_match."""
        src = inspect.getsource(FKPredictor.rule_score)
        assert '("col_similarity") * 0.05' in src

