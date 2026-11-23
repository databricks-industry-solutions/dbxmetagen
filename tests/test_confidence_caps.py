"""Confidence and uniqueness values stay within [0, 1]."""

from unittest.mock import MagicMock

# Heavy modules are imported inside the tests: importing them at collection time
# binds pyspark/AnalysisException before other test modules install their stubs.


class TestUniquenessRatio:
    def test_approximate_distinct_above_rows_is_capped(self):
        from dbxmetagen.fk_prediction import _uniqueness_ratio
        # Real case: approx distinct 9,405 over 9,029 rows used to give 1.04.
        assert _uniqueness_ratio(9405, 9029) == 1.0

    def test_normal_ratio_unchanged(self):
        from dbxmetagen.fk_prediction import _uniqueness_ratio
        assert _uniqueness_ratio(6169, 8514) == 6169 / 8514


def _validator():
    from dbxmetagen.ontology_validator import OntologyValidator, OntologyValidatorConfig
    spark = MagicMock()
    return OntologyValidator(spark, OntologyValidatorConfig(catalog_name="cat", schema_name="sch")), spark


def _sql_calls(spark):
    return [c.args[0] for c in spark.sql.call_args_list]


def _result(adj):
    return [{"entity_id": "e1",
             "validation_result": {"is_valid": True, "confidence_adjustment": adj, "reasoning": "ok"}}]


class TestValidatorConfidenceClamp:
    def test_merge_clamps_to_one(self):
        v, spark = _validator()
        v.batch_update_entity_validation(_result(0.1))
        merge = next(s for s in _sql_calls(spark) if "MERGE INTO" in s)
        assert "GREATEST(0.0, LEAST(1.0, target.confidence + " in merge

    def test_fallback_update_clamps_to_one(self):
        v, spark = _validator()
        v.update_entity_validation(_result(0.1))
        upd = next(s for s in _sql_calls(spark) if "UPDATE" in s)
        assert "GREATEST(0.0, LEAST(1.0, confidence + 0.1))" in upd


class TestFkRejectedPairs:
    def _predictor(self, **cfg):
        from dbxmetagen.fk_prediction import FKPredictionConfig, FKPredictor
        p = FKPredictor(MagicMock(), FKPredictionConfig(catalog_name="cat", schema_name="sch", **cfg))
        p._table_samples = {}
        return p

    def test_short_col(self):
        from dbxmetagen.fk_prediction import FKPredictor
        assert FKPredictor._short_col("c.s.bronze_edc_cm.usubjid") == "bronze_edc_cm.usubjid"
        assert FKPredictor._short_col(None) == ""

    def test_pair_rows_formats_and_truncates(self):
        p = self._predictor()
        row = MagicMock()
        row.asDict.return_value = {
            "col_a": "c.s.bronze_edc_cm.usubjid", "col_b": "c.s.bronze_edc_subjects.usubjid",
            "rule_score": 0.6512, "ai_confidence": 0.0, "source_rank": 3,
            "ai_reasoning": "budget_overflow_heuristic" + "x" * 500,
        }
        df = MagicMock()
        df.columns = ["col_a", "col_b", "rule_score", "ai_confidence", "ai_reasoning", "source_rank"]
        df.select.return_value.orderBy.return_value.limit.return_value.collect.return_value = [row]
        out = p._pair_rows(df, "rule_score", 50)
        assert out == [{
            "pair": "bronze_edc_cm.usubjid <-> bronze_edc_subjects.usubjid",
            "rule_score": 0.651, "ai_confidence": 0.0, "source_rank": 3,
            "ai_reasoning": ("budget_overflow_heuristic" + "x" * 500)[:200],
        }]
        df.select.return_value.orderBy.return_value.limit.assert_called_once_with(50)

    def test_pair_rows_disabled_with_zero_limit(self):
        df = MagicMock()
        assert self._predictor()._pair_rows(df, "rule_score", 0) == []
        df.select.assert_not_called()

    def test_diagnostics_off_by_default(self):
        assert self._predictor().config.diagnostics is False


class TestFkDebugDump:
    def test_debug_rows_written_with_fixed_schema_and_never_raise(self):
        from dbxmetagen.fk_prediction import FKPredictionConfig, FKPredictor
        p = FKPredictor(MagicMock(), FKPredictionConfig(catalog_name="cat", schema_name="sch", diagnostics=True))
        judged = MagicMock()
        judged.columns = ["col_a", "col_b", "rule_score", "ai_confidence"]
        p._write_debug_rows(judged, None)  # MagicMock pipeline; must not raise
        judged.select.assert_called_once()
        out = judged.select.return_value.withColumn.return_value
        out.write.mode.assert_called_once_with("overwrite")
        target = out.write.mode.return_value.option.return_value.saveAsTable.call_args[0][0]
        assert target.endswith("fk_prediction_debug")

    def test_debug_write_failure_is_swallowed(self):
        from dbxmetagen.fk_prediction import FKPredictionConfig, FKPredictor
        p = FKPredictor(MagicMock(), FKPredictionConfig(catalog_name="cat", schema_name="sch", diagnostics=True))
        judged = MagicMock()
        judged.columns = []
        judged.select.side_effect = RuntimeError("boom")
        p._write_debug_rows(judged, None)
