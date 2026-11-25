"""Small FK precision fixes: column-property skip-AI needs a key side; demoted rows
drop below the 0.5 readers' threshold; Delta value samples are deterministic."""

import inspect
from unittest.mock import MagicMock

from dbxmetagen.fk_prediction import FKPredictionConfig, FKPredictor


def _predictor(**cfg):
    return FKPredictor(MagicMock(), FKPredictionConfig(catalog_name="c", schema_name="s", **cfg))


def test_column_property_skip_ai_requires_a_key_side():
    src = inspect.getsource(FKPredictor._with_skip_ai_flags)
    assert "SR_COL_PROP" in src
    assert "skip_ai_min_parent_uniqueness" in src
    assert '"_card_ratio_a"' in src and '"_card_ratio_b"' in src
    assert FKPredictionConfig(catalog_name="c", schema_name="s").skip_ai_min_parent_uniqueness == 0.95


def test_skip_flags_run_after_cardinality():
    src = inspect.getsource(FKPredictor.run)
    assert src.index("self.cardinality_analysis(candidates)") < src.index("self._with_skip_ai_flags(candidates)")


def test_demoted_rows_get_half_confidence():
    src = inspect.getsource(FKPredictor.write_predictions)
    assert '"_demoted", resolvable & (F.col("_fk_rank") > 1)' in src
    assert 'F.col("final_confidence") * F.lit(0.5)' in src
    # is_fk and confidence both key off the precomputed flag, not on the updated is_fk
    assert src.index('withColumn("_demoted"') < src.index('"is_fk", F.when(F.col("_demoted")')


def test_delta_sample_is_repeatable_and_ordered():
    sql = _predictor(cardinality_sample_rows=1000)._sample_sql("c.s.t", "CAST(`a` AS STRING) AS `a`", ["a", "b"], 15)
    assert "TABLESAMPLE (1000 ROWS) REPEATABLE (42)" in sql
    assert "ORDER BY xxhash64(`a`, `b`) LIMIT 15" in sql


def test_federation_sample_keeps_plain_limit():
    sql = _predictor(federation_mode=True)._sample_sql("c.s.t", "CAST(`a` AS STRING) AS `a`", ["a"], 15)
    assert sql.endswith("LIMIT 15") and "TABLESAMPLE" not in sql and "ORDER BY" not in sql
