"""Federated profiling's distinct_count = 0 is 'unknown', not 'non-unique'."""

import inspect

from dbxmetagen.fk_prediction import FKPredictor, _profiled_uniqueness


def test_zero_distinct_on_nonempty_table_is_unknown():
    # What federated profiling stores for every column.
    assert _profiled_uniqueness(0, 9029) is None


def test_missing_stats_are_unknown():
    assert _profiled_uniqueness(None, 100) is None
    assert _profiled_uniqueness(5, None) is None
    assert _profiled_uniqueness(5, 0) is None


def test_measured_stats_unchanged():
    assert _profiled_uniqueness(6169, 8514) == 6169 / 8514
    assert _profiled_uniqueness(9405, 9029) == 1.0  # approximate distinct, capped


def test_cardinality_analysis_routes_unknown_to_sampling():
    src = inspect.getsource(FKPredictor.cardinality_analysis)
    assert "_profiled_uniqueness(" in src
    assert "if ratio is not None:" in src
    assert "unprofiled_by_table.setdefault" in src
