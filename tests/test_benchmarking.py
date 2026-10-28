"""setup_benchmarking must never fail metadata generation over MLflow access.

When jobs run as a service principal (run_as override), the notebook's default
experiment belongs to the deployer and start_run fails. Benchmarking then falls
back to an experiment in the running identity's home folder, or disables itself.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from dbxmetagen import benchmarking


def _config(**kw):
    base = dict(enable_benchmarking=True, notebook_path="/nb/generate_metadata",
                run_id=None, job_id=None)
    base.update(kw)
    return SimpleNamespace(**base)


def _mlflow(start_run_errors=()):
    """MLflow stub whose start_run raises the given errors in order, then succeeds."""
    m = MagicMock()
    state = {"run": None}
    errors = list(start_run_errors)

    def start_run():
        if errors:
            raise errors.pop(0)
        state["run"] = SimpleNamespace(info=SimpleNamespace(start_time=123, experiment_id="1"))

    m.start_run.side_effect = start_run
    m.active_run.side_effect = lambda: state["run"]
    return m


def _spark(identity="sp-123"):
    spark = MagicMock()
    spark.sql.return_value.first.return_value = [identity]
    session = MagicMock()
    session.builder.getOrCreate.return_value = spark
    return session


def test_disabled_returns_none():
    assert benchmarking.setup_benchmarking(_config(enable_benchmarking=False)) is None


def test_default_experiment_used_when_writable():
    m = _mlflow()
    with patch.object(benchmarking, "mlflow", m):
        result = benchmarking.setup_benchmarking(_config())
    assert result == ("/nb/generate_metadata", 123)
    m.set_experiment.assert_not_called()


def test_falls_back_to_identity_home_when_default_not_writable():
    m = _mlflow([Exception("RESOURCE_DOES_NOT_EXIST: Could not find experiment with ID None")])
    with patch.object(benchmarking, "mlflow", m), \
            patch.object(benchmarking, "SparkSession", _spark("sp-123")):
        result = benchmarking.setup_benchmarking(_config())
    assert result == ("/Users/sp-123/dbxmetagen_benchmarks", 123)
    m.set_experiment.assert_called_once_with("/Users/sp-123/dbxmetagen_benchmarks")


def test_disables_instead_of_failing_when_fallback_also_fails():
    m = _mlflow([Exception("default not writable"), Exception("fallback not writable")])
    with patch.object(benchmarking, "mlflow", m), \
            patch.object(benchmarking, "SparkSession", _spark()):
        assert benchmarking.setup_benchmarking(_config()) is None
