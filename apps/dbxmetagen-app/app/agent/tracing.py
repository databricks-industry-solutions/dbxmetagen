"""Centralized MLflow tracing setup for dbxmetagen showcase agents.

Imports this module to get `trace` decorator and `MLFLOW_ENABLED` flag.
If MLflow is unavailable (local dev, missing deps), tracing degrades to no-ops.
"""

import logging
import os

logger = logging.getLogger(__name__)

MLFLOW_ENABLED = False
MLFLOW_EXPERIMENT_ID = None
MLFLOW_EXPERIMENT = os.environ.get("MLFLOW_EXPERIMENT", "/Shared/dbxmetagen-app/traces")

_mlflow_mod = None

try:
    import mlflow as _mlflow_mod

    _mlflow_mod.set_tracking_uri("databricks")
    exp = _mlflow_mod.set_experiment(MLFLOW_EXPERIMENT)
    MLFLOW_EXPERIMENT_ID = exp.experiment_id
    _mlflow_mod.langchain.autolog(log_traces=True, silent=True)
    trace = _mlflow_mod.trace
    MLFLOW_ENABLED = True
    logger.info(
        "MLflow tracing enabled, experiment=%s (id=%s), tracking_uri=%s",
        MLFLOW_EXPERIMENT, MLFLOW_EXPERIMENT_ID, _mlflow_mod.get_tracking_uri(),
    )
except Exception as _exc:  # noqa: BLE001
    logger.warning("MLflow tracing not available: %s", _exc)

    def trace(**_kwargs):
        """No-op decorator when mlflow is unavailable."""
        return lambda fn: fn


def ensure_mlflow_context():
    """Re-apply MLflow experiment context in the current thread.

    Call this at the start of any threaded agent execution to ensure the
    experiment is set (MLflow experiment state can be thread-local in some
    versions).
    """
    if _mlflow_mod is None or not MLFLOW_ENABLED:
        return
    try:
        _mlflow_mod.set_tracking_uri("databricks")
        _mlflow_mod.set_experiment(MLFLOW_EXPERIMENT)
    except Exception as exc:
        logger.debug("ensure_mlflow_context: %s", exc)
