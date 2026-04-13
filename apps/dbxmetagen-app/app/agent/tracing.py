"""Centralized MLflow tracing setup for dbxmetagen showcase agents.

Imports this module to get `trace` decorator and `MLFLOW_ENABLED` flag.
If MLflow is unavailable (local dev, missing deps), tracing degrades to no-ops.

All Databricks API calls are deferred to first runtime use so that importing
this module never requires a live Databricks connection (important for unit tests).
"""

import contextvars
import functools
import logging
import os
from contextlib import nullcontext

logger = logging.getLogger(__name__)

MLFLOW_ENABLED = False

# When True, manual mlflow.start_span() calls are suppressed because
# langchain.autolog() is managing the trace hierarchy (LangGraph path).
AUTOLOG_ACTIVE = contextvars.ContextVar("autolog_active", default=False)
MLFLOW_EXPERIMENT_ID = None
MLFLOW_EXPERIMENT = os.environ.get("MLFLOW_EXPERIMENT", "")

_mlflow_mod = None
_tracing_initialized = False


def _resolve_experiment_name() -> str:
    if MLFLOW_EXPERIMENT:
        return MLFLOW_EXPERIMENT
    try:
        from databricks.sdk import WorkspaceClient
        me = WorkspaceClient().current_user.me().user_name
        return f"/Users/{me}/experiments/dbxmetagen_app_traces"
    except Exception as exc:
        logger.warning("Could not resolve user for experiment path, using catalog-scoped default: %s", exc)
        catalog = os.environ.get("CATALOG_NAME", "default")
        return f"/Users/{catalog}/experiments/dbxmetagen_app_traces"


def _init_tracing():
    """One-time lazy initialization of MLflow tracing on first real use."""
    global MLFLOW_ENABLED, MLFLOW_EXPERIMENT, MLFLOW_EXPERIMENT_ID, _mlflow_mod, _tracing_initialized
    if _tracing_initialized:
        return
    _tracing_initialized = True

    try:
        import mlflow as _mlf
        _mlflow_mod = _mlf

        _mlflow_mod.set_tracking_uri("databricks")
        MLFLOW_EXPERIMENT = _resolve_experiment_name()
        try:
            from databricks.sdk import WorkspaceClient
            _parent = "/".join(MLFLOW_EXPERIMENT.split("/")[:-1])
            if _parent:
                WorkspaceClient().workspace.mkdirs(_parent)
        except Exception:
            pass
        exp = _mlflow_mod.set_experiment(MLFLOW_EXPERIMENT)
        MLFLOW_EXPERIMENT_ID = exp.experiment_id
        _mlflow_mod.langchain.autolog(log_traces=True, silent=True)
        MLFLOW_ENABLED = True
        logger.info(
            "MLflow tracing enabled, experiment=%s (id=%s), tracking_uri=%s",
            MLFLOW_EXPERIMENT, MLFLOW_EXPERIMENT_ID, _mlflow_mod.get_tracking_uri(),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("MLflow tracing not available: %s", exc)


def trace(**kwargs):
    """Lazy trace decorator: safe to apply at import time, activates at first call."""
    def decorator(fn):
        _traced_fn = None

        @functools.wraps(fn)
        def wrapper(*args, **kw):
            nonlocal _traced_fn
            if _traced_fn is None:
                _init_tracing()
                if MLFLOW_ENABLED and _mlflow_mod is not None:
                    _traced_fn = _mlflow_mod.trace(**kwargs)(fn)
                else:
                    _traced_fn = fn
            return _traced_fn(*args, **kw)

        return wrapper
    return decorator


def ensure_mlflow_context():
    """Re-apply MLflow experiment context and autolog in the current thread.

    Call this at the start of any threaded agent execution to ensure the
    experiment is set and langchain autolog is active (both can be
    thread-local in some MLflow versions).
    """
    _init_tracing()
    if _mlflow_mod is None or not MLFLOW_ENABLED:
        return
    try:
        _mlflow_mod.set_tracking_uri("databricks")
        _mlflow_mod.set_experiment(MLFLOW_EXPERIMENT)
        _mlflow_mod.langchain.autolog(log_traces=True, silent=True)
    except Exception as exc:
        logger.warning("ensure_mlflow_context failed: %s", exc)


def get_mlflow():
    """Return the mlflow module (or None if unavailable)."""
    return _mlflow_mod if MLFLOW_ENABLED else None


_mlflow_client = None


def get_mlflow_client():
    """Return a singleton MlflowClient, or None if tracing is off."""
    global _mlflow_client
    if not MLFLOW_ENABLED or _mlflow_mod is None:
        return None
    if _mlflow_client is None:
        _mlflow_client = _mlflow_mod.MlflowClient()
    return _mlflow_client


def tag_trace(session_id: str = None, **extra):
    """Tag the current trace with session_id and optional metadata."""
    if not MLFLOW_ENABLED or _mlflow_mod is None:
        return
    tags = {k: str(v) for k, v in extra.items() if v is not None}
    if session_id:
        tags["mlflow.trace.session_id"] = session_id
    if tags:
        try:
            _mlflow_mod.update_current_trace(tags=tags)
        except Exception:
            pass


def maybe_span(name: str, span_type: str = "CHAIN"):
    """Create an MLflow span only when manual tracing is appropriate.

    Returns nullcontext() when AUTOLOG_ACTIVE is set (LangGraph path) or
    when MLflow is unavailable. This prevents orphaned root traces in the
    graph path while preserving span hierarchy in the non-graph path.
    """
    if AUTOLOG_ACTIVE.get(False):
        return nullcontext()
    if not MLFLOW_ENABLED or _mlflow_mod is None:
        return nullcontext()
    return _mlflow_mod.start_span(name=name, span_type=span_type)
