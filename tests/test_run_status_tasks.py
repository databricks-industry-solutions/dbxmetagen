"""Tests for api_server._latest_task_attempts (Run History retry collapse).

Uses the same lightweight module mocks as test_genie_strip_joins so api_server imports.
"""

import os
import sys
import types
import pytest
from unittest.mock import MagicMock

_MOCK_MODULES = [
    "fastapi", "fastapi.responses", "fastapi.staticfiles", "fastapi.middleware", "fastapi.middleware.cors",
    "starlette", "starlette.middleware", "starlette.middleware.base", "starlette.requests", "starlette.responses",
    "sqlalchemy", "sqlalchemy.orm",
    "uvicorn", "cachetools",
    "langchain_core", "langchain_core.tools", "langchain_core.messages",
    "langchain_databricks", "langchain_community", "langchain_community.chat_models",
    "langgraph", "langgraph.graph", "langgraph.graph.message",
    "langgraph.prebuilt", "requests", "pydantic",
]


class _AutoMockModule(types.ModuleType):
    def __getattr__(self, name):
        mock = MagicMock()
        setattr(self, name, mock)
        return mock


def _install_mock_modules():
    _already_loaded = set(sys.modules) & set(_MOCK_MODULES)
    for mod_name in _MOCK_MODULES:
        if mod_name not in sys.modules:
            mod = _AutoMockModule(mod_name)
            if any(m.startswith(mod_name + ".") for m in _MOCK_MODULES):
                mod.__path__ = []
            sys.modules[mod_name] = mod
    if "fastapi" not in _already_loaded:
        def _http_exc_init(self, status_code=None, detail=None, **kw):
            self.status_code = status_code
            self.detail = detail
        sys.modules["fastapi"].HTTPException = type(
            "HTTPException", (Exception,), {"__init__": _http_exc_init}
        )
    if "cachetools" not in _already_loaded:
        sys.modules["cachetools"].cached = lambda *a, **kw: (lambda fn: fn)


_install_mock_modules()

APP_DIR = os.path.join(os.path.dirname(__file__), "..", "apps", "dbxmetagen-app", "app")
sys.path.insert(0, APP_DIR)
os.environ.setdefault("CATALOG_NAME", "test_cat")
os.environ.setdefault("SCHEMA_NAME", "test_schema")
os.environ.setdefault("LAKEBASE_CATALOG", "lb_cat")
os.environ.setdefault("WAREHOUSE_ID", "wh123")

import api_server  # noqa: E402


def _t(key, attempt=0, start=0, result=None):
    return types.SimpleNamespace(task_key=key, attempt_number=attempt, start_time=start,
                                 state=types.SimpleNamespace(result_state=result))


class TestLatestTaskAttempts:
    def test_retries_collapse_to_one_entry_per_task(self):
        tasks = [_t("generate", 0, 10, "FAILED"), _t("kb", 0, 5, "SUCCESS"),
                 _t("generate", 1, 20, "FAILED"), _t("generate", 2, 30, "SUCCESS")]
        out = api_server._latest_task_attempts(tasks)
        assert [(t.task_key, n) for t, n in out] == [("generate", 3), ("kb", 1)]
        assert out[0][0].attempt_number == 2
        assert out[0][0].state.result_state == "SUCCESS"

    def test_out_of_order_attempts_keep_highest(self):
        tasks = [_t("a", 1, 20, "SUCCESS"), _t("a", 0, 10, "FAILED")]
        (latest, n), = api_server._latest_task_attempts(tasks)
        assert latest.attempt_number == 1 and n == 2

    def test_missing_attempt_number_falls_back_to_start_time(self):
        tasks = [_t("a", None, 10, "FAILED"), _t("a", None, 30, "SUCCESS")]
        (latest, _), = api_server._latest_task_attempts(tasks)
        assert latest.state.result_state == "SUCCESS"

    def test_empty_and_none(self):
        assert api_server._latest_task_attempts(None) == []
        assert api_server._latest_task_attempts([None]) == []
