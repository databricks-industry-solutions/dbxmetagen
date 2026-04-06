"""Tests for api_server pure helpers and job param wiring.

Uses the same lightweight module mock pattern as test_genie_strip_joins.py
so api_server can import without real Databricks/FastAPI deps.
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
    for mod_name in _MOCK_MODULES:
        if mod_name not in sys.modules:
            mod = _AutoMockModule(mod_name)
            if any(m.startswith(mod_name + ".") for m in _MOCK_MODULES):
                mod.__path__ = []
            sys.modules[mod_name] = mod
    if not isinstance(sys.modules.get("fastapi"), _AutoMockModule):
        return
    def _http_exc_init(self, status_code=None, detail=None, **kw):
        self.status_code = status_code
        self.detail = detail
    sys.modules["fastapi"].HTTPException = type(
        "HTTPException", (Exception,), {"__init__": _http_exc_init}
    )
    sys.modules["cachetools"].cached = lambda *a, **kw: (lambda fn: fn)


_install_mock_modules()

APP_DIR = os.path.join(os.path.dirname(__file__), "..", "apps", "dbxmetagen-app", "app")
sys.path.insert(0, APP_DIR)
os.environ.setdefault("CATALOG_NAME", "test_cat")
os.environ.setdefault("SCHEMA_NAME", "test_schema")
os.environ.setdefault("LAKEBASE_CATALOG", "lb_cat")
os.environ.setdefault("WAREHOUSE_ID", "wh123")

import api_server  # noqa: E402


# ---------------------------------------------------------------------------
# _safe_bundle_path
# ---------------------------------------------------------------------------
class TestSafeBundlePath:
    def test_valid_subpath(self, tmp_path):
        root = str(tmp_path / "bundles")
        os.makedirs(root, exist_ok=True)
        result = api_server._safe_bundle_path(root, "healthcare")
        assert result is not None
        assert result.startswith(root)

    def test_rejects_path_traversal(self, tmp_path):
        root = str(tmp_path / "bundles")
        os.makedirs(root, exist_ok=True)
        result = api_server._safe_bundle_path(root, "../../etc/passwd")
        assert result is None

    def test_rejects_absolute_escape(self, tmp_path):
        root = str(tmp_path / "bundles")
        os.makedirs(root, exist_ok=True)
        result = api_server._safe_bundle_path(root, "/tmp/evil")
        assert result is None

    def test_dot_resolves_to_root(self, tmp_path):
        root = str(tmp_path / "bundles")
        os.makedirs(root, exist_ok=True)
        result = api_server._safe_bundle_path(root, ".")
        assert result == os.path.normpath(root)


# ---------------------------------------------------------------------------
# _validate_filter
# ---------------------------------------------------------------------------
class TestValidateFilter:
    def test_accepts_normal_identifier(self):
        api_server._validate_filter("my_catalog.my_schema", "catalog")

    def test_accepts_none(self):
        api_server._validate_filter(None, "catalog")

    def test_accepts_empty(self):
        api_server._validate_filter("", "catalog")

    def test_rejects_sql_injection(self):
        HTTPException = sys.modules["fastapi"].HTTPException
        with pytest.raises(HTTPException) as exc_info:
            api_server._validate_filter("'; DROP TABLE--", "catalog")
        assert exc_info.value.status_code == 400

    def test_rejects_backtick(self):
        HTTPException = sys.modules["fastapi"].HTTPException
        with pytest.raises(HTTPException):
            api_server._validate_filter("cat`evil", "catalog")

    def test_accepts_hyphens_and_spaces(self):
        api_server._validate_filter("my-catalog name", "catalog")


# ---------------------------------------------------------------------------
# Job param merge (extra_params override)
# ---------------------------------------------------------------------------
class TestJobParamMerge:
    """Verifies that extra_params correctly override/extend base params.
    This replicates the pattern from POST /api/jobs/run."""

    def test_extra_params_override_base(self):
        params = {"mode": "comment", "sample_size": "5"}
        extra = {"sample_size": "10", "columns_per_call": "15"}
        params.update(extra)
        assert params["sample_size"] == "10"
        assert params["columns_per_call"] == "15"
        assert params["mode"] == "comment"

    def test_extra_params_add_new_keys(self):
        params = {"mode": "all"}
        extra = {"columns_per_call": "20", "batch_ddl_apply": "true"}
        params.update(extra)
        assert "columns_per_call" in params
        assert "batch_ddl_apply" in params

    def test_empty_extra_params_is_noop(self):
        params = {"mode": "comment", "sample_size": "5"}
        original = dict(params)
        params.update({})
        assert params == original
