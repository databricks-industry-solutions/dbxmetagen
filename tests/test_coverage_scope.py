"""Coverage endpoints scope to SOURCE catalogs, not the app's output catalog.

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

from unittest.mock import patch


def _run(fn, **kw):
    sqls = []

    def _fake(sql, *a, **k):
        sqls.append(" ".join(sql.split()))
        if "COUNT(*) as profiled" in sql:
            return [{"profiled": 7, "with_comments": 7, "with_pii": 3, "with_domain": 7}]
        return []

    with patch.object(api_server, "execute_sql", side_effect=_fake):
        out = fn(**kw)
    return out, sqls


class TestCoverageScope:
    def test_default_does_not_filter_on_output_catalog(self):
        # Results live in CATALOG (test_cat); sources are elsewhere. No catalog
        # param must NOT restrict to the output catalog.
        out, sqls = _run(api_server._coverage_holistic, catalog=None)
        assert out["profiled"] == 7 and out["with_comments"] == 7
        assert sqls, "expected queries"
        for q in sqls:
            assert "'test_cat" not in q, q
        inv = [q for q in sqls if "information_schema.tables" in q]
        assert inv and "table_catalog IN (SELECT DISTINCT catalog FROM" in inv[0]

    def test_explicit_catalog_still_filters(self):
        _, sqls = _run(api_server._coverage_holistic, catalog="src_cat")
        kb = next(q for q in sqls if "COUNT(*) as profiled" in q)
        assert "LOWER(catalog) = LOWER('src_cat')" in kb
        assert any("LIKE 'src_cat.%'" in q for q in sqls if "fk_predictions" in q)
        inv = next(q for q in sqls if "information_schema.tables" in q)
        assert "table_catalog = 'src_cat'" in inv

    def test_catalog_quote_is_escaped(self):
        _, sqls = _run(api_server._coverage_holistic, catalog="a'b")
        assert all("'a'b" not in q for q in sqls)

    def test_review_summary_default_unscoped(self):
        _, sqls = _run(api_server._coverage_review_summary, catalog=None)
        assert "WHERE TRUE" in sqls[0] and "LIKE 'test_cat" not in sqls[0]
