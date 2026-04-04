"""Tests for api_server._strip_out_of_scope_sql join_specs filtering.

Uses the same lightweight module mocks as test_graph_traversal so api_server imports.
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
    if "fastapi" in sys.modules and not hasattr(sys.modules["fastapi"], "HTTPException"):
        sys.modules["fastapi"].HTTPException = type(
            "HTTPException", (Exception,), {"__init__": lambda self, *a, **kw: None}
        )
    if "cachetools" in sys.modules:
        sys.modules["cachetools"].cached = lambda *a, **kw: (lambda fn: fn)


_install_mock_modules()

APP_DIR = os.path.join(os.path.dirname(__file__), "..", "apps", "dbxmetagen-app", "app")
sys.path.insert(0, APP_DIR)
os.environ.setdefault("CATALOG_NAME", "test_cat")
os.environ.setdefault("SCHEMA_NAME", "test_schema")
os.environ.setdefault("LAKEBASE_CATALOG", "lb_cat")
os.environ.setdefault("WAREHOUSE_ID", "wh123")

import api_server  # noqa: E402


class TestStripOutOfScopeJoinSpecs:
    """join_specs are kept only when ON clause table aliases exist in data_sources."""

    def _base_space(self):
        return {
            "data_sources": {
                "tables": [
                    {"identifier": "a.b.t1"},
                    {"identifier": "a.b.t2"},
                ],
                "metric_views": [],
            },
            "instructions": {
                "join_specs": [],
                "example_question_sqls": [],
                "sql_snippets": {"measures": [], "filters": [], "expressions": []},
            },
        }

    def test_keeps_join_when_sql_refs_only_in_scope_tables(self):
        ss = self._base_space()
        ss["instructions"]["join_specs"] = [
            {
                "left": {"identifier": "a.b.t1"},
                "right": {"identifier": "a.b.t2"},
                "sql": ["t1.id = t2.id"],
            }
        ]
        out = api_server._strip_out_of_scope_sql(ss)
        assert len(out["instructions"]["join_specs"]) == 1

    def test_drops_join_when_sql_refs_unknown_table(self):
        ss = self._base_space()
        ss["instructions"]["join_specs"] = [
            {
                "left": {"identifier": "a.b.t1"},
                "right": {"identifier": "a.b.t2"},
                "sql": ["t1.id = t3.missing_col"],
            }
        ]
        out = api_server._strip_out_of_scope_sql(ss)
        assert len(out["instructions"]["join_specs"]) == 0

    def test_metric_view_identifiers_in_valid_set_with_table_join(self):
        """MV + tables: join between two tables uses short names present in data_sources."""
        ss = {
            "data_sources": {
                "tables": [{"identifier": "a.b.orders"}, {"identifier": "a.b.customers"}],
                "metric_views": [{"identifier": "a.b.mv_revenue"}],
            },
            "instructions": {
                "join_specs": [
                    {
                        "left": {"identifier": "a.b.orders"},
                        "right": {"identifier": "a.b.customers"},
                        "sql": ["orders.cust_id = customers.id"],
                    }
                ],
                "example_question_sqls": [],
                "sql_snippets": {"measures": [], "filters": [], "expressions": []},
            },
        }
        out = api_server._strip_out_of_scope_sql(ss)
        assert len(out["instructions"]["join_specs"]) == 1
