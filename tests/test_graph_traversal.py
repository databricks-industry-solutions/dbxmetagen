"""Unit tests for GraphRAG graph traversal and tools.

Tests focus on expected behavior of multi_hop_traverse and the agent tools,
using a mock execute_sql to simulate graph data.

External deps (fastapi, langchain, databricks-sdk) are mocked at the module
level so the tests run in the main project's environment.
"""

import os
import re
import sys
import types
import pytest
from unittest.mock import patch, MagicMock

# ---------------------------------------------------------------------------
# Mock external modules before importing app code
# ---------------------------------------------------------------------------

APP_DIR = os.path.join(os.path.dirname(__file__), "..", "apps", "dbxmetagen-app", "app")

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
    """Module stub that returns MagicMock for any missing attribute."""
    def __getattr__(self, name):
        mock = MagicMock()
        setattr(self, name, mock)
        return mock


def _install_mock_modules():
    """Insert lightweight stubs for missing third-party packages."""
    _already_loaded = set(sys.modules) & set(_MOCK_MODULES)

    for mod_name in _MOCK_MODULES:
        if mod_name not in sys.modules:
            mod = _AutoMockModule(mod_name)
            if any(m.startswith(mod_name + ".") for m in _MOCK_MODULES):
                mod.__path__ = []
            sys.modules[mod_name] = mod

    # Specific overrides for behaviour the tests or app code depend on
    if "fastapi" not in _already_loaded:
        fm = sys.modules["fastapi"]
        fm.HTTPException = type("HTTPException", (Exception,), {"__init__": lambda self, *a, **kw: None})

    if "starlette.middleware.base" not in _already_loaded:
        sys.modules["starlette.middleware.base"].BaseHTTPMiddleware = type(
            "BaseHTTPMiddleware", (), {"__init__": lambda self, *a, **kw: None}
        )

    if "sqlalchemy.orm" not in _already_loaded:
        sys.modules["sqlalchemy.orm"].DeclarativeBase = type("DeclarativeBase", (), {})

    if "cachetools" not in _already_loaded:
        sys.modules["cachetools"].cached = lambda *a, **kw: (lambda fn: fn)

    if "pydantic" not in _already_loaded:
        class _BaseModel:
            def __init_subclass__(cls, **kw):
                super().__init_subclass__(**kw)
        sys.modules["pydantic"].BaseModel = _BaseModel

    # Always override @tool regardless of whether langchain_core was pre-loaded,
    # so the stub is used even when the real package is installed.
    def _tool(fn=None, **kw):
        if fn is None:
            return _tool
        fn.name = fn.__name__
        fn.invoke = lambda kwargs: fn(**kwargs)
        return fn
    lc_tools = sys.modules.get("langchain_core.tools")
    if lc_tools is None:
        lc_tools = _AutoMockModule("langchain_core.tools")
        sys.modules["langchain_core.tools"] = lc_tools
    lc_tools.tool = _tool


_install_mock_modules()
sys.path.insert(0, APP_DIR)

# Now safe to import
os.environ.setdefault("CATALOG_NAME", "test_cat")
os.environ.setdefault("SCHEMA_NAME", "test_schema")
os.environ.setdefault("LAKEBASE_CATALOG", "lb_cat")
os.environ.setdefault("WAREHOUSE_ID", "wh123")

import api_server  # noqa: E402
from agent import tools as agent_tools  # noqa: E402

# ---------------------------------------------------------------------------
# Helper: build a mock execute_sql from a fake graph
# ---------------------------------------------------------------------------

def _make_graph_executor(nodes: list[dict], edges: list[dict]):
    """Return a function that mimics execute_sql against in-memory graph data."""
    def fake_execute_sql(query: str, warehouse_id=None):
        q = query.lower()

        # Node lookup by IN clause
        if "graph_nodes" in q and "in (" in q:
            ids = _extract_in_values(query)
            result = [n for n in nodes if n["id"] in ids]
            # Apply extra equality filters
            if "node_type = " in query:
                val = _extract_eq(query, "node_type")
                result = [n for n in result if n.get("node_type") == val]
            if "domain = " in query:
                val = _extract_eq(query, "domain")
                result = [n for n in result if n.get("domain") == val]
            return result

        # Node query without IN clause (tools' SELECT queries)
        if "graph_nodes" in q:
            result = list(nodes)
            if "node_type = " in query:
                val = _extract_eq(query, "node_type")
                result = [n for n in result if n.get("node_type") == val]
            if "domain = " in query:
                val = _extract_eq(query, "domain")
                result = [n for n in result if n.get("domain") == val]
            if "id = " in query:
                val = _extract_eq(query, "id")
                result = [n for n in result if n["id"] == val]
            return result

        # Edge queries
        if "graph_edges" in q:
            result = list(edges)
            has_or = " or " in q

            if has_or:
                # "both" direction: (src IN (...) OR dst IN (...))
                ids = _extract_in_values(query)
                result = [e for e in edges if e["src"] in ids or e["dst"] in ids]
            elif "src in" in q:
                ids = _extract_in_values(query)
                result = [e for e in result if e["src"] in ids]
            elif "dst in" in q:
                ids = _extract_in_values(query)
                result = [e for e in result if e["dst"] in ids]
            elif "src = " in query:
                val = _extract_eq(query, "src")
                result = [e for e in result if e["src"] == val]

            if "relationship = " in query:
                val = _extract_eq(query, "relationship")
                result = [e for e in result if e["relationship"] == val]
            return result

        return []
    return fake_execute_sql


def _extract_in_values(query: str) -> set[str]:
    matches = re.findall(r"in\s*\(([^)]+)\)", query, re.IGNORECASE)
    ids: set[str] = set()
    for m in matches:
        ids.update(re.findall(r"'([^']+)'", m))
    return ids


def _extract_eq(query: str, col: str) -> str:
    m = re.search(rf"{col}\s*=\s*'([^']+)'", query, re.IGNORECASE)
    return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# Sample graph data
# ---------------------------------------------------------------------------

NODES = [
    {"id": "A", "node_type": "table", "domain": "sales", "subdomain": None,
     "security_level": "PUBLIC", "comment": "Orders table", "table_name": "orders"},
    {"id": "B", "node_type": "column", "domain": "sales", "subdomain": None,
     "security_level": "PUBLIC", "comment": "Order ID column", "table_name": "orders"},
    {"id": "C", "node_type": "column", "domain": "sales", "subdomain": None,
     "security_level": "PII", "comment": "Customer name", "table_name": "orders"},
    {"id": "D", "node_type": "table", "domain": "finance", "subdomain": None,
     "security_level": "PUBLIC", "comment": "Invoices table", "table_name": "invoices"},
    {"id": "E", "node_type": "column", "domain": "finance", "subdomain": None,
     "security_level": "PUBLIC", "comment": "Amount", "table_name": "invoices"},
]

EDGES = [
    {"src": "A", "dst": "B", "relationship": "has_column", "weight": "1.0"},
    {"src": "A", "dst": "C", "relationship": "has_column", "weight": "1.0"},
    {"src": "A", "dst": "D", "relationship": "similar_embedding", "weight": "0.92"},
    {"src": "D", "dst": "E", "relationship": "has_column", "weight": "1.0"},
]


@pytest.fixture(autouse=True)
def _mock_sql():
    """Patch execute_sql AND graph_query with fake graph data for every test.

    multi_hop_traverse calls execute_sql (with UC-rewritten table names).
    agent tools call graph_query (with public.graph_* table names).
    The fake executor matches on 'graph_nodes'/'graph_edges' substrings,
    so it handles both naming conventions.
    """
    fake = _make_graph_executor(NODES, EDGES)
    with patch.object(api_server, "execute_sql", side_effect=fake), \
         patch.object(api_server, "graph_query", side_effect=fake):
        yield


# ===========================================================================
# Tests: multi_hop_traverse
# ===========================================================================

class TestMultiHopTraverse:
    """Tests for the iterative multi-hop graph traversal function."""

    def test_single_hop_returns_direct_neighbors(self):
        """1 hop from A should reach B, C, D (direct outgoing edges from A)."""
        result = api_server.multi_hop_traverse(start_node="A", max_hops=1)
        node_ids = set(result["nodes"].keys())
        assert {"A", "B", "C", "D"}.issubset(node_ids)
        assert "E" not in node_ids

    def test_two_hops_reaches_transitive_neighbors(self):
        """2 hops from A should also reach E (A->D->E)."""
        result = api_server.multi_hop_traverse(start_node="A", max_hops=2)
        node_ids = set(result["nodes"].keys())
        assert "E" in node_ids

    def test_relationship_filter_restricts_edges(self):
        """Filtering by 'has_column' from A should NOT traverse to D (similar_embedding)."""
        result = api_server.multi_hop_traverse(start_node="A", max_hops=2, relationship="has_column")
        node_ids = set(result["nodes"].keys())
        assert "B" in node_ids
        assert "C" in node_ids
        assert "D" not in node_ids

    def test_leaf_node_returns_no_edges(self):
        """Starting from a leaf node with no outgoing edges returns empty edges."""
        result = api_server.multi_hop_traverse(start_node="E", max_hops=3)
        assert result["edges"] == []
        assert "E" in result["nodes"]

    def test_incoming_direction(self):
        """Incoming traversal from B should find A (A->B edge, reversed)."""
        result = api_server.multi_hop_traverse(start_node="B", max_hops=1, direction="incoming")
        edge_srcs = {e["src"] for e in result["edges"]}
        assert "A" in edge_srcs

    def test_both_direction(self):
        """Bidirectional from D should find A (incoming) and E (outgoing)."""
        result = api_server.multi_hop_traverse(start_node="D", max_hops=1, direction="both")
        node_ids = set(result["nodes"].keys())
        assert "A" in node_ids
        assert "E" in node_ids

    def test_zero_hops_returns_start_only(self):
        """max_hops=0 means no traversal at all."""
        result = api_server.multi_hop_traverse(start_node="A", max_hops=0)
        assert result["edges"] == []
        assert "A" in result["nodes"]

    def test_hops_value_matches_max(self):
        result = api_server.multi_hop_traverse(start_node="A", max_hops=1)
        assert result["hops"] == 1

    def test_result_has_required_keys(self):
        result = api_server.multi_hop_traverse(start_node="A", max_hops=1)
        for key in ("nodes", "edges", "start_node", "hops", "node_count", "edge_count"):
            assert key in result

    def test_edges_collected_across_hops(self):
        """All edges traversed should be accumulated, not just the last hop."""
        result = api_server.multi_hop_traverse(start_node="A", max_hops=2)
        assert len(result["edges"]) >= 4

    def test_node_details_fetched_for_all_discovered(self):
        """Every node id appearing in edges should have its details fetched."""
        result = api_server.multi_hop_traverse(start_node="A", max_hops=2)
        edge_node_ids = set()
        for e in result["edges"]:
            edge_node_ids.add(e["src"])
            edge_node_ids.add(e["dst"])
        fetched_ids = set(result["nodes"].keys())
        assert edge_node_ids.issubset(fetched_ids)


# ===========================================================================
# Tests: lb_fq / fq helpers
# ===========================================================================

class TestHelpers:
    def test_fq_uses_main_catalog(self):
        assert api_server.fq("my_table") == f"`{api_server.CATALOG}`.`{api_server.SCHEMA}`.`my_table`"


# ===========================================================================
# Tests: agent tools
# ===========================================================================

class TestQueryGraphNodes:
    def test_no_filter_returns_all(self):
        result = agent_tools.query_graph_nodes.invoke({})
        assert len(result) == len(NODES)

    def test_filter_by_node_type(self):
        result = agent_tools.query_graph_nodes.invoke({"node_type": "table"})
        assert all(n["node_type"] == "table" for n in result)
        assert len(result) == 2  # A and D

    def test_filter_by_domain(self):
        result = agent_tools.query_graph_nodes.invoke({"domain": "finance"})
        assert all(n["domain"] == "finance" for n in result)

    def test_combined_filters(self):
        result = agent_tools.query_graph_nodes.invoke({"node_type": "table", "domain": "sales"})
        assert len(result) == 1
        assert result[0]["id"] == "A"


class TestGetNodeDetails:
    def test_existing_node(self):
        result = agent_tools.get_node_details.invoke({"node_id": "A"})
        assert result["id"] == "A"
        assert result["node_type"] == "table"

    def test_missing_node_returns_empty_dict(self):
        result = agent_tools.get_node_details.invoke({"node_id": "NONEXISTENT"})
        assert result == {}


class TestFindSimilarNodes:
    def test_returns_similar_embedding_edges(self):
        with patch.object(api_server, "graph_query", return_value=[
            {"similar_node": "D", "similarity": "0.92", "node_type": "table", "domain": "finance", "comment": "Invoices"}
        ]):
            result = agent_tools.find_similar_nodes.invoke({"node_id": "A"})
            assert len(result) == 1
            assert result[0]["similar_node"] == "D"

    def test_respects_min_similarity(self):
        """Passing a high min_similarity should filter out weaker matches."""
        with patch.object(api_server, "graph_query", return_value=[]) as mock_gq:
            agent_tools.find_similar_nodes.invoke({"node_id": "A", "min_similarity": 0.99})
            called_query = mock_gq.call_args[0][0]
            assert "0.99" in called_query


class TestTraverseGraph:
    def test_caps_max_hops_at_five(self):
        """traverse_graph should cap max_hops at 5 regardless of input."""
        with patch.object(api_server, "multi_hop_traverse", return_value={
            "nodes": [], "edges": [], "paths": [], "hops_completed": 0
        }) as mock_t:
            agent_tools.traverse_graph.invoke({"start_node": "A", "max_hops": 100})
            _, kw = mock_t.call_args
            assert kw["max_hops"] == 5

    def test_passes_relationship_filter(self):
        with patch.object(api_server, "multi_hop_traverse", return_value={
            "nodes": [], "edges": [], "paths": [], "hops_completed": 0
        }) as mock_t:
            agent_tools.traverse_graph.invoke({"start_node": "A", "relationship": "has_column"})
            _, kw = mock_t.call_args
            assert kw["relationship"] == "has_column"

    def test_passes_direction(self):
        with patch.object(api_server, "multi_hop_traverse", return_value={
            "nodes": [], "edges": [], "paths": [], "hops_completed": 0
        }) as mock_t:
            agent_tools.traverse_graph.invoke({"start_node": "D", "direction": "both"})
            _, kw = mock_t.call_args
            assert kw["direction"] == "both"


# ===========================================================================
# Tests: tool registration
# ===========================================================================

class TestToolRegistration:
    def test_all_tools_contains_expected_tools(self):
        expected = {"query_graph_nodes", "get_node_details", "find_similar_nodes", "traverse_graph"}
        actual = {t.name for t in agent_tools.ALL_TOOLS}
        assert actual == expected

    def test_traverse_graph_is_registered(self):
        """The agent must have traverse_graph for multi-hop queries."""
        assert any(t.name == "traverse_graph" for t in agent_tools.ALL_TOOLS)


# ===========================================================================
# Tests: apply-tags selection filtering
# ===========================================================================


class TestApplyTagsSelectionFiltering:
    """Tests that _apply_ontology_tags_from_tables respects selections."""

    def _mock_execute_sql(self, entities=None, tkb=None, props=None, rels=None):
        """Return a side_effect for execute_sql that serves canned data by query shape."""
        def _side_effect(query, **kw):
            q = query.strip().upper()
            if "ONTOLOGY_ENTITIES" in q:
                return entities or []
            if "TABLE_KNOWLEDGE_BASE" in q:
                return tkb or []
            if "ONTOLOGY_COLUMN_PROPERTIES" in q:
                return props or []
            if "ONTOLOGY_RELATIONSHIPS" in q:
                return rels or []
            if "ALTER TABLE" in q:
                return []
            return []
        return _side_effect

    @patch.dict(os.environ, {"WAREHOUSE_ID": "wh123", "CATALOG_NAME": "c", "SCHEMA_NAME": "s"})
    @patch.object(api_server, "execute_sql")
    def test_no_selections_applies_all(self, mock_sql):
        entities = [
            {"entity_type": "Patient", "confidence": 0.9, "source_tables": '["c.s.patients"]',
             "entity_role": "primary", "granularity": "table"},
            {"entity_type": "Order", "confidence": 0.8, "source_tables": '["c.s.orders"]',
             "entity_role": "primary", "granularity": "table"},
        ]
        mock_sql.side_effect = self._mock_execute_sql(entities=entities)
        result = api_server._apply_ontology_tags_from_tables(selections=None)
        alter_calls = [c for c in mock_sql.call_args_list if "ALTER TABLE" in c[0][0]]
        assert len(alter_calls) == 2

    @patch.dict(os.environ, {"WAREHOUSE_ID": "wh123", "CATALOG_NAME": "c", "SCHEMA_NAME": "s"})
    @patch.object(api_server, "execute_sql")
    def test_selections_filters_to_matching_entity(self, mock_sql):
        entities = [
            {"entity_type": "Patient", "confidence": 0.9, "source_tables": '["c.s.patients"]',
             "entity_role": "primary", "granularity": "table"},
            {"entity_type": "Order", "confidence": 0.8, "source_tables": '["c.s.orders"]',
             "entity_role": "primary", "granularity": "table"},
        ]
        mock_sql.side_effect = self._mock_execute_sql(entities=entities)
        sels = [{"entity_type": "Patient", "source_tables": ["c.s.patients"]}]
        result = api_server._apply_ontology_tags_from_tables(selections=sels)
        alter_calls = [c for c in mock_sql.call_args_list if "ALTER TABLE" in c[0][0]]
        assert len(alter_calls) == 1
        assert "patients" in alter_calls[0][0][0]

    @patch.dict(os.environ, {"WAREHOUSE_ID": "wh123", "CATALOG_NAME": "c", "SCHEMA_NAME": "s"})
    @patch.object(api_server, "execute_sql")
    def test_selections_filters_column_tags(self, mock_sql):
        props = [
            {"table_name": "c.s.patients", "column_name": "mrn", "property_role": "primary_key",
             "confidence": 0.9, "linked_entity_type": ""},
            {"table_name": "c.s.orders", "column_name": "order_id", "property_role": "primary_key",
             "confidence": 0.85, "linked_entity_type": ""},
        ]
        mock_sql.side_effect = self._mock_execute_sql(entities=[], props=props)
        sels = [{"entity_type": "Patient", "source_tables": ["c.s.patients"]}]
        result = api_server._apply_ontology_tags_from_tables(selections=sels)
        alter_calls = [c for c in mock_sql.call_args_list if "ALTER TABLE" in c[0][0] and "ALTER COLUMN" in c[0][0]]
        assert len(alter_calls) == 1
        assert "patients" in alter_calls[0][0][0]

    @patch.dict(os.environ, {"WAREHOUSE_ID": "wh123", "CATALOG_NAME": "c", "SCHEMA_NAME": "s"})
    @patch.object(api_server, "execute_sql")
    def test_empty_selections_applies_all(self, mock_sql):
        entities = [
            {"entity_type": "Order", "confidence": 0.8, "source_tables": '["c.s.orders"]',
             "entity_role": "primary", "granularity": "table"},
        ]
        mock_sql.side_effect = self._mock_execute_sql(entities=entities)
        result = api_server._apply_ontology_tags_from_tables(selections=[])
        alter_calls = [c for c in mock_sql.call_args_list if "ALTER TABLE" in c[0][0]]
        assert len(alter_calls) == 1
