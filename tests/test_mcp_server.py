"""Tests for the custom agent MCP server (apps/dbxmetagen-app/app/mcp_server.py).

Runs with the REAL `mcp` / starlette / pydantic packages, so it must NOT run in the
same process as tests/test_api_routes.py (which mocks those modules). run_tests.sh
runs this file as its own isolated suite -- never `pytest tests/` directly.
"""

import asyncio
import os
import sys
import types
from unittest.mock import MagicMock

import pytest

APP_DIR = os.path.join(os.path.dirname(__file__), "..", "apps", "dbxmetagen-app", "app")
sys.path.insert(0, APP_DIR)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("ENABLE_AGENT_MCP", raising=False)
    yield


def _fresh_mcp_server():
    """Import mcp_server with a reset singleton so tool registration re-runs."""
    sys.modules.pop("mcp_server", None)
    import mcp_server  # noqa: E402
    return mcp_server


# ---------------------------------------------------------------------------
# Gating
# ---------------------------------------------------------------------------
class TestMcpGating:
    def test_disabled_by_default(self, monkeypatch):
        monkeypatch.delenv("ENABLE_AGENT_MCP", raising=False)
        m = _fresh_mcp_server()
        assert m.mcp_enabled() is False

    def test_enabled_when_flag_set(self, monkeypatch):
        monkeypatch.setenv("ENABLE_AGENT_MCP", "true")
        m = _fresh_mcp_server()
        # Only true when the mcp package is importable; it is in this suite.
        assert m.mcp_enabled() is True

    def test_flag_case_insensitive_and_off_values(self, monkeypatch):
        m = _fresh_mcp_server()
        for val, expected in [("TRUE", True), ("false", False), ("0", False), ("", False)]:
            monkeypatch.setenv("ENABLE_AGENT_MCP", val)
            assert m.mcp_enabled() is expected


# ---------------------------------------------------------------------------
# Tool registration
# ---------------------------------------------------------------------------
class TestToolRegistration:
    def test_single_research_tool_registered(self):
        m = _fresh_mcp_server()
        srv = m.get_mcp_server()
        names = {t.name for t in asyncio.run(srv.list_tools())}
        assert names == {"ask_metadata_agent"}

    def test_server_is_singleton(self):
        m = _fresh_mcp_server()
        assert m.get_mcp_server() is m.get_mcp_server()

    def test_tools_have_descriptions(self):
        m = _fresh_mcp_server()
        srv = m.get_mcp_server()
        for t in asyncio.run(srv.list_tools()):
            assert t.description and len(t.description) > 20

    def test_module_has_no_future_annotations_import(self):
        """`from __future__ import annotations` stringifies annotations, which
        crashes tool registration (issubclass on a str) on mcp 1.10-1.12 and thus
        app startup. It must never be reintroduced into mcp_server.py."""
        import os
        path = os.path.join(APP_DIR, "mcp_server.py")
        with open(path) as f:
            # A real import is a statement at column 0; the explanatory comment that
            # mentions the string is indented / prefixed with '#', so match line starts.
            offending = [
                ln for ln in f.read().splitlines()
                if ln.strip().startswith("from __future__ import annotations")
            ]
        assert not offending, f"future-annotations import present: {offending}"

    def test_tool_annotations_are_real_classes_not_strings(self):
        """Guards the stringified-annotation crash at the behavior level: the tool
        must register (which exercises the mcp path that crashed on stringified
        annotations)."""
        m = _fresh_mcp_server()
        srv = m.get_mcp_server()
        tools = asyncio.run(srv.list_tools())
        assert len(tools) == 1

    def test_build_asgi_app_uses_clean_root_route(self):
        """streamable_http_path must be '/' so the sub-app's internal route is '/'.
        Mounted at '/mcp' in api_server, that yields a clean '/mcp' endpoint instead
        of the doubled '/mcp/mcp' the default ('/mcp') would produce."""
        m = _fresh_mcp_server()
        app = m.build_mcp_asgi_app()
        paths = [getattr(r, "path", "") for r in app.routes]
        assert "/" in paths
        assert "/mcp" not in paths  # the internal doubled route must be gone


class TestTransportSecurity:
    """FastMCP defaults to DNS-rebinding protection ON with only localhost allowed,
    which 421s behind Databricks Apps ingress (real hostname). We must default it
    OFF (ingress authenticates) unless MCP_ALLOWED_HOSTS pins specific hosts."""

    def test_default_disables_host_check(self, monkeypatch):
        monkeypatch.delenv("MCP_ALLOWED_HOSTS", raising=False)
        m = _fresh_mcp_server()
        ts = m._transport_security()
        assert ts.enable_dns_rebinding_protection is False

    def test_allowed_hosts_env_pins_hosts(self, monkeypatch):
        monkeypatch.setenv("MCP_ALLOWED_HOSTS", "myapp.databricksapps.com, other.com")
        m = _fresh_mcp_server()
        ts = m._transport_security()
        assert ts.enable_dns_rebinding_protection is True
        assert "myapp.databricksapps.com" in ts.allowed_hosts
        assert "other.com" in ts.allowed_hosts

    def test_server_uses_non_localhost_permissive_default(self, monkeypatch):
        monkeypatch.delenv("MCP_ALLOWED_HOSTS", raising=False)
        m = _fresh_mcp_server()
        srv = m.get_mcp_server()
        # The server's effective transport security must not be the localhost-only default.
        assert srv.settings.transport_security.enable_dns_rebinding_protection is False


# ---------------------------------------------------------------------------
# ask_metadata_agent delegation to the research agent (run_deep_analysis, stubbed)
# ---------------------------------------------------------------------------
class TestAskMetadataAgent:
    def _install_agent_stubs(self, monkeypatch, run=None, validate=None):
        """Stub agent.deep_analysis.run_deep_analysis + agent.guardrails.validate_input,
        which ask_metadata_agent imports at call time."""
        da = types.ModuleType("agent.deep_analysis")
        da.run_deep_analysis = run or (lambda q, mode: {"answer": "ok", "mode": mode})
        gr = types.ModuleType("agent.guardrails")
        gr.validate_input = validate or (lambda q: (True, ""))
        # Ensure the parent package exists so submodule import resolves.
        if "agent" not in sys.modules:
            monkeypatch.setitem(sys.modules, "agent", types.ModuleType("agent"))
        monkeypatch.setitem(sys.modules, "agent.deep_analysis", da)
        monkeypatch.setitem(sys.modules, "agent.guardrails", gr)

    def _call_tool(self, srv, name, **kwargs):
        return asyncio.run(srv.call_tool(name, kwargs))

    def test_forwards_question_to_run_deep_analysis(self, monkeypatch):
        captured = {}

        def _run(question, mode):
            captured["question"] = question
            captured["mode"] = mode
            return {"answer": "PHI in 3 tables", "mode": mode}

        self._install_agent_stubs(monkeypatch, run=_run)
        m = _fresh_mcp_server()
        srv = m.get_mcp_server()
        self._call_tool(srv, "ask_metadata_agent", question="which tables hold PHI?")
        assert captured["question"] == "which tables hold PHI?"
        # Default mode is the research pipeline.
        assert captured["mode"] == "graphrag"

    def test_baseline_mode_forwarded(self, monkeypatch):
        captured = {}
        self._install_agent_stubs(monkeypatch, run=lambda q, mode: captured.update(mode=mode) or {"answer": "x"})
        m = _fresh_mcp_server()
        srv = m.get_mcp_server()
        self._call_tool(srv, "ask_metadata_agent", question="q", mode="baseline")
        assert captured["mode"] == "baseline"

    def test_invalid_mode_falls_back_to_graphrag(self, monkeypatch):
        captured = {}
        self._install_agent_stubs(monkeypatch, run=lambda q, mode: captured.update(mode=mode) or {"answer": "x"})
        m = _fresh_mcp_server()
        srv = m.get_mcp_server()
        self._call_tool(srv, "ask_metadata_agent", question="q", mode="quick")  # not a research mode
        assert captured["mode"] == "graphrag"

    def test_guardrail_rejection_short_circuits(self, monkeypatch):
        ran = {"called": False}

        def _run(question, mode):
            ran["called"] = True
            return {"answer": "should not run"}

        self._install_agent_stubs(
            monkeypatch, run=_run, validate=lambda q: (False, "blocked by guardrail")
        )
        m = _fresh_mcp_server()
        srv = m.get_mcp_server()
        result = self._call_tool(srv, "ask_metadata_agent", question="bad")
        assert ran["called"] is False
        structured = result[1] if isinstance(result, tuple) else result
        assert "blocked by guardrail" in str(structured)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
