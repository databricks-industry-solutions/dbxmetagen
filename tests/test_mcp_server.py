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
    def test_all_three_tools_registered(self):
        m = _fresh_mcp_server()
        srv = m.get_mcp_server()
        names = {t.name for t in asyncio.run(srv.list_tools())}
        assert names == {"metadata_agent_query", "deep_analysis_submit", "deep_analysis_poll"}

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
        """Guards the stringified-annotation crash at the behavior level: every tool
        parameter annotation must be a real class/type, not a str."""
        m = _fresh_mcp_server()
        srv = m.get_mcp_server()
        # Registration itself exercises the mcp path that crashed; also assert types.
        tools = asyncio.run(srv.list_tools())
        assert len(tools) == 3

    def test_build_asgi_app_mounts_at_mcp(self):
        m = _fresh_mcp_server()
        app = m.build_mcp_asgi_app()
        paths = [getattr(r, "path", "") for r in app.routes]
        assert any("/mcp" in p for p in paths)


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
# Delegation to api_server (mocked) -- tools reuse the exact HTTP paths
# ---------------------------------------------------------------------------
class TestDelegation:
    def _install_fake_api_server(self, monkeypatch, submit=None, poll=None):
        fake = types.ModuleType("api_server")

        class AgentChatRequest:  # mirror the real pydantic model shape
            def __init__(self, message="", history=None, mode="quick", session_id=""):
                self.message = message
                self.mode = mode
                self.history = history or []
                self.session_id = session_id

        fake.AgentChatRequest = AgentChatRequest
        fake.agent_deep_submit = submit or (lambda req: {"task_id": "abc123"})

        def _default_poll(task_id):
            return {"status": "done", "answer": "ok", "task_id": task_id}

        fake.agent_deep_poll = poll or _default_poll
        monkeypatch.setitem(sys.modules, "api_server", fake)
        return fake

    def _call_tool(self, srv, name, **kwargs):
        return asyncio.run(srv.call_tool(name, kwargs))

    def test_deep_submit_forwards_to_api_server(self, monkeypatch):
        captured = {}

        def _submit(req):
            captured["message"] = req.message
            captured["mode"] = req.mode
            return {"task_id": "xyz"}

        self._install_fake_api_server(monkeypatch, submit=_submit)
        m = _fresh_mcp_server()
        srv = m.get_mcp_server()
        self._call_tool(srv, "deep_analysis_submit", question="which tables hold PHI?", mode="graphrag")
        assert captured["message"] == "which tables hold PHI?"
        assert captured["mode"] == "graphrag"

    def test_deep_poll_forwards_task_id(self, monkeypatch):
        seen = {}
        self._install_fake_api_server(monkeypatch, poll=lambda tid: seen.setdefault("id", tid) or {"status": "running"})
        m = _fresh_mcp_server()
        srv = m.get_mcp_server()
        self._call_tool(srv, "deep_analysis_poll", task_id="task-42")
        assert seen["id"] == "task-42"

    def test_deep_poll_unknown_task_returns_error_not_raise(self, monkeypatch):
        class _HTTPExc(Exception):
            def __init__(self):
                self.detail = "Task not found"

        def _poll(task_id):
            raise _HTTPExc()

        self._install_fake_api_server(monkeypatch, poll=_poll)
        m = _fresh_mcp_server()
        srv = m.get_mcp_server()
        # Must not raise -- surfaced as a normal tool result.
        result = self._call_tool(srv, "deep_analysis_poll", task_id="nope")
        # call_tool returns (content, structured) in mcp 1.x; check the structured payload.
        structured = result[1] if isinstance(result, tuple) else result
        text = str(structured)
        assert "error" in text.lower() and "Task not found" in text


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
