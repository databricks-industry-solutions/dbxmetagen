"""Custom MCP server exposing the dbxmetagen metadata AGENT (not just query
functions) to other MCP clients -- notably another Databricks agent.

Why this exists
---------------
The two *managed* MCP servers dbxmetagen provisions (UC Functions + Vector Search,
see docs/MCP_SERVERS.md) expose the metadata *data* as purpose-built SQL/search
tools. They cannot expose the metadata *agent's reasoning* -- the intent-routed
ReAct agent and the multi-hop GraphRAG deep-analysis pipeline. This module wraps
that agent as MCP tools so an external agent can delegate a whole metadata question
("which tables hold PHI and how do they join to the encounters fact?") and get the
agent's synthesized answer, not just raw rows.

Auth / OBO
----------
The tools run *in the same request context* as the mounted ASGI app, so the
`_obo_token_var` ContextVar that `OBOMiddleware` sets from `x-forwarded-access-token`
is visible here. The agent's tools resolve identity through
`api_server._get_effective_client()`, which reads that ContextVar -- so every MCP
call automatically runs under the INVOKING USER's Unity Catalog identity, with NO
agent-code changes, provided the caller forwards the user token. When no token is
present the app falls back to its service principal (same semantics as every other
route). Propagation of the ContextVar from the parent app's middleware into a tool
running inside this mounted sub-app was verified empirically before shipping.

Wiring (in api_server.py)
-------------------------
- gated on env `ENABLE_AGENT_MCP` (default off -- does not ship enabled to customers);
- the app lifespan must run `get_mcp_server().session_manager.run()`;
- mount `build_mcp_asgi_app()` at `/mcp` BEFORE the `/` static catch-all mount, or
  the SPA StaticFiles handler shadows it.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

# MCP is an optional dependency: if the package is absent (e.g. a partial install)
# the app must still start -- the /mcp route is simply not mounted.
try:
    from mcp.server.fastmcp import FastMCP
    from mcp.server.transport_security import TransportSecuritySettings
    _MCP_AVAILABLE = True
except Exception as _e:  # pragma: no cover - import guard
    FastMCP = None  # type: ignore
    TransportSecuritySettings = None  # type: ignore
    _MCP_AVAILABLE = False
    logger.info("mcp package not installed -- custom agent MCP route disabled (%s)", _e)


def _transport_security():
    """Transport security for the streamable-HTTP server.

    FastMCP defaults to DNS-rebinding protection ON with only localhost/127.0.0.1 in
    allowed_hosts. Behind Databricks Apps ingress the Host header is the app's real
    domain, so the default would reject EVERY request with 421 Misdirected Request.
    Databricks Apps ingress already terminates TLS and authenticates the caller (and
    injects the OBO token), so the browser-oriented DNS-rebinding vector this guard
    targets does not apply here. `MCP_ALLOWED_HOSTS` (comma-separated) can re-enable
    strict host pinning to specific app hostnames if desired.
    """
    hosts = os.environ.get("MCP_ALLOWED_HOSTS", "").strip()
    if hosts:
        allowed = [h.strip() for h in hosts.split(",") if h.strip()]
        return TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=allowed,
            allowed_origins=allowed,
        )
    return TransportSecuritySettings(enable_dns_rebinding_protection=False)


def mcp_enabled() -> bool:
    """True only when explicitly enabled AND the mcp package is importable."""
    return _MCP_AVAILABLE and os.environ.get("ENABLE_AGENT_MCP", "false").lower() == "true"


_mcp_singleton: Optional["FastMCP"] = None


def get_mcp_server() -> "FastMCP":
    """Return the process-wide FastMCP server, building + registering tools once."""
    global _mcp_singleton
    if _mcp_singleton is not None:
        return _mcp_singleton
    if not _MCP_AVAILABLE:
        raise RuntimeError("mcp package is not installed")

    # stateless_http=True: each call is self-contained (no server-side session state
    # to coordinate across the app's workers) -- the right mode for a tool surface
    # fronted by Databricks Apps.
    server = FastMCP(
        name="dbxmetagen-metadata-agent",
        instructions=(
            "Tools for delegating metadata questions to the dbxmetagen metadata "
            "intelligence agent. Use `metadata_agent_query` for most questions "
            "(fast, intent-routed). Use `deep_analysis_submit` + `deep_analysis_poll` "
            "for complex multi-hop questions that need GraphRAG traversal. All calls "
            "run under the invoking user's Unity Catalog identity."
        ),
        stateless_http=True,
        transport_security=_transport_security(),
    )
    _register_tools(server)
    _mcp_singleton = server
    return server


def _register_tools(server: "FastMCP") -> None:
    """Register the agent tools. Agent + api_server imports are deferred to call
    time to avoid an import cycle (api_server imports this module)."""

    @server.tool()
    async def metadata_agent_query(question: str, mode: str = "quick") -> dict:
        """Ask the dbxmetagen metadata agent a question about the data catalog.

        Runs the intent-routed metadata agent over vector search, knowledge-base
        SQL, and the knowledge graph, and returns its synthesized answer. Best for
        table/column discovery, PII/PHI governance, lineage, and relationship
        questions. For heavy multi-hop analysis prefer deep_analysis_submit.

        Args:
            question: The natural-language metadata question.
            mode: "quick" (default; fast intent-routed ReAct) or "graphrag"
                  (multi-hop; slower -- consider deep_analysis_submit for those).

        Returns:
            {answer, tool_calls, intent, mode, elapsed_ms, ...} from the agent.
        """
        from agent.guardrails import validate_input
        from agent.metadata_agent import run_metadata_agent

        ok, err = validate_input(question)
        if not ok:
            return {"error": err, "answer": ""}
        safe_mode = mode if mode in ("quick", "graphrag", "baseline", "deep") else "quick"
        result = await run_metadata_agent(question, mode=safe_mode)
        return result if isinstance(result, dict) else {"answer": str(result)}

    @server.tool()
    def deep_analysis_submit(question: str, mode: str = "graphrag") -> dict:
        """Start a long-running GraphRAG deep analysis as a background task.

        Returns immediately with a task_id; call deep_analysis_poll(task_id) until
        status is "done" or "error". Use this (not metadata_agent_query) for complex
        questions that need multi-hop graph traversal and can take a minute or more.

        Args:
            question: The natural-language metadata question.
            mode: "graphrag" (default, full graph) or "baseline" (KB-only).

        Returns:
            {task_id}.
        """
        # Reuse the exact HTTP submit path so behavior (guardrails, timeouts, OBO
        # thread propagation via _spawn_with_obo, task cleanup) is identical.
        import api_server

        req = api_server.AgentChatRequest(message=question, mode=mode)
        return api_server.agent_deep_submit(req)

    @server.tool()
    def deep_analysis_poll(task_id: str) -> dict:
        """Poll a deep analysis task started with deep_analysis_submit.

        Returns the task record: {status: running|done|error, stage, answer?,
        tool_calls?, elapsed_ms, ...}. Poll until status is "done" or "error".
        """
        import api_server

        try:
            return api_server.agent_deep_poll(task_id)
        except Exception as exc:
            # agent_deep_poll raises HTTPException(404) for unknown ids; surface it
            # as a normal tool result rather than a transport error.
            detail = getattr(exc, "detail", str(exc))
            return {"status": "error", "error": str(detail), "task_id": task_id}


def build_mcp_asgi_app() -> Any:
    """Return the streamable-HTTP ASGI app to mount at /mcp."""
    return get_mcp_server().streamable_http_app()
