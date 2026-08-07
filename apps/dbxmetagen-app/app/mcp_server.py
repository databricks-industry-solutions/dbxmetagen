"""Custom MCP server exposing the dbxmetagen metadata AGENT (not just query
functions) to other MCP clients -- notably another Databricks agent.

Why this exists
---------------
The two *managed* MCP servers dbxmetagen provisions (UC Functions + Vector Search,
see docs/MCP_SERVERS.md) expose the metadata *data* as purpose-built SQL/search
tools. They cannot expose the metadata *agent's reasoning* -- the multi-hop GraphRAG
research pipeline. This module wraps that research agent behind a SINGLE MCP tool,
`ask_metadata_agent`, so an external agent can delegate a whole metadata question
("which tables hold PHI and how do they join to the encounters fact?") and get the
agent's synthesized answer, not just raw rows. The tool blocks until the research
run completes.

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

import logging
import os
from typing import Any, Optional

# NB: deliberately NO `from __future__ import annotations` here. It stringifies all
# annotations at runtime, and mcp 1.10-1.12's Tool.from_function does
# `issubclass(param.annotation, Context)` -- which raises TypeError on a string
# annotation, crashing tool registration (and app startup) on those versions. Newer
# mcp (1.29+) tolerates it, which is why this only surfaced on the app's resolved
# version, not locally. Keep annotations as real objects. Verified on mcp 1.12.0.

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
            "Delegate a metadata question to the dbxmetagen metadata research agent "
            "via the single `ask_metadata_agent` tool. It runs the full GraphRAG "
            "research pipeline (vector search + multi-hop knowledge-graph traversal + "
            "knowledge-base SQL) and returns a synthesized answer, running under the "
            "invoking user's Unity Catalog identity. It blocks until the analysis "
            "completes (typically ~90s; may take a minute or more for complex questions)."
        ),
        stateless_http=True,
        transport_security=_transport_security(),
    )
    _register_tools(server)
    _mcp_singleton = server
    return server


def _register_tools(server: "FastMCP") -> None:
    """Register the single agent tool. Agent imports are deferred to call time to
    avoid an import cycle (api_server imports this module)."""

    @server.tool()
    async def ask_metadata_agent(question: str, mode: str = "graphrag") -> dict:
        """Ask the dbxmetagen metadata RESEARCH agent a question about the data catalog.

        Runs the full GraphRAG research pipeline -- hybrid vector search over the
        metadata, multi-hop knowledge-graph traversal, and knowledge-base SQL -- and
        returns a single synthesized answer. Handles table/column discovery, PII/PHI
        governance, lineage, relationships, and open-ended analytical questions. This
        BLOCKS until the analysis completes (typically ~90s; longer for complex
        questions). Runs under the invoking user's Unity Catalog identity.

        Args:
            question: The natural-language metadata question.
            mode: "graphrag" (default, full knowledge graph) or "baseline"
                  (restricted to the 3 core knowledge-base tables -- faster, less rich).

        Returns:
            {answer, tool_calls, mode, intent, ...} from the research agent.
        """
        import asyncio

        from agent.guardrails import validate_input
        from agent.deep_analysis import run_deep_analysis

        ok, err = validate_input(question)
        if not ok:
            return {"error": err, "answer": ""}
        safe_mode = mode if mode in ("graphrag", "baseline") else "graphrag"
        # run_deep_analysis is synchronous and CPU/IO-heavy. asyncio.to_thread copies
        # the current context into the worker thread, so the OBO _obo_token_var set by
        # OBOMiddleware propagates and the agent's tools run as the invoking user.
        result = await asyncio.to_thread(run_deep_analysis, question, safe_mode)
        return result if isinstance(result, dict) else {"answer": str(result)}


def build_mcp_asgi_app() -> Any:
    """Return the streamable-HTTP ASGI app to mount at /mcp."""
    return get_mcp_server().streamable_http_app()
