"""Programmatic Genie space generation from dbxmetagen metadata.

Public API:
    assemble_genie_context  -- gather KB/FK/ontology/MV metadata into context
    build_genie_space       -- end-to-end: assemble -> LLM agent -> create space
"""

import json
import logging
import queue
from typing import Any, Dict, List, Optional

from databricks.sdk import WorkspaceClient

from .context import GenieContextAssembler
from .agent import run_genie_agent
from .schema import build_serialized_space

logger = logging.getLogger(__name__)

__all__ = ["assemble_genie_context", "build_genie_space"]


def assemble_genie_context(
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_identifiers: List[str],
    questions: Optional[List[str]] = None,
    metric_view_names: Optional[List[str]] = None,
    ws: Optional[WorkspaceClient] = None,
) -> Dict[str, Any]:
    """Gather KB/FK/ontology/MV metadata into structured context for Genie generation.

    Args:
        warehouse_id: SQL warehouse ID for Statement Execution API queries.
        catalog: Unity Catalog catalog name.
        schema: Unity Catalog schema name.
        table_identifiers: FQN table identifiers (catalog.schema.table).
        questions: Optional sample business questions.
        metric_view_names: Optional filter to specific metric views.
        ws: Optional WorkspaceClient (created via default auth if omitted).

    Returns:
        Dict with keys: context_text, join_specs, data_sources, sql_snippets, questions, reference_text.
    """
    ws = ws or WorkspaceClient()
    assembler = GenieContextAssembler(ws, warehouse_id, catalog, schema)
    return assembler.assemble(table_identifiers, questions, metric_view_names)


def build_genie_space(
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_identifiers: List[str],
    title: str,
    questions: Optional[List[str]] = None,
    metric_view_names: Optional[List[str]] = None,
    model_endpoint: str = "databricks-claude-sonnet-4-6",
    description: Optional[str] = None,
    create: bool = True,
    ws: Optional[WorkspaceClient] = None,
) -> Dict[str, Any]:
    """End-to-end: assemble context -> 3-phase LLM agent -> validate -> optionally create Genie space.

    Args:
        warehouse_id: SQL warehouse ID.
        catalog: Unity Catalog catalog name.
        schema: Unity Catalog schema name.
        table_identifiers: FQN table identifiers.
        title: Display name for the Genie space.
        questions: Optional sample business questions.
        metric_view_names: Optional filter to specific metric views.
        model_endpoint: LLM endpoint for generation (default: databricks-claude-sonnet-4-6).
        description: Optional space description (auto-generated from context if omitted).
        create: If True, create the space via Genie REST API. If False, return serialized_space only.
        ws: Optional WorkspaceClient.

    Returns:
        Dict with keys: space_id (if create=True), title, description, serialized_space.
    """
    ws = ws or WorkspaceClient()

    ctx = assemble_genie_context(
        warehouse_id, catalog, schema, table_identifiers, questions, metric_view_names, ws
    )

    progress = queue.Queue()
    raw = run_genie_agent(ws, warehouse_id, ctx, progress, model_endpoint)

    space_json = build_serialized_space(raw)

    auto_desc = description or raw.get("description", f"Genie space for {catalog}.{schema}")

    if not create:
        return {"title": title, "description": auto_desc, "serialized_space": space_json}

    payload = {
        "title": title,
        "warehouse_id": warehouse_id,
        "description": auto_desc,
        "serialized_space": json.dumps(space_json),
    }
    resp = ws.api_client.do("POST", "/api/2.0/genie/spaces", body=payload)
    space_id = resp.get("space_id", resp.get("id"))
    logger.info("Created Genie space '%s' (%s)", title, space_id)

    return {
        "space_id": space_id,
        "title": title,
        "description": auto_desc,
        "serialized_space": space_json,
    }
