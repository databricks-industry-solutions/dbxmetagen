"""Dual-mode Text-to-SQL analyst agent.

Blind mode: only schema introspection (DESCRIBE, SHOW TABLES, sample rows).
Enriched mode: full semantic layer (vector search, KB, ontology, FK, profiling, metrics).
Compare mode: runs both in parallel for side-by-side comparison.
"""

import json
import logging
import queue
import threading
import time
from typing import Any, Dict, List, Optional

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.prebuilt import create_react_agent

from agent.common import (
    CATALOG, SCHEMA,
    get_llm, build_react_graph, history_to_messages, extract_tool_calls, extract_token_usage,
)
from agent.guardrails import GuardrailConfig, SAFETY_PROMPT_BLOCK, sanitize_output
from agent.analyst_tools import BLIND_TOOLS, ENRICHED_TOOLS, execute_query
from agent.tracing import trace, ensure_mlflow_context, tag_trace

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Data schema discovery
# -------------------------------------------------------------------

_data_schemas_cache = None


def _get_data_schemas() -> str:
    """Query table_knowledge_base for distinct catalog.schema pairs the app has indexed."""
    global _data_schemas_cache
    if _data_schemas_cache is not None:
        return _data_schemas_cache
    try:
        from agent.analyst_tools import _run_sql, _fq
        result = _run_sql(
            f"SELECT DISTINCT catalog, `schema` FROM {_fq('table_knowledge_base')}",
            timeout="15s", max_rows=100,
        )
        if result["success"] and result["rows"]:
            pairs = [f"{r['catalog']}.{r['schema']}" for r in result["rows"] if r.get("catalog")]
            _data_schemas_cache = ", ".join(sorted(set(pairs)))
            return _data_schemas_cache
    except Exception as e:
        logger.warning("Could not fetch data schemas: %s", e)
    _data_schemas_cache = f"{CATALOG}.{SCHEMA}"
    return _data_schemas_cache


# -------------------------------------------------------------------
# System prompts
# -------------------------------------------------------------------

def _blind_prompt() -> str:
    schemas = _get_data_schemas()
    kb_schema = f"{CATALOG}.{SCHEMA}"
    return f"""You are a SQL analyst for Databricks. You must write correct Spark SQL
to answer the user's question about their data.

DATA TABLES are in these catalogs/schemas: {schemas}

OFF-LIMITS: The schema {kb_schema} contains internal metadata infrastructure.
You MUST NOT query, join, or reference any tables in {kb_schema}.
Only query the user's actual data tables in the schemas listed above.

Tools available:
- list_tables: see what tables exist (call ONCE per schema)
- describe_table: column names, types, comments for a specific table
- sample_rows: see a few rows of actual data
- query_information_schema: detailed column metadata (use table_name param to scope)
- test_sql: validate SQL syntax before executing
- execute_query: run your final query and return results

CRITICAL WORKFLOW -- budget your tool calls carefully:
1. Call list_tables ONCE to discover tables.
2. Call describe_table on the 2-3 most relevant tables.
3. Write your SQL query.
4. Call test_sql to validate it.
5. Call execute_query to run it and return results.

You MUST call execute_query as your final step. Never stop at test_sql.
Do NOT call list_tables more than once -- the table list doesn't change.

When writing SQL, use fully qualified table names (catalog.schema.table).
{SAFETY_PROMPT_BLOCK}"""


def _enriched_prompt() -> str:
    schemas = _get_data_schemas()
    kb_schema = f"{CATALOG}.{SCHEMA}"
    return f"""You are a SQL analyst for Databricks with access to a rich semantic metadata layer.
You write high-quality Spark SQL by leveraging metadata BEFORE writing queries.

DATA TABLES are in these catalogs/schemas: {schemas}

== KNOWLEDGE BASE TABLES (in {kb_schema}) ==
You can query these directly with execute_query for broad or catalog-wide analysis:
- table_knowledge_base: table-level metadata (comment, domain, subdomain, has_pii, has_phi)
- column_knowledge_base: column-level metadata (comment, data_type, classification, classification_type, schema, catalog)
- column_profiling_stats: profiling results (distinct_count, null_rate, min/max, pattern_detected, sample_values)
- fk_predictions: predicted foreign keys (src_table, src_column, dst_table, dst_column, final_confidence)
- ontology_entities: entity types (entity_name, entity_type, confidence, source_tables)
- ontology_column_properties: column roles (property_role, owning_entity_type, linked_entity_type)
- metric_view_definitions: metric SQL definitions (metric_view_name, source_table, json_definition, status)
- metadata_documents: vector search documents (doc_id, doc_type, content, table_name)

== CHOOSING YOUR APPROACH ==
For TARGETED questions about specific tables (e.g. "average LOS by hospital"):
  1. find_relevant_tables -> get_column_context -> get_join_path -> write SQL

For BROAD/CATALOG-WIDE questions (e.g. "find all sensitive fields", "what document types exist"):
  Query the KB tables directly with execute_query. This is more powerful than calling
  get_column_context table-by-table. For example:
  - "Find undocumented sensitive columns" -> query column_knowledge_base directly
  - "What document types exist" -> query metadata_documents directly

== SEMANTIC TOOLS ==
- find_relevant_tables: hybrid (vector + keyword) search for tables matching a question.
  Accepts optional filters: domain="healthcare", schema_name="med_logistics_nba", include_columns=True.
  Best for targeted queries. For broad catalog-wide questions, query KB tables directly instead.
- get_column_context: rich column metadata for ONE table (comments, ontology roles, profiling, sensitivity)
- get_join_path: FK-predicted join paths between two tables
- get_metric_definitions: pre-defined metric view SQL definitions
- get_value_distribution: profiling stats for a specific column
- list_tables / describe_table / sample_rows: basic schema introspection
- test_sql / execute_query: validate and run SQL

You MUST call execute_query as your final step. Never stop at test_sql.

KEY ADVANTAGES of the semantic layer:
- Column comments tell you what columns actually mean (not just their names).
- FK predictions give you correct join conditions with confidence scores.
- Ontology column roles tell you which columns are identifiers, measures, or links.
- Profiling stats tell you valid filter values, null rates, and cardinality.
- Metric view definitions give you pre-validated aggregation logic.
- Sensitivity flags warn you about PII/PHI columns.

FLAG any PII/PHI columns in your response.
When writing SQL, use fully qualified table names (catalog.schema.table).
{SAFETY_PROMPT_BLOCK}"""


# -------------------------------------------------------------------
# Agent graph builder (delegates to common.build_react_graph)
# -------------------------------------------------------------------

def _build_analyst_graph(tools, system_prompt: str):
    return build_react_graph(tools, system_prompt)


# -------------------------------------------------------------------
# Single-mode execution
# -------------------------------------------------------------------

@trace(name="analyst_single", span_type="CHAIN")
def run_analyst_single(
    question: str,
    mode: str,
    history: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """Run the analyst agent in a single mode (blind or enriched)."""
    ensure_mlflow_context()
    tag_trace(agent="analyst", mode=mode)
    tools = BLIND_TOOLS if mode == "blind" else ENRICHED_TOOLS
    prompt = _blind_prompt() if mode == "blind" else _enriched_prompt()
    graph = _build_analyst_graph(tools, prompt)

    messages = history_to_messages(history, question)

    start = time.time()
    result = graph.invoke(
        {"messages": messages},
        config={"recursion_limit": GuardrailConfig.MAX_RECURSION_LIMIT},
    )
    elapsed_ms = int((time.time() - start) * 1000)

    final_msg = result["messages"][-1]
    tool_calls_used = extract_tool_calls(result["messages"])
    sql_generated = None
    for msg in result["messages"]:
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            for tc in msg.tool_calls:
                if tc["name"] in ("test_sql", "execute_query") and "sql" in tc.get("args", {}):
                    sql_generated = tc["args"]["sql"]

    result_columns, result_rows = None, None
    for msg in reversed(result["messages"]):
        if hasattr(msg, "name") and msg.name == "execute_query" and hasattr(msg, "content"):
            try:
                parsed = json.loads(msg.content) if isinstance(msg.content, str) else msg.content
                if isinstance(parsed, dict) and "columns" in parsed and "rows" in parsed:
                    result_columns = parsed["columns"]
                    result_rows = parsed["rows"][:50]
                    break
            except (json.JSONDecodeError, TypeError):
                pass

    token_usage = extract_token_usage(result["messages"])

    return {
        "answer": sanitize_output(final_msg.content),
        "sql": sql_generated,
        "result_columns": result_columns,
        "result_rows": result_rows,
        "tool_calls": tool_calls_used,
        "mode": mode,
        "elapsed_ms": elapsed_ms,
        "steps": len(result["messages"]),
        "token_usage": token_usage,
    }


# -------------------------------------------------------------------
# Compare mode (runs both in parallel)
# -------------------------------------------------------------------

@trace(name="analyst_compare", span_type="CHAIN")
def run_analyst_compare(
    question: str,
    progress_queue: queue.Queue,
) -> Dict[str, Any]:
    """Run both blind and enriched agents in parallel and return paired results."""
    ensure_mlflow_context()
    tag_trace(agent="analyst", mode="compare")
    results = {"blind": None, "enriched": None}
    errors = {"blind": None, "enriched": None}

    def _run_mode(mode):
        try:
            ensure_mlflow_context()
            progress_queue.put({"stage": f"{mode}_running"})
            results[mode] = run_analyst_single(question, mode)
            progress_queue.put({"stage": f"{mode}_done"})
        except Exception as e:
            logger.exception("Analyst %s mode failed", mode)
            errors[mode] = str(e)
            progress_queue.put({"stage": f"{mode}_error", "error": str(e)})

    t_blind = threading.Thread(target=_run_mode, args=("blind",), daemon=True)
    t_enriched = threading.Thread(target=_run_mode, args=("enriched",), daemon=True)

    progress_queue.put({"stage": "starting"})
    t_blind.start()
    t_enriched.start()
    t_blind.join(timeout=GuardrailConfig.AGENT_TIMEOUT_SECONDS + 30)
    t_enriched.join(timeout=GuardrailConfig.AGENT_TIMEOUT_SECONDS + 30)

    blind_res = results["blind"] or {"error": errors["blind"] or "Timeout"}
    enriched_res = results["enriched"] or {"error": errors["enriched"] or "Timeout"}

    comparison = None
    if not blind_res.get("error") and not enriched_res.get("error"):
        try:
            progress_queue.put({"stage": "comparing"})
            comparison = generate_comparison_analysis(question, blind_res, enriched_res)
        except Exception as e:
            logger.warning("Comparison analysis failed: %s", e)

    return {
        "blind": blind_res,
        "enriched": enriched_res,
        "comparison_analysis": comparison,
    }


@trace(name="analyst_comparison_llm", span_type="CHAIN")
def generate_comparison_analysis(
    question: str,
    blind_result: Dict[str, Any],
    enriched_result: Dict[str, Any],
) -> str:
    """LLM call comparing blind vs enriched results to articulate the semantic layer's value."""
    llm = get_llm()
    prompt = f"""You are an objective evaluator comparing two SQL analyst agents that answered the same question.
Agent A ("Blind") only had access to raw schema introspection (DESCRIBE TABLE, SHOW TABLES, INFORMATION_SCHEMA).
Agent B ("Enriched") had access to a semantic metadata layer (column descriptions, FK predictions, ontology roles, profiling stats, metric definitions, sensitivity flags).

QUESTION: {question}

BLIND AGENT:
- SQL: {blind_result.get('sql', 'None')}
- Answer: {(blind_result.get('answer', '') or '')[:500]}
- Tools: {blind_result.get('tool_calls', [])}
- Time: {blind_result.get('elapsed_ms', '?')}ms, Steps: {blind_result.get('steps', '?')}

ENRICHED AGENT:
- SQL: {enriched_result.get('sql', 'None')}
- Answer: {(enriched_result.get('answer', '') or '')[:500]}
- Tools: {enriched_result.get('tool_calls', [])}
- Time: {enriched_result.get('elapsed_ms', '?')}ms, Steps: {enriched_result.get('steps', '?')}

Write 2-4 sentences of objective analysis. Cover:
1. Which agent produced more accurate or complete SQL, and why?
2. How did each agent's discovery process differ (steps, tool usage, time)?
3. If the semantic layer helped, what specific assets mattered (FK predictions, column comments, metric definitions, sensitivity flags)?
4. If the blind agent performed comparably, note that honestly.

Be factual and balanced. Do NOT assume one agent is inherently better. Write flowing prose, no bullet points."""

    resp = llm.invoke([HumanMessage(content=prompt)])
    return sanitize_output(resp.content)


# -------------------------------------------------------------------
# Plot generation
# -------------------------------------------------------------------

PLOT_SYSTEM_PROMPT = """You are a data visualization agent. You will receive a SQL analyst's response
and optionally the SQL query that produced it. Your job is to determine if the data can be plotted,
query the database for precise chart data, and return a JSON chart specification.

Use the execute_query tool to run SQL for chart data, then return ONLY a JSON object (no markdown) in this format:

If data IS suitable for plotting:
{"type": "bar"|"line"|"pie"|"area", "title": "Chart Title", "data": [{"name": "Label", "value": 123}, ...], "xKey": "name", "yKeys": ["value"], "text": "Brief one-sentence caption"}

For multi-series data use multiple yKeys:
{"type": "bar", "title": "...", "data": [{"name": "X", "series1": 10, "series2": 20}, ...], "xKey": "name", "yKeys": ["series1", "series2"], "text": "..."}

If data is NOT suitable for plotting:
{"no_data": true, "reason": "Brief explanation why"}

Rules:
- Always query the database for precise numbers -- do not invent data.
- If the original SQL is provided, adapt it for chart-friendly output (aggregations, limited rows).
- Choose the chart type that best represents the data (bar for comparisons, line for time series, pie for proportions).
- Keep data arrays to 20 items or fewer. Aggregate if needed.
- Return ONLY the JSON object, nothing else."""


@trace(name="analyst_plot_spec", span_type="CHAIN")
def create_analyst_plot_spec(
    content: str,
    sql: Optional[str] = None,
    history: Optional[List[Dict]] = None,
) -> Dict:
    """Use an LLM agent to generate a chart spec from an analyst response."""
    try:
        llm = get_llm()
        agent = create_react_agent(llm, [execute_query])
        context = f"Agent response to visualize:\n\n{content}"
        if sql:
            context += f"\n\nOriginal SQL query:\n{sql}"
        if history:
            recent = history[-4:]
            context = "Recent conversation:\n" + "\n".join(
                f"{m['role']}: {m['content'][:300]}" for m in recent
            ) + f"\n\n{context}"

        result = agent.invoke({
            "messages": [SystemMessage(content=PLOT_SYSTEM_PROMPT), HumanMessage(content=context)]
        })

        for msg in reversed(result.get("messages", [])):
            if isinstance(msg, AIMessage) and msg.content:
                text = msg.content.strip()
                if text.startswith("```"):
                    text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                    text = text.rsplit("```", 1)[0].strip()
                return json.loads(text)
        return {"no_data": True, "reason": "Agent did not produce a chart specification."}
    except Exception as e:
        logger.error("Plot agent error: %s", e, exc_info=True)
        return {"no_data": True, "reason": f"Error generating chart: {e}"}
