"""Unified intent classification, context summarization, and clarity assessment.

Single LLM call (fast model) that replaces the keyword-based classify_intent()
and the naive _build_query() history concatenation. Inspired by
dbx-unifiedchat's unified_intent_context_clarification_node.
"""

import json
import logging
import os
import threading
from dataclasses import dataclass
from typing import Dict, List, Literal, Optional

from databricks_langchain import ChatDatabricks

logger = logging.getLogger(__name__)

INTENT_MODEL = os.environ.get("INTENT_MODEL", "databricks-claude-haiku-4-5")
CATALOG = os.environ.get("CATALOG_NAME", "")
SCHEMA = os.environ.get("SCHEMA_NAME", "")

_cached_intent_llm = None
_intent_llm_lock = threading.Lock()


def _get_intent_llm() -> ChatDatabricks:
    global _cached_intent_llm
    if _cached_intent_llm is None:
        with _intent_llm_lock:
            if _cached_intent_llm is None:
                _cached_intent_llm = ChatDatabricks(
                    endpoint=INTENT_MODEL, temperature=0, max_retries=2, request_timeout=15,
                )
    return _cached_intent_llm


@dataclass
class IntentResult:
    intent_type: Literal["new_question", "refinement", "continuation", "meta", "irrelevant"]
    context_summary: str
    question_clear: bool
    domain: Optional[str] = None
    complexity: Literal["simple", "moderate", "complex"] = "moderate"
    clarification_reason: Optional[str] = None
    clarification_options: Optional[List[str]] = None
    meta_answer: Optional[str] = None


KB_TABLE_CONTEXT = f"""Available knowledge base tables in {CATALOG}.{SCHEMA}:
- table_knowledge_base: table_name, comment, domain, subdomain, has_pii, has_phi, row_count
- column_knowledge_base: table_name, column_name, comment, data_type, classification
- ontology_entities: entity_id, entity_name, entity_type, description, source_tables, confidence, entity_uri, source_ontology
- fk_predictions: src_table, src_column, dst_table, dst_column, final_confidence, cardinality
- metric_view_definitions: metric_view_name, source_table, json_definition, status
- profiling_results: table_name, column_name, distinct_count, null_count"""


def _build_intent_prompt(question: str, history: Optional[List[Dict[str, str]]] = None) -> str:
    conv_ctx = "No previous conversation (first query)."
    if history:
        turns = history[-5:]
        conv_ctx = "Previous conversation:\n" + "\n".join(
            f"  {i}. [{m.get('role','user')}] {m.get('content','')[:300]}"
            for i, m in enumerate(turns, 1)
        )

    return f"""Analyze the user's query in conversation context. Respond with ONLY a JSON object.

Current Query: {question}

Conversation History:
{conv_ctx}

{KB_TABLE_CONTEXT}

## Tasks

1. **Irrelevant detection**: Is this completely unrelated to the data in the catalog?
   Examples of irrelevant: greetings, weather, jokes, general coding help.
   **Data questions about tables, metrics, or business data ARE NOT irrelevant** -- classify
   those as domain "query" even if they reference external systems (the user may have that data
   in their catalog). When in doubt, treat it as a new_question with domain "query".

2. **Meta-question detection**: Is this about the system itself?
   Examples: "what tables do you have?", "what can I ask?", "show me example questions".
   If meta, provide a direct markdown answer using the KB table context above.

3. **Intent classification**: One of:
   - new_question: Different topic from prior conversation
   - refinement: Narrowing/filtering the previous query on same topic
   - continuation: Follow-up exploring same topic from a different angle
   (Only use refinement/continuation when history is present and relevant.)

4. **Context summary**: Write a 1-2 sentence self-contained restatement of what the user wants,
   incorporating relevant history context. This will be used as the query for downstream tools.
   Must be specific and actionable (good for SQL/vector search).

5. **Domain classification**: One of: governance, relationship, query, discovery, general.
   - governance: PII, PHI, compliance, security, sensitive data
   - relationship: foreign keys, joins, how tables connect
   - query: counts, aggregations, listings, data questions
   - discovery: finding tables, understanding schemas, exploring metadata
   - general: anything else

6. **Clarity check**: Is the query clear enough to proceed? BE LENIENT -- default to true.
   Only mark unclear if critical information is truly missing.

7. **Complexity**: simple, moderate, or complex.

## Output (JSON only, no markdown fencing)
{{
  "intent_type": "new_question" | "refinement" | "continuation" | "meta" | "irrelevant",
  "context_summary": "Self-contained restatement of the question",
  "question_clear": true,
  "domain": "governance" | "relationship" | "query" | "discovery" | "general",
  "complexity": "simple" | "moderate" | "complex",
  "clarification_reason": null,
  "clarification_options": null,
  "meta_answer": null
}}

If intent_type is "meta", set meta_answer to a helpful markdown response.
If intent_type is "irrelevant", set meta_answer to a polite redirect.
If question_clear is false, set clarification_reason and clarification_options (2-3 options).
"""


def classify_and_contextualize(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
) -> IntentResult:
    """Single LLM call for intent + context summary + meta detection + clarity."""
    prompt = _build_intent_prompt(question, history)
    try:
        resp = _get_intent_llm().invoke(prompt)
        raw = (resp.content or "").strip()
        raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        data = json.loads(raw)

        intent_type = data.get("intent_type", "new_question")
        if intent_type not in ("new_question", "refinement", "continuation", "meta", "irrelevant"):
            intent_type = "new_question"

        question_clear = data.get("question_clear", True)

        complexity = data.get("complexity", "moderate")
        if complexity not in ("simple", "moderate", "complex"):
            complexity = "moderate"

        return IntentResult(
            intent_type=intent_type,
            context_summary=data.get("context_summary") or question,
            question_clear=question_clear,
            domain=data.get("domain", "general"),
            complexity=complexity,
            clarification_reason=data.get("clarification_reason"),
            clarification_options=data.get("clarification_options"),
            meta_answer=data.get("meta_answer"),
        )
    except Exception as e:
        logger.warning("[intent] LLM intent classification failed, using defaults: %s", e)
        return IntentResult(
            intent_type="new_question",
            context_summary=_fallback_context_summary(question, history),
            question_clear=True,
            domain=keyword_domain(question),
            complexity="moderate",
        )


def format_clarification(result: IntentResult) -> str:
    """Format a clarification request as user-facing markdown."""
    md = f"### Clarification Needed\n\n{result.clarification_reason or 'Could you be more specific?'}\n\n"
    if result.clarification_options:
        md += "**Please choose or provide your own clarification:**\n\n"
        for i, opt in enumerate(result.clarification_options, 1):
            md += f"{i}. {opt}\n"
    return md.strip()


def _fallback_context_summary(question: str, history: Optional[List[Dict[str, str]]] = None) -> str:
    """Non-LLM fallback: replaces the old _build_query concatenation."""
    if not history:
        return question
    lines = [f"{m.get('role', 'user')}: {m['content']}" for m in history[-6:]]
    lines.append(f"user: {question}")
    return "Conversation so far:\n" + "\n".join(lines) + "\n\nRespond to the latest user message."


def keyword_domain(message: str) -> str:
    """Fast keyword fallback for domain classification."""
    m = message.lower()
    if any(kw in m for kw in ["pii", "phi", "sensitive", "governance", "security", "compliance"]):
        return "governance"
    if any(kw in m for kw in ["foreign key", "fk", "join", "relationship", "connected", "related to"]):
        return "relationship"
    if any(kw in m for kw in ["how many", "count", "list", "show me", "select", "average", "total", "sum"]):
        return "query"
    if any(kw in m for kw in ["find", "search", "similar", "about", "describe", "what is", "what are"]):
        return "discovery"
    return "general"
