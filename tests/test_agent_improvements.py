"""Unit tests for agent improvement modules: intent, structured errors, VS cache, timing."""

import json
import sys
import os
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

# Stub heavy deps before importing agent modules
for _mod in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "databricks_langchain", "langgraph", "langgraph.graph", "langgraph.graph.message",
    "langgraph.prebuilt", "langchain_core", "langchain_core.messages",
    "langchain_core.tools",
]:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

# Ensure app dir is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "apps", "dbxmetagen-app", "app"))

# Stub agent sub-dependencies that import Databricks tooling
for _mod in ["agent.metadata_tools", "agent.graph_skill"]:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

# agent.tracing needs a passthrough @trace decorator, not a MagicMock wrapper
_tracing_stub = MagicMock()
_tracing_stub.trace = lambda **kw: (lambda fn: fn)  # passthrough decorator
_tracing_stub.ensure_mlflow_context = lambda: None
_tracing_stub.get_mlflow = lambda: None
sys.modules["agent.tracing"] = _tracing_stub


# ---------------------------------------------------------------------------
# Test IntentResult parsing
# ---------------------------------------------------------------------------

class TestIntentClassifier:
    """Tests for agent.intent.classify_and_contextualize."""

    def _mock_llm_response(self, content: str):
        resp = MagicMock()
        resp.content = content
        return resp

    def test_parse_new_question(self):
        from agent.intent import classify_and_contextualize
        payload = json.dumps({
            "intent_type": "new_question",
            "context_summary": "Find all PII tables in the catalog",
            "question_clear": True,
            "domain": "governance",
            "complexity": "moderate",
            "clarification_reason": None,
            "clarification_options": None,
            "meta_answer": None,
        })
        with patch("agent.intent._get_intent_llm") as mock_llm:
            mock_llm.return_value.invoke.return_value = self._mock_llm_response(payload)
            result = classify_and_contextualize("Find PII tables")

        assert result.intent_type == "new_question"
        assert result.domain == "governance"
        assert result.question_clear is True
        assert result.context_summary == "Find all PII tables in the catalog"

    def test_parse_meta_question(self):
        from agent.intent import classify_and_contextualize
        payload = json.dumps({
            "intent_type": "meta",
            "context_summary": "User is asking what tables are available",
            "question_clear": True,
            "domain": "general",
            "complexity": "simple",
            "meta_answer": "I have access to table_knowledge_base, column_knowledge_base, and more.",
        })
        with patch("agent.intent._get_intent_llm") as mock_llm:
            mock_llm.return_value.invoke.return_value = self._mock_llm_response(payload)
            result = classify_and_contextualize("What tables do you have?")

        assert result.intent_type == "meta"
        assert result.meta_answer is not None

    def test_parse_irrelevant(self):
        from agent.intent import classify_and_contextualize
        payload = json.dumps({
            "intent_type": "irrelevant",
            "context_summary": "User asked about the weather",
            "question_clear": True,
            "domain": "general",
            "complexity": "simple",
            "meta_answer": "I'm a metadata assistant. Please ask about your data catalog.",
        })
        with patch("agent.intent._get_intent_llm") as mock_llm:
            mock_llm.return_value.invoke.return_value = self._mock_llm_response(payload)
            result = classify_and_contextualize("What's the weather?")

        assert result.intent_type == "irrelevant"

    def test_fallback_on_llm_error(self):
        from agent.intent import classify_and_contextualize
        with patch("agent.intent._get_intent_llm") as mock_llm:
            mock_llm.return_value.invoke.side_effect = RuntimeError("LLM down")
            result = classify_and_contextualize("Find PII tables")

        assert result.intent_type == "new_question"
        assert result.question_clear is True
        assert result.context_summary == "Find PII tables"

    def test_invalid_intent_type_defaults(self):
        from agent.intent import classify_and_contextualize
        payload = json.dumps({
            "intent_type": "banana",
            "context_summary": "Something",
            "question_clear": True,
            "domain": "general",
            "complexity": "moderate",
        })
        with patch("agent.intent._get_intent_llm") as mock_llm:
            mock_llm.return_value.invoke.return_value = self._mock_llm_response(payload)
            result = classify_and_contextualize("Test")

        assert result.intent_type == "new_question"

    def test_refinement_with_history(self):
        from agent.intent import classify_and_contextualize
        payload = json.dumps({
            "intent_type": "refinement",
            "context_summary": "Show only PII tables in the claims domain, refining earlier PII table search",
            "question_clear": True,
            "domain": "governance",
            "complexity": "moderate",
        })
        history = [
            {"role": "user", "content": "Find PII tables"},
            {"role": "assistant", "content": "Found 15 PII tables across domains..."},
        ]
        with patch("agent.intent._get_intent_llm") as mock_llm:
            mock_llm.return_value.invoke.return_value = self._mock_llm_response(payload)
            result = classify_and_contextualize("Only in claims domain", history)

        assert result.intent_type == "refinement"
        assert "claims" in result.context_summary.lower()

    def test_keyword_domain(self):
        from agent.intent import keyword_domain
        assert keyword_domain("find PII tables") == "governance"
        assert keyword_domain("foreign key joins") == "relationship"
        assert keyword_domain("how many tables") == "query"
        assert keyword_domain("find similar tables") == "discovery"
        assert keyword_domain("hello world") == "general"


# ---------------------------------------------------------------------------
# Test ToolResult and VS cache
# ---------------------------------------------------------------------------

class TestToolResultAndCache:
    """Tests for ToolResult, VS cache, and measure_phase in common.py."""

    def test_tool_result_success(self):
        from agent.common import ToolResult
        tr = ToolResult(success=True, data="some data", label="test_tool")
        assert tr.success
        assert tr.data == "some data"
        assert tr.error_type is None

    def test_tool_result_failure(self):
        from agent.common import ToolResult
        tr = ToolResult(success=False, data=None, error_type="timeout",
                        error_hint="Timed out after 30s", label="slow_tool")
        assert not tr.success
        assert tr.error_type == "timeout"

    def test_vs_cache_put_get_refinement(self):
        """Cache returns data only for refinement/continuation intents."""
        from agent.common import vs_cache_put, vs_cache_get, _vs_cache
        _vs_cache.clear()
        vs_cache_put("session1", "test query", '{"matches": []}')
        assert vs_cache_get("session1", intent_type="refinement") == '{"matches": []}'
        assert vs_cache_get("session1", intent_type="continuation") == '{"matches": []}'

    def test_vs_cache_skips_new_question(self):
        """Cache returns None for new_question intent even if entry exists."""
        from agent.common import vs_cache_put, vs_cache_get, _vs_cache
        _vs_cache.clear()
        vs_cache_put("session2", "test query", '{"matches": []}')
        assert vs_cache_get("session2", intent_type="new_question") is None
        assert vs_cache_get("session2") is None  # default = new_question

    def test_vs_cache_miss(self):
        from agent.common import vs_cache_get, _vs_cache
        _vs_cache.clear()
        result = vs_cache_get("nonexistent", intent_type="refinement")
        assert result is None

    def test_vs_cache_ttl_expiry(self):
        from agent.common import vs_cache_put, vs_cache_get, _vs_cache, VS_CACHE_TTL
        _vs_cache.clear()
        vs_cache_put("session_old", "old query", "old data")
        _vs_cache["session_old"]["timestamp"] = time.time() - VS_CACHE_TTL.total_seconds() - 1
        result = vs_cache_get("session_old", intent_type="refinement")
        assert result is None

    def test_vs_cache_lru_eviction(self):
        """Cache evicts oldest entries when exceeding max size."""
        from agent.common import vs_cache_put, _vs_cache, VS_CACHE_MAX_ENTRIES
        _vs_cache.clear()
        for i in range(VS_CACHE_MAX_ENTRIES + 50):
            vs_cache_put(f"s{i}", f"q{i}", f"r{i}")
        assert len(_vs_cache) <= VS_CACHE_MAX_ENTRIES

    def test_measure_phase(self):
        from agent.common import measure_phase
        metrics = {}
        with measure_phase("test_phase", metrics):
            time.sleep(0.05)
        assert "test_phase" in metrics
        assert metrics["test_phase"] >= 0.04

    def test_conversation_turn(self):
        from agent.common import ConversationTurn
        turn = ConversationTurn(
            turn_id="t1", query="Find PII tables",
            intent_type="new_question", domain="governance",
        )
        assert turn.turn_id == "t1"
        assert turn.intent_type == "new_question"


# ---------------------------------------------------------------------------
# Test EvidenceBudget
# ---------------------------------------------------------------------------

class TestEvidenceBudget:
    def test_budget_values(self):
        from agent.guardrails import EvidenceBudget
        assert EvidenceBudget.VS_RESULTS > 0
        assert EvidenceBudget.TOTAL >= (
            EvidenceBudget.VS_RESULTS + EvidenceBudget.GRAPH_EXPANSION +
            EvidenceBudget.GRAPH_TRAVERSAL + EvidenceBudget.FK_PREDICTIONS +
            EvidenceBudget.STRUCTURED_RETRIEVAL
        )


# ---------------------------------------------------------------------------
# Test format_clarification
# ---------------------------------------------------------------------------

class TestFormatClarification:
    def test_format_with_options(self):
        from agent.intent import IntentResult, format_clarification
        result = IntentResult(
            intent_type="new_question",
            context_summary="",
            question_clear=False,
            clarification_reason="Which domain are you interested in?",
            clarification_options=["Healthcare", "Finance", "Retail"],
        )
        md = format_clarification(result)
        assert "Clarification Needed" in md
        assert "Healthcare" in md
        assert "Finance" in md

    def test_format_without_options(self):
        from agent.intent import IntentResult, format_clarification
        result = IntentResult(
            intent_type="new_question",
            context_summary="",
            question_clear=False,
            clarification_reason="Could you specify the table name?",
        )
        md = format_clarification(result)
        assert "table name" in md


# ---------------------------------------------------------------------------
# Integration: intent short-circuits in deep_analysis
# ---------------------------------------------------------------------------

class TestDeepAnalysisIntentShortCircuit:
    """Verify that irrelevant/meta intents skip the gathering pipeline."""

    def _mock_intent(self, intent_type, meta_answer=None):
        from agent.intent import IntentResult
        return IntentResult(
            intent_type=intent_type,
            context_summary="test",
            question_clear=True,
            domain="general",
            meta_answer=meta_answer,
        )

    def _fresh_run_deep_analysis(self):
        """Force-reimport deep_analysis to pick up fresh stubs."""
        sys.modules.pop("agent.deep_analysis", None)
        from agent.deep_analysis import run_deep_analysis
        return run_deep_analysis

    def test_irrelevant_skips_pipeline(self):
        run_deep_analysis = self._fresh_run_deep_analysis()
        with patch("agent.deep_analysis.classify_and_contextualize") as mock_cls, \
             patch("agent.deep_analysis._run_pipeline") as mock_pipe:
            mock_cls.return_value = self._mock_intent("irrelevant", "Not a data question.")
            result = run_deep_analysis("What's the weather?")

        assert result["intent"] == "irrelevant"
        assert result["answer"] == "Not a data question."
        mock_pipe.assert_not_called()

    def test_meta_skips_pipeline(self):
        run_deep_analysis = self._fresh_run_deep_analysis()
        with patch("agent.deep_analysis.classify_and_contextualize") as mock_cls, \
             patch("agent.deep_analysis._run_pipeline") as mock_pipe:
            mock_cls.return_value = self._mock_intent("meta", "I can help with metadata.")
            result = run_deep_analysis("What can you do?")

        assert result["intent"] == "meta"
        mock_pipe.assert_not_called()

    def test_new_question_calls_pipeline(self):
        run_deep_analysis = self._fresh_run_deep_analysis()
        with patch("agent.deep_analysis.classify_and_contextualize") as mock_cls, \
             patch("agent.deep_analysis._run_pipeline") as mock_pipe:
            mock_cls.return_value = self._mock_intent("new_question")
            mock_pipe.return_value = {"answer": "Analysis done.", "tool_calls": [], "mode": "graphrag"}
            result = run_deep_analysis("Find PII tables")

        mock_pipe.assert_called_once()
        assert result["answer"] == "Analysis done."


# ---------------------------------------------------------------------------
# Integration: SQL retry path in structured retrieval
# ---------------------------------------------------------------------------

class TestStructuredRetrievalSQLRetry:
    """Verify that _structured_retrieval retries on first SQL failure."""

    def test_sql_retry_on_first_failure(self):
        from agent.deep_analysis import _structured_retrieval, ToolResult
        cancel = threading.Event()

        call_count = {"fetch_schemas": 0, "sql_writer": 0, "execute": 0}

        with patch("agent.deep_analysis._fetch_table_schemas") as mock_schemas, \
             patch("agent.deep_analysis._sql_writer") as mock_writer, \
             patch("agent.deep_analysis._safe_tool_call") as mock_tool:

            mock_schemas.return_value = "### my_table\n  id INT\n  name STRING"

            mock_writer.side_effect = [
                "SELECT * FROM bad_table",  # first attempt
                "SELECT id, name FROM my_table LIMIT 10",  # retry
            ]

            def tool_side_effect(tool_fn, args, timeout, label, step, total):
                if "retry" in label:
                    return ToolResult(success=True, data='{"rows": [{"id": 1}]}', label=label)
                return ToolResult(success=False, data="SQL error: table not found",
                                  error_type="sql_error", error_hint="table not found", label=label)

            mock_tool.side_effect = tool_side_effect

            result = _structured_retrieval("Show me data", ["my_table"], cancel, 7, 7)

        assert result.success
        assert mock_writer.call_count == 2
