"""Unit tests for genie_sql_puller: scope selection + curated-SQL extraction.

The Genie REST reads are mocked via a fake ws.api_client; no network. Spark is
only used for table/index writes (not exercised here)."""

import inspect
from unittest.mock import MagicMock

import pytest

from dbxmetagen.genie_sql_puller import (
    GenieSQLPuller,
    GenieSQLPullerConfig,
    _example_id,
    query_examples,
)


class _FakeApiClient:
    """Routes ws.api_client.do(GET, path) to canned JSON by path."""

    def __init__(self, responses: dict):
        self.responses = responses
        self.calls = []

    def do(self, method, path, **kw):
        self.calls.append((method, path))
        for key, val in self.responses.items():
            if key in path:
                return val
        return {}


def _ws(responses):
    ws = MagicMock()
    ws.api_client = _FakeApiClient(responses)
    return ws


def _cfg(**kw):
    base = dict(catalog_name="c", schema_name="s")
    base.update(kw)
    return GenieSQLPullerConfig(**base)


class TestScopeRequirement:
    def test_empty_scope_raises(self):
        puller = GenieSQLPuller(MagicMock(), _cfg(), ws=_ws({}))
        with pytest.raises(ValueError, match="explicit scope"):
            puller.extract_examples()

    def test_selects_by_space_id(self):
        ws = _ws({
            "/genie/spaces": {"spaces": [
                {"space_id": "keep", "title": "Trusted"},
                {"space_id": "drop", "title": "Junk"},
            ]},
            "/data-rooms/keep/curated-questions": {"curated_questions": []},
            "/data-rooms/keep": {"table_identifiers": []},
        })
        puller = GenieSQLPuller(MagicMock(), _cfg(space_ids=["keep"]), ws=ws)
        selected = puller._select_spaces()
        assert [s["space_id"] for s in selected] == ["keep"]

    def test_selects_by_title_substring(self):
        ws = _ws({
            "/genie/spaces": {"spaces": [
                {"space_id": "a", "title": "Commercial Analytics"},
                {"space_id": "b", "title": "Random Test"},
            ]},
        })
        puller = GenieSQLPuller(MagicMock(), _cfg(title_contains=["commercial"]), ws=ws)
        selected = puller._select_spaces()
        assert [s["space_id"] for s in selected] == ["a"]


class TestExtraction:
    def _puller(self, curated):
        ws = _ws({
            "/genie/spaces": {"spaces": [{"space_id": "sp1", "title": "Sales"}]},
            "/data-rooms/sp1/curated-questions": {"curated_questions": curated},
            "/data-rooms/sp1": {"table_identifiers": ["c.s.orders", "c.s.returns"]},
        })
        return GenieSQLPuller(MagicMock(), _cfg(space_ids=["sp1"]), ws=ws)

    def test_keeps_benchmark_sql_only(self):
        curated = [
            {"question_type": "BENCHMARK", "question_text": "Q1", "answer_text": "SELECT 1"},
            {"question_type": "BENCHMARK_SUGGESTION", "question_text": "Q2", "answer_text": "SELECT 2"},
            {"question_type": "SAMPLE_QUESTION", "question_text": "Q3", "answer_text": ""},
        ]
        rows = self._puller(curated).extract_examples()
        assert len(rows) == 2
        assert {r["sql"] for r in rows} == {"SELECT 1", "SELECT 2"}

    def test_skips_benchmark_with_empty_sql(self):
        curated = [{"question_type": "BENCHMARK", "question_text": "Q", "answer_text": "  "}]
        assert self._puller(curated).extract_examples() == []

    def test_content_includes_question_tables_and_sql(self):
        curated = [{"question_type": "BENCHMARK", "question_text": "Show revenue", "answer_text": "SELECT x"}]
        rows = self._puller(curated).extract_examples()
        content = rows[0]["content"]
        assert "Show revenue" in content
        assert "c.s.orders" in content
        assert "SELECT x" in content
        assert rows[0]["table_identifiers"] == "c.s.orders, c.s.returns"

    def test_sample_questions_included_when_flagged(self):
        curated = [{"question_type": "SAMPLE_QUESTION", "question_text": "Q", "answer_text": ""}]
        ws = _ws({
            "/genie/spaces": {"spaces": [{"space_id": "sp1", "title": "Sales"}]},
            "/data-rooms/sp1/curated-questions": {"curated_questions": curated},
            "/data-rooms/sp1": {"table_identifiers": []},
        })
        puller = GenieSQLPuller(MagicMock(), _cfg(space_ids=["sp1"], include_sample_questions=True), ws=ws)
        assert len(puller.extract_examples()) == 1


class TestExampleId:
    def test_deterministic_and_space_prefixed(self):
        a = _example_id("sp1", "q", "SELECT 1")
        b = _example_id("sp1", "q", "SELECT 1")
        assert a == b and a.startswith("sp1::")

    def test_differs_on_sql_change(self):
        assert _example_id("sp1", "q", "SELECT 1") != _example_id("sp1", "q", "SELECT 2")


class TestQueryExamples:
    def test_uses_hybrid_and_expected_columns(self):
        src = inspect.getsource(query_examples)
        assert 'query_type="HYBRID"' in src
        for col in ("question_text", "sql", "space_title", "table_identifiers"):
            assert col in src


class TestTableAndIndexContract:
    def test_table_has_cdf_enabled(self):
        src = inspect.getsource(GenieSQLPuller.ensure_table)
        assert "delta.enableChangeDataFeed" in src and "'true'" in src
        assert "deletedFileRetentionDuration" in src

    def test_merge_is_keyed_on_example_id(self):
        src = inspect.getsource(GenieSQLPuller.write_examples)
        assert "t.example_id = s.example_id" in src
        assert "WHEN NOT MATCHED THEN INSERT" in src

    def test_index_primary_key_and_shared_endpoint(self):
        from dbxmetagen.genie_sql_puller import build_genie_examples_index
        src = inspect.getsource(build_genie_examples_index)
        assert 'primary_key="example_id"' in src
        assert "DELTA_SYNC" in src
