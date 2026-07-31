"""Unit tests for genie_sql_puller: scope selection + curated-SQL extraction.

The Genie REST reads are mocked via a fake ws.api_client; no network. The puller
is Spark-free (runs in the app); table writes + index build are the app's job."""

import inspect
from unittest.mock import MagicMock

import pytest

from dbxmetagen.genie_sql_puller import (
    GenieSQLPuller,
    GenieSQLPullerConfig,
    _coerce_text,
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
        # Longest matching key wins so a specific path (…/spaces/sp1?serialized)
        # isn't shadowed by a prefix key (…/spaces).
        best = None
        for key, val in self.responses.items():
            if key in path and (best is None or len(key) > len(best[0])):
                best = (key, val)
        return best[1] if best else {}


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
        puller = GenieSQLPuller(_cfg(), ws=_ws({}))
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
        puller = GenieSQLPuller(_cfg(space_ids=["keep"]), ws=ws)
        selected = puller._select_spaces()
        assert [s["space_id"] for s in selected] == ["keep"]

    def test_selects_by_title_substring(self):
        ws = _ws({
            "/genie/spaces": {"spaces": [
                {"space_id": "a", "title": "Commercial Analytics"},
                {"space_id": "b", "title": "Random Test"},
            ]},
        })
        puller = GenieSQLPuller(_cfg(title_contains=["commercial"]), ws=ws)
        selected = puller._select_spaces()
        assert [s["space_id"] for s in selected] == ["a"]


class TestExtraction:
    def _puller(self, curated):
        ws = _ws({
            "/genie/spaces": {"spaces": [{"space_id": "sp1", "title": "Sales"}]},
            "/data-rooms/sp1/curated-questions": {"curated_questions": curated},
            "/data-rooms/sp1": {"table_identifiers": ["c.s.orders", "c.s.returns"]},
        })
        return GenieSQLPuller(_cfg(space_ids=["sp1"]), ws=ws)

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
        puller = GenieSQLPuller(_cfg(space_ids=["sp1"], include_sample_questions=True), ws=ws)
        assert len(puller.extract_examples()) == 1


class TestSerializedSpaceExtraction:
    """Serialized space (?include_serialized_space=true) carries the richest set:
    instructions.example_question_sqls (question->SQL pairs), often repr'd lists."""

    def _puller(self, serialized, curated=None):
        import json as _json
        ws = _ws({
            "/genie/spaces/sp1?include_serialized_space=true": {
                "space_id": "sp1", "serialized_space": _json.dumps(serialized)},
            "/genie/spaces": {"spaces": [{"space_id": "sp1", "title": "Epic"}]},
            "/data-rooms/sp1/curated-questions": {"curated_questions": curated or []},
            "/data-rooms/sp1": {"table_identifiers": ["c.s.fact"]},
        })
        return GenieSQLPuller(_cfg(space_ids=["sp1"]), ws=ws)

    def test_pulls_example_question_sqls(self):
        serialized = {"instructions": {"example_question_sqls": [
            {"question": "Total by type", "sql": "SELECT type, COUNT(*) FROM t GROUP BY type"},
            {"question": "Avg amount", "sql": "SELECT AVG(amt) FROM t"},
        ]}}
        rows = self._puller(serialized).extract_examples()
        assert len(rows) == 2
        assert all(r["question_type"] == "EXAMPLE_SQL" for r in rows)
        assert {r["question_text"] for r in rows} == {"Total by type", "Avg amount"}

    def test_coerces_repr_list_fragments(self):
        # Real API returns question/sql as repr'd lists of string fragments.
        serialized = {"instructions": {"example_question_sqls": [
            {"question": "['Total encounters']",
             "sql": "['SELECT encounter_type,\\n', '       COUNT(*)\\n', 'FROM t']"},
        ]}}
        rows = self._puller(serialized).extract_examples()
        assert len(rows) == 1
        assert rows[0]["question_text"] == "Total encounters"
        assert rows[0]["sql"].startswith("SELECT encounter_type,")
        assert "['" not in rows[0]["sql"]

    def test_skips_example_with_empty_sql(self):
        serialized = {"instructions": {"example_question_sqls": [
            {"question": "Q", "sql": ""}]}}
        assert self._puller(serialized).extract_examples() == []

    def test_merges_serialized_and_curated_dedup(self):
        # Same question+SQL from both sources -> one row (deduped by example_id).
        serialized = {"instructions": {"example_question_sqls": [
            {"question": "Rev", "sql": "SELECT 1"}]}}
        curated = [{"question_type": "BENCHMARK", "question_text": "Other", "answer_text": "SELECT 2"}]
        rows = self._puller(serialized, curated).extract_examples()
        assert len(rows) == 2  # distinct
        # identical pair collapses:
        curated_dup = [{"question_type": "BENCHMARK", "question_text": "Rev", "answer_text": "SELECT 1"}]
        rows2 = self._puller(serialized, curated_dup).extract_examples()
        assert len(rows2) == 1

    def test_no_serialized_space_falls_back_to_curated(self):
        # serialized_space absent -> {} -> only curated path contributes.
        ws = _ws({
            "/genie/spaces": {"spaces": [{"space_id": "sp1", "title": "X"}]},
            "/data-rooms/sp1/curated-questions": {"curated_questions": [
                {"question_type": "BENCHMARK", "question_text": "Q", "answer_text": "SELECT 9"}]},
            "/data-rooms/sp1": {"table_identifiers": []},
        })
        rows = GenieSQLPuller(_cfg(space_ids=["sp1"]), ws=ws).extract_examples()
        assert len(rows) == 1 and rows[0]["sql"] == "SELECT 9"


class TestCoerceText:
    def test_repr_list(self):
        assert _coerce_text("['a\\n', 'b']") == "a\nb"

    def test_plain_string(self):
        assert _coerce_text("SELECT 1") == "SELECT 1"

    def test_actual_list(self):
        assert _coerce_text(["x", "y"]) == "xy"

    def test_none(self):
        assert _coerce_text(None) == ""

    def test_non_list_bracket_string_left_alone(self):
        # a malformed bracket string that isn't a valid list literal stays as-is
        assert _coerce_text("[not a list") == "[not a list"


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
    def test_create_table_sql_has_cdf(self):
        from dbxmetagen.genie_sql_puller import create_table_sql
        ddl = create_table_sql("c.s.genie_sql_examples")
        assert "CREATE TABLE IF NOT EXISTS c.s.genie_sql_examples" in ddl
        assert "delta.enableChangeDataFeed" in ddl and "'true'" in ddl
        assert "deletedFileRetentionDuration" in ddl
        assert "example_id STRING NOT NULL" in ddl

    def test_puller_is_spark_free(self):
        # __init__ must not require a spark arg (runs inside the app).
        params = list(inspect.signature(GenieSQLPuller.__init__).parameters)
        assert params == ["self", "config", "ws"]

    def test_index_primary_key_and_shared_endpoint(self):
        from dbxmetagen.genie_sql_puller import build_genie_examples_index
        src = inspect.getsource(build_genie_examples_index)
        assert 'primary_key="example_id"' in src
        assert "DELTA_SYNC" in src
