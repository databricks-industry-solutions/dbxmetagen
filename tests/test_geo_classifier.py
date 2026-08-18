"""Tests for geo_classifier module -- keyword classification, config, Pydantic models."""

import pytest
from unittest.mock import MagicMock, patch
from dbxmetagen.geo_classifier import (
    GeoConfig,
    GeoClassifier,
    GeoColumnResult,
    BatchGeoClassificationResult,
)


# ── GeoConfig ─────────────────────────────────────────────────────────


class TestGeoConfig:

    def test_fq_results(self):
        c = GeoConfig(catalog_name="cat", schema_name="sch")
        assert c.fq_results == "cat.sch.geo_classifications"

    def test_fq_column_kb(self):
        c = GeoConfig(catalog_name="cat", schema_name="sch")
        assert c.fq_column_kb == "cat.sch.column_knowledge_base"

    def test_fq_column_kb_custom(self):
        c = GeoConfig(catalog_name="cat", schema_name="sch", column_kb_table="custom_kb")
        assert c.fq_column_kb == "cat.sch.custom_kb"

    def test_load_categories_from_file(self, tmp_path):
        cfg_file = tmp_path / "geo.yaml"
        cfg_file.write_text("categories:\n  geographic:\n    keywords: [city]\n  non_geographic:\n    keywords: [file_location]\ndefault_category: non_geographic\n")
        c = GeoConfig(catalog_name="cat", schema_name="sch", config_path=str(cfg_file))
        cats = c.load_categories()
        assert "categories" in cats
        assert "city" in cats["categories"]["geographic"]["keywords"]

    def test_load_categories_raises_on_missing(self):
        c = GeoConfig(catalog_name="cat", schema_name="sch", config_path="/nonexistent/geo.yaml")
        with pytest.raises(FileNotFoundError):
            c.load_categories()


# ── Pydantic Models ───────────────────────────────────────────────────


class TestPydanticModels:

    def test_geo_column_result_valid(self):
        r = GeoColumnResult(column_name="city", classification="geographic", confidence=0.95)
        assert r.column_name == "city"
        assert r.confidence == 0.95

    def test_geo_column_result_clamps_confidence(self):
        with pytest.raises(Exception):
            GeoColumnResult(column_name="x", classification="geo", confidence=1.5)

    def test_batch_result(self):
        items = [
            GeoColumnResult(column_name="city", classification="geographic", confidence=0.9),
            GeoColumnResult(column_name="order_id", classification="non_geographic", confidence=0.8),
        ]
        batch = BatchGeoClassificationResult(classifications=items)
        assert len(batch.classifications) == 2


# ── Keyword Classification ────────────────────────────────────────────


@pytest.fixture
def classifier(tmp_path):
    """GeoClassifier with a test YAML config."""
    cfg_file = tmp_path / "geo.yaml"
    cfg_file.write_text(
        "categories:\n"
        "  geographic:\n"
        "    keywords:\n"
        "      - address\n"
        "      - city\n"
        "      - state\n"
        "      - zipcode\n"
        "      - zip\n"
        "      - latitude\n"
        "      - longitude\n"
        "      - county\n"
        "      - region\n"
        "  non_geographic:\n"
        "    keywords:\n"
        "      - memory_location\n"
        "      - file_location\n"
        "      - storage_location\n"
        "default_category: non_geographic\n"
    )
    spark = MagicMock()
    config = GeoConfig(catalog_name="cat", schema_name="sch", config_path=str(cfg_file))
    return GeoClassifier(spark, config)


class TestKeywordClassify:

    def test_geographic_address(self, classifier):
        assert classifier._keyword_classify("billing_address") == "geographic"

    def test_geographic_city(self, classifier):
        assert classifier._keyword_classify("city") == "geographic"

    def test_geographic_zipcode(self, classifier):
        assert classifier._keyword_classify("zip_code") == "geographic"

    def test_non_geographic_memory_location(self, classifier):
        assert classifier._keyword_classify("memory_location") == "non_geographic"

    def test_non_geographic_file_location(self, classifier):
        assert classifier._keyword_classify("file_location") == "non_geographic"

    def test_ambiguous_returns_none(self, classifier):
        assert classifier._keyword_classify("order_id") is None

    def test_geographic_latitude(self, classifier):
        assert classifier._keyword_classify("latitude") == "geographic"

    def test_geographic_state_code(self, classifier):
        assert classifier._keyword_classify("state_code") == "geographic"

    def test_non_geographic_wins_when_both_match(self, classifier):
        # "storage_location" matches non_geographic keyword exactly
        assert classifier._keyword_classify("storage_location") == "non_geographic"


class TestGeoResilientChunking:
    """ON-19 (geo sibling): geo classification bisects a truncated batch instead
    of SILENTLY defaulting every column to non_geographic."""

    def test_truncation_bisects_not_silent_default(self, classifier):
        from dbxmetagen.chat_client import StructuredTruncationError
        cols = [(f"col_{i}", "string", "") for i in range(4)]
        calls = []

        def fake(table, chunk):
            calls.append(len(chunk))
            if len(chunk) > 2:
                raise StructuredTruncationError("truncated")
            return [(cn, "geographic", 0.8) for cn, _, _ in chunk]

        with patch.object(classifier, "_classify_geo_chunk", side_effect=fake):
            out = classifier._classify_geo_chunk_resilient("cat.sch.t", cols)
        # All 4 recovered as real classifications, not defaulted.
        assert len(out) == 4
        assert all(cls == "geographic" for _, cls, _ in out)
        assert calls[0] == 4 and 2 in calls[1:]

    def test_single_column_truncation_defaults_as_last_resort(self, classifier):
        from dbxmetagen.chat_client import StructuredTruncationError
        cols = [("col_0", "string", "")]
        with patch.object(classifier, "_classify_geo_chunk",
                          side_effect=StructuredTruncationError("t")):
            out = classifier._classify_geo_chunk_resilient("cat.sch.t", cols)
        assert len(out) == 1
        assert out[0][1] == classifier._default

    def test_wide_table_splits_by_budget(self, classifier):
        from dbxmetagen.geo_classifier import _GEO_COLS_PER_CHUNK
        cols = [(f"col_{i}", "string", "") for i in range(144)]
        seen = []
        with patch.object(classifier, "_classify_geo_chunk",
                          side_effect=lambda t, ch: seen.append(len(ch)) or
                          [(cn, "non_geographic", 0.7) for cn, _, _ in ch]):
            out = classifier._ai_classify_batch("cat.sch.t", cols)
        assert len(out) == 144
        assert max(seen) <= _GEO_COLS_PER_CHUNK
