"""Unit tests for ontology_chunker module."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import yaml

from dbxmetagen.ontology_chunker import (
    build_ontology_chunks_table,
    chunks_from_bundle,
    _entity_content,
    _edge_content,
)


def _write_bundle(tmpdir, bundle_data):
    p = Path(tmpdir) / "test_bundle.yaml"
    p.write_text(yaml.dump(bundle_data), encoding="utf-8")
    return str(p)


MINIMAL_BUNDLE = {
    "metadata": {"name": "test"},
    "ontology": {
        "entities": {
            "definitions": {
                "Patient": {
                    "description": "A healthcare patient",
                    "keywords": ["patient", "person"],
                    "typical_attributes": ["patient_id", "name"],
                },
            },
        },
        "edge_catalog": {
            "has_diagnosis": {
                "domain": "Patient",
                "range": "Diagnosis",
                "description": "Links patient to diagnosis",
            },
        },
    },
}


class TestEntityContent:
    def test_includes_name_and_description(self):
        content = _entity_content("Patient", {"description": "A person receiving care"})
        assert "Patient" in content
        assert "A person receiving care" in content

    def test_includes_keywords(self):
        content = _entity_content("Patient", {"keywords": ["mrn", "patient_id"]})
        assert "mrn" in content

    def test_includes_column_patterns(self):
        content = _entity_content("Patient", {"typical_attributes": ["patient_id", "mrn"]})
        assert "patient_id" in content


class TestEdgeContent:
    def test_includes_relationship(self):
        content = _edge_content("has_diagnosis", {"domain": "Patient", "range": "Diagnosis"})
        assert "Patient" in content
        assert "Diagnosis" in content
        assert "has_diagnosis" in content


class TestChunksFromBundle:
    def test_produces_entity_and_edge_chunks(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_bundle(tmpdir, MINIMAL_BUNDLE)
            chunks = chunks_from_bundle(path, "test_bundle")
            entity_chunks = [c for c in chunks if c["chunk_type"] == "entity"]
            edge_chunks = [c for c in chunks if c["chunk_type"] == "edge"]
            assert len(entity_chunks) == 1
            assert entity_chunks[0]["name"] == "Patient"
            assert len(edge_chunks) == 1
            assert edge_chunks[0]["name"] == "has_diagnosis"

    def test_chunk_id_format(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_bundle(tmpdir, MINIMAL_BUNDLE)
            chunks = chunks_from_bundle(path, "mybundle")
            for c in chunks:
                assert c["chunk_id"].startswith(("entity::mybundle::", "edge::mybundle::"))

    def test_empty_bundle_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_bundle(tmpdir, {"metadata": {}, "ontology": {}})
            chunks = chunks_from_bundle(path, "empty")
            assert chunks == []


class TestBuildOntologyChunksTable:
    def test_creates_table_and_merges(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_bundle(tmpdir, MINIMAL_BUNDLE)
            mock_spark = MagicMock()
            mock_spark.sql.return_value.collect.return_value = [{"cnt": 2}]

            count = build_ontology_chunks_table(mock_spark, "cat", "sch", "test", path)
            sql_calls = [c[0][0] for c in mock_spark.sql.call_args_list]
            assert any("CREATE TABLE IF NOT EXISTS" in s for s in sql_calls)
            assert any("MERGE INTO" in s for s in sql_calls)
            assert count == 2

    def test_returns_zero_for_empty_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_bundle(tmpdir, {"metadata": {}, "ontology": {}})
            mock_spark = MagicMock()
            count = build_ontology_chunks_table(mock_spark, "cat", "sch", "test", path)
            assert count == 0
