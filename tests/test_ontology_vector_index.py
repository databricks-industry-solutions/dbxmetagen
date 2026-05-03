"""Unit tests for ontology_vector_index query functions."""

import inspect

from dbxmetagen.ontology_vector_index import query_entities, query_edges


class TestQueryEntitiesFilters:
    def test_filters_by_bundle_and_entity_type(self):
        src = inspect.getsource(query_entities)
        assert '"ontology_bundle": bundle' in src
        assert '"chunk_type": "entity"' in src

    def test_uses_hybrid_search(self):
        src = inspect.getsource(query_entities)
        assert 'query_type="HYBRID"' in src

    def test_returns_expected_columns(self):
        src = inspect.getsource(query_entities)
        for col in ("name", "content", "uri", "parent_entities", "keywords"):
            assert col in src


class TestQueryEdgesFilters:
    def test_filters_by_bundle_and_edge_type(self):
        src = inspect.getsource(query_edges)
        assert '"ontology_bundle": bundle' in src
        assert '"chunk_type": "edge"' in src

    def test_uses_hybrid_search(self):
        src = inspect.getsource(query_edges)
        assert 'query_type="HYBRID"' in src

    def test_returns_expected_columns(self):
        src = inspect.getsource(query_edges)
        for col in ("name", "content", "uri", "domain", "range_entity"):
            assert col in src
