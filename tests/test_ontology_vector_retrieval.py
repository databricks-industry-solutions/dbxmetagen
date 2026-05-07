"""Unit tests for ontology vector retrieval pipeline (chunker, vector index, predictor vector mode)."""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml


# ---------------------------------------------------------------------------
# Chunker: chunks_from_bundle
# ---------------------------------------------------------------------------

class TestChunksFromBundle(unittest.TestCase):
    """Test chunk generation from curated bundle YAML."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.bundle = {
            "metadata": {
                "name": "Test Bundle",
                "version": "3.0",
                "industry": "test",
                "standards_alignment": "TestStd",
            },
            "ontology": {
                "entities": {
                    "definitions": {
                        "Patient": {
                            "description": "A person receiving care",
                            "keywords": ["patient", "person"],
                            "typical_attributes": ["id", "name", "dob"],
                            "synonyms": ["member", "enrollee"],
                            "parent": "Person",
                            "relationships": {
                                "has_encounter": {"target": "Encounter", "cardinality": "one-to-many"},
                            },
                            "properties": {
                                "mrn": {"typical_attributes": ["mrn", "medical_record_number", "patient_mrn"]},
                                "name": {"typical_attributes": ["name", "full_name", "first_name"]},
                            },
                            "business_questions": [
                                "How many patients were admitted last month?",
                                "What is the average length of stay?",
                            ],
                        },
                        "Encounter": {
                            "description": "A healthcare visit",
                            "keywords": ["visit", "encounter"],
                            "typical_attributes": ["id", "date"],
                            "relationships": {},
                            "properties": {},
                        },
                    },
                },
                "edge_catalog": {
                    "has_encounter": {
                        "domain": "Patient",
                        "range": "Encounter",
                        "inverse": "encounter_of",
                        "symmetric": False,
                        "category": "clinical",
                    },
                },
            },
        }
        self.bundle_path = os.path.join(self.tmpdir, "test.yaml")
        with open(self.bundle_path, "w") as f:
            yaml.dump(self.bundle, f)

    def test_produces_entity_and_edge_chunks(self):
        from dbxmetagen.ontology_chunker import chunks_from_bundle
        chunks = chunks_from_bundle(self.bundle_path, "test")
        entity_chunks = [c for c in chunks if c["chunk_type"] == "entity"]
        edge_chunks = [c for c in chunks if c["chunk_type"] == "edge"]
        self.assertEqual(len(entity_chunks), 2)
        self.assertGreaterEqual(len(edge_chunks), 1)

    def test_entity_chunk_fields(self):
        from dbxmetagen.ontology_chunker import chunks_from_bundle
        chunks = chunks_from_bundle(self.bundle_path, "test")
        patient = next(c for c in chunks if c["name"] == "Patient")
        self.assertEqual(patient["chunk_id"], "entity::test::Patient")
        self.assertEqual(patient["chunk_type"], "entity")
        self.assertEqual(patient["ontology_bundle"], "test")
        self.assertIn("person receiving care", patient["content"])
        self.assertIn("patient", patient["keywords"])
        self.assertIn("Person", patient["parent_entities"])

    def test_edge_chunk_fields(self):
        from dbxmetagen.ontology_chunker import chunks_from_bundle
        chunks = chunks_from_bundle(self.bundle_path, "test")
        edge = next(c for c in chunks if c["name"] == "has_encounter")
        self.assertEqual(edge["chunk_id"], "edge::test::has_encounter")
        self.assertEqual(edge["chunk_type"], "edge")
        self.assertEqual(edge["domain"], "Patient")
        self.assertEqual(edge["range_entity"], "Encounter")
        self.assertIn("Patient", edge["content"])
        self.assertIn("Encounter", edge["content"])

    def test_content_richness(self):
        from dbxmetagen.ontology_chunker import chunks_from_bundle
        chunks = chunks_from_bundle(self.bundle_path, "test")
        patient = next(c for c in chunks if c["name"] == "Patient")
        self.assertIn("Column patterns:", patient["content"])
        self.assertIn("Keywords:", patient["content"])
        self.assertIn("Parents:", patient["content"])
        self.assertIn("Related:", patient["content"])

    def test_property_aliases_extracted(self):
        from dbxmetagen.ontology_chunker import chunks_from_bundle
        chunks = chunks_from_bundle(self.bundle_path, "test")
        patient = next(c for c in chunks if c["name"] == "Patient")
        for alias in ["medical_record_number", "patient_mrn", "full_name", "first_name"]:
            self.assertIn(alias, patient["content"])

    def test_business_questions_included(self):
        from dbxmetagen.ontology_chunker import chunks_from_bundle
        chunks = chunks_from_bundle(self.bundle_path, "test")
        patient = next(c for c in chunks if c["name"] == "Patient")
        self.assertIn("Questions:", patient["content"])
        self.assertIn("admitted last month", patient["content"])

    def test_edge_endpoint_descriptions(self):
        from dbxmetagen.ontology_chunker import chunks_from_bundle
        chunks = chunks_from_bundle(self.bundle_path, "test")
        edge = next(c for c in chunks if c["name"] == "has_encounter")
        self.assertIn("person receiving care", edge["content"])
        self.assertIn("healthcare visit", edge["content"])

    def test_tier_label_is_tier1_plus(self):
        from dbxmetagen.ontology_chunker import chunks_from_bundle
        chunks = chunks_from_bundle(self.bundle_path, "test")
        for c in chunks:
            self.assertEqual(c["tier"], "tier1_plus")

    def test_entity_content_order(self):
        """Column patterns should appear before description/keywords."""
        from dbxmetagen.ontology_chunker import chunks_from_bundle
        chunks = chunks_from_bundle(self.bundle_path, "test")
        patient = next(c for c in chunks if c["name"] == "Patient")
        content = patient["content"]
        col_idx = content.index("Column patterns:")
        desc_idx = content.index("person receiving care")
        kw_idx = content.index("Keywords:")
        self.assertLess(col_idx, desc_idx)
        self.assertLess(desc_idx, kw_idx)

    def test_no_duplicate_edges(self):
        from dbxmetagen.ontology_chunker import chunks_from_bundle
        chunks = chunks_from_bundle(self.bundle_path, "test")
        edge_names = [c["name"] for c in chunks if c["chunk_type"] == "edge"]
        self.assertEqual(len(edge_names), len(set(edge_names)))


# ---------------------------------------------------------------------------
# Chunker: chunks_from_owl
# ---------------------------------------------------------------------------

class TestChunksFromOwl(unittest.TestCase):
    """Test chunk generation from OWL/TTL files via rdflib."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.ttl_content = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix ex: <http://example.org/> .

ex:Person a owl:Class ;
    rdfs:label "Person" ;
    rdfs:comment "A human being" .

ex:Organization a owl:Class ;
    rdfs:label "Organization" ;
    rdfs:comment "A group of people" .

ex:Employee a owl:Class ;
    rdfs:label "Employee" ;
    rdfs:comment "A person employed by an organization" ;
    rdfs:subClassOf ex:Person .

ex:worksFor a owl:ObjectProperty ;
    rdfs:domain ex:Employee ;
    rdfs:range ex:Organization ;
    rdfs:comment "Employment relationship" .

ex:age a owl:DatatypeProperty ;
    rdfs:domain ex:Person ;
    rdfs:range <http://www.w3.org/2001/XMLSchema#integer> .
"""
        self.ttl_path = os.path.join(self.tmpdir, "test.ttl")
        with open(self.ttl_path, "w") as f:
            f.write(self.ttl_content)

    def test_produces_chunks(self):
        try:
            import rdflib  # noqa: F401
        except ImportError:
            self.skipTest("rdflib not installed")

        from dbxmetagen.ontology_chunker import chunks_from_owl
        chunks = chunks_from_owl(self.ttl_path, "test_owl")
        entity_chunks = [c for c in chunks if c["chunk_type"] == "entity"]
        edge_chunks = [c for c in chunks if c["chunk_type"] == "edge"]
        self.assertGreaterEqual(len(entity_chunks), 3)
        self.assertGreaterEqual(len(edge_chunks), 1)

    def test_owl_entity_content(self):
        try:
            import rdflib  # noqa: F401
        except ImportError:
            self.skipTest("rdflib not installed")

        from dbxmetagen.ontology_chunker import chunks_from_owl
        chunks = chunks_from_owl(self.ttl_path, "test_owl")
        person = next((c for c in chunks if c["name"] == "Person"), None)
        self.assertIsNotNone(person)
        self.assertIn("human being", person["content"])
        self.assertEqual(person["ontology_bundle"], "test_owl")

    def test_owl_edge_content(self):
        try:
            import rdflib  # noqa: F401
        except ImportError:
            self.skipTest("rdflib not installed")

        from dbxmetagen.ontology_chunker import chunks_from_owl
        chunks = chunks_from_owl(self.ttl_path, "test_owl")
        edge = next((c for c in chunks if c["name"] == "worksFor"), None)
        self.assertIsNotNone(edge)
        self.assertIn("Employee", edge["content"])
        self.assertIn("Organization", edge["content"])
        self.assertEqual(edge["domain"], "Employee")
        self.assertEqual(edge["range_entity"], "Organization")

    def test_subclass_parent(self):
        try:
            import rdflib  # noqa: F401
        except ImportError:
            self.skipTest("rdflib not installed")

        from dbxmetagen.ontology_chunker import chunks_from_owl
        chunks = chunks_from_owl(self.ttl_path, "test_owl")
        emp = next((c for c in chunks if c["name"] == "Employee"), None)
        self.assertIsNotNone(emp)
        self.assertIn("Person", emp["parent_entities"])

    def test_data_property_enrichment(self):
        try:
            import rdflib  # noqa: F401
        except ImportError:
            self.skipTest("rdflib not installed")

        from dbxmetagen.ontology_chunker import chunks_from_owl
        chunks = chunks_from_owl(self.ttl_path, "test_owl")
        person = next((c for c in chunks if c["name"] == "Person"), None)
        self.assertIn("age", person["content"])

    def test_owl_keywords_include_label(self):
        try:
            import rdflib  # noqa: F401
        except ImportError:
            self.skipTest("rdflib not installed")

        from dbxmetagen.ontology_chunker import chunks_from_owl
        chunks = chunks_from_owl(self.ttl_path, "test_owl")
        person = next((c for c in chunks if c["name"] == "Person"), None)
        self.assertIn("person", person["keywords"])

    def test_owl_tier_label(self):
        try:
            import rdflib  # noqa: F401
        except ImportError:
            self.skipTest("rdflib not installed")

        from dbxmetagen.ontology_chunker import chunks_from_owl
        chunks = chunks_from_owl(self.ttl_path, "test_owl")
        for c in chunks:
            self.assertEqual(c["tier"], "tier1_plus")

    def test_owl_edge_endpoint_descriptions(self):
        try:
            import rdflib  # noqa: F401
        except ImportError:
            self.skipTest("rdflib not installed")

        from dbxmetagen.ontology_chunker import chunks_from_owl
        chunks = chunks_from_owl(self.ttl_path, "test_owl")
        edge = next((c for c in chunks if c["name"] == "worksFor"), None)
        self.assertIn("person employed", edge["content"])
        self.assertIn("group of people", edge["content"])


# ---------------------------------------------------------------------------
# OntologyVectorIndexConfig
# ---------------------------------------------------------------------------

class TestOntologyVectorIndexConfig(unittest.TestCase):

    def test_fq_properties(self):
        from dbxmetagen.ontology_vector_index import OntologyVectorIndexConfig
        cfg = OntologyVectorIndexConfig(catalog_name="cat", schema_name="sch")
        self.assertEqual(cfg.fq_documents, "cat.sch.ontology_chunks")
        self.assertEqual(cfg.fq_index, "cat.sch.ontology_vs_index")
        self.assertEqual(cfg.endpoint_name, "dbxmetagen-vs")
        self.assertEqual(cfg.embedding_model, "databricks-gte-large-en")


# ---------------------------------------------------------------------------
# Predictor vector mode
# ---------------------------------------------------------------------------

class TestPredictorVectorMode(unittest.TestCase):
    """Test that pass0_mode='vector' skips Pass 1 and calls VS query."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        t1 = [{"name": "Patient", "description": "A patient"},
              {"name": "Encounter", "description": "A visit"}]
        t2 = {"Patient": {"description": "A patient", "source_ontology": "FHIR", "uri": "http://fhir/Patient", "edges": []},
              "Encounter": {"description": "A visit", "source_ontology": "FHIR", "uri": "http://fhir/Encounter", "edges": []}}
        t3 = {"Patient": {"description": "A patient", "source_ontology": "FHIR", "uri": "http://fhir/Patient",
                          "keywords": [], "synonyms": [], "typical_attributes": [], "business_questions": [],
                          "relationships": {}, "properties": {}},
              "Encounter": {"description": "A visit", "source_ontology": "FHIR", "uri": "http://fhir/Encounter",
                            "keywords": [], "synonyms": [], "typical_attributes": [], "business_questions": [],
                            "relationships": {}, "properties": {}}}
        uris = {"Patient": "http://fhir/Patient", "Encounter": "http://fhir/Encounter"}
        for fname, data in [("entities_tier1.yaml", t1), ("entities_tier2.yaml", t2),
                            ("entities_tier3.yaml", t3), ("equivalent_class_uris.yaml", uris)]:
            with open(os.path.join(self.tmpdir, fname), "w") as f:
                yaml.dump(data, f)

    def _make_llm_fn(self, response_json):
        def llm_fn(system_prompt, user_prompt):
            return json.dumps(response_json)
        return llm_fn

    @patch("dbxmetagen.ontology_predictor.vs_query_entities")
    def test_vector_mode_skips_pass1(self, mock_vs_query):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        from dbxmetagen.ontology_predictor import predict_entity

        mock_vs_query.return_value = [
            {"name": "Patient", "content": "A patient", "uri": "http://fhir/Patient"},
        ]
        loader = OntologyIndexLoader(base_dir=self.tmpdir)
        llm_fn = self._make_llm_fn({
            "predicted_entity": "Patient",
            "equivalent_class_uri": "http://fhir/Patient",
            "confidence_score": 0.9,
            "rationale": "Strong match",
            "needs_deep_pass": False,
        })

        result = predict_entity(
            table_name="patients",
            columns="id, name, dob",
            sample="Description: table of patient records",
            loader=loader,
            llm_fn=llm_fn,
            pass0_mode="vector",
            vs_index="cat.sch.ontology_vs_index",
            vs_bundle="test",
        )

        mock_vs_query.assert_called_once()
        self.assertEqual(result.predicted_entity, "Patient")
        self.assertGreaterEqual(result.confidence_score, 0.9)

    @patch("dbxmetagen.ontology_predictor.vs_query_entities")
    def test_vector_mode_empty_results_returns_unknown(self, mock_vs_query):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        from dbxmetagen.ontology_predictor import predict_entity

        mock_vs_query.return_value = []
        loader = OntologyIndexLoader(base_dir=self.tmpdir)
        llm_fn = self._make_llm_fn({})

        result = predict_entity(
            table_name="unknown_table",
            columns="x, y, z",
            sample="Description: mystery table",
            loader=loader,
            llm_fn=llm_fn,
            pass0_mode="vector",
            vs_index="cat.sch.ontology_vs_index",
            vs_bundle="test",
        )

        self.assertEqual(result.predicted_entity, "Unknown")
        self.assertTrue(result.needs_human_review)

    def test_standard_mode_unchanged(self):
        """Verify pass0_mode='off' still works without VS params."""
        from dbxmetagen.ontology_index import OntologyIndexLoader
        from dbxmetagen.ontology_predictor import predict_entity

        loader = OntologyIndexLoader(base_dir=self.tmpdir)
        llm_fn = self._make_llm_fn({
            "top_candidates": ["Patient"],
            "confidence": "high",
            "reasoning": "obvious",
        })

        # Mock Pass 2 response
        call_count = [0]
        original_fn = llm_fn
        def counting_llm_fn(system, user):
            call_count[0] += 1
            if call_count[0] == 1:
                return json.dumps({
                    "top_candidates": ["Patient"],
                    "confidence": "high",
                    "reasoning": "obvious",
                })
            else:
                return json.dumps({
                    "predicted_entity": "Patient",
                    "equivalent_class_uri": "http://fhir/Patient",
                    "confidence_score": 0.85,
                    "rationale": "Good match",
                    "needs_deep_pass": False,
                })

        result = predict_entity(
            table_name="patients",
            columns="id, name, dob",
            sample="Description: patient table",
            loader=loader,
            llm_fn=counting_llm_fn,
            pass0_mode="off",
        )

        self.assertEqual(result.predicted_entity, "Patient")
        self.assertEqual(call_count[0], 2)


# ---------------------------------------------------------------------------
# Predictor edge vector mode
# ---------------------------------------------------------------------------

class TestEdgeVectorMode(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        et1 = [{"name": "has_encounter", "domain": "Patient", "range": "Encounter", "cardinality": "1:N"}]
        et2 = {"has_encounter": {"name": "has_encounter", "domain": "Patient", "range": "Encounter",
                                 "uri": "http://fhir/has_encounter", "inverse": "encounter_of"}}
        et3 = dict(et2)
        for fname, data in [("edges_tier1.yaml", et1), ("edges_tier2.yaml", et2),
                            ("edges_tier3.yaml", et3),
                            ("entities_tier1.yaml", []), ("entities_tier2.yaml", {}),
                            ("entities_tier3.yaml", {}), ("equivalent_class_uris.yaml", {})]:
            with open(os.path.join(self.tmpdir, fname), "w") as f:
                yaml.dump(data, f)

    @patch("dbxmetagen.ontology_predictor.vs_query_edges")
    def test_edge_vector_retrieval(self, mock_vs_edges):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        from dbxmetagen.ontology_predictor import predict_edge

        mock_vs_edges.return_value = [
            {"name": "has_encounter", "content": "relationship from Patient to Encounter",
             "uri": "http://fhir/has_encounter", "domain": "Patient", "range_entity": "Encounter"},
        ]

        loader = OntologyIndexLoader(base_dir=self.tmpdir)

        def llm_fn(system, user):
            return json.dumps({
                "predicted_edge": "has_encounter",
                "edge_uri": "http://fhir/has_encounter",
                "inverse": "encounter_of",
                "confidence_score": 0.88,
                "rationale": "Strong match",
                "needs_deep_pass": False,
            })

        result = predict_edge(
            src_entity="Patient", dst_entity="Encounter",
            loader=loader, llm_fn=llm_fn,
            from_table="patients", from_column="id",
            to_table="encounters", to_column="patient_id",
            vs_index="cat.sch.ontology_vs_index",
            vs_bundle="test",
        )

        mock_vs_edges.assert_called_once()
        self.assertEqual(result.predicted_edge, "has_encounter")
        self.assertGreaterEqual(result.confidence_score, 0.5)


# ---------------------------------------------------------------------------
# owl_to_chunks wrapper in ontology_import.py
# ---------------------------------------------------------------------------

class TestOwlToChunksImport(unittest.TestCase):

    def test_owl_to_chunks_delegates(self):
        try:
            import rdflib  # noqa: F401
        except ImportError:
            self.skipTest("rdflib not installed")

        tmpdir = tempfile.mkdtemp()
        ttl = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix ex: <http://example.org/> .
ex:Foo a owl:Class ; rdfs:label "Foo" ; rdfs:comment "A foo thing" .
"""
        path = os.path.join(tmpdir, "mini.ttl")
        with open(path, "w") as f:
            f.write(ttl)

        from dbxmetagen.ontology_import import owl_to_chunks
        chunks = owl_to_chunks(path, "mini_bundle")
        self.assertTrue(any(c["name"] == "Foo" for c in chunks))


if __name__ == "__main__":
    unittest.main()
