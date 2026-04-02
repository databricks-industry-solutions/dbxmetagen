"""Unit tests for formal ontology grounding modules (M1-M4)."""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml


# ---------------------------------------------------------------------------
# OntologyIndexLoader tests
# ---------------------------------------------------------------------------

class TestOntologyIndexLoader(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        # Write tier files
        self.t1 = [{"name": "Patient", "description": "A patient"}, {"name": "Encounter", "description": "A visit"}]
        self.t2 = {"Patient": {"description": "A patient", "source": "FHIR R4", "uri": "http://hl7.org/fhir/Patient", "parent": None, "edges": ["has_encounter -> Encounter"]},
                    "Encounter": {"description": "A visit", "source": "FHIR R4", "uri": "http://hl7.org/fhir/Encounter", "parent": None, "edges": []}}
        self.t3 = {"Patient": {"description": "A patient", "source": "FHIR R4", "uri": "http://hl7.org/fhir/Patient", "parent": None, "outgoing_edges": [{"name": "has_encounter", "range": "Encounter"}]},
                    "Encounter": {"description": "A visit", "source": "FHIR R4", "uri": "http://hl7.org/fhir/Encounter", "parent": None, "outgoing_edges": []}}
        self.uris = {"Patient": "http://hl7.org/fhir/Patient", "Encounter": "http://hl7.org/fhir/Encounter"}

        for fname, data in [("entities_tier1.yaml", self.t1), ("entities_tier2.yaml", self.t2),
                            ("entities_tier3.yaml", self.t3), ("equivalent_class_uris.yaml", self.uris)]:
            with open(os.path.join(self.tmpdir, fname), "w") as f:
                yaml.dump(data, f)

    def test_has_tier_indexes(self):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        loader = OntologyIndexLoader(base_dir=self.tmpdir)
        self.assertTrue(loader.has_tier_indexes)

    def test_no_tier_indexes(self):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        empty = tempfile.mkdtemp()
        loader = OntologyIndexLoader(base_dir=empty)
        self.assertFalse(loader.has_tier_indexes)

    def test_tier1_returns_all(self):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        loader = OntologyIndexLoader(base_dir=self.tmpdir)
        result = loader.get_entities_tier1()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["name"], "Patient")

    def test_tier2_scoped(self):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        loader = OntologyIndexLoader(base_dir=self.tmpdir)
        result = loader.get_entities_tier2_scoped(["Patient"])
        self.assertIn("Patient", result)
        self.assertNotIn("Encounter", result)

    def test_tier3_scoped_missing_key(self):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        loader = OntologyIndexLoader(base_dir=self.tmpdir)
        result = loader.get_entities_tier3_scoped(["NonExistent"])
        self.assertEqual(result, {})

    def test_uri_lookup(self):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        loader = OntologyIndexLoader(base_dir=self.tmpdir)
        self.assertEqual(loader.get_uri("Patient"), "http://hl7.org/fhir/Patient")
        self.assertIsNone(loader.get_uri("Unknown"))

    def test_entity_count(self):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        loader = OntologyIndexLoader(base_dir=self.tmpdir)
        self.assertEqual(loader.entity_count(), 2)

    def test_caching(self):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        loader = OntologyIndexLoader(base_dir=self.tmpdir)
        r1 = loader.get_entities_tier1()
        r2 = loader.get_entities_tier1()
        self.assertIs(r1, r2)

    def test_edge_tier_returns_empty_when_missing(self):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        loader = OntologyIndexLoader(base_dir=self.tmpdir)
        self.assertEqual(loader.get_edges_tier1(), [])


# ---------------------------------------------------------------------------
# ontology_predictor tests
# ---------------------------------------------------------------------------

class TestOntologyPredictor(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        t1 = [{"name": "Patient", "description": "A patient"}, {"name": "Claim", "description": "Insurance claim"}]
        t2 = {"Patient": {"description": "A patient", "source": "FHIR R4", "uri": "http://hl7.org/fhir/Patient", "parent": None, "edges": []},
               "Claim": {"description": "Insurance claim", "source": "FHIR R4", "uri": "http://hl7.org/fhir/Claim", "parent": None, "edges": []}}
        t3 = t2
        uris = {"Patient": "http://hl7.org/fhir/Patient", "Claim": "http://hl7.org/fhir/Claim"}
        for fname, data in [("entities_tier1.yaml", t1), ("entities_tier2.yaml", t2),
                            ("entities_tier3.yaml", t3), ("equivalent_class_uris.yaml", uris)]:
            with open(os.path.join(self.tmpdir, fname), "w") as f:
                yaml.dump(data, f)

    def _make_loader(self):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        return OntologyIndexLoader(base_dir=self.tmpdir)

    def test_predict_entity_high_confidence(self):
        from dbxmetagen.ontology_predictor import predict_entity
        responses = [
            json.dumps({"top_candidates": ["Patient"], "confidence": "high", "reasoning": "obvious"}),
            json.dumps({"predicted_entity": "Patient", "source_ontology": "FHIR R4",
                        "equivalent_class_uri": "http://hl7.org/fhir/Patient",
                        "confidence_score": 0.95, "rationale": "matches", "needs_deep_pass": False}),
        ]
        call_idx = [0]
        def llm_fn(sys, usr):
            r = responses[call_idx[0]]
            call_idx[0] += 1
            return r

        result = predict_entity("patients", "id INT, name STRING", "sample rows", self._make_loader(), llm_fn)
        self.assertEqual(result.predicted_entity, "Patient")
        self.assertGreater(result.confidence_score, 0.9)
        self.assertEqual(result.passes_run, 2)

    def test_predict_entity_low_confidence_triggers_pass3(self):
        from dbxmetagen.ontology_predictor import predict_entity
        responses = [
            json.dumps({"top_candidates": ["Patient", "Claim"], "confidence": "low", "reasoning": "unclear"}),
            json.dumps({"predicted_entity": "Patient", "source_ontology": "FHIR R4",
                        "equivalent_class_uri": None, "confidence_score": 0.5,
                        "rationale": "maybe", "needs_deep_pass": True}),
            json.dumps({"predicted_entity": "Claim", "source_ontology": "FHIR R4",
                        "equivalent_class_uri": "http://hl7.org/fhir/Claim",
                        "confidence_score": 0.8, "rationale": "after deep analysis",
                        "matched_properties": ["claim_id -> Claim.id"]}),
        ]
        call_idx = [0]
        def llm_fn(sys, usr):
            r = responses[call_idx[0]]
            call_idx[0] += 1
            return r

        result = predict_entity("claims_table", "claim_id INT", "sample", self._make_loader(), llm_fn)
        self.assertEqual(result.predicted_entity, "Claim")
        self.assertEqual(result.passes_run, 3)
        self.assertTrue(len(result.matched_properties) > 0)

    def test_predict_entity_no_tier_indexes(self):
        from dbxmetagen.ontology_predictor import predict_entity
        from dbxmetagen.ontology_index import OntologyIndexLoader
        empty = tempfile.mkdtemp()
        loader = OntologyIndexLoader(base_dir=empty)
        result = predict_entity("t", "c", "s", loader, lambda s, u: "")
        self.assertEqual(result.predicted_entity, "Unknown")
        self.assertEqual(result.confidence_score, 0.0)

    def test_predict_entity_empty_candidates(self):
        from dbxmetagen.ontology_predictor import predict_entity
        def llm_fn(sys, usr):
            return json.dumps({"top_candidates": [], "confidence": "low", "reasoning": "none"})
        result = predict_entity("t", "c", "s", self._make_loader(), llm_fn)
        self.assertEqual(result.predicted_entity, "Unknown")
        self.assertTrue(result.needs_human_review)

    def test_parse_json_strips_markdown(self):
        from dbxmetagen.ontology_predictor import _parse_json
        text = "```json\n{\"key\": \"value\"}\n```"
        self.assertEqual(_parse_json(text), {"key": "value"})


# ---------------------------------------------------------------------------
# ontology_turtle tests
# ---------------------------------------------------------------------------

class TestOntologyTurtle(unittest.TestCase):

    def test_build_turtle_returns_none_without_rdflib(self):
        with patch.dict("sys.modules", {"rdflib": None}):
            # Force re-evaluation
            from dbxmetagen import ontology_turtle
            with patch.object(ontology_turtle, "_require_rdflib", return_value=False):
                result = ontology_turtle.build_turtle("cat", "sch", {}, [])
                self.assertIsNone(result)

    def test_build_turtle_with_rdflib(self):
        try:
            import rdflib  # noqa: F401
        except ImportError:
            self.skipTest("rdflib not installed")

        from dbxmetagen.ontology_turtle import build_turtle
        preds = {"patients": {"predicted_entity": "Patient", "equivalent_class_uri": "http://hl7.org/fhir/Patient", "source_ontology": "FHIR R4"}}
        edges = [{"from_table": "patients", "to_table": "encounters", "predicted_edge": "has_encounter", "edge_uri": None, "inverse": "encounter_for"}]
        ttl = build_turtle("catalog", "schema", preds, edges)
        self.assertIsNotNone(ttl)
        self.assertIn("Patient", ttl)
        self.assertIn("has_encounter", ttl)
        self.assertIn("owl:equivalentClass", ttl)

    def test_write_turtle(self):
        try:
            import rdflib  # noqa: F401
        except ImportError:
            self.skipTest("rdflib not installed")

        from dbxmetagen.ontology_turtle import write_turtle
        with tempfile.TemporaryDirectory() as tmpdir:
            preds = {"t": {"predicted_entity": "E", "equivalent_class_uri": None, "source_ontology": None}}
            path = write_turtle("c", "s", preds, [], output_dir=tmpdir)
            self.assertIsNotNone(path)
            self.assertTrue(Path(path).exists())


# ---------------------------------------------------------------------------
# ontology_graph_store tests
# ---------------------------------------------------------------------------

class TestOntologyGraphStore(unittest.TestCase):

    def test_is_available_returns_bool(self):
        from dbxmetagen.ontology_graph_store import is_available
        self.assertIsInstance(is_available(), bool)

    def test_store_raises_without_pyoxigraph(self):
        from dbxmetagen import ontology_graph_store
        with patch.object(ontology_graph_store, "is_available", return_value=False):
            with self.assertRaises(ImportError):
                ontology_graph_store.OntologyGraphStore()

    def test_store_with_pyoxigraph(self):
        try:
            import pyoxigraph  # noqa: F401
        except ImportError:
            self.skipTest("pyoxigraph not installed")

        from dbxmetagen.ontology_graph_store import OntologyGraphStore
        store = OntologyGraphStore()
        self.assertEqual(store.triple_count, 0)


# ---------------------------------------------------------------------------
# ontology_import v2 + migration tests
# ---------------------------------------------------------------------------

class TestOntologyImportV2(unittest.TestCase):

    def test_owl_to_v2_includes_format_version(self):
        try:
            import rdflib  # noqa: F401
        except ImportError:
            self.skipTest("rdflib not installed")

        with tempfile.NamedTemporaryFile(suffix=".ttl", mode="w", delete=False) as f:
            f.write("""
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex: <http://example.org/> .
            ex:Patient rdf:type owl:Class ; rdfs:comment "A patient" .
            ex:hasName rdf:type owl:DatatypeProperty ; rdfs:domain ex:Patient .
            """)
            f.flush()
            from dbxmetagen.ontology_import import owl_to_bundle_yaml
            bundle = owl_to_bundle_yaml(f.name, format_version="2.0")
            self.assertEqual(bundle["metadata"]["format_version"], "2.0")
            patient = bundle["ontology"]["entities"]["definitions"]["Patient"]
            self.assertIn("uri", patient)
            self.assertIn("owl_properties", patient)
            self.assertTrue(len(patient["owl_properties"]) > 0)
            os.unlink(f.name)

    def test_v1_format_no_format_version(self):
        try:
            import rdflib  # noqa: F401
        except ImportError:
            self.skipTest("rdflib not installed")

        with tempfile.NamedTemporaryFile(suffix=".ttl", mode="w", delete=False) as f:
            f.write("""
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex: <http://example.org/> .
            ex:Thing rdf:type owl:Class .
            """)
            f.flush()
            from dbxmetagen.ontology_import import owl_to_bundle_yaml
            bundle = owl_to_bundle_yaml(f.name, format_version="1.0")
            self.assertNotIn("format_version", bundle["metadata"])
            thing = bundle["ontology"]["entities"]["definitions"].get("Thing", {})
            self.assertNotIn("uri", thing)
            os.unlink(f.name)


class TestMigrateV1ToV2(unittest.TestCase):

    def test_migration_adds_fields(self):
        from dbxmetagen.ontology_import import migrate_v1_to_v2
        bundle = {
            "metadata": {"name": "test_bundle", "version": "1.0"},
            "ontology": {
                "entities": {
                    "definitions": {
                        "Patient": {"description": "A patient", "keywords": ["patient"], "parent": "Person"},
                        "Encounter": {"description": "A visit", "keywords": ["encounter"]},
                    }
                },
                "edge_catalog": {
                    "has_encounter": {"domain": "Patient", "range": "Encounter"},
                },
            },
        }
        result = migrate_v1_to_v2(bundle)
        self.assertEqual(result["metadata"]["format_version"], "2.0")
        patient = result["ontology"]["entities"]["definitions"]["Patient"]
        self.assertIn("uri", patient)
        self.assertEqual(patient["source_ontology"], "test_bundle")
        self.assertEqual(patient["subclass_of"], "Person")
        self.assertIn("uri", result["ontology"]["edge_catalog"]["has_encounter"])

    def test_idempotent_migration(self):
        from dbxmetagen.ontology_import import migrate_v1_to_v2
        bundle = {"metadata": {"name": "x", "format_version": "2.0"},
                   "ontology": {"entities": {"definitions": {"A": {"description": "a"}}}, "edge_catalog": {}}}
        result = migrate_v1_to_v2(bundle)
        self.assertEqual(result["metadata"]["format_version"], "2.0")


# ---------------------------------------------------------------------------
# OntologyLoader v2 detection
# ---------------------------------------------------------------------------

class TestOntologyLoaderV2(unittest.TestCase):

    def test_v2_format_detected(self):
        from dbxmetagen.ontology import OntologyLoader
        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
            yaml.dump({
                "metadata": {"format_version": "2.0", "name": "Test"},
                "ontology": {
                    "entities": {
                        "definitions": {
                            "Patient": {
                                "description": "A patient",
                                "keywords": ["patient"],
                                "uri": "http://hl7.org/fhir/Patient",
                                "source_ontology": "FHIR R4",
                                "subclass_of": "Person",
                                "owl_properties": [{"name": "birth_date", "type": "data_property", "uri": "http://hl7.org/fhir/Patient.birthDate", "datatype": "xsd:date"}],
                            }
                        }
                    }
                },
            }, f)
            f.flush()
            config = OntologyLoader.load_config(f.name)
            self.assertEqual(config["_format_version"], "2.0")
            os.unlink(f.name)

    def test_v1_format_defaults(self):
        from dbxmetagen.ontology import OntologyLoader
        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
            yaml.dump({
                "metadata": {"name": "Test"},
                "ontology": {
                    "entities": {"definitions": {"Thing": {"description": "A thing", "keywords": ["thing"]}}}
                },
            }, f)
            f.flush()
            config = OntologyLoader.load_config(f.name)
            self.assertEqual(config["_format_version"], "1.0")
            os.unlink(f.name)

    def test_v2_entity_definitions_parse_uri(self):
        from dbxmetagen.ontology import OntologyLoader
        config = {
            "entities": {
                "definitions": {
                    "Patient": {
                        "description": "A patient",
                        "keywords": ["patient"],
                        "uri": "http://hl7.org/fhir/Patient",
                        "source_ontology": "FHIR R4",
                        "subclass_of": "Person",
                        "owl_properties": [
                            {"name": "birth_date", "type": "data_property", "uri": "http://hl7.org/fhir/Patient.birthDate"},
                        ],
                    }
                }
            }
        }
        entities = OntologyLoader.get_entity_definitions(config)
        self.assertEqual(len(entities), 1)
        p = entities[0]
        self.assertEqual(p.uri, "http://hl7.org/fhir/Patient")
        self.assertEqual(p.source_ontology, "FHIR R4")
        self.assertEqual(p.subclass_of, "Person")
        # owl_properties get merged into properties
        self.assertTrue(any(prop.name == "birth_date" for prop in p.properties))

    def test_v2_edge_catalog_parse_uri(self):
        from dbxmetagen.ontology import OntologyLoader
        config = {
            "entities": {"definitions": {"A": {"description": "a", "keywords": ["a"]}}},
            "edge_catalog": {
                "has_encounter": {
                    "domain": "Patient", "range": "Encounter",
                    "uri": "http://hl7.org/fhir/Patient.encounter",
                    "owl_type": "ObjectProperty",
                }
            }
        }
        catalog = OntologyLoader.get_edge_catalog(config)
        self.assertIn("has_encounter", catalog)
        self.assertEqual(catalog["has_encounter"].uri, "http://hl7.org/fhir/Patient.encounter")
        self.assertEqual(catalog["has_encounter"].owl_type, "ObjectProperty")


if __name__ == "__main__":
    unittest.main()
