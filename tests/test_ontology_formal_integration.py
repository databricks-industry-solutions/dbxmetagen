"""Integration tests for formal ontology grounding pipeline.

Tests the end-to-end flow: import -> load -> predict -> serialize.
Some tests require rdflib and are skipped if not installed.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml


def _rdflib_available():
    try:
        import rdflib  # noqa: F401
        return True
    except ImportError:
        return False


class TestImportLoadRoundTrip(unittest.TestCase):
    """Test that OWL import produces YAML that OntologyLoader can parse."""

    @unittest.skipUnless(_rdflib_available(), "rdflib not installed")
    def test_v2_import_then_load(self):
        from dbxmetagen.ontology_import import owl_to_bundle_yaml
        from dbxmetagen.ontology import OntologyLoader

        with tempfile.NamedTemporaryFile(suffix=".ttl", mode="w", delete=False) as ttl:
            ttl.write("""
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex: <http://example.org/> .
            ex:Patient rdf:type owl:Class ; rdfs:comment "A patient entity" .
            ex:Encounter rdf:type owl:Class ; rdfs:comment "A medical encounter" .
            ex:hasEncounter rdf:type owl:ObjectProperty ; rdfs:domain ex:Patient ; rdfs:range ex:Encounter .
            ex:birthDate rdf:type owl:DatatypeProperty ; rdfs:domain ex:Patient .
            """)
            ttl.flush()
            ttl_path = ttl.name

        try:
            with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as out:
                out_path = out.name

            bundle = owl_to_bundle_yaml(ttl_path, output_path=out_path, format_version="2.0")
            self.assertEqual(bundle["metadata"]["format_version"], "2.0")

            config = OntologyLoader.load_config(out_path)
            self.assertEqual(config["_format_version"], "2.0")

            entities = OntologyLoader.get_entity_definitions(config)
            names = {e.name for e in entities}
            self.assertIn("Patient", names)
            self.assertIn("Encounter", names)

            patient = next(e for e in entities if e.name == "Patient")
            self.assertIsNotNone(patient.uri)
            self.assertTrue(any(p.name == "hasEncounter" for p in patient.properties))
            self.assertTrue(any(p.name == "birthDate" for p in patient.properties))

            edge_catalog = OntologyLoader.get_edge_catalog(config)
            self.assertIn("hasEncounter", edge_catalog)
            self.assertIsNotNone(edge_catalog["hasEncounter"].uri)
        finally:
            os.unlink(ttl_path)
            os.unlink(out_path)

    @unittest.skipUnless(_rdflib_available(), "rdflib not installed")
    def test_v1_import_backward_compatible(self):
        from dbxmetagen.ontology_import import owl_to_bundle_yaml
        from dbxmetagen.ontology import OntologyLoader

        with tempfile.NamedTemporaryFile(suffix=".ttl", mode="w", delete=False) as ttl:
            ttl.write("""
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex: <http://example.org/> .
            ex:Widget rdf:type owl:Class .
            """)
            ttl.flush()
            ttl_path = ttl.name

        try:
            with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as out:
                out_path = out.name

            bundle = owl_to_bundle_yaml(ttl_path, output_path=out_path, format_version="1.0")
            config = OntologyLoader.load_config(out_path)
            self.assertEqual(config["_format_version"], "1.0")
            entities = OntologyLoader.get_entity_definitions(config)
            self.assertTrue(any(e.name == "Widget" for e in entities))
            # v1 entities should NOT have uri
            widget = next(e for e in entities if e.name == "Widget")
            self.assertIsNone(widget.uri)
        finally:
            os.unlink(ttl_path)
            os.unlink(out_path)


class TestMigrationThenLoad(unittest.TestCase):
    """Test that v1 bundles can be migrated and loaded as v2."""

    def test_migrate_and_load(self):
        from dbxmetagen.ontology_import import migrate_v1_to_v2
        from dbxmetagen.ontology import OntologyLoader

        v1_bundle = {
            "metadata": {"name": "healthcare", "version": "1.0"},
            "ontology": {
                "entities": {
                    "definitions": {
                        "Patient": {"description": "A patient", "keywords": ["patient"], "parent": "Person"},
                        "Encounter": {"description": "A visit", "keywords": ["visit"]},
                    }
                },
                "edge_catalog": {"has_encounter": {"domain": "Patient", "range": "Encounter"}},
            },
        }

        v2 = migrate_v1_to_v2(v1_bundle)
        self.assertEqual(v2["metadata"]["format_version"], "2.0")

        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
            yaml.dump(v2, f)
            f.flush()
            path = f.name

        try:
            config = OntologyLoader.load_config(path)
            self.assertEqual(config["_format_version"], "2.0")
            entities = OntologyLoader.get_entity_definitions(config)
            patient = next(e for e in entities if e.name == "Patient")
            self.assertIsNotNone(patient.uri)
            self.assertEqual(patient.source_ontology, "healthcare")
        finally:
            os.unlink(path)


class TestThreePassWithTierFiles(unittest.TestCase):
    """Test three-pass prediction end-to-end with generated tier files."""

    def test_predict_through_all_passes(self):
        from dbxmetagen.ontology_predictor import predict_entity
        from dbxmetagen.ontology_index import OntologyIndexLoader

        tmpdir = tempfile.mkdtemp()
        t1 = [{"name": "Patient", "description": "Healthcare recipient"},
              {"name": "Claim", "description": "Insurance claim"},
              {"name": "Drug", "description": "Pharmaceutical"}]
        t2 = {n["name"]: {"description": n["description"], "source": "FHIR R4",
              "uri": f"http://example.org/{n['name']}", "parent": None, "edges": []}
              for n in t1}
        t3 = {n["name"]: {"description": n["description"], "source": "FHIR R4",
              "uri": f"http://example.org/{n['name']}", "parent": None, "outgoing_edges": []}
              for n in t1}
        uris = {n["name"]: f"http://example.org/{n['name']}" for n in t1}

        for fname, data in [("entities_tier1.yaml", t1), ("entities_tier2.yaml", t2),
                            ("entities_tier3.yaml", t3), ("equivalent_class_uris.yaml", uris)]:
            with open(os.path.join(tmpdir, fname), "w") as f:
                yaml.dump(data, f)

        loader = OntologyIndexLoader(base_dir=tmpdir)
        self.assertTrue(loader.has_tier_indexes)

        call_count = [0]
        def llm_fn(system, user):
            call_count[0] += 1
            if call_count[0] == 1:
                return json.dumps({"top_candidates": ["Patient", "Drug"], "confidence": "medium", "reasoning": "unclear"})
            elif call_count[0] == 2:
                return json.dumps({"predicted_entity": "Patient", "source_ontology": "FHIR R4",
                    "equivalent_class_uri": "http://example.org/Patient",
                    "confidence_score": 0.88, "rationale": "good match", "needs_deep_pass": False})
            return json.dumps({"predicted_entity": "Patient", "confidence_score": 0.95,
                "source_ontology": "FHIR R4", "rationale": "deep match", "matched_properties": []})

        result = predict_entity("dim_patient", "patient_id INT, name STRING, dob DATE",
                              "sample data", loader, llm_fn)
        self.assertEqual(result.predicted_entity, "Patient")
        self.assertEqual(result.passes_run, 2)
        self.assertGreater(result.confidence_score, 0.8)


class TestTurtleRoundTrip(unittest.TestCase):
    """Test import -> predict -> serialize -> re-import round trip."""

    @unittest.skipUnless(_rdflib_available(), "rdflib not installed")
    def test_turtle_contains_expected_triples(self):
        from dbxmetagen.ontology_turtle import build_turtle

        preds = {
            "patients": {"predicted_entity": "Patient", "equivalent_class_uri": "http://hl7.org/fhir/Patient", "source_ontology": "FHIR R4"},
            "encounters": {"predicted_entity": "Encounter", "equivalent_class_uri": "http://hl7.org/fhir/Encounter", "source_ontology": "FHIR R4"},
        }
        edges = [{"from_table": "patients", "to_table": "encounters",
                  "predicted_edge": "has_encounter", "edge_uri": "http://hl7.org/fhir/Patient.encounter",
                  "inverse": "encounter_for"}]

        ttl = build_turtle("my_catalog", "my_schema", preds, edges)
        self.assertIsNotNone(ttl)

        import rdflib
        g = rdflib.Graph()
        g.parse(data=ttl, format="turtle")
        self.assertGreater(len(g), 5)

        # Verify key triples
        from rdflib import OWL
        equiv_triples = list(g.triples((None, OWL.equivalentClass, None)))
        self.assertTrue(len(equiv_triples) >= 2)

        obj_props = list(g.triples((None, rdflib.RDF.type, OWL.ObjectProperty)))
        self.assertTrue(len(obj_props) >= 1)


class TestBuildTiersFromScript(unittest.TestCase):
    """Test the build_tiers function from the build script."""

    def test_build_tiers_dry_run(self):
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
        from build_ontology_indexes import build_tiers

        entities = {
            "Patient": {"description": "A patient", "source": "FHIR R4",
                       "uri": "http://hl7.org/fhir/Patient", "parent": None,
                       "outgoing_edges": [{"name": "has_encounter", "uri": "http://...", "range": "Encounter", "inverse": None}]},
            "Encounter": {"description": "A visit", "source": "FHIR R4",
                         "uri": "http://hl7.org/fhir/Encounter", "parent": None,
                         "outgoing_edges": []},
        }

        tmpdir = Path(tempfile.mkdtemp())
        counts = build_tiers(entities, tmpdir, dry_run=True)
        self.assertEqual(counts["entities_tier1"], 2)
        self.assertFalse((tmpdir / "entities_tier1.yaml").exists())

    def test_build_tiers_writes_files(self):
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
        from build_ontology_indexes import build_tiers

        entities = {
            "Patient": {"description": "A patient", "source": "FHIR R4",
                       "uri": "http://hl7.org/fhir/Patient", "parent": None,
                       "outgoing_edges": []},
        }

        tmpdir = Path(tempfile.mkdtemp())
        counts = build_tiers(entities, tmpdir)
        self.assertTrue((tmpdir / "entities_tier1.yaml").exists())
        self.assertTrue((tmpdir / "entities_tier2.yaml").exists())
        self.assertTrue((tmpdir / "entities_tier3.yaml").exists())
        self.assertTrue((tmpdir / "equivalent_class_uris.yaml").exists())

        t1 = yaml.safe_load((tmpdir / "entities_tier1.yaml").read_text())
        self.assertEqual(len(t1), 1)
        self.assertEqual(t1[0]["name"], "Patient")


if __name__ == "__main__":
    unittest.main()
