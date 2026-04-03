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
        self.t2 = {"Patient": {"description": "A patient", "source_ontology": "FHIR R4", "uri": "http://hl7.org/fhir/Patient", "edges": ["has_encounter"]},
                    "Encounter": {"description": "A visit", "source_ontology": "FHIR R4", "uri": "http://hl7.org/fhir/Encounter", "edges": []}}
        self.t3 = {"Patient": {"description": "A patient", "source_ontology": "FHIR R4", "uri": "http://hl7.org/fhir/Patient",
                                "keywords": ["patient"], "synonyms": ["person"], "typical_attributes": ["id", "name"],
                                "business_questions": ["How many patients?"], "relationships": {"has_encounter": {"target": "Encounter", "cardinality": "one-to-many"}}, "properties": {}},
                    "Encounter": {"description": "A visit", "source_ontology": "FHIR R4", "uri": "http://hl7.org/fhir/Encounter",
                                  "keywords": ["visit"], "synonyms": ["appointment"], "typical_attributes": ["id", "date"],
                                  "business_questions": ["How many encounters?"], "relationships": {}, "properties": {}}}
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
        t2 = {"Patient": {"description": "A patient", "source_ontology": "FHIR R4", "uri": "http://hl7.org/fhir/Patient", "edges": []},
               "Claim": {"description": "Insurance claim", "source_ontology": "FHIR R4", "uri": "http://hl7.org/fhir/Claim", "edges": []}}
        t3 = {"Patient": {"description": "A patient", "source_ontology": "FHIR R4", "uri": "http://hl7.org/fhir/Patient",
                           "keywords": [], "synonyms": [], "typical_attributes": [], "business_questions": [], "relationships": {}, "properties": {}},
               "Claim": {"description": "Insurance claim", "source_ontology": "FHIR R4", "uri": "http://hl7.org/fhir/Claim",
                          "keywords": [], "synonyms": [], "typical_attributes": [], "business_questions": [], "relationships": {}, "properties": {}}}
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


class TestBuildTiersSchema(unittest.TestCase):
    """Validate that build_tiers() produces the correct schema for the predictor."""

    def _make_entities(self):
        return {
            "Patient": {
                "description": "A patient record",
                "source": "FHIR R4",
                "uri": "http://hl7.org/fhir/Patient",
                "parent": "Person",
                "outgoing_edges": [
                    {"name": "has_encounter", "uri": "http://hl7.org/fhir/Patient.encounter", "range": "Encounter", "inverse": "subject"},
                ],
                "keywords": ["patient", "member"],
                "synonyms": ["person"],
                "typical_attributes": ["id", "name", "dob"],
                "business_questions": ["How many patients?"],
                "relationships": {"has_encounter": {"target": "Encounter", "cardinality": "one-to-many"}},
                "properties": {"status": "active|inactive"},
            },
            "Encounter": {
                "description": "A visit",
                "source": "FHIR R4",
                "uri": "http://hl7.org/fhir/Encounter",
                "parent": None,
                "outgoing_edges": [],
                "keywords": ["visit"],
                "synonyms": [],
                "typical_attributes": ["id", "date"],
                "business_questions": [],
                "relationships": {},
                "properties": {},
            },
        }

    def test_tier1_is_list_of_name_description(self):
        from scripts.build_ontology_indexes import build_tiers
        outdir = Path(tempfile.mkdtemp())
        build_tiers(self._make_entities(), outdir)
        t1 = yaml.safe_load((outdir / "entities_tier1.yaml").read_text())
        self.assertIsInstance(t1, list)
        for entry in t1:
            self.assertIn("name", entry)
            self.assertIn("description", entry)
            self.assertNotIn("source", entry)

    def test_tier2_has_source_ontology_not_source(self):
        from scripts.build_ontology_indexes import build_tiers
        outdir = Path(tempfile.mkdtemp())
        build_tiers(self._make_entities(), outdir)
        t2 = yaml.safe_load((outdir / "entities_tier2.yaml").read_text())
        for name, profile in t2.items():
            self.assertIn("source_ontology", profile, f"{name} missing source_ontology")
            self.assertNotIn("source", profile, f"{name} has deprecated 'source' key")
            self.assertNotIn("parent", profile, f"{name} has deprecated 'parent' key")
            self.assertIsInstance(profile["edges"], list)
            for e in profile["edges"]:
                self.assertIsInstance(e, str, f"Edge should be a short name string, got {type(e)}")
                self.assertNotIn("->", e, "Edge should be short name, not 'name -> range' format")

    def test_tier3_has_rich_fields_not_outgoing_edges(self):
        from scripts.build_ontology_indexes import build_tiers
        outdir = Path(tempfile.mkdtemp())
        build_tiers(self._make_entities(), outdir)
        t3 = yaml.safe_load((outdir / "entities_tier3.yaml").read_text())
        for name, profile in t3.items():
            self.assertIn("source_ontology", profile)
            self.assertNotIn("source", profile)
            self.assertNotIn("outgoing_edges", profile)
            self.assertNotIn("parent", profile)
            for field in ("keywords", "synonyms", "typical_attributes", "business_questions"):
                self.assertIn(field, profile, f"{name} missing {field}")
                self.assertIsInstance(profile[field], list)
            self.assertIn("relationships", profile)
            self.assertIsInstance(profile["relationships"], dict)
            self.assertIn("properties", profile)

    def test_tier3_patient_has_relationships(self):
        from scripts.build_ontology_indexes import build_tiers
        outdir = Path(tempfile.mkdtemp())
        build_tiers(self._make_entities(), outdir)
        t3 = yaml.safe_load((outdir / "entities_tier3.yaml").read_text())
        rels = t3["Patient"]["relationships"]
        self.assertIn("has_encounter", rels)
        self.assertEqual(rels["has_encounter"]["target"], "Encounter")

    def test_equivalent_class_uris(self):
        from scripts.build_ontology_indexes import build_tiers
        outdir = Path(tempfile.mkdtemp())
        build_tiers(self._make_entities(), outdir)
        uris = yaml.safe_load((outdir / "equivalent_class_uris.yaml").read_text())
        self.assertEqual(uris["Patient"], "http://hl7.org/fhir/Patient")
        self.assertEqual(uris["Encounter"], "http://hl7.org/fhir/Encounter")


class TestEntitiesFromBundle(unittest.TestCase):
    """Validate that _entities_from_bundle preserves rich fields."""

    def test_preserves_keywords_and_relationships(self):
        from scripts.build_ontology_indexes import _entities_from_bundle
        bundle = {
            "ontology": {
                "entities": {
                    "definitions": {
                        "Account": {
                            "description": "A financial account",
                            "uri": "https://schema.org/BankAccount",
                            "source_ontology": "schema.org",
                            "keywords": ["account", "savings"],
                            "synonyms": ["deposit account"],
                            "typical_attributes": ["id", "balance"],
                            "business_questions": ["Average balance?"],
                            "properties": {"status": "open|closed"},
                            "relationships": {
                                "owned_by": {"target": "Person", "cardinality": "many-to-one"},
                            },
                        }
                    }
                },
                "edge_catalog": {},
            }
        }
        tmpfile = Path(tempfile.mkdtemp()) / "test_bundle.yaml"
        tmpfile.write_text(yaml.dump(bundle), encoding="utf-8")
        entities = _entities_from_bundle(tmpfile)
        acct = entities["Account"]
        self.assertEqual(acct["keywords"], ["account", "savings"])
        self.assertEqual(acct["synonyms"], ["deposit account"])
        self.assertEqual(acct["typical_attributes"], ["id", "balance"])
        self.assertEqual(acct["business_questions"], ["Average balance?"])
        self.assertEqual(acct["properties"], {"status": "open|closed"})
        self.assertIn("owned_by", acct["relationships"])
        self.assertEqual(acct["relationships"]["owned_by"]["target"], "Person")


class TestMergeBundleFields(unittest.TestCase):
    """Validate hybrid merge mode."""

    def test_merge_enriches_auto_extracted(self):
        from scripts.build_ontology_indexes import _merge_bundle_fields
        auto = {
            "Patient": {
                "description": "A patient from OWL",
                "source": "FHIR R4",
                "uri": "http://hl7.org/fhir/Patient",
                "outgoing_edges": [],
                "keywords": [],
                "synonyms": [],
                "typical_attributes": [],
                "business_questions": [],
                "relationships": {},
                "properties": {},
            }
        }
        bundle = {
            "ontology": {
                "entities": {
                    "definitions": {
                        "Patient": {
                            "description": "A patient",
                            "uri": "http://hl7.org/fhir/Patient",
                            "source_ontology": "FHIR R4",
                            "keywords": ["patient", "member"],
                            "synonyms": ["person"],
                            "typical_attributes": ["id", "name"],
                            "business_questions": ["How many?"],
                            "properties": {"status": "active"},
                            "relationships": {
                                "treated_by": {"target": "Practitioner", "cardinality": "many-to-many"},
                            },
                        }
                    }
                },
                "edge_catalog": {},
            }
        }
        tmpfile = Path(tempfile.mkdtemp()) / "merge.yaml"
        tmpfile.write_text(yaml.dump(bundle), encoding="utf-8")
        merged = _merge_bundle_fields(auto, tmpfile)
        p = merged["Patient"]
        self.assertEqual(p["keywords"], ["patient", "member"])
        self.assertEqual(p["synonyms"], ["person"])
        self.assertIn("treated_by", p["relationships"])
        self.assertEqual(p["uri"], "http://hl7.org/fhir/Patient")


# ---------------------------------------------------------------------------
# _derive_keywords tests
# ---------------------------------------------------------------------------

class TestDeriveKeywords(unittest.TestCase):

    def test_camel_case_split(self):
        from scripts.build_ontology_indexes import _derive_keywords
        kw = _derive_keywords("AllergyIntolerance")
        self.assertIn("allergy", kw)
        self.assertIn("intolerance", kw)

    def test_snake_case_split(self):
        from scripts.build_ontology_indexes import _derive_keywords
        kw = _derive_keywords("VISIT_OCCURRENCE")
        self.assertIn("visit", kw)
        self.assertIn("occurrence", kw)

    def test_description_extraction(self):
        from scripts.build_ontology_indexes import _derive_keywords
        kw = _derive_keywords("Patient", "Demographics and administrative information about individuals")
        self.assertIn("patient", kw)
        self.assertIn("demographics", kw)
        self.assertIn("administrative", kw)

    def test_stopwords_excluded(self):
        from scripts.build_ontology_indexes import _derive_keywords
        kw = _derive_keywords("Thing", "A thing that is very important")
        self.assertNotIn("a", kw)
        self.assertNotIn("is", kw)
        self.assertNotIn("that", kw)

    def test_deduplication(self):
        from scripts.build_ontology_indexes import _derive_keywords
        kw = _derive_keywords("Drug", "A drug substance")
        self.assertEqual(len(kw), len(set(kw)))


# ---------------------------------------------------------------------------
# _emit_bundle_yaml tests
# ---------------------------------------------------------------------------

class TestEmitBundleYaml(unittest.TestCase):

    def test_generates_valid_bundle_structure(self):
        from scripts.build_ontology_indexes import _emit_bundle_yaml
        entities = {
            "Patient": {
                "description": "A patient",
                "source": "FHIR R4",
                "uri": "http://hl7.org/fhir/Patient",
                "keywords": ["patient"],
                "synonyms": [],
                "typical_attributes": ["id"],
                "business_questions": [],
                "relationships": {"has_encounter": {"target": "Encounter", "cardinality": "one-to-many"}},
                "properties": {},
                "outgoing_edges": [{"name": "has_encounter", "uri": "", "range": "Encounter", "inverse": None}],
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "test_formal.yaml"
            _emit_bundle_yaml(entities, out, "Test Bundle", "Test desc", "healthcare", "FHIR R4",
                              source_urls=["https://hl7.org/fhir/R4/fhir.ttl"])
            self.assertTrue(out.exists())
            raw = yaml.safe_load(out.read_text())
            self.assertEqual(raw["metadata"]["name"], "Test Bundle")
            self.assertEqual(raw["metadata"]["format_version"], "2.0")
            self.assertEqual(raw["metadata"]["bundle_type"], "formal_ontology")
            self.assertIn("source_url", raw["metadata"])
            defs = raw["ontology"]["entities"]["definitions"]
            self.assertIn("Patient", defs)
            self.assertEqual(defs["Patient"]["uri"], "http://hl7.org/fhir/Patient")
            self.assertEqual(defs["Patient"]["keywords"], ["patient"])
            self.assertIn("has_encounter", defs["Patient"]["relationships"])
            self.assertIn("has_encounter", raw["ontology"]["edge_catalog"])

    def test_derives_keywords_when_empty(self):
        from scripts.build_ontology_indexes import _emit_bundle_yaml
        entities = {
            "AllergyIntolerance": {
                "description": "Allergy records",
                "source": "FHIR R4",
                "uri": "http://hl7.org/fhir/AllergyIntolerance",
                "keywords": [],
                "synonyms": [],
                "typical_attributes": [],
                "business_questions": [],
                "relationships": {},
                "properties": {},
                "outgoing_edges": [],
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "test.yaml"
            _emit_bundle_yaml(entities, out, "T", "D", "healthcare", "FHIR R4")
            raw = yaml.safe_load(out.read_text())
            kw = raw["ontology"]["entities"]["definitions"]["AllergyIntolerance"]["keywords"]
            self.assertTrue(len(kw) > 0)
            self.assertIn("allergy", kw)


# ---------------------------------------------------------------------------
# Formal bundle end-to-end loading test
# ---------------------------------------------------------------------------

class TestFormalBundleE2E(unittest.TestCase):

    def test_generated_bundle_loads_via_ontology_loader(self):
        """Verify a generated formal bundle YAML is loadable by OntologyLoader."""
        fhir_yaml = Path(__file__).resolve().parent.parent / "configurations" / "ontology_bundles" / "fhir_r4.yaml"
        if not fhir_yaml.exists():
            self.skipTest("fhir_r4.yaml not generated yet")

        from dbxmetagen.ontology import OntologyLoader
        config = OntologyLoader.load_config(str(fhir_yaml))
        entities = OntologyLoader.get_entity_definitions(config)
        self.assertTrue(len(entities) >= 20, f"Expected >=20 entities, got {len(entities)}")
        patient = next((e for e in entities if e.name == "Patient"), None)
        self.assertIsNotNone(patient, "Patient entity not found")
        self.assertTrue(len(patient.keywords) > 0, "Patient should have keywords")
        self.assertIn("http://hl7.org/fhir/Patient", patient.uri or "")


class TestSchemaDomainEdgeExtraction(unittest.TestCase):
    """Validate that schema:domainIncludes/rangeIncludes edges are extracted."""

    def test_schema_org_has_edges(self):
        schema_tier1 = Path(__file__).resolve().parent.parent / "configurations" / "ontology_bundles" / "schema_org" / "edges_tier1.yaml"
        if not schema_tier1.exists():
            self.skipTest("schema_org tier files not generated yet")
        data = yaml.safe_load(schema_tier1.read_text())
        self.assertIsInstance(data, list)
        self.assertTrue(len(data) > 50, f"Expected >50 Schema.org edges, got {len(data)}")

    def test_schema_org_has_health_insurance_plan(self):
        schema_t1 = Path(__file__).resolve().parent.parent / "configurations" / "ontology_bundles" / "schema_org" / "entities_tier1.yaml"
        if not schema_t1.exists():
            self.skipTest("schema_org tier files not generated yet")
        data = yaml.safe_load(schema_t1.read_text())
        names = [e["name"] for e in data]
        self.assertIn("HealthInsurancePlan", names)
        self.assertGreaterEqual(len(data), 21)


class TestUnionDomainEdgeExtraction(unittest.TestCase):
    """Validate that owl:unionOf domain edges are extracted for OMOP."""

    def test_omop_has_more_edges_than_simple_domain(self):
        omop_edges = Path(__file__).resolve().parent.parent / "configurations" / "ontology_bundles" / "omop_cdm" / "edges_tier1.yaml"
        if not omop_edges.exists():
            self.skipTest("omop_cdm tier files not generated yet")
        data = yaml.safe_load(omop_edges.read_text())
        self.assertIsInstance(data, list)
        self.assertTrue(len(data) > 50, f"Expected >50 OMOP edges (union domains), got {len(data)}")


class TestPrefixFiltering(unittest.TestCase):
    """Validate that URI prefix filtering works correctly."""

    def test_extract_classes_filters_by_prefix(self):
        try:
            import rdflib
        except ImportError:
            self.skipTest("rdflib not installed")

        from scripts.build_ontology_indexes import _extract_classes

        g = rdflib.Graph()
        g.add((rdflib.URIRef("https://schema.org/Person"), rdflib.RDF.type, rdflib.RDFS.Class))
        g.add((rdflib.URIRef("https://schema.org/Person"), rdflib.RDFS.comment, rdflib.Literal("A person")))
        g.add((rdflib.URIRef("https://other.org/Person"), rdflib.RDF.type, rdflib.RDFS.Class))
        g.add((rdflib.URIRef("https://other.org/Person"), rdflib.RDFS.comment, rdflib.Literal("Other person")))

        entities = _extract_classes(g, {"Person"}, "Test", "https://schema.org/")
        self.assertEqual(len(entities), 1)
        self.assertEqual(entities["Person"]["uri"], "https://schema.org/Person")


class TestDiscoverNamedRelationshipsDelegation(unittest.TestCase):
    """Verify discover_named_relationships delegates to self.discoverer, not self."""

    def test_llm_edge_section_uses_discoverer_attributes(self):
        """The LLM-predicted edges section must read _validation_cfg and
        _get_index_loader from self.discoverer, not self."""
        import inspect  # noqa: E401
        from dbxmetagen.ontology import OntologyBuilder

        src = inspect.getsource(OntologyBuilder.discover_named_relationships)
        self.assertNotIn("self._validation_cfg", src,
                         "Should use discoverer._validation_cfg, not self._validation_cfg")
        self.assertNotIn("self._get_index_loader", src,
                         "Should use discoverer._get_index_loader, not self._get_index_loader")
        self.assertNotIn("self._make_llm_fn", src,
                         "_make_llm_fn never existed; should be inlined or delegated")

    def test_ontology_builder_has_no_validation_cfg(self):
        """OntologyBuilder should NOT have _validation_cfg as a class attribute."""
        from dbxmetagen.ontology import OntologyBuilder
        cls_attrs = [a for a in vars(OntologyBuilder) if not a.startswith("__")]
        self.assertNotIn("_validation_cfg", cls_attrs,
                         "OntologyBuilder should not define _validation_cfg")


# ---------------------------------------------------------------------------
# _enforce_entity_value fuzzy matching tests
# ---------------------------------------------------------------------------

class TestEnforceEntityValue(unittest.TestCase):

    def test_exact_match_case_insensitive(self):
        from dbxmetagen.ontology import _enforce_entity_value
        val, exact = _enforce_entity_value("Patient", ["Patient", "Encounter"])
        self.assertEqual(val, "Patient")
        self.assertTrue(exact)

    def test_exact_match_lowercase(self):
        from dbxmetagen.ontology import _enforce_entity_value
        val, exact = _enforce_entity_value("patient", ["Patient", "Encounter"])
        self.assertEqual(val, "Patient")
        self.assertTrue(exact)

    def test_fuzzy_match_typo(self):
        from dbxmetagen.ontology import _enforce_entity_value
        val, exact = _enforce_entity_value("CareePlan", ["CarePlan", "InsurancePlan", "Claim"])
        self.assertEqual(val, "CarePlan")
        self.assertFalse(exact)

    def test_fuzzy_match_close_name(self):
        from dbxmetagen.ontology import _enforce_entity_value
        val, exact = _enforce_entity_value("Patients", ["Patient", "Encounter", "Claim"])
        self.assertEqual(val, "Patient")
        self.assertFalse(exact)

    def test_ambiguous_substring_no_longer_matches_first(self):
        """Old substring logic would match "Plan" to "CarePlan" or "InsurancePlan"
        non-deterministically. Fuzzy matching should pick the closest."""
        from dbxmetagen.ontology import _enforce_entity_value
        val, exact = _enforce_entity_value("Plan", ["CarePlan", "InsurancePlan", "Medication"])
        self.assertFalse(exact)
        self.assertIn(val, ["CarePlan", "InsurancePlan", "DataTable"])

    def test_fallback_to_datatable(self):
        from dbxmetagen.ontology import _enforce_entity_value
        val, exact = _enforce_entity_value("CompletelyUnrelated", ["Patient", "Encounter"])
        self.assertEqual(val, "DataTable")
        self.assertFalse(exact)

    def test_custom_fallback(self):
        from dbxmetagen.ontology import _enforce_entity_value
        val, exact = _enforce_entity_value("xyz", ["A", "B"], fallback="Unknown")
        self.assertEqual(val, "Unknown")
        self.assertFalse(exact)


# ---------------------------------------------------------------------------
# Tier schema validation tests
# ---------------------------------------------------------------------------

class TestTierSchemaValidation(unittest.TestCase):

    def test_validate_valid_entities_tier1(self):
        from dbxmetagen.ontology_index import _validate_list_of_dicts, _ENTITIES_T1_REQUIRED
        data = [{"name": "Patient", "description": "A patient"}]
        valid, issues = _validate_list_of_dicts(data, _ENTITIES_T1_REQUIRED, "entities_tier1.yaml")
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(issues), 0)

    def test_validate_entities_tier1_filters_bad_entries(self):
        from dbxmetagen.ontology_index import _validate_list_of_dicts, _ENTITIES_T1_REQUIRED
        data = [
            {"name": "Good", "description": "ok"},
            {"name": "Bad"},  # missing description
            "not a dict",
        ]
        valid, issues = _validate_list_of_dicts(data, _ENTITIES_T1_REQUIRED, "entities_tier1.yaml")
        self.assertEqual(len(valid), 1)
        self.assertEqual(valid[0]["name"], "Good")
        self.assertEqual(len(issues), 2)

    def test_validate_entities_tier1_wrong_type(self):
        from dbxmetagen.ontology_index import _validate_list_of_dicts, _ENTITIES_T1_REQUIRED
        valid, issues = _validate_list_of_dicts("not a list", _ENTITIES_T1_REQUIRED, "entities_tier1.yaml")
        self.assertEqual(valid, [])
        self.assertEqual(len(issues), 1)

    def test_validate_dict_of_dicts_valid(self):
        from dbxmetagen.ontology_index import _validate_dict_of_dicts, _ENTITIES_T2_REQUIRED
        data = {"Patient": {"description": "d", "source_ontology": "FHIR", "uri": "http://x"}}
        valid, issues = _validate_dict_of_dicts(data, _ENTITIES_T2_REQUIRED, "entities_tier2.yaml")
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(issues), 0)

    def test_validate_dict_of_dicts_warns_missing_keys(self):
        from dbxmetagen.ontology_index import _validate_dict_of_dicts, _ENTITIES_T2_REQUIRED
        data = {"Patient": {"description": "d"}}  # missing source_ontology, uri
        valid, issues = _validate_dict_of_dicts(data, _ENTITIES_T2_REQUIRED, "entities_tier2.yaml")
        self.assertEqual(len(valid), 1)  # still included, just warned
        self.assertEqual(len(issues), 1)

    def test_validate_uri_map_valid(self):
        from dbxmetagen.ontology_index import _validate_uri_map
        data = {"Patient": "http://hl7.org/fhir/Patient"}
        valid, issues = _validate_uri_map(data, "uris.yaml")
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(issues), 0)

    def test_validate_uri_map_filters_non_string(self):
        from dbxmetagen.ontology_index import _validate_uri_map
        data = {"Patient": "http://ok", "Bad": 42}
        valid, issues = _validate_uri_map(data, "uris.yaml")
        self.assertEqual(len(valid), 1)
        self.assertNotIn("Bad", valid)
        self.assertEqual(len(issues), 1)

    def test_validate_bundle_on_real_bundle(self):
        from dbxmetagen.ontology_index import validate_bundle
        fhir_dir = Path(__file__).resolve().parent.parent / "configurations" / "ontology_bundles" / "fhir_r4"
        if not fhir_dir.exists():
            self.skipTest("fhir_r4 tier files not generated yet")
        issues = validate_bundle("fhir_r4")
        self.assertIsInstance(issues, list)

    def test_validate_bundle_on_broken_files(self):
        from dbxmetagen.ontology_index import validate_bundle
        tmpdir = tempfile.mkdtemp()
        with open(os.path.join(tmpdir, "entities_tier1.yaml"), "w") as f:
            f.write("not_a_list: true\n")
        with open(os.path.join(tmpdir, "equivalent_class_uris.yaml"), "w") as f:
            yaml.dump({"Patient": 42}, f)
        issues = validate_bundle("unused", base_dir=tmpdir)
        self.assertTrue(len(issues) >= 2)

    def test_loader_filters_invalid_tier1_entries(self):
        """OntologyIndexLoader._load should filter out malformed tier-1 entries."""
        from dbxmetagen.ontology_index import OntologyIndexLoader
        tmpdir = tempfile.mkdtemp()
        data = [
            {"name": "Good", "description": "ok"},
            {"name": "Bad"},
        ]
        with open(os.path.join(tmpdir, "entities_tier1.yaml"), "w") as f:
            yaml.dump(data, f)
        loader = OntologyIndexLoader(base_dir=tmpdir)
        result = loader.get_entities_tier1()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "Good")


# ---------------------------------------------------------------------------
# Prompt template correctness tests
# ---------------------------------------------------------------------------

class TestPromptTemplates(unittest.TestCase):

    def test_pass1_uses_description_not_sample(self):
        from dbxmetagen.ontology_predictor import PASS1_USER
        self.assertIn("Description:", PASS1_USER)
        self.assertNotIn("Sample (5 rows)", PASS1_USER)

    def test_pass2_uses_description_not_sample(self):
        from dbxmetagen.ontology_predictor import PASS2_USER
        self.assertIn("Description:", PASS2_USER)
        self.assertNotIn("Sample:", PASS2_USER.split("Description:")[0])

    def test_pass3_uses_description_not_sample(self):
        from dbxmetagen.ontology_predictor import PASS3_USER
        self.assertIn("Description:", PASS3_USER)
        self.assertNotIn("Sample:", PASS3_USER.split("Description:")[0])

    def test_all_templates_have_required_placeholders(self):
        from dbxmetagen.ontology_predictor import PASS1_USER, PASS2_USER, PASS3_USER
        for tmpl in [PASS1_USER, PASS2_USER, PASS3_USER]:
            self.assertIn("{table_name}", tmpl)
            self.assertIn("{columns}", tmpl)
            self.assertIn("{sample}", tmpl)


# ---------------------------------------------------------------------------
# Three-pass enforcement with tier names
# ---------------------------------------------------------------------------

class TestThreePassTierNameEnforcement(unittest.TestCase):
    """Verify _three_pass_classify_table enforces against tier-1 names."""

    def test_source_uses_tier1_union(self):
        """The enforcement in _three_pass_classify_table should use the union
        of self._entity_names and tier-1 entity names."""
        import inspect
        from dbxmetagen.ontology import EntityDiscoverer
        src = inspect.getsource(EntityDiscoverer._three_pass_classify_table)
        self.assertIn("tier1_names", src,
                      "Should build tier1_names from loader.get_entities_tier1()")
        self.assertIn("set(self._entity_names)", src,
                      "Should union with self._entity_names")

    def test_enforce_accepts_tier_only_name(self):
        """If an entity exists in tier-1 but not in bundle definitions,
        enforcement should accept it."""
        from dbxmetagen.ontology import _enforce_entity_value
        bundle_names = ["Patient", "Encounter"]
        tier_names = ["AllergyIntolerance", "ClinicalImpression"]
        allowed = list(set(bundle_names) | set(tier_names))
        val, exact = _enforce_entity_value("AllergyIntolerance", allowed)
        self.assertEqual(val, "AllergyIntolerance")
        self.assertTrue(exact)


# ---------------------------------------------------------------------------
# Error boundary and response validation tests
# ---------------------------------------------------------------------------

class TestSafeHelpers(unittest.TestCase):

    def test_safe_float_numeric(self):
        from dbxmetagen.ontology_predictor import _safe_float
        self.assertEqual(_safe_float(0.9), 0.9)
        self.assertEqual(_safe_float("0.75"), 0.75)

    def test_safe_float_non_numeric(self):
        from dbxmetagen.ontology_predictor import _safe_float
        self.assertEqual(_safe_float("high"), 0.0)
        self.assertEqual(_safe_float(None), 0.0)
        self.assertEqual(_safe_float("high", default=0.5), 0.5)

    def test_validate_pass1_valid(self):
        from dbxmetagen.ontology_predictor import _validate_pass1
        self.assertTrue(_validate_pass1({"top_candidates": ["Patient"]}))

    def test_validate_pass1_invalid(self):
        from dbxmetagen.ontology_predictor import _validate_pass1
        self.assertFalse(_validate_pass1({"top_candidates": "Patient"}))
        self.assertFalse(_validate_pass1({}))

    def test_validate_pass2_valid(self):
        from dbxmetagen.ontology_predictor import _validate_pass2
        self.assertTrue(_validate_pass2({"predicted_entity": "Patient", "confidence_score": 0.9}))

    def test_validate_pass2_missing_entity(self):
        from dbxmetagen.ontology_predictor import _validate_pass2
        self.assertFalse(_validate_pass2({"confidence_score": 0.9}))

    def test_safe_parse_response_valid_json(self):
        from dbxmetagen.ontology_predictor import _safe_parse_response
        llm = lambda s, u: '{"key": "val"}'
        result = _safe_parse_response(llm, "sys", "usr", "test")
        self.assertEqual(result, {"key": "val"})

    def test_safe_parse_response_bad_json(self):
        from dbxmetagen.ontology_predictor import _safe_parse_response
        llm = lambda s, u: "not json at all"
        result = _safe_parse_response(llm, "sys", "usr", "test")
        self.assertIsNone(result)

    def test_safe_parse_response_llm_exception(self):
        from dbxmetagen.ontology_predictor import _safe_parse_response
        def bad_llm(s, u):
            raise ConnectionError("network down")
        result = _safe_parse_response(bad_llm, "sys", "usr", "test")
        self.assertIsNone(result)


class TestPredictEntityErrorBoundaries(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        t1 = [{"name": "Patient", "description": "A patient"}, {"name": "Claim", "description": "Insurance claim"}]
        t2 = {"Patient": {"description": "A patient", "source_ontology": "FHIR R4", "uri": "http://hl7.org/fhir/Patient", "edges": []},
               "Claim": {"description": "Insurance claim", "source_ontology": "FHIR R4", "uri": "http://hl7.org/fhir/Claim", "edges": []}}
        t3 = {"Patient": {"description": "A patient", "source_ontology": "FHIR R4", "uri": "http://hl7.org/fhir/Patient",
                           "keywords": [], "synonyms": [], "typical_attributes": [], "business_questions": [], "relationships": {}, "properties": {}},
               "Claim": {"description": "Insurance claim", "source_ontology": "FHIR R4", "uri": "http://hl7.org/fhir/Claim",
                          "keywords": [], "synonyms": [], "typical_attributes": [], "business_questions": [], "relationships": {}, "properties": {}}}
        uris = {"Patient": "http://hl7.org/fhir/Patient", "Claim": "http://hl7.org/fhir/Claim"}
        for fname, data in [("entities_tier1.yaml", t1), ("entities_tier2.yaml", t2),
                            ("entities_tier3.yaml", t3), ("equivalent_class_uris.yaml", uris)]:
            with open(os.path.join(self.tmpdir, fname), "w") as f:
                yaml.dump(data, f)

    def _loader(self):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        return OntologyIndexLoader(base_dir=self.tmpdir)

    def test_pass1_malformed_json_returns_unknown(self):
        from dbxmetagen.ontology_predictor import predict_entity
        result = predict_entity("t", "c", "d", self._loader(), lambda s, u: "not json")
        self.assertEqual(result.predicted_entity, "Unknown")
        self.assertTrue(result.needs_human_review)
        self.assertEqual(result.passes_run, 1)

    def test_pass1_llm_exception_returns_unknown(self):
        from dbxmetagen.ontology_predictor import predict_entity
        def exploding_llm(s, u):
            raise RuntimeError("boom")
        result = predict_entity("t", "c", "d", self._loader(), exploding_llm)
        self.assertEqual(result.predicted_entity, "Unknown")
        self.assertTrue(result.needs_human_review)

    def test_pass2_malformed_returns_pass1_candidate(self):
        from dbxmetagen.ontology_predictor import predict_entity
        call_count = [0]
        def llm_fn(s, u):
            call_count[0] += 1
            if call_count[0] == 1:
                return json.dumps({"top_candidates": ["Patient"], "confidence": "high", "reasoning": "ok"})
            return "MALFORMED"
        result = predict_entity("t", "c", "d", self._loader(), llm_fn)
        self.assertEqual(result.predicted_entity, "Patient")
        self.assertTrue(result.needs_human_review)
        self.assertEqual(result.passes_run, 1)

    def test_pass3_malformed_returns_pass2_result(self):
        from dbxmetagen.ontology_predictor import predict_entity
        call_count = [0]
        def llm_fn(s, u):
            call_count[0] += 1
            if call_count[0] == 1:
                return json.dumps({"top_candidates": ["Patient", "Claim"], "confidence": "low", "reasoning": "unclear"})
            if call_count[0] == 2:
                return json.dumps({"predicted_entity": "Patient", "source_ontology": "FHIR R4",
                                   "confidence_score": 0.5, "rationale": "maybe", "needs_deep_pass": True})
            return "TOTALLY BROKEN"
        result = predict_entity("t", "c", "d", self._loader(), llm_fn)
        self.assertEqual(result.predicted_entity, "Patient")
        self.assertEqual(result.passes_run, 2)
        self.assertTrue(result.needs_human_review)

    def test_non_numeric_confidence_does_not_crash(self):
        from dbxmetagen.ontology_predictor import predict_entity
        call_count = [0]
        def llm_fn(s, u):
            call_count[0] += 1
            if call_count[0] == 1:
                return json.dumps({"top_candidates": ["Patient"], "confidence": "high", "reasoning": "ok"})
            return json.dumps({"predicted_entity": "Patient", "source_ontology": "FHIR R4",
                               "confidence_score": "very high", "rationale": "obvious", "needs_deep_pass": False})
        result = predict_entity("t", "c", "d", self._loader(), llm_fn)
        self.assertEqual(result.predicted_entity, "Patient")
        self.assertEqual(result.confidence_score, 0.0)


class TestPredictEdgeErrorBoundaries(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        edges_t1 = [{"name": "Patient.encounter", "domain": "Patient", "range": "Encounter"}]
        edges_t2 = {"Patient.encounter": {"domain": "Patient", "range": "Encounter", "description": "link"}}
        for fname, data in [("entities_tier1.yaml", [{"name": "Patient", "description": "p"}]),
                            ("edges_tier1.yaml", edges_t1), ("edges_tier2.yaml", edges_t2)]:
            with open(os.path.join(self.tmpdir, fname), "w") as f:
                yaml.dump(data, f)

    def _loader(self):
        from dbxmetagen.ontology_index import OntologyIndexLoader
        return OntologyIndexLoader(base_dir=self.tmpdir)

    def test_edge_pass1_failure_returns_default(self):
        from dbxmetagen.ontology_predictor import predict_edge
        result = predict_edge(
            src_entity="Patient", dst_entity="Encounter",
            loader=self._loader(), llm_fn=lambda s, u: "bad json",
            from_table="t1", to_table="t2",
        )
        self.assertEqual(result.predicted_edge, "references")
        self.assertEqual(result.confidence_score, 0.0)

    def test_edge_pass2_failure_returns_pass1_candidate(self):
        from dbxmetagen.ontology_predictor import predict_edge
        call_count = [0]
        def llm_fn(s, u):
            call_count[0] += 1
            if call_count[0] == 1:
                return json.dumps({"top_candidates": ["Patient.encounter"], "confidence": "high", "reasoning": "ok"})
            return "BROKEN"
        result = predict_edge(
            src_entity="Patient", dst_entity="Encounter",
            loader=self._loader(), llm_fn=llm_fn,
            from_table="t1", to_table="t2",
        )
        self.assertEqual(result.predicted_edge, "Patient.encounter")
        self.assertEqual(result.confidence_score, 0.3)
        self.assertEqual(result.passes_run, 1)


class TestAiClassifyTableFallback(unittest.TestCase):
    """Verify _ai_classify_table falls back to single-pass when three-pass raises."""

    def test_three_pass_exception_falls_through(self):
        import inspect
        from dbxmetagen.ontology import EntityDiscoverer
        src = inspect.getsource(EntityDiscoverer._ai_classify_table)
        self.assertIn("except Exception", src,
                      "_ai_classify_table should catch three-pass exceptions")
        self.assertIn("falling back to single-pass", src,
                      "Should log a fallback message")


if __name__ == "__main__":
    unittest.main()
