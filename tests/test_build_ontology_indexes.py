"""Unit tests for scripts/build_ontology_indexes.py using synthetic rdflib graphs."""
from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any, Dict

import pytest
import yaml

try:
    import rdflib
    from rdflib import OWL, RDF, RDFS, BNode, Graph, Literal, Namespace, URIRef
    from rdflib.collection import Collection

    HAS_RDFLIB = True
    NS = Namespace("http://test.org/")
except ImportError:
    HAS_RDFLIB = False
    NS = None  # type: ignore[assignment]

pytestmark = pytest.mark.skipif(not HAS_RDFLIB, reason="rdflib not installed")

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from build_ontology_indexes import (
    FHIR_CONFORMANCE_SKIP,
    OMOP_ABSTRACT_SKIP,
    _derive_keywords,
    _discover_fhir_resources,
    _discover_omop_classes,
    _discover_schema_org_classes,
    _extract_classes,
    _extract_schema_domain_edges,
    _extract_single_class,
    _extract_union_domain_edges,
    _get_comment,
    _get_label,
    _resolve_cardinality,
)
from dbxmetagen.ontology_bundle_indexes import build_tiers
from dbxmetagen.ontology_index import OntologyIndexLoader, validate_bundle


def _make_basic_graph() -> Graph:
    """Graph with two classes, one object property, one datatype property, one restriction."""
    g = Graph()
    g.add((NS.Patient, RDF.type, OWL.Class))
    g.add((NS.Patient, RDFS.comment, Literal("A person receiving healthcare services")))

    g.add((NS.Encounter, RDF.type, OWL.Class))
    g.add((NS.Encounter, RDFS.comment, Literal("An interaction between a patient and healthcare provider")))
    g.add((NS.Encounter, RDFS.subClassOf, NS.Patient))

    # Object property: Encounter -> Patient
    g.add((NS.hasPatient, RDF.type, OWL.ObjectProperty))
    g.add((NS.hasPatient, RDFS.domain, NS.Encounter))
    g.add((NS.hasPatient, RDFS.range, NS.Patient))
    g.add((NS.hasPatient, OWL.inverseOf, NS.encounterFor))

    # Datatype property: Patient.birthDate
    g.add((NS.birthDate, RDF.type, OWL.DatatypeProperty))
    g.add((NS.birthDate, RDFS.domain, NS.Patient))

    # Datatype property: Patient.gender
    g.add((NS.gender, RDF.type, OWL.DatatypeProperty))
    g.add((NS.gender, RDFS.domain, NS.Patient))

    # Restriction: Patient has exactly 1 birthDate
    restriction = BNode()
    g.add((NS.Patient, RDFS.subClassOf, restriction))
    g.add((restriction, OWL.onProperty, NS.birthDate))
    g.add((restriction, OWL.cardinality, Literal(1)))

    return g


def _make_schema_org_graph() -> Graph:
    """Graph mimicking Schema.org style with domainIncludes/rangeIncludes."""
    g = Graph()
    SCHEMA = Namespace("https://schema.org/")
    SCHEMA_DOMAIN = URIRef("https://schema.org/domainIncludes")
    SCHEMA_RANGE = URIRef("https://schema.org/rangeIncludes")

    g.add((SCHEMA.Person, RDF.type, RDFS.Class))
    g.add((SCHEMA.Person, RDFS.comment, Literal("A person alive, dead, undead, or fictional")))

    g.add((SCHEMA.Organization, RDF.type, RDFS.Class))
    g.add((SCHEMA.Organization, RDFS.comment, Literal("An organization such as a company")))

    # Object property: Person -> Organization
    g.add((SCHEMA.worksFor, SCHEMA_DOMAIN, SCHEMA.Person))
    g.add((SCHEMA.worksFor, SCHEMA_RANGE, SCHEMA.Organization))

    # Data property: Person -> Text (should become typical_attribute)
    g.add((SCHEMA.givenName, SCHEMA_DOMAIN, SCHEMA.Person))
    g.add((SCHEMA.givenName, SCHEMA_RANGE, SCHEMA.Text))

    g.add((SCHEMA.familyName, SCHEMA_DOMAIN, SCHEMA.Person))
    g.add((SCHEMA.familyName, SCHEMA_RANGE, SCHEMA.Text))

    # Data property: Person -> Date
    g.add((SCHEMA.birthDate, SCHEMA_DOMAIN, SCHEMA.Person))
    g.add((SCHEMA.birthDate, SCHEMA_RANGE, SCHEMA.Date))

    return g


def _make_union_domain_graph() -> Graph:
    """Graph mimicking OMOP CDM style with owl:unionOf domains."""
    g = Graph()

    g.add((NS.Person, RDF.type, OWL.Class))
    g.add((NS.Person, RDFS.comment, Literal("A person in the dataset")))

    g.add((NS.VisitOccurrence, RDF.type, OWL.Class))
    g.add((NS.VisitOccurrence, RDFS.comment, Literal("A visit occurrence")))

    g.add((NS.Concept, RDF.type, OWL.Class))

    # Union domain property: has_visit applies to Person and VisitOccurrence
    union_node = BNode()
    domain_node = BNode()
    Collection(g, union_node, [NS.Person, NS.VisitOccurrence])
    g.add((domain_node, OWL.unionOf, union_node))
    g.add((NS.has_visit, RDF.type, OWL.ObjectProperty))
    g.add((NS.has_visit, RDFS.domain, domain_node))
    g.add((NS.has_visit, RDFS.range, NS.VisitOccurrence))
    g.add((NS.has_visit, OWL.inverseOf, NS.visit_of))

    return g


# ---------------------------------------------------------------------------
# Tests: _get_comment
# ---------------------------------------------------------------------------


class TestGetComment:
    def test_returns_comment_up_to_300(self):
        g = Graph()
        long_text = "A" * 500
        g.add((NS.Foo, RDFS.comment, Literal(long_text)))
        result = _get_comment(g, NS.Foo, RDFS)
        assert len(result) == 300

    def test_returns_empty_when_no_comment(self):
        g = Graph()
        assert _get_comment(g, NS.Foo, RDFS) == ""

    def test_custom_limit(self):
        g = Graph()
        g.add((NS.Foo, RDFS.comment, Literal("Hello world")))
        assert _get_comment(g, NS.Foo, RDFS, limit=5) == "Hello"


# ---------------------------------------------------------------------------
# Tests: _derive_keywords
# ---------------------------------------------------------------------------


class TestDeriveKeywords:
    def test_camel_case_split(self):
        kw = _derive_keywords("MedicationRequest")
        assert "medication" in kw
        assert "request" in kw

    def test_underscore_split(self):
        kw = _derive_keywords("visit_occurrence")
        assert "visit" in kw
        assert "occurrence" in kw

    def test_description_tokens_added(self):
        kw = _derive_keywords("Foo", "contains important clinical data")
        assert "contains" in kw
        assert "important" in kw

    def test_stopwords_excluded(self):
        kw = _derive_keywords("X", "the quick brown fox")
        assert "the" not in kw
        assert "quick" in kw

    def test_max_keywords(self):
        kw = _derive_keywords("A", "word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11")
        assert len(kw) <= 10


# ---------------------------------------------------------------------------
# Tests: _extract_single_class (data properties, cardinality, parents, ranges)
# ---------------------------------------------------------------------------


class TestExtractSingleClass:
    def test_extracts_data_properties_as_typical_attributes(self):
        g = _make_basic_graph()
        coi = {"Patient", "Encounter"}
        entities: Dict[str, Any] = {}
        _extract_single_class(g, NS.Patient, coi, "Test", entities, OWL, RDF, RDFS)

        assert "Patient" in entities
        attrs = entities["Patient"]["typical_attributes"]
        assert "birthDate" in attrs
        assert "gender" in attrs

    def test_extracts_object_property_edges(self):
        g = _make_basic_graph()
        coi = {"Patient", "Encounter"}
        entities: Dict[str, Any] = {}
        _extract_single_class(g, NS.Encounter, coi, "Test", entities, OWL, RDF, RDFS)

        assert "Encounter" in entities
        edge_names = [e["name"] for e in entities["Encounter"]["outgoing_edges"]]
        assert "hasPatient" in edge_names

    def test_stores_parents_as_list(self):
        g = _make_basic_graph()
        coi = {"Patient", "Encounter"}
        entities: Dict[str, Any] = {}
        _extract_single_class(g, NS.Encounter, coi, "Test", entities, OWL, RDF, RDFS)

        assert entities["Encounter"]["parents"] == ["Patient"]

    def test_stores_ranges_list_on_edges(self):
        g = _make_basic_graph()
        coi = {"Patient", "Encounter"}
        entities: Dict[str, Any] = {}
        _extract_single_class(g, NS.Encounter, coi, "Test", entities, OWL, RDF, RDFS)

        edge = entities["Encounter"]["outgoing_edges"][0]
        assert "ranges" in edge
        assert edge["range"] == "Patient"
        assert "Patient" in edge["ranges"]

    def test_description_not_truncated_to_120(self):
        g = Graph()
        long_comment = "A" * 250
        g.add((NS.Foo, RDF.type, OWL.Class))
        g.add((NS.Foo, RDFS.comment, Literal(long_comment)))
        entities: Dict[str, Any] = {}
        _extract_single_class(g, NS.Foo, {"Foo"}, "Test", entities, OWL, RDF, RDFS)
        assert len(entities["Foo"]["description"]) == 250


# ---------------------------------------------------------------------------
# Tests: _resolve_cardinality
# ---------------------------------------------------------------------------


class TestResolveCardinality:
    def test_exact_cardinality(self):
        g = _make_basic_graph()
        card = _resolve_cardinality(g, NS.Patient, OWL, RDFS)
        assert card.get("birthDate") == "one-to-one"

    def test_min_max_cardinality(self):
        g = Graph()
        g.add((NS.Foo, RDF.type, OWL.Class))
        restriction = BNode()
        g.add((NS.Foo, RDFS.subClassOf, restriction))
        g.add((restriction, OWL.onProperty, NS.items))
        g.add((restriction, OWL.minCardinality, Literal(0)))
        g.add((restriction, OWL.maxCardinality, Literal(100)))
        card = _resolve_cardinality(g, NS.Foo, OWL, RDFS)
        assert card.get("items") == "one-to-many"

    def test_max_one_is_one_to_one(self):
        g = Graph()
        g.add((NS.Foo, RDF.type, OWL.Class))
        restriction = BNode()
        g.add((NS.Foo, RDFS.subClassOf, restriction))
        g.add((restriction, OWL.onProperty, NS.single))
        g.add((restriction, OWL.maxCardinality, Literal(1)))
        card = _resolve_cardinality(g, NS.Foo, OWL, RDFS)
        assert card.get("single") == "one-to-one"

    def test_no_restrictions_returns_empty(self):
        g = Graph()
        g.add((NS.Foo, RDF.type, OWL.Class))
        card = _resolve_cardinality(g, NS.Foo, OWL, RDFS)
        assert card == {}


# ---------------------------------------------------------------------------
# Tests: _extract_classes (integration of all extractors)
# ---------------------------------------------------------------------------


class TestExtractClasses:
    def test_basic_graph_extraction(self):
        g = _make_basic_graph()
        entities = _extract_classes(g, {"Patient", "Encounter"}, "Test", "http://test.org/")
        assert "Patient" in entities
        assert "Encounter" in entities
        assert "birthDate" in entities["Patient"]["typical_attributes"]

    def test_cardinality_propagated(self):
        g = _make_basic_graph()
        entities = _extract_classes(g, {"Patient", "Encounter"}, "Test", "http://test.org/")
        edge = entities["Encounter"]["outgoing_edges"][0]
        assert edge["name"] == "hasPatient"
        assert entities["Encounter"]["relationships"]["hasPatient"]["target"] == "Patient"


# ---------------------------------------------------------------------------
# Tests: _extract_schema_domain_edges (Schema.org style)
# ---------------------------------------------------------------------------


class TestExtractSchemaDomainEdges:
    def test_object_edges_extracted(self):
        g = _make_schema_org_graph()
        coi = {"Person", "Organization"}
        entities = _extract_classes(g, coi, "Schema.org", "https://schema.org/")

        assert "Person" in entities
        rels = entities["Person"]["relationships"]
        assert "worksFor" in rels
        assert rels["worksFor"]["target"] == "Organization"

    def test_data_properties_become_typical_attributes(self):
        g = _make_schema_org_graph()
        coi = {"Person", "Organization"}
        entities = _extract_classes(g, coi, "Schema.org", "https://schema.org/")

        attrs = entities["Person"]["typical_attributes"]
        assert "givenName" in attrs
        assert "familyName" in attrs
        assert "birthDate" in attrs


# ---------------------------------------------------------------------------
# Tests: _extract_union_domain_edges (OMOP CDM style)
# ---------------------------------------------------------------------------


class TestExtractUnionDomainEdges:
    def test_union_edges_applied_to_all_members(self):
        g = _make_union_domain_graph()
        coi = {"Person", "VisitOccurrence", "Concept"}
        entities = _extract_classes(g, coi, "OMOP", "http://test.org/")

        person_edges = [e["name"] for e in entities["Person"]["outgoing_edges"]]
        visit_edges = [e["name"] for e in entities["VisitOccurrence"]["outgoing_edges"]]
        assert "has_visit" in person_edges
        assert "has_visit" in visit_edges

    def test_inverse_captured(self):
        g = _make_union_domain_graph()
        coi = {"Person", "VisitOccurrence", "Concept"}
        entities = _extract_classes(g, coi, "OMOP", "http://test.org/")

        edge = [e for e in entities["Person"]["outgoing_edges"] if e["name"] == "has_visit"][0]
        assert edge["inverse"] == "visit_of"


# ---------------------------------------------------------------------------
# Tests: build_tiers (round-trip validation)
# ---------------------------------------------------------------------------


class TestBuildTiers:
    def test_tier_files_written_and_loadable(self):
        g = _make_basic_graph()
        entities = _extract_classes(g, {"Patient", "Encounter"}, "Test", "http://test.org/")

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "test_bundle"
            counts = build_tiers(entities, output)

            assert counts["entities_tier1"] == 2
            assert counts["entities_tier3"] == 2

            t1 = yaml.safe_load((output / "entities_tier1.yaml").read_text())
            assert len(t1) == 2

            t3 = yaml.safe_load((output / "entities_tier3.yaml").read_text())
            assert "Patient" in t3
            assert t3["Patient"]["typical_attributes"]
            assert t3["Patient"]["parents"] == []

            t3_enc = t3["Encounter"]
            assert t3_enc["parents"] == ["Patient"]

    def test_tier_description_lengths(self):
        g = Graph()
        g.add((NS.Foo, RDF.type, OWL.Class))
        g.add((NS.Foo, RDFS.comment, Literal("A" * 400)))
        entities = _extract_classes(g, {"Foo"}, "Test", "http://test.org/")

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "test"
            build_tiers(entities, output)

            t1 = yaml.safe_load((output / "entities_tier1.yaml").read_text())
            assert len(t1[0]["description"]) <= 200

            t3 = yaml.safe_load((output / "entities_tier3.yaml").read_text())
            assert len(t3["Foo"]["description"]) <= 300

    def test_edges_tier2_has_ranges(self):
        g = _make_basic_graph()
        entities = _extract_classes(g, {"Patient", "Encounter"}, "Test", "http://test.org/")

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "test"
            build_tiers(entities, output)

            et2 = yaml.safe_load((output / "edges_tier2.yaml").read_text())
            for _, v in et2.items():
                assert "ranges" in v

    def test_passes_ontology_index_validation(self):
        g = _make_basic_graph()
        entities = _extract_classes(g, {"Patient", "Encounter"}, "Test", "http://test.org/")

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "test_bundle"
            build_tiers(entities, output)
            issues = validate_bundle("test_bundle", base_dir=str(output))
            assert issues == [], f"Validation issues: {issues}"

    def test_loader_round_trip(self):
        g = _make_basic_graph()
        entities = _extract_classes(g, {"Patient", "Encounter"}, "Test", "http://test.org/")

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "test_bundle"
            build_tiers(entities, output)

            loader = OntologyIndexLoader("test_bundle", base_dir=str(output))
            assert loader.has_tier_indexes
            t1 = loader.get_entities_tier1()
            assert len(t1) == 2

            t2 = loader.get_entities_tier2_scoped(["Patient"])
            assert "Patient" in t2
            assert "parents" in t2["Patient"]

            t3 = loader.get_entities_tier3_scoped(["Patient"])
            assert t3["Patient"]["typical_attributes"]


# ---------------------------------------------------------------------------
# Tests for auto-discovery helpers
# ---------------------------------------------------------------------------

if HAS_RDFLIB:
    FHIR_NS = Namespace("http://hl7.org/fhir/")

    def _make_fhir_graph() -> Graph:
        """Minimal FHIR-like graph with Resource -> DomainResource -> concrete resources."""
        g = Graph()
        g.add((FHIR_NS.Resource, RDF.type, OWL.Class))
        g.add((FHIR_NS.DomainResource, RDF.type, OWL.Class))
        g.add((FHIR_NS.DomainResource, RDFS.subClassOf, FHIR_NS.Resource))
        # Clinical resources
        for name in ("Patient", "Encounter", "Observation", "Condition"):
            uri = FHIR_NS[name]
            g.add((uri, RDF.type, OWL.Class))
            g.add((uri, RDFS.subClassOf, FHIR_NS.DomainResource))
        # Conformance resource that should be skipped
        g.add((FHIR_NS.CapabilityStatement, RDF.type, OWL.Class))
        g.add((FHIR_NS.CapabilityStatement, RDFS.subClassOf, FHIR_NS.DomainResource))
        # Backbone component (dotted name) that should be skipped
        uri_bb = URIRef("http://hl7.org/fhir/Patient.contact")
        g.add((uri_bb, RDF.type, OWL.Class))
        g.add((uri_bb, RDFS.subClassOf, FHIR_NS.DomainResource))
        # Data type (lowercase) that should be skipped
        g.add((FHIR_NS.string, RDF.type, OWL.Class))
        return g

    OMOP_NS = Namespace("https://w3id.org/omop/ontology/")

    def _make_omop_graph() -> Graph:
        g = Graph()
        for name in ("Person", "VisitOccurrence", "DrugExposure"):
            g.add((OMOP_NS[name], RDF.type, OWL.Class))
        for abstract in ("OmopCDMThing", "ClinicalElement", "Exposure"):
            g.add((OMOP_NS[abstract], RDF.type, OWL.Class))
        return g

    SCHEMA_NS = Namespace("https://schema.org/")

    def _make_schema_graph() -> Graph:
        g = Graph()
        for name in ("Person", "Organization", "MedicalCondition", "Hospital", "Thing"):
            g.add((SCHEMA_NS[name], RDF.type, RDFS.Class))
        return g


class TestDiscoverFhirResources:
    def test_finds_clinical_resources(self):
        g = _make_fhir_graph()
        result = _discover_fhir_resources(g, "http://hl7.org/fhir/")
        for name in ("Patient", "Encounter", "Observation", "Condition"):
            assert name in result, f"{name} should be discovered"

    def test_skips_conformance(self):
        g = _make_fhir_graph()
        result = _discover_fhir_resources(g, "http://hl7.org/fhir/")
        assert "CapabilityStatement" not in result

    def test_skips_backbone_and_datatypes(self):
        g = _make_fhir_graph()
        result = _discover_fhir_resources(g, "http://hl7.org/fhir/")
        assert "Patient.contact" not in result
        assert "string" not in result

    def test_skips_abstract_base_types(self):
        g = _make_fhir_graph()
        result = _discover_fhir_resources(g, "http://hl7.org/fhir/")
        assert "DomainResource" not in result
        assert "Resource" not in result


class TestDiscoverOmopClasses:
    def test_finds_concrete_classes(self):
        g = _make_omop_graph()
        result = _discover_omop_classes(g, "https://w3id.org/omop/ontology/")
        assert result == {"Person", "VisitOccurrence", "DrugExposure"}

    def test_skips_abstract_classes(self):
        g = _make_omop_graph()
        result = _discover_omop_classes(g, "https://w3id.org/omop/ontology/")
        for abstract in ("OmopCDMThing", "ClinicalElement", "Exposure"):
            assert abstract not in result


class TestDiscoverSchemaOrgClasses:
    def test_returns_all_classes(self):
        g = _make_schema_graph()
        result = _discover_schema_org_classes(g, "https://schema.org/")
        assert result == {"Person", "Organization", "MedicalCondition", "Hospital", "Thing"}


class TestGetLabel:
    def test_rdfs_label(self):
        g = Graph()
        cls_uri = NS["Patient"]
        g.add((cls_uri, RDFS.label, Literal("Patient Resource")))
        assert _get_label(g, cls_uri, RDFS) == "Patient Resource"

    def test_dc_title_fallback(self):
        g = Graph()
        cls_uri = NS["Patient"]
        DC_TITLE = URIRef("http://purl.org/dc/elements/1.1/title")
        g.add((cls_uri, DC_TITLE, Literal("Patient Title")))
        assert _get_label(g, cls_uri, RDFS) == "Patient Title"

    def test_camel_case_fallback(self):
        g = Graph()
        cls_uri = NS["AllergyIntolerance"]
        assert _get_label(g, cls_uri, RDFS) == "Allergy Intolerance"


class TestResolveCardinalityExtended:
    """Tests for someValuesFrom / allValuesFrom cardinality mapping."""

    def test_some_values_from_maps_to_one_to_many(self):
        g = Graph()
        cls = NS["Patient"]
        prop = NS["hasEncounter"]
        restriction = BNode()
        g.add((cls, RDFS.subClassOf, restriction))
        g.add((restriction, OWL.onProperty, prop))
        g.add((restriction, OWL.someValuesFrom, NS["Encounter"]))
        result = _resolve_cardinality(g, cls, OWL, RDFS)
        assert result["hasEncounter"] == "one-to-many"

    def test_all_values_from_maps_to_zero_to_many(self):
        g = Graph()
        cls = NS["Patient"]
        prop = NS["hasAddress"]
        restriction = BNode()
        g.add((cls, RDFS.subClassOf, restriction))
        g.add((restriction, OWL.onProperty, prop))
        g.add((restriction, OWL.allValuesFrom, NS["Address"]))
        result = _resolve_cardinality(g, cls, OWL, RDFS)
        assert result["hasAddress"] == "zero-to-many"

    def test_explicit_cardinality_overrides_some_values_from(self):
        g = Graph()
        cls = NS["Patient"]
        prop = NS["hasIdentifier"]
        r1 = BNode()
        g.add((cls, RDFS.subClassOf, r1))
        g.add((r1, OWL.onProperty, prop))
        g.add((r1, OWL.maxCardinality, Literal(1)))
        r2 = BNode()
        g.add((cls, RDFS.subClassOf, r2))
        g.add((r2, OWL.onProperty, prop))
        g.add((r2, OWL.someValuesFrom, NS["Identifier"]))
        result = _resolve_cardinality(g, cls, OWL, RDFS)
        assert result["hasIdentifier"] == "one-to-one"


class TestExtractSingleClassLabel:
    """Verify _extract_single_class populates label from rdfs:label."""

    def test_label_extracted(self):
        g = Graph()
        cls = NS["Patient"]
        g.add((cls, RDF.type, OWL.Class))
        g.add((cls, RDFS.comment, Literal("A patient record")))
        g.add((cls, RDFS.label, Literal("Patient Resource")))

        entities = {}
        _extract_single_class(g, cls, {"Patient"}, "test", entities, OWL, RDF, RDFS)
        assert entities["Patient"]["label"] == "Patient Resource"


class TestPropertyLabelExtraction:
    """Verify _extract_single_class extracts labels on object properties."""

    def test_property_label_from_rdfs_label(self):
        g = Graph()
        g.add((NS.Encounter, RDF.type, OWL.Class))
        g.add((NS.Encounter, RDFS.comment, Literal("An encounter")))
        g.add((NS.Patient, RDF.type, OWL.Class))
        g.add((NS.subject, RDF.type, OWL.ObjectProperty))
        g.add((NS.subject, RDFS.domain, NS.Encounter))
        g.add((NS.subject, RDFS.range, NS.Patient))
        g.add((NS.subject, RDFS.label, Literal("Subject")))

        entities: Dict[str, Any] = {}
        _extract_single_class(g, NS.Encounter, {"Encounter", "Patient"}, "Test", entities, OWL, RDF, RDFS)
        edge = entities["Encounter"]["outgoing_edges"][0]
        assert edge["label"] == "Subject"

    def test_property_label_camelcase_fallback(self):
        g = Graph()
        g.add((NS.Encounter, RDF.type, OWL.Class))
        g.add((NS.Encounter, RDFS.comment, Literal("An encounter")))
        g.add((NS.Patient, RDF.type, OWL.Class))
        g.add((NS.managingOrganization, RDF.type, OWL.ObjectProperty))
        g.add((NS.managingOrganization, RDFS.domain, NS.Encounter))
        g.add((NS.managingOrganization, RDFS.range, NS.Patient))

        entities: Dict[str, Any] = {}
        _extract_single_class(g, NS.Encounter, {"Encounter", "Patient"}, "Test", entities, OWL, RDF, RDFS)
        edge = entities["Encounter"]["outgoing_edges"][0]
        assert edge["label"] == "managing Organization"


class TestW5FacetExtraction:
    """Verify _extract_single_class extracts rdfs:subPropertyOf and derives W5 facets."""

    def test_w5_facet_derived(self):
        g = Graph()
        g.add((NS.Encounter, RDF.type, OWL.Class))
        g.add((NS.Encounter, RDFS.comment, Literal("An encounter")))
        g.add((NS.Patient, RDF.type, OWL.Class))
        W5 = Namespace("http://hl7.org/fhir/w5#")
        g.add((NS.subject, RDF.type, OWL.ObjectProperty))
        g.add((NS.subject, RDFS.domain, NS.Encounter))
        g.add((NS.subject, RDFS.range, NS.Patient))
        g.add((NS.subject, RDFS.subPropertyOf, W5["who.focus"]))

        entities: Dict[str, Any] = {}
        _extract_single_class(g, NS.Encounter, {"Encounter", "Patient"}, "Test", entities, OWL, RDF, RDFS)
        edge = entities["Encounter"]["outgoing_edges"][0]
        assert edge["facet"] == "who"
        assert "who.focus" in edge["sub_property_of"]

    def test_no_facet_when_no_sub_property(self):
        g = _make_basic_graph()
        entities: Dict[str, Any] = {}
        _extract_single_class(g, NS.Encounter, {"Encounter", "Patient"}, "Test", entities, OWL, RDF, RDFS)
        edge = entities["Encounter"]["outgoing_edges"][0]
        assert edge["facet"] is None
        assert edge["sub_property_of"] is None

    def test_union_domain_edges_get_label_and_facet(self):
        g = _make_union_domain_graph()
        g.add((NS.has_visit, RDFS.label, Literal("Has Visit")))
        coi = {"Person", "VisitOccurrence", "Concept"}
        entities = _extract_classes(g, coi, "OMOP", "http://test.org/")
        edge = [e for e in entities["Person"]["outgoing_edges"] if e["name"] == "has_visit"][0]
        assert edge["label"] == "Has Visit"
        assert edge["facet"] is None


class TestFullPipelineRoundTrip:
    """North-star test: OWL extraction -> tier files -> loader must preserve semantics.

    This exercises the complete pipeline from an rdflib graph through
    _extract_classes -> build_tiers -> OntologyIndexLoader, verifying that
    property labels, W5 facets, cardinality, and sub_property_of survive
    every stage without being silently dropped.
    """

    def test_extraction_to_tier_roundtrip_preserves_edge_semantics(self):
        """Property label, facet, cardinality, ranges, and sub_property_of survive
        extract -> build_tiers -> OntologyIndexLoader load."""
        import json as _json

        g = Graph()
        g.add((NS.Encounter, RDF.type, OWL.Class))
        g.add((NS.Encounter, RDFS.comment, Literal("An encounter")))
        g.add((NS.Patient, RDF.type, OWL.Class))
        g.add((NS.Patient, RDFS.comment, Literal("A patient")))

        g.add((NS.subject, RDF.type, OWL.ObjectProperty))
        g.add((NS.subject, RDFS.domain, NS.Encounter))
        g.add((NS.subject, RDFS.range, NS.Patient))
        g.add((NS.subject, RDFS.label, Literal("Subject")))

        W5 = Namespace("http://hl7.org/fhir/w5#")
        g.add((NS.subject, RDFS.subPropertyOf, W5["who.focus"]))

        restriction = BNode()
        g.add((NS.Encounter, RDFS.subClassOf, restriction))
        g.add((restriction, OWL.onProperty, NS.subject))
        g.add((restriction, OWL.someValuesFrom, NS.Patient))

        entities = _extract_classes(g, {"Encounter", "Patient"}, "Test", "http://test.org/")

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "test_bundle"
            build_tiers(entities, output)

            loader = OntologyIndexLoader("test_bundle", base_dir=str(output))
            assert loader.has_tier_indexes

            t1_edges = loader.get_edges_tier1()
            edge = [e for e in t1_edges if e["name"] == "subject"][0]
            assert edge["label"] == "Subject"
            assert edge["facet"] == "who"
            assert edge["cardinality"] == "one-to-many"

            t2_raw = loader._load("edges_tier2.yaml")
            assert t2_raw is not None
            t2_edge = t2_raw["subject"]
            assert t2_edge["label"] == "Subject"
            assert t2_edge["sub_property_of"] == ["who.focus"]
            assert t2_edge["facet"] == "who"
            assert "Patient" in t2_edge.get("ranges", [])


