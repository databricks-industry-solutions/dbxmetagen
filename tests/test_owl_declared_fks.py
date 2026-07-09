"""Tests for OWL declared constraint extraction and resolution."""

import os
import tempfile
import textwrap

import pytest

from dbxmetagen.owl_constraints import (
    apply_uri_property_overrides,
    dedupe_string_map,
    extract_declared_constraints,
    is_uri_key_property,
    is_uri_parent_property,
    merge_owl_foreign_keys,
    owl_fk_managed_columns_for_table,
    resolve_declared_constraints_to_tables,
)


OWL_URI_PARENT_SNIPPET = textwrap.dedent("""
    @prefix : <http://example.com/ontologies/demo/pharma#> .
    @prefix owl: <http://www.w3.org/2002/07/owl#> .
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

    :Target a owl:Class .
    :TargetTrainprogram a owl:Class .
    :DiseaseHierarchy a owl:Class .
    :ProjectDiseaseRolledup a owl:Class .

    :targetId a owl:ObjectProperty ;
        rdfs:domain :TargetTrainprogram ;
        rdfs:range :Target .

    :diseaseUri a owl:DatatypeProperty ;
        rdfs:domain :DiseaseHierarchy ;
        rdfs:range xsd:string .

    :diseaseUri a owl:DatatypeProperty ;
        rdfs:domain :ProjectDiseaseRolledup ;
        rdfs:range xsd:string .

    :diseaseUriParent a owl:DatatypeProperty ;
        rdfs:domain :ProjectDiseaseRolledup ;
        rdfs:range xsd:string ;
        rdfs:comment "URI of parent disease for hierarchy traversal" .
""")


class TestUriPropertyDetection:
    def test_uri_key(self):
        assert is_uri_key_property("diseaseUri")
        assert not is_uri_key_property("diseaseUriParent")

    def test_uri_parent(self):
        assert is_uri_parent_property("diseaseUriParent")
        assert not is_uri_parent_property("diseaseUri")


class TestOwlImportDeclaredConstraints:
    def _load_bundle(self):
        pytest.importorskip("rdflib")
        from dbxmetagen.ontology_import import owl_to_bundle_yaml

        with tempfile.NamedTemporaryFile("w", suffix=".ttl", delete=False) as f:
            f.write(OWL_URI_PARENT_SNIPPET)
            path = f.name
        try:
            return owl_to_bundle_yaml(path, bundle_name="uri_parent_bundle_test")
        finally:
            os.unlink(path)

    def test_bundle_has_declared_constraints(self):
        bundle = self._load_bundle()
        dc = bundle.get("ontology", {}).get("declared_constraints", {})
        assert dc.get("foreign_keys"), "expected foreign_keys in declared_constraints"
        fk_targets = {(f["source_entity"], f["target_entity"]) for f in dc["foreign_keys"]}
        assert ("TargetTrainprogram", "Target") in fk_targets

    def test_uri_parent_marked_object_property(self):
        bundle = self._load_bundle()
        entities = bundle["ontology"]["entities"]["definitions"]
        props = entities["ProjectDiseaseRolledup"]["properties"]
        parent_prop = props.get("diseaseUriParent") or props.get("disease_uri_parent")
        assert parent_prop is not None
        assert parent_prop["role"] == "object_property"
        assert parent_prop.get("target_entity") == "DiseaseHierarchy"

    def test_hierarchy_uri_is_primary_key(self):
        bundle = self._load_bundle()
        entities = bundle["ontology"]["entities"]["definitions"]
        props = entities["DiseaseHierarchy"]["properties"]
        uri_prop = props.get("diseaseUri") or props.get("disease_uri")
        assert uri_prop is not None
        assert uri_prop["role"] == "primary_key"


class TestResolveDeclaredConstraints:
    def test_resolves_junction_and_uri_fks(self):
        entities = {
            "TargetTrainprogram": {
                "properties": {
                    "targetId": {
                        "role": "object_property",
                        "target_entity": "Target",
                        "typical_attributes": ["target_id"],
                    }
                },
                "typical_attributes": ["target_id"],
            },
            "Target": {
                "properties": {
                    "targetId": {"role": "primary_key", "typical_attributes": ["target_id"]},
                },
                "typical_attributes": ["target_id"],
            },
            "DiseaseHierarchy": {
                "properties": {
                    "diseaseUri": {"role": "primary_key", "typical_attributes": ["disease_uri"]},
                },
                "typical_attributes": ["disease_uri"],
            },
            "ProjectDiseaseRolledup": {
                "properties": {
                    "diseaseUriParent": {
                        "role": "object_property",
                        "target_entity": "DiseaseHierarchy",
                        "typical_attributes": ["disease_uri_parent"],
                    },
                },
                "typical_attributes": ["disease_uri", "disease_uri_parent"],
            },
        }
        apply_uri_property_overrides(entities)
        constraints = extract_declared_constraints(entities)
        entity_tables = {
            "TargetTrainprogram": ["cat.sch.bridge_target_program"],
            "Target": ["cat.sch.dim_target"],
            "DiseaseHierarchy": ["cat.sch.dim_disease_hierarchy"],
            "ProjectDiseaseRolledup": ["cat.sch.fct_disease_rolledup"],
        }
        table_columns = {
            "cat.sch.bridge_target_program": {"target_id", "trainprogram_id"},
            "cat.sch.dim_target": {"target_id"},
            "cat.sch.dim_disease_hierarchy": {"disease_uri"},
            "cat.sch.fct_disease_rolledup": {
                "disease_uri", "disease_uri_parent", "project_id",
            },
        }
        resolved = resolve_declared_constraints_to_tables(
            constraints, entity_tables, table_columns,
        )
        junction = resolved["cat.sch.bridge_target_program"][1]
        assert junction["target_id"] == "cat.sch.dim_target.target_id"

        rolled = resolved["cat.sch.fct_disease_rolledup"][1]
        assert rolled["disease_uri_parent"] == "cat.sch.dim_disease_hierarchy.disease_uri"

    def test_no_cross_entity_uri_pk_fks(self):
        entities = {
            "DiseaseHierarchy": {
                "properties": {
                    "diseaseUri": {"role": "primary_key", "typical_attributes": ["disease_uri"]},
                },
                "typical_attributes": ["disease_uri"],
            },
            "ProjectDiseaseRolledup": {
                "properties": {
                    "diseaseUri": {"role": "primary_key", "typical_attributes": ["disease_uri"]},
                    "diseaseUriParent": {
                        "role": "object_property",
                        "target_entity": "DiseaseHierarchy",
                        "typical_attributes": ["disease_uri_parent"],
                    },
                },
                "typical_attributes": ["disease_uri", "disease_uri_parent"],
            },
        }
        apply_uri_property_overrides(entities)
        constraints = extract_declared_constraints(entities)
        fk_pairs = {
            (f["source_entity"], f["target_entity"], tuple(f["source_columns"]))
            for f in constraints.get("foreign_keys", [])
        }
        assert ("DiseaseHierarchy", "ProjectDiseaseRolledup", ("disease_uri",)) not in fk_pairs
        assert ("ProjectDiseaseRolledup", "DiseaseHierarchy", ("disease_uri_parent",)) in fk_pairs


class TestDedupeStringMap:
    def test_collapses_case_variants(self):
        out = dedupe_string_map(
            {"target_id": "a.b.c", "TARGET_ID": "a.b.d"},
            context="test",
        )
        assert len(out) == 1
        assert list(out.values())[0] in ("a.b.c", "a.b.d")

    def test_drops_blank_keys(self):
        assert dedupe_string_map({"": "x", "  ": "y", "ok": "z"}) == {"ok": "z"}


class TestExtractSkipsNonEntityTargets:
    def test_skips_string_target_fks(self):
        entities = {
            "TargetTrainprogram": {
                "properties": {
                    "targetId": {
                        "role": "object_property",
                        "target_entity": "string",
                        "typical_attributes": ["target_id"],
                    },
                },
                "typical_attributes": ["target_id"],
            },
        }
        constraints = extract_declared_constraints(entities)
        assert not constraints.get("foreign_keys")


class TestMergeOwlForeignKeys:
    _ENTITY_TABLES = {
        "TargetTrainprogram": ["cat.sch.junction"],
        "Target": ["cat.sch.target"],
        "DiseaseHierarchy": ["cat.sch.hierarchy"],
        "ProjectDiseaseRolledup": ["cat.sch.rolledup"],
    }
    _TABLE_COLUMNS = {
        "cat.sch.junction": {"target_id", "trainprogram_id", "legacy_companion_fk"},
        "cat.sch.target": {"target_id"},
        "cat.sch.hierarchy": {"disease_uri"},
        "cat.sch.rolledup": {"disease_uri", "disease_uri_parent", "legacy_companion_fk"},
    }

    def _constraints(self):
        entities = {
            "TargetTrainprogram": {
                "properties": {
                    "targetId": {
                        "role": "object_property",
                        "target_entity": "Target",
                        "typical_attributes": ["target_id"],
                    },
                },
                "typical_attributes": ["target_id"],
            },
            "Target": {
                "properties": {
                    "targetId": {"role": "primary_key", "typical_attributes": ["target_id"]},
                },
                "typical_attributes": ["target_id"],
            },
            "DiseaseHierarchy": {
                "properties": {
                    "diseaseUri": {"role": "primary_key", "typical_attributes": ["disease_uri"]},
                },
                "typical_attributes": ["disease_uri"],
            },
            "ProjectDiseaseRolledup": {
                "properties": {
                    "diseaseUriParent": {
                        "role": "object_property",
                        "target_entity": "DiseaseHierarchy",
                        "typical_attributes": ["disease_uri_parent"],
                    },
                },
                "typical_attributes": ["disease_uri", "disease_uri_parent"],
            },
        }
        apply_uri_property_overrides(entities)
        return extract_declared_constraints(entities)

    def test_sweep_clears_stale_uri_pk_fk_on_hierarchy(self):
        constraints = self._constraints()
        fresh = resolve_declared_constraints_to_tables(
            constraints, self._ENTITY_TABLES, self._TABLE_COLUMNS,
        )
        hierarchy_fresh = fresh["cat.sch.hierarchy"][1]
        assert hierarchy_fresh == {}

        exist = {
            "disease_uri": "cat.sch.rolledup.disease_uri",
        }
        merged, _ = merge_owl_foreign_keys(
            exist,
            hierarchy_fresh,
            constraints=constraints,
            entity_tables=self._ENTITY_TABLES,
            table_columns=self._TABLE_COLUMNS,
            table_fqn="cat.sch.hierarchy",
            table_pk_cols=["disease_uri"],
            sweep_owl_fks=True,
        )
        assert merged == {}

    def test_sweep_without_flag_preserves_stale_fks(self):
        constraints = self._constraints()
        exist = {"disease_uri": "cat.sch.rolledup.disease_uri"}
        merged, _ = merge_owl_foreign_keys(
            exist,
            {},
            constraints=constraints,
            entity_tables=self._ENTITY_TABLES,
            table_columns=self._TABLE_COLUMNS,
            table_fqn="cat.sch.hierarchy",
            table_pk_cols=["disease_uri"],
            sweep_owl_fks=False,
        )
        assert merged == exist

    def test_sweep_preserves_companion_only_fk(self):
        constraints = self._constraints()
        fresh = resolve_declared_constraints_to_tables(
            constraints, self._ENTITY_TABLES, self._TABLE_COLUMNS,
        )
        rolledup_fresh = fresh["cat.sch.rolledup"][1]
        exist = {
            "disease_uri": "cat.sch.hierarchy.disease_uri",
            "legacy_companion_fk": "cat.sch.target.target_id",
            "disease_uri_parent": "cat.sch.hierarchy.stale_parent",
        }
        merged, _ = merge_owl_foreign_keys(
            exist,
            rolledup_fresh,
            constraints=constraints,
            entity_tables=self._ENTITY_TABLES,
            table_columns=self._TABLE_COLUMNS,
            table_fqn="cat.sch.rolledup",
            table_pk_cols=["disease_uri", "disease_uri_parent"],
            sweep_owl_fks=True,
        )
        assert merged["legacy_companion_fk"] == "cat.sch.target.target_id"
        assert merged["disease_uri_parent"] == rolledup_fresh["disease_uri_parent"]
        assert "disease_uri" not in merged

    def test_junction_owl_and_companion_coexist(self):
        constraints = self._constraints()
        fresh = resolve_declared_constraints_to_tables(
            constraints, self._ENTITY_TABLES, self._TABLE_COLUMNS,
        )
        junction_fresh = fresh["cat.sch.junction"][1]
        exist = {
            "target_id": "cat.sch.target.stale_target",
            "legacy_companion_fk": "cat.sch.target.target_id",
        }
        merged, _ = merge_owl_foreign_keys(
            exist,
            junction_fresh,
            constraints=constraints,
            entity_tables=self._ENTITY_TABLES,
            table_columns=self._TABLE_COLUMNS,
            table_fqn="cat.sch.junction",
            table_pk_cols=[],
            sweep_owl_fks=True,
        )
        assert merged["target_id"] == junction_fresh["target_id"]
        assert merged["legacy_companion_fk"] == "cat.sch.target.target_id"

    def test_managed_columns_includes_declared_sources_and_orphan_pks(self):
        constraints = self._constraints()
        managed = owl_fk_managed_columns_for_table(
            constraints,
            self._ENTITY_TABLES,
            self._TABLE_COLUMNS,
            "cat.sch.hierarchy",
            fresh_fk_cols=set(),
            table_pk_cols=["disease_uri"],
        )
        assert managed == {"disease_uri"}
