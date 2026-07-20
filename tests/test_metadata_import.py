"""Tests for companion DCAT/TTL metadata import (declared FK companion path)."""

import os
import pytest

from dbxmetagen.metadata_import import (
    build_companion_constraints,
    merge_constraint_maps,
    resolve_fk_target_column,
    resolve_table_fqn,
)
from dbxmetagen.ontology_properties import attribute_name_variants


FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "companion_fk_snippet.ttl")

KB_TABLES = [
    "federated_cat.dwh.dim_target",
    "federated_cat.dwh.bridge_target_program",
    "federated_cat.dwh.dim_disease_hierarchy",
    "federated_cat.dwh.fct_disease_rolledup",
]


class TestAttributeNameVariants:
    def test_camelcase_to_snake(self):
        assert "disease_uri" in attribute_name_variants("diseaseUri")


class TestResolveTableFqn:
    def test_suffix_match(self):
        assert resolve_table_fqn("dwh.dim_target", KB_TABLES) == KB_TABLES[0]

    def test_missing_returns_none(self):
        assert resolve_table_fqn("other.schema.table", KB_TABLES) is None


class TestResolveFkTargetColumn:
    def test_parent_suffix_maps_to_pk(self):
        assert resolve_fk_target_column("disease_uri_parent", ["disease_uri"]) == "disease_uri"

    def test_exact_match(self):
        assert resolve_fk_target_column("target_id", ["target_id"]) == "target_id"


class TestCompanionMetadataParsing:
    @pytest.fixture(autouse=True)
    def _require_rdflib(self):
        pytest.importorskip("rdflib")

    def test_build_constraints_fqn_and_fk_target(self):
        constraints = build_companion_constraints(FIXTURE, KB_TABLES)
        junction = KB_TABLES[1]
        target = KB_TABLES[0]
        assert junction in constraints
        pks, fks = constraints[junction]
        assert "target_id" in pks
        assert fks["target_id"] == f"{target}.target_id"

    def test_disease_uri_parent_resolves_to_disease_uri(self):
        constraints = build_companion_constraints(FIXTURE, KB_TABLES)
        rolled = KB_TABLES[3]
        hierarchy = KB_TABLES[2]
        _, fks = constraints[rolled]
        assert fks["disease_uri_parent"] == f"{hierarchy}.disease_uri"


class TestMergeConstraintMaps:
    def test_uc_wins_on_fk_conflict(self):
        uc = {"cat.sch.t": (["id"], {"col": "cat.sch.other.uc_col"})}
        companion = {"cat.sch.t": ([], {"col": "cat.sch.other.ttl_col"})}
        merged = merge_constraint_maps(uc, companion)
        assert merged["cat.sch.t"][1]["col"] == "cat.sch.other.uc_col"

    def test_companion_fills_empty_uc(self):
        merged = merge_constraint_maps({}, {
            "cat.sch.t": (["pk"], {"fk": "cat.sch.dst.pk"}),
        })
        assert merged["cat.sch.t"][0] == ["pk"]
