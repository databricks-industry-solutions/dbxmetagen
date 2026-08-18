"""Tests for resolve_bundle_path: bundle name/key -> YAML path resolution.

Regression guard for the "FHIR R4.yaml not found" bug: a display label
("FHIR R4") reaching the resolver instead of the file stem ("fhir_r4") must
still resolve to the curated bundle via the slugified fallback.
"""

import os

import pytest

from dbxmetagen.ontology import resolve_bundle_path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUNDLES = os.path.join(ROOT, "configurations", "ontology_bundles")


def test_stem_resolves():
    p = resolve_bundle_path("fhir_r4")
    assert p.endswith("fhir_r4.yaml")
    assert os.path.exists(p)


def test_display_name_resolves_via_slug():
    """'FHIR R4' (metadata display name) must resolve to fhir_r4.yaml."""
    p = resolve_bundle_path("FHIR R4")
    assert p.endswith("fhir_r4.yaml")
    assert os.path.exists(p)


def test_omop_display_name_resolves():
    p = resolve_bundle_path("OMOP CDM")
    assert p.endswith("omop_cdm.yaml")
    assert os.path.exists(p)


def test_yaml_suffix_is_stripped_not_doubled():
    p = resolve_bundle_path("healthcare.yaml")
    assert p.endswith("healthcare.yaml")
    assert not p.endswith(".yaml.yaml")
    assert os.path.exists(p)


def test_verbatim_wins_when_file_exists():
    """A real stem with no slug change resolves to itself (no false rewrite)."""
    p = resolve_bundle_path("general")
    assert p.endswith("general.yaml")
    assert os.path.exists(p)


def test_unknown_bundle_falls_back_to_bare_path():
    """Unknown name returns a bare path (caller surfaces the FileNotFoundError),
    and does not raise inside the resolver."""
    p = resolve_bundle_path("definitely_not_a_bundle_xyz")
    assert p.endswith("definitely_not_a_bundle_xyz.yaml")


def test_display_name_when_slug_differs_from_stem():
    """Curated bundles whose metadata.name does NOT slugify to the stem must
    still resolve via the metadata.name scan (the slug fallback can't rescue
    these). 'General Cross-Industry' -> general.yaml, not general_cross_industry."""
    p = resolve_bundle_path("General Cross-Industry")
    assert p.endswith("general.yaml")
    assert os.path.exists(p)


def test_healthcare_display_name_resolves():
    p = resolve_bundle_path("Healthcare & Life Sciences")
    assert p.endswith("healthcare.yaml")
    assert os.path.exists(p)
