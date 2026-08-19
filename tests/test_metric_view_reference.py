"""Contract tests for the runtime metric-view guidance asset.

configurations/agent_references/metric_view_reference.json is the single source
of truth for metric-view quality: it is loaded and injected into the generation
prompts by BOTH the library (semantic_layer._load_reference / _format_reference_
section) and the app backend (_load_reference_rules / _load_agent_reference).

These tests guard the asset<->loader contract so a rename or accidental deletion
of a section can't silently stop the guidance from reaching the prompts.
"""

import json
from pathlib import Path

import pytest

from dbxmetagen.semantic_layer import _load_reference, _format_reference_section

ROOT = Path(__file__).resolve().parents[1]
REF = ROOT / "configurations" / "agent_references" / "metric_view_reference.json"

# Sections the prompts explicitly select -- if one is renamed/removed the guidance
# silently vanishes from generation, so pin the contract here.
REQUIRED_KEYS = {
    "yaml_syntax_rules",
    "measure_patterns",
    "join_templates",
    "format_patterns",
    "guiding_principles",
    "fact_dimension_model",
    "anti_patterns",
    "validation_checklist",
}


def _ref():
    with open(REF) as f:
        return json.load(f)


def test_reference_is_valid_json():
    assert REF.is_file(), f"missing reference asset: {REF}"
    _ref()  # must not raise


def test_required_sections_present():
    ref = _ref()
    missing = REQUIRED_KEYS - set(ref.keys())
    assert not missing, f"metric_view_reference.json missing sections: {missing}"


def test_fact_dimension_model_defines_roles():
    fdm = _ref()["fact_dimension_model"]
    assert isinstance(fdm, dict)
    for role in ("fact", "dimension", "bridge"):
        assert role in fdm and fdm[role], f"fact_dimension_model missing role '{role}'"


def test_guiding_principles_cover_fact_sourcing():
    gps = " ".join(_ref()["guiding_principles"]).lower()
    assert "fact" in gps and "dimension" in gps
    assert "source" in gps  # "source from facts, join to dimensions"


def test_dimensional_model_premise_is_stated():
    """Metric views target a fact+dimension (or fact-like) structure -- this
    premise must reach the agents, so pin it in principles + fact_dimension_model."""
    gps = " ".join(_ref()["guiding_principles"]).lower()
    assert "fact + dimension" in gps or "fact+dimension" in gps
    assert "shaped like" in gps  # tables shaped like fact+dim also work
    fdm = _ref()["fact_dimension_model"]
    assert "fact_like_tables" in fdm and fdm["fact_like_tables"]
    assert "poor_fit" in fdm and fdm["poor_fit"]


def test_anti_patterns_include_fact_to_fact():
    aps = " ".join(_ref()["anti_patterns"]).lower()
    assert "fact" in aps and ("fan-out" in aps or "fan out" in aps)


def test_library_loader_renders_new_sections():
    """_load_reference + _format_reference_section (library path) emit the new
    principles/fact-dimension guidance as non-empty prompt text."""
    ref = _load_reference("metric_view_reference.json")
    assert ref, "library _load_reference returned empty"
    text = _format_reference_section(
        ref, ["guiding_principles", "fact_dimension_model", "anti_patterns"]
    )
    assert text.strip()
    assert "guiding_principles" in text
    assert "fact_dimension_model" in text


def test_format_reference_section_empty_ref_is_safe():
    assert _format_reference_section({}) == ""
