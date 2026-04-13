"""Golden-file snapshot tests for ontology tier generation.

Runs build_tiers() on a fixed test bundle and compares output to committed
golden files. Any unintentional change to tier output will show up as a diff.

To update golden files after an intentional change:
    uv run python -c "
    from pathlib import Path
    from dbxmetagen.ontology_bundle_indexes import entities_from_bundle, build_tiers
    entities = entities_from_bundle(Path('tests/golden/test_bundle.yaml'))
    build_tiers(entities, Path('tests/golden'))
    "
"""

import json
import tempfile
from pathlib import Path

import pytest

from dbxmetagen.ontology_bundle_indexes import entities_from_bundle, build_tiers

GOLDEN_DIR = Path(__file__).parent / "golden"
BUNDLE_PATH = GOLDEN_DIR / "test_bundle.yaml"

TIER_FILES = [
    "entities_tier1.json",
    "entities_tier2.json",
    "entities_tier3.json",
    "edges_tier1.json",
    "edges_tier2.json",
    "edges_tier3.json",
    "equivalent_class_uris.json",
]


@pytest.fixture(scope="module")
def generated_tiers():
    """Run build_tiers on the test bundle into a temp dir."""
    entities = entities_from_bundle(BUNDLE_PATH)
    with tempfile.TemporaryDirectory() as tmp:
        build_tiers(entities, Path(tmp))
        result = {}
        for name in TIER_FILES:
            p = Path(tmp) / name
            if p.exists():
                result[name] = json.loads(p.read_text())
        yield result


class TestGoldenTierSnapshots:
    """Each tier file must match the committed golden snapshot."""

    @pytest.mark.parametrize("filename", TIER_FILES)
    def test_tier_matches_golden(self, filename, generated_tiers):
        golden_path = GOLDEN_DIR / filename
        assert golden_path.exists(), f"Golden file missing: {filename}"
        expected = json.loads(golden_path.read_text())
        actual = generated_tiers[filename]
        assert actual == expected, (
            f"Tier output for {filename} differs from golden file. "
            f"If this is intentional, regenerate golden files."
        )


class TestGoldenTierStructure:
    """Structural invariants that must hold regardless of content."""

    def test_entities_tier1_is_sorted_list(self, generated_tiers):
        t1 = generated_tiers["entities_tier1.json"]
        assert isinstance(t1, list)
        names = [e["name"] for e in t1]
        assert names == sorted(names)

    def test_entities_tier1_has_required_fields(self, generated_tiers):
        for entry in generated_tiers["entities_tier1.json"]:
            assert "name" in entry
            assert "description" in entry
            assert "label" in entry

    def test_edges_tier1_is_sorted_list(self, generated_tiers):
        t1 = generated_tiers["edges_tier1.json"]
        assert isinstance(t1, list)
        names = [e["name"] for e in t1]
        assert names == sorted(names)

    def test_uris_all_string_values(self, generated_tiers):
        uris = generated_tiers["equivalent_class_uris.json"]
        for k, v in uris.items():
            assert isinstance(v, str), f"URI for {k} is not a string: {v}"

    def test_all_tier_files_produced(self, generated_tiers):
        for f in TIER_FILES:
            assert f in generated_tiers, f"Missing tier file: {f}"

    def test_entity_count_consistent(self, generated_tiers):
        t1_count = len(generated_tiers["entities_tier1.json"])
        t2_count = len(generated_tiers["entities_tier2.json"])
        t3_count = len(generated_tiers["entities_tier3.json"])
        assert t1_count == t2_count == t3_count
