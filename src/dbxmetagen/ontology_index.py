"""Tiered ontology index loader for large published ontologies.

Supports three tiers of increasing detail for both entities and edges,
enabling progressive LLM-based classification without exceeding context windows.

Tier structure (progressive refinement):
  - Tier 1: Compact index (name + description) for broad screening
  - Tier 2: Scoped profiles (source, URI, parent, top edges) for confirmation
  - Tier 3: Full profiles (keywords, relationships, attributes) for deep passes
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tier YAML schema expectations (used for validation)
# ---------------------------------------------------------------------------

_ENTITIES_T1_REQUIRED = {"name", "description"}
_ENTITIES_T2_REQUIRED = {"description", "source_ontology", "uri"}
_ENTITIES_T3_REQUIRED = {"description", "source_ontology", "uri", "keywords"}
_EDGES_T1_REQUIRED = {"name", "domain", "range"}
_EDGES_T2_REQUIRED = {"name", "domain", "range"}


def _validate_list_of_dicts(
    data: Any, required_keys: set, filename: str,
) -> Tuple[List[Dict], List[str]]:
    """Validate a tier file expected to be list[dict]. Returns (valid_entries, issues)."""
    issues: List[str] = []
    if not isinstance(data, list):
        issues.append(f"{filename}: expected list, got {type(data).__name__}")
        return [], issues
    valid = []
    for i, entry in enumerate(data):
        if not isinstance(entry, dict):
            issues.append(f"{filename}[{i}]: expected dict, got {type(entry).__name__}")
            continue
        missing = required_keys - set(entry.keys())
        if missing:
            issues.append(f"{filename}[{i}]: missing keys {missing}")
            continue
        valid.append(entry)
    return valid, issues


def _validate_dict_of_dicts(
    data: Any, required_keys: set, filename: str,
) -> Tuple[Dict[str, Any], List[str]]:
    """Validate a tier file expected to be dict[str, dict]. Returns (valid_entries, issues)."""
    issues: List[str] = []
    if not isinstance(data, dict):
        issues.append(f"{filename}: expected dict, got {type(data).__name__}")
        return {}, issues
    valid = {}
    for name, entry in data.items():
        if not isinstance(entry, dict):
            issues.append(f"{filename}[{name}]: expected dict, got {type(entry).__name__}")
            continue
        missing = required_keys - set(entry.keys())
        if missing:
            issues.append(f"{filename}[{name}]: missing keys {missing}")
            continue
        valid[name] = entry
    return valid, issues


def _validate_uri_map(data: Any, filename: str) -> Tuple[Dict[str, str], List[str]]:
    """Validate equivalent_class_uris.yaml: dict[str, str]."""
    issues: List[str] = []
    if not isinstance(data, dict):
        issues.append(f"{filename}: expected dict, got {type(data).__name__}")
        return {}, issues
    valid = {}
    for k, v in data.items():
        if not isinstance(v, str):
            issues.append(f"{filename}[{k}]: expected str URI, got {type(v).__name__}")
            continue
        valid[k] = v
    return valid, issues


_TIER_STEMS = [
    "entities_tier1", "entities_tier2", "entities_tier3",
    "edges_tier1", "edges_tier2", "edges_tier3",
    "equivalent_class_uris",
]

_VALIDATORS = {
    "entities_tier1": lambda d: _validate_list_of_dicts(d, _ENTITIES_T1_REQUIRED, "entities_tier1"),
    "entities_tier2": lambda d: _validate_dict_of_dicts(d, _ENTITIES_T2_REQUIRED, "entities_tier2"),
    "entities_tier3": lambda d: _validate_dict_of_dicts(d, _ENTITIES_T3_REQUIRED, "entities_tier3"),
    "edges_tier1": lambda d: _validate_list_of_dicts(d, _EDGES_T1_REQUIRED, "edges_tier1"),
    "edges_tier2": lambda d: _validate_dict_of_dicts(d, _EDGES_T2_REQUIRED, "edges_tier2"),
    "edges_tier3": lambda d: _validate_dict_of_dicts(d, _EDGES_T2_REQUIRED, "edges_tier3"),
    "equivalent_class_uris": lambda d: _validate_uri_map(d, "equivalent_class_uris"),
}

# Legacy key mapping for backward compatibility
_VALIDATORS_COMPAT = {f"{k}.yaml": v for k, v in _VALIDATORS.items()}


def _resolve_tier_path(bundle_dir: Path, stem: str) -> Optional[Path]:
    """Return the JSON path if it exists, else YAML (legacy), else None."""
    json_path = bundle_dir / f"{stem}.json"
    if json_path.is_file():
        return json_path
    yaml_path = bundle_dir / f"{stem}.yaml"
    if yaml_path.is_file():
        return yaml_path
    return None


def _parse_tier_file(path: Path) -> Any:
    """Parse a tier file (JSON preferred, YAML supported for custom bundles).

    On corrupt JSON, falls back to a YAML sibling (same stem) if one exists.
    """
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".json":
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            yaml_sibling = path.with_suffix(".yaml")
            if yaml_sibling.is_file():
                return yaml.safe_load(yaml_sibling.read_text(encoding="utf-8"))
            raise
    return yaml.safe_load(text)


def validate_bundle(bundle_name: str, base_dir: Optional[str] = None) -> List[str]:
    """Run schema validation on all tier files for a bundle. Returns list of issues."""
    if base_dir:
        bundle_dir = Path(base_dir)
    else:
        pkg_root = Path(__file__).resolve().parent.parent.parent
        bundle_dir = pkg_root / "configurations" / "ontology_bundles" / bundle_name

    all_issues: List[str] = []
    for stem, validator in _VALIDATORS.items():
        path = _resolve_tier_path(bundle_dir, stem)
        if not path:
            continue
        try:
            data = _parse_tier_file(path)
        except Exception as e:
            all_issues.append(f"{path.name}: parse error: {e}")
            continue
        if data is None:
            all_issues.append(f"{path.name}: file is empty")
            continue
        _, issues = validator(data)
        all_issues.extend(issues)
    return all_issues


class OntologyIndexLoader:
    """Loads tiered YAML indexes for progressive ontology prediction.

    Tier files are generated by scripts/build_ontology_indexes.py from
    published ontologies (FHIR R4, OMOP CDM, Schema.org). They live in
    a subdirectory named after the bundle alongside the main bundle YAML.

    Schema validation runs on load and filters/warns about malformed entries.
    """

    def __init__(self, bundle_name: str = "healthcare", base_dir: Optional[str] = None):
        if base_dir:
            self._bundle_dir = Path(base_dir)
        else:
            pkg_root = Path(__file__).resolve().parent.parent.parent
            self._bundle_dir = pkg_root / "configurations" / "ontology_bundles" / bundle_name
        self._cache: Dict[str, Any] = {}

    @property
    def has_tier_indexes(self) -> bool:
        """Check whether tier index files exist for this bundle."""
        return (
            (self._bundle_dir / "entities_tier1.json").is_file()
            or (self._bundle_dir / "entities_tier1.yaml").is_file()
        )

    def _load(self, filename: str) -> Any:
        if filename not in self._cache:
            stem = filename.rsplit(".", 1)[0]
            path = _resolve_tier_path(self._bundle_dir, stem)
            if not path:
                logger.debug("Tier file not found: %s/{%s.json,%s}", self._bundle_dir, stem, filename)
                self._cache[filename] = None
                return None
            raw = _parse_tier_file(path)
            validator = _VALIDATORS.get(stem)
            if validator and raw is not None:
                validated, issues = validator(raw)
                for issue in issues:
                    logger.warning("Tier validation: %s", issue)
                self._cache[filename] = validated
            else:
                self._cache[filename] = raw
        return self._cache[filename]

    # --- Entity tiers ---

    def get_entities_tier1(self) -> List[Dict[str, str]]:
        """Tier 1: compact list of {name, description} for broad screening."""
        return self._load("entities_tier1.yaml") or []

    def get_entities_tier2_scoped(self, entity_names: List[str]) -> Dict[str, Any]:
        """Tier 2: scoped detail for named entities only (source, URI, parent, edges capped at 8)."""
        full = self._load("entities_tier2.yaml")
        if not full:
            return {}
        return {k: full[k] for k in entity_names if k in full}

    def get_entities_tier3_scoped(self, entity_names: List[str]) -> Dict[str, Any]:
        """Tier 3: full profiles for final deep classification."""
        full = self._load("entities_tier3.yaml")
        if not full:
            return {}
        return {k: full[k] for k in entity_names if k in full}

    # --- Edge tiers ---

    def get_edges_tier1(self) -> List[Dict[str, str]]:
        """Tier 1: compact edge list with name, domain, range."""
        return self._load("edges_tier1.yaml") or []

    def get_edges_tier2_scoped(self, edge_names: List[str]) -> Dict[str, Any]:
        """Tier 2: scoped edge detail."""
        full = self._load("edges_tier2.yaml")
        if not full:
            return {}
        return {k: full[k] for k in edge_names if k in full}

    def get_edges_tier3_scoped(self, edge_names: List[str]) -> Dict[str, Any]:
        """Tier 3: full edge profiles with URIs."""
        full = self._load("edges_tier3.yaml")
        if not full:
            return {}
        return {k: full[k] for k in edge_names if k in full}

    # --- URI lookup ---

    def get_uri(self, entity_name: str) -> Optional[str]:
        """Look up the owl:equivalentClass URI for an entity (case-insensitive)."""
        uris = self._load("equivalent_class_uris.yaml")
        if not uris:
            return None
        hit = uris.get(entity_name)
        if hit is not None:
            return hit
        lower_map = {k.lower(): v for k, v in uris.items()}
        return lower_map.get(entity_name.lower())

    def get_all_uris(self) -> Dict[str, str]:
        """Return the full entity_name -> URI mapping."""
        return self._load("equivalent_class_uris.yaml") or {}

    def entity_count(self) -> int:
        """Number of entities available in tier 1."""
        return len(self.get_entities_tier1())
