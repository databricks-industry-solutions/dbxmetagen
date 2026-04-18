#!/usr/bin/env python3
"""Validate ontology bundle YAML files for schema correctness and referential integrity.

Usage:
    uv run python scripts/validate_ontology_bundles.py
    uv run python scripts/validate_ontology_bundles.py --bundle healthcare
    uv run python scripts/validate_ontology_bundles.py --strict  # treat warnings as errors
"""

import argparse
import sys
from pathlib import Path
from typing import Any

import yaml

BUNDLES_DIR = Path(__file__).parent.parent / "configurations" / "ontology_bundles"

ENTITY_REQUIRED_FIELDS_TIER1 = {"name", "description"}
ENTITY_OPTIONAL_FIELDS_TIER1 = {"label", "keywords", "properties", "parent", "parents"}

ENTITY_REQUIRED_FIELDS_TIER2 = {"description"}
ENTITY_OPTIONAL_FIELDS_TIER2 = {
    "label", "source_ontology", "uri", "parents", "edges",
    "keywords", "properties", "parent", "relationships",
    "typical_attributes", "synonyms", "business_questions",
}

EDGE_REQUIRED_FIELDS = {"name", "domain", "range"}
EDGE_OPTIONAL_FIELDS = {"cardinality", "inverse", "description", "label"}

try:
    from dbxmetagen.ontology_roles import VALID_ROLE_NAMES as VALID_PROPERTY_ROLES
except ImportError:
    VALID_PROPERTY_ROLES = {
        "object_property", "primary_key", "business_key", "dimension",
        "geographic", "audit", "temporal", "label", "measure",
        "derived", "composite_component",
    }

VALID_CARDINALITIES = {"one-to-one", "one-to-many", "many-to-one", "many-to-many"}


class ValidationResult:
    def __init__(self):
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def error(self, msg: str):
        self.errors.append(msg)

    def warn(self, msg: str):
        self.warnings.append(msg)

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0


def load_yaml(path: Path) -> Any:
    if not path.exists():
        return None
    with open(path) as f:
        return yaml.safe_load(f)


def collect_entity_names(bundle_dir: Path) -> set[str]:
    """Collect all entity names across all tiers."""
    names = set()
    for tier in ["entities_tier1.yaml", "entities_tier2.yaml", "entities_tier3.yaml"]:
        data = load_yaml(bundle_dir / tier)
        if data is None:
            continue
        if isinstance(data, list):
            for ent in data:
                if isinstance(ent, dict) and "name" in ent:
                    names.add(ent["name"])
        elif isinstance(data, dict):
            names.update(data.keys())
    return names


def validate_tier1_entities(data: Any, path: Path, result: ValidationResult):
    """Tier1 entities are a list of dicts with {name, description, ...}."""
    if data is None:
        return
    if not isinstance(data, list):
        result.error(f"{path}: Expected list, got {type(data).__name__}")
        return
    seen = set()
    for i, ent in enumerate(data):
        if not isinstance(ent, dict):
            result.error(f"{path}[{i}]: Expected dict, got {type(ent).__name__}")
            continue
        name = ent.get("name", f"<unnamed_{i}>")
        if name in seen:
            result.error(f"{path}: Duplicate entity name '{name}'")
        seen.add(name)
        for req in ENTITY_REQUIRED_FIELDS_TIER1:
            if req not in ent:
                result.error(f"{path}: Entity '{name}' missing required field '{req}'")
        validate_properties(ent.get("properties"), name, path, result)


def validate_tier2_entities(
    data: Any, path: Path, all_entities: set[str], result: ValidationResult,
    edge_catalog_names: set[str] | None = None,
):
    """Tier2/3 entities are a dict of {EntityName: {description, edges, ...}}."""
    if data is None:
        return
    if not isinstance(data, dict):
        result.error(f"{path}: Expected dict, got {type(data).__name__}")
        return
    for name, defn in data.items():
        if not isinstance(defn, dict):
            result.error(f"{path}: Entity '{name}' value should be dict")
            continue
        if "description" not in defn:
            result.error(f"{path}: Entity '{name}' missing required field 'description'")
        parents = defn.get("parents", [])
        if isinstance(parents, list):
            for p in parents:
                if p and p not in all_entities:
                    result.warn(f"{path}: Entity '{name}' parent '{p}' not found in bundle")
        validate_properties(defn.get("properties"), name, path, result,
                            all_entities=all_entities, edge_catalog_names=edge_catalog_names)


def validate_properties(
    props: Any,
    entity_name: str,
    path: Path,
    result: ValidationResult,
    all_entities: set[str] | None = None,
    edge_catalog_names: set[str] | None = None,
):
    """Validate property definitions if present. Accepts list or dict format."""
    if props is None:
        return
    if isinstance(props, dict):
        _validate_dict_properties(props, entity_name, path, result, all_entities, edge_catalog_names)
        return
    if not isinstance(props, list):
        result.error(f"{path}: Entity '{entity_name}' properties should be a list or dict")
        return
    for i, prop in enumerate(props):
        if not isinstance(prop, dict):
            result.error(f"{path}: Entity '{entity_name}' property[{i}] should be dict")
            continue
        pname = prop.get("name", f"<unnamed_{i}>")
        if "name" not in prop:
            result.error(f"{path}: Entity '{entity_name}' property[{i}] missing 'name'")
        if "role" in prop and prop["role"] not in VALID_PROPERTY_ROLES:
            result.error(
                f"{path}: Entity '{entity_name}' property '{pname}' "
                f"invalid role '{prop['role']}' (valid: {sorted(VALID_PROPERTY_ROLES)})"
            )
        if prop.get("role") == "link":
            result.error(
                f"{path}: Entity '{entity_name}' property '{pname}' "
                f"uses deprecated role 'link' -- use 'object_property' instead"
            )


def _validate_dict_properties(
    props: dict,
    entity_name: str,
    path: Path,
    result: ValidationResult,
    all_entities: set[str] | None = None,
    edge_catalog_names: set[str] | None = None,
):
    """Validate dict-format properties (generated or hand-curated)."""
    for pname, pinfo in props.items():
        if not isinstance(pinfo, dict):
            result.error(f"{path}: Entity '{entity_name}' property '{pname}' value should be dict")
            continue
        role = pinfo.get("role")
        if role and role not in VALID_PROPERTY_ROLES:
            result.error(
                f"{path}: Entity '{entity_name}' property '{pname}' "
                f"invalid role '{role}' (valid: {sorted(VALID_PROPERTY_ROLES)})"
            )
        kind = pinfo.get("kind")
        if kind and kind not in ("data_property", "object_property"):
            result.error(
                f"{path}: Entity '{entity_name}' property '{pname}' "
                f"invalid kind '{kind}' (expected data_property or object_property)"
            )
        if role == "object_property" and kind and kind != "object_property":
            result.warn(
                f"{path}: Entity '{entity_name}' property '{pname}' "
                f"role is object_property but kind is '{kind}'"
            )
        attrs = pinfo.get("typical_attributes")
        if attrs is not None and not isinstance(attrs, list):
            result.error(
                f"{path}: Entity '{entity_name}' property '{pname}' "
                f"typical_attributes should be a list"
            )
        elif isinstance(attrs, list) and len(attrs) == 0:
            result.warn(
                f"{path}: Entity '{entity_name}' property '{pname}' "
                f"typical_attributes is empty"
            )
        if role == "object_property":
            target = pinfo.get("target_entity")
            if target and all_entities:
                targets = target if isinstance(target, list) else [target]
                for t in targets:
                    if t not in all_entities:
                        result.warn(
                            f"{path}: Entity '{entity_name}' property '{pname}' "
                            f"target_entity '{t}' not found in bundle entities"
                        )
            edge = pinfo.get("edge")
            if edge and edge_catalog_names and edge not in edge_catalog_names:
                result.warn(
                    f"{path}: Entity '{entity_name}' property '{pname}' "
                    f"edge '{edge}' not found in edge_catalog"
                )


def _normalize_edges(data: Any) -> list[dict]:
    """Convert list-format or dict-format edges to a uniform list of dicts."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        edges = []
        for key, val in data.items():
            if isinstance(val, dict):
                edge = dict(val)
                edge.setdefault("name", key)
                edges.append(edge)
        return edges
    return []


def validate_edges(
    data: Any, path: Path, all_entities: set[str], result: ValidationResult
):
    """Validate edge definitions (list or dict format)."""
    if data is None:
        return
    edges = _normalize_edges(data)
    if not edges and data is not None:
        result.warn(f"{path}: No parseable edges found")
        return
    seen = set()
    for i, edge in enumerate(edges):
        if not isinstance(edge, dict):
            result.error(f"{path}[{i}]: Expected dict, got {type(edge).__name__}")
            continue
        name = edge.get("name", f"<unnamed_{i}>")
        for req in EDGE_REQUIRED_FIELDS:
            if req not in edge:
                result.error(f"{path}: Edge '{name}' missing required field '{req}'")
        if name in seen:
            result.warn(f"{path}: Duplicate edge name '{name}'")
        seen.add(name)

        domain = edge.get("domain")
        range_ = edge.get("range")
        if domain and domain != "Any" and not isinstance(domain, list):
            if domain not in all_entities:
                result.warn(f"{path}: Edge '{name}' domain '{domain}' not found in entities")
        if range_ and range_ != "Any" and not isinstance(range_, list):
            if range_ not in all_entities:
                result.warn(f"{path}: Edge '{name}' range '{range_}' not found in entities")
        if isinstance(domain, list):
            for d in domain:
                if d not in all_entities:
                    result.warn(f"{path}: Edge '{name}' domain list item '{d}' not found")
        if isinstance(range_, list):
            for r in range_:
                if r not in all_entities:
                    result.warn(f"{path}: Edge '{name}' range list item '{r}' not found")

        card = edge.get("cardinality")
        if card and card not in VALID_CARDINALITIES:
            result.warn(
                f"{path}: Edge '{name}' cardinality '{card}' not in {VALID_CARDINALITIES}"
            )

        inv = edge.get("inverse")
        if inv and inv == name:
            result.warn(f"{path}: Edge '{name}' inverse points to itself")


def validate_bundle(bundle_dir: Path, result: ValidationResult):
    """Validate a single bundle directory."""
    all_entities = collect_entity_names(bundle_dir)
    if not all_entities:
        result.warn(f"{bundle_dir}: No entities found")
        return

    # Load root bundle YAML for cross-referencing (edge_catalog, entity names)
    root_yaml = load_yaml(bundle_dir.parent / f"{bundle_dir.name}.yaml")
    edge_catalog_names: set[str] = set()
    if root_yaml and isinstance(root_yaml, dict):
        ec = root_yaml.get("ontology", {}).get("edge_catalog", {})
        if isinstance(ec, dict):
            edge_catalog_names = set(ec.keys())

    for tier_file in ["entities_tier1.yaml"]:
        data = load_yaml(bundle_dir / tier_file)
        validate_tier1_entities(data, bundle_dir / tier_file, result)

    for tier_file in ["entities_tier2.yaml", "entities_tier3.yaml"]:
        data = load_yaml(bundle_dir / tier_file)
        validate_tier2_entities(data, bundle_dir / tier_file, all_entities, result,
                                edge_catalog_names=edge_catalog_names)

    for edge_file in ["edges_tier1.yaml", "edges_tier2.yaml", "edges_tier3.yaml"]:
        data = load_yaml(bundle_dir / edge_file)
        validate_edges(data, bundle_dir / edge_file, all_entities, result)

    # Check inverse edge pairs
    all_edges = {}
    for edge_file in ["edges_tier1.yaml", "edges_tier2.yaml", "edges_tier3.yaml"]:
        data = load_yaml(bundle_dir / edge_file)
        if data is not None:
            for edge in _normalize_edges(data):
                if isinstance(edge, dict) and "name" in edge:
                    all_edges[edge["name"]] = edge

    for ename, edge in all_edges.items():
        inv = edge.get("inverse")
        if inv and inv not in all_edges:
            result.warn(f"{bundle_dir}: Edge '{ename}' inverse '{inv}' not defined as an edge")


def main():
    parser = argparse.ArgumentParser(description="Validate ontology bundle YAMLs")
    parser.add_argument("--bundle", help="Validate only this bundle (directory name)")
    parser.add_argument("--strict", action="store_true", help="Treat warnings as errors")
    args = parser.parse_args()

    if not BUNDLES_DIR.exists():
        print(f"Bundles directory not found: {BUNDLES_DIR}")
        sys.exit(1)

    bundle_dirs = sorted(
        d for d in BUNDLES_DIR.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    )

    if args.bundle:
        bundle_dirs = [d for d in bundle_dirs if d.name == args.bundle]
        if not bundle_dirs:
            print(f"Bundle '{args.bundle}' not found in {BUNDLES_DIR}")
            sys.exit(1)

    total_errors = 0
    total_warnings = 0

    for bundle_dir in bundle_dirs:
        result = ValidationResult()
        validate_bundle(bundle_dir, result)

        status = "PASS" if result.ok else "FAIL"
        if result.warnings and result.ok:
            status = "WARN"

        print(f"\n{'='*60}")
        print(f"Bundle: {bundle_dir.name}  [{status}]")
        print(f"  Entities: {len(collect_entity_names(bundle_dir))}")
        print(f"  Errors: {len(result.errors)}, Warnings: {len(result.warnings)}")

        for err in result.errors:
            print(f"  ERROR: {err}")
        for warn in result.warnings:
            print(f"  WARN:  {warn}")

        total_errors += len(result.errors)
        total_warnings += len(result.warnings)

    print(f"\n{'='*60}")
    print(f"Total: {len(bundle_dirs)} bundles, {total_errors} errors, {total_warnings} warnings")

    if total_errors > 0 or (args.strict and total_warnings > 0):
        sys.exit(1)


if __name__ == "__main__":
    main()
