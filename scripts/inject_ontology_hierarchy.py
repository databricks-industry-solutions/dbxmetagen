#!/usr/bin/env python3
"""Inject the class hierarchy (and FHIR W5 categories) into a formal bundle YAML.

Reads the bundle's stored OWL/TTL source, extracts each entity's superclasses, and
writes them back into the main bundle YAML on two separate axes:

  - `parents`    : true class taxonomy only (non-dotted supers, e.g. DomainResource,
                   Resource, OmopCDMThing) -> drives is_a edges at runtime.
  - `categories` : dotted FHIR W5 groupings (clinical.general, workflow.order, ...)
                   -> drives a separate `categorized_as` edge, so categorization
                   never pollutes the class hierarchy.

Abstract class ancestors and category nodes are added as minimal stub entities so
runtime seeding/edge-building has real targets, and a `categorized_as` entry is
added to the bundle edge_catalog when any categories are injected.

Surgical + idempotent: only fills fields that are missing and only adds stubs that
are absent; never touches relationships, cardinalities, properties, validation, or
domains. Re-running is a no-op. Rebuild tiers afterwards with build_bundle.py.

Usage:
    python scripts/inject_ontology_hierarchy.py --bundle fhir_r4
    python scripts/inject_ontology_hierarchy.py --bundle omop_cdm --dry-run
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from build_ontology_indexes import (  # noqa: E402
    SOURCES,
    _derive_keywords,
    _discover_fhir_resources,
    _discover_omop_classes,
    _discover_schema_org_classes,
    _extract_classes,
)

logger = logging.getLogger(__name__)

BUNDLES_DIR = REPO_ROOT / "configurations" / "ontology_bundles"
_DISCOVER = {
    "fhir_r4": _discover_fhir_resources,
    "omop_cdm": _discover_omop_classes,
    "schema_org": _discover_schema_org_classes,
}


def _is_category(name: str) -> bool:
    """W5 category groupings are encoded as dotted local names (clinical.general)."""
    return "." in name


def _extract_parents_from_source(bundle: str) -> Dict[str, List[str]]:
    """Return {entity_name: [superclass local names]} from the bundle's TTL source."""
    src = SOURCES[bundle]
    ttl = BUNDLES_DIR / bundle / f"source_{bundle}.ttl"
    if not ttl.is_file():
        logger.error("Source TTL not found: %s", ttl)
        sys.exit(1)
    import rdflib

    g = rdflib.Graph()
    g.parse(str(ttl), format=src["format"])
    classes = _DISCOVER[bundle](g, src["prefix"])
    entities = _extract_classes(g, classes, src["label"], src["prefix"])
    return {name: list(data.get("parents", []) or []) for name, data in entities.items()}


def _split(parents: List[str]) -> Tuple[List[str], List[str]]:
    """Split into (class parents for is_a, dotted W5 categories)."""
    klass = [p for p in parents if not _is_category(p)]
    cats = [p for p in parents if _is_category(p)]
    return klass, cats


def inject(bundle: str, dry_run: bool = False) -> int:
    parents_by_entity = _extract_parents_from_source(bundle)

    yaml_path = BUNDLES_DIR / f"{bundle}.yaml"
    doc = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    defs = doc.setdefault("ontology", {}).setdefault("entities", {}).setdefault("definitions", {})

    referenced_classes: set = set()
    referenced_categories: set = set()
    backfilled = added_class = added_cat = 0

    for name, raw_parents in parents_by_entity.items():
        klass, cats = _split(raw_parents)
        for k in klass:
            referenced_classes.add(k)
        for c in cats:
            referenced_categories.add(c)
        entry = defs.get(name)
        if not isinstance(entry, dict):
            continue
        if klass and not entry.get("parents"):
            entry["parents"] = klass
            backfilled += 1
        if cats and not entry.get("categories"):
            entry["categories"] = cats

    def _stub(name: str, suffix: str) -> Dict[str, Any]:
        desc = f"{name} ({suffix})"
        return {
            "description": desc,
            "keywords": _derive_keywords(name, desc),
            "typical_attributes": [],
            "relationships": {},
            "properties": {},
        }

    # Class ancestor stubs (carry their own class parents for a multi-level chain).
    for name in sorted(referenced_classes):
        if name in defs:
            continue
        stub = _stub(name, "abstract base class")
        klass, _ = _split(parents_by_entity.get(name, []))
        if klass:
            stub["parents"] = klass
        defs[name] = stub
        added_class += 1

    # Category node stubs (flat; no parents/categories of their own).
    for name in sorted(referenced_categories):
        if name in defs:
            continue
        defs[name] = _stub(name, "category")
        added_cat += 1

    # NOTE: categorized_as is intentionally NOT added to the bundle edge_catalog.
    # It is a structural edge auto-merged at runtime (OntologyLoader._STRUCTURAL_EDGES)
    # and emitted deterministically by _build_category_edges. Persisting it here
    # would push it into the tier indexes and make it a wildcard candidate for the
    # LLM edge predictor and FK inference (producing bogus "X categorized_as Y" edges).

    doc.setdefault("metadata", {})["entity_count"] = len(defs)

    logger.info(
        "%s: +%d parents on existing entities, +%d class stubs, +%d category stubs",
        bundle, backfilled, added_class, added_cat,
    )
    if dry_run:
        logger.info("Dry run, not writing %s", yaml_path)
        return 0

    yaml_path.write_text(
        yaml.dump(doc, default_flow_style=False, sort_keys=False, allow_unicode=True, width=120),
        encoding="utf-8",
    )
    logger.info("Wrote %s", yaml_path)
    return backfilled + added_class + added_cat


def main():
    parser = argparse.ArgumentParser(description="Inject class hierarchy + W5 categories into a bundle YAML")
    parser.add_argument("--bundle", required=True, choices=sorted(_DISCOVER), help="Formal bundle name")
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    inject(args.bundle, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
