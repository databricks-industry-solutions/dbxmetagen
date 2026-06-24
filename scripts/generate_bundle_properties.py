#!/usr/bin/env python3
"""Generate `properties:` blocks for ontology bundle entities.

Thin CLI wrapper around :mod:`dbxmetagen.ontology_properties`, which holds the
extraction logic (shared with the app importer). This script keeps the file IO,
summary printing, and argument parsing.

Two strategies (see ontology_properties):
  - OWL-type: for formal_ontology bundles (fhir_r4, omop_cdm, schema_org).
  - Pattern: for curated bundles (healthcare, financial_services, retail_cpg, general).

Usage:
    python scripts/generate_bundle_properties.py --bundle healthcare        # dry-run
    python scripts/generate_bundle_properties.py --bundle healthcare --write
    python scripts/generate_bundle_properties.py --all --write
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from dbxmetagen.ontology_properties import (  # noqa: F401  (re-exported for callers/tests)
    detect_source,
    generate_owl_properties,
    generate_pattern_properties,
    process_bundle,
)

logger = logging.getLogger(__name__)

BUNDLES_DIR = Path(__file__).resolve().parent.parent / "configurations" / "ontology_bundles"

ALL_BUNDLES = [
    "healthcare", "financial_services", "retail_cpg", "general",
    "fhir_r4", "omop_cdm", "schema_org",
]


def print_summary(bundle: Dict[str, Any]):
    """Print role distribution and sample entities."""
    defs = bundle["ontology"]["entities"]["definitions"]
    roles_hist: Dict[str, int] = {}
    entities_with_props = 0
    for defn in defs.values():
        props = defn.get("properties", {})
        if props:
            entities_with_props += 1
        for p in (props.values() if isinstance(props, dict) else []):
            r = p.get("role", "unknown")
            roles_hist[r] = roles_hist.get(r, 0) + 1

    print(f"\n  Entities: {len(defs)}, with properties: {entities_with_props}")
    print("  Role distribution:")
    for r, c in sorted(roles_hist.items(), key=lambda x: -x[1]):
        print(f"    {r}: {c}")


def main():
    parser = argparse.ArgumentParser(description="Generate properties for ontology bundle entities")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--bundle", help="Bundle name (e.g. healthcare, fhir_r4)")
    group.add_argument("--all", action="store_true", help="Process all bundles")
    parser.add_argument("--write", action="store_true", help="Write changes to YAML files")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    bundles = ALL_BUNDLES if args.all else [args.bundle]

    for bname in bundles:
        yaml_path = BUNDLES_DIR / f"{bname}.yaml"
        if not yaml_path.is_file():
            logger.warning("Bundle not found: %s", yaml_path)
            continue

        logger.info("Processing %s", bname)
        raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))

        ent_count, new_props, preserved = process_bundle(raw, bname)
        logger.info("  %d entities, %d new properties, %d preserved", ent_count, new_props, preserved)
        print_summary(raw)

        if args.write:
            yaml_path.write_text(
                yaml.dump(raw, default_flow_style=False, sort_keys=False, allow_unicode=True, width=120),
                encoding="utf-8",
            )
            logger.info("  Wrote %s", yaml_path)

    if not args.write:
        print("\nDry run. Use --write to save changes.")


if __name__ == "__main__":
    main()
