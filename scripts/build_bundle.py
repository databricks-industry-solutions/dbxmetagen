#!/usr/bin/env python3
"""Integrated ontology bundle build pipeline.

Orchestrates: generate properties -> build tier indexes -> validate -> report.

Usage:
    python scripts/build_bundle.py --bundle healthcare          # single bundle
    python scripts/build_bundle.py --all                         # all bundles
    python scripts/build_bundle.py --bundle healthcare --validate-only
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = REPO_ROOT / "scripts"
BUNDLES_DIR = REPO_ROOT / "configurations" / "ontology_bundles"

ALL_BUNDLES = [
    "healthcare", "financial_services", "retail_cpg", "general",
    "fhir_r4", "omop_cdm", "schema_org",
]


def run(cmd: list[str], label: str) -> bool:
    logger.info("[%s] %s", label, " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout.strip():
        for line in result.stdout.strip().splitlines():
            print(f"  {line}")
    if result.returncode != 0:
        logger.error("[%s] FAILED (exit %d)", label, result.returncode)
        if result.stderr.strip():
            for line in result.stderr.strip().splitlines():
                print(f"  ERR: {line}")
        return False
    return True


def build_bundle(name: str, validate_only: bool = False) -> bool:
    print(f"\n{'='*60}")
    print(f"Building: {name}")
    print(f"{'='*60}")
    ok = True

    if not validate_only:
        # Step 1: Generate/update properties
        ok = run(
            [sys.executable, str(SCRIPTS / "generate_bundle_properties.py"),
             "--bundle", name, "--write"],
            "generate-properties",
        ) and ok

        # Step 2: Build tier indexes
        ok = run(
            [sys.executable, str(SCRIPTS / "build_ontology_indexes.py"),
             "--from-bundle", name, "--bundle-name", name],
            "build-tiers",
        ) and ok

    # Step 3: Validate
    tier_dir = BUNDLES_DIR / name
    if tier_dir.is_dir():
        ok = run(
            [sys.executable, str(SCRIPTS / "validate_ontology_bundles.py"),
             "--bundle", name],
            "validate",
        ) and ok
    else:
        logger.warning("No tier directory for %s, skipping validation", name)

    # Step 4: Coverage report
    _report_coverage(name)

    return ok


def _report_coverage(name: str):
    """Report property coverage from the root bundle YAML."""
    import yaml
    yaml_path = BUNDLES_DIR / f"{name}.yaml"
    if not yaml_path.is_file():
        return
    raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    defs = raw.get("ontology", {}).get("entities", {}).get("definitions", {})
    total = len(defs)
    with_props = sum(1 for d in defs.values() if d.get("properties"))
    prop_count = sum(
        len(d.get("properties", {}))
        for d in defs.values()
        if isinstance(d.get("properties"), dict)
    )
    print(f"\n  Coverage: {with_props}/{total} entities have properties ({prop_count} total properties)")


def main():
    parser = argparse.ArgumentParser(description="Build ontology bundle (properties + tiers + validate)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--bundle", help="Bundle name")
    group.add_argument("--all", action="store_true", help="Process all bundles")
    parser.add_argument("--validate-only", action="store_true", help="Only validate, don't generate")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    bundles = ALL_BUNDLES if args.all else [args.bundle]
    results = {}

    for name in bundles:
        results[name] = build_bundle(name, validate_only=args.validate_only)

    print(f"\n{'='*60}")
    print("Summary:")
    for name, ok in results.items():
        print(f"  {name}: {'PASS' if ok else 'FAIL'}")

    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
