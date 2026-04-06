#!/usr/bin/env python3
"""CI: tier YAML validation + optional OWL source checks + v2 JSON-schema-lite."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUNDLES = ROOT / "configurations" / "ontology_bundles"


def _v2_lite_check(bundle_yaml: Path) -> list[str]:
    issues: list[str] = []
    try:
        import yaml

        raw = yaml.safe_load(bundle_yaml.read_text(encoding="utf-8"))
    except Exception as e:
        return [f"{bundle_yaml.name}: {e}"]
    meta = raw.get("metadata") or {}
    if meta.get("format_version") == "2.0":
        if not meta.get("name"):
            issues.append(f"{bundle_yaml.name}: v2 bundle missing metadata.name")
        ont = raw.get("ontology") or {}
        if not ont.get("entities", {}).get("definitions"):
            issues.append(f"{bundle_yaml.name}: v2 bundle missing ontology.entities.definitions")
    return issues


def _edge_tier3_duplicate_warnings(bundle_dir: Path) -> list[str]:
    """Flag bundles where edges_tier3 is identical to edges_tier2 (no forward enrichment)."""
    t2 = bundle_dir / "edges_tier2.yaml"
    t3 = bundle_dir / "edges_tier3.yaml"
    if not t2.is_file() or not t3.is_file():
        return []
    try:
        import yaml

        d2 = yaml.safe_load(t2.read_text(encoding="utf-8")) or {}
        d3 = yaml.safe_load(t3.read_text(encoding="utf-8")) or {}
    except Exception:
        return []
    if not isinstance(d2, dict) or not isinstance(d3, dict):
        return []
    if not d2 or not d3:
        return []
    if d2 == d3:
        return [f"{bundle_dir.name}: edges_tier3 equals edges_tier2 (no tier-3 enrichment)"]
    return []


def main() -> int:
    sys.path.insert(0, str(ROOT / "src"))
    from dbxmetagen.ontology_index import validate_bundle as validate_tiers

    tier_errors: list[str] = []
    for d in sorted(BUNDLES.iterdir()):
        if not d.is_dir():
            continue
        if not (d / "entities_tier1.yaml").is_file():
            continue
        issues = validate_tiers(d.name)
        tier_errors.extend(f"{d.name}: {i}" for i in issues)

    v2_issues: list[str] = []
    for f in sorted(BUNDLES.glob("*.yaml")):
        v2_issues.extend(_v2_lite_check(f))

    edge_tier3_dup: list[str] = []
    for d in sorted(BUNDLES.iterdir()):
        if not d.is_dir():
            continue
        edge_tier3_dup.extend(_edge_tier3_duplicate_warnings(d))

    strict_edge = os.environ.get("STRICT_EDGE_TIER3", "").strip() in ("1", "true", "yes")

    owl_issues: list[str] = []
    try:
        from dbxmetagen.ontology_validate import validate_bundle_auto
    except Exception:
        validate_bundle_auto = None  # type: ignore

    if validate_bundle_auto:
        for d in sorted(BUNDLES.iterdir()):
            if not d.is_dir():
                continue
            src = list(d.glob("source_*.ttl")) + list(d.glob("source_*.owl")) + list(d.glob("source.*"))
            if not src:
                continue
            rep = validate_bundle_auto(d.name, str(BUNDLES))
            if rep.get("error"):
                continue
            if rep.get("invalid_entities"):
                for e in rep["invalid_entities"][:3]:
                    owl_issues.append(f"{d.name}: entity {e.get('name')}: {e.get('reason')}")

    all_err = tier_errors + v2_issues + owl_issues
    for line in all_err:
        print(line, file=sys.stderr)
    for line in edge_tier3_dup:
        print(line, file=sys.stderr)
    if tier_errors or v2_issues:
        print(f"FAIL: {len(tier_errors)} tier issues, {len(v2_issues)} v2 issues", file=sys.stderr)
        return 1
    if strict_edge and edge_tier3_dup:
        print(
            f"FAIL: STRICT_EDGE_TIER3: {len(edge_tier3_dup)} edge tier-3 duplicate bundles",
            file=sys.stderr,
        )
        return 1
    if edge_tier3_dup and not strict_edge:
        print(f"WARN: {len(edge_tier3_dup)} edge tier-3 duplicate notes (set STRICT_EDGE_TIER3=1 to fail)", file=sys.stderr)
    if owl_issues:
        print(f"WARN: {len(owl_issues)} OWL cross-check notes (non-fatal)", file=sys.stderr)
    print("OK: ontology bundle validation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
