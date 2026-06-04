#!/usr/bin/env python3
"""Resolve FHIR Reference indirection in the committed fhir_r4.yaml bundle.

Transforms ``target: Reference`` entries in both ``relationships`` and
``edge_catalog`` to concrete entity types using a static map derived from
FHIR R4 naming conventions.

This is a YAML-level patch -- it does NOT re-parse the FHIR TTL.
Run with ``--dry-run`` to preview changes without writing.

Usage:
    python scripts/resolve_fhir_references.py [--dry-run]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict

import yaml

_FHIR_REFERENCE_TARGETS: Dict[str, str] = {
    "subject": "Patient", "patient": "Patient", "encounter": "Encounter",
    "performer": "Practitioner", "provider": "Practitioner",
    "practitioner": "Practitioner", "organization": "Organization",
    "managingOrganization": "Organization", "serviceProvider": "Organization",
    "location": "Location", "insurer": "Organization", "facility": "Location",
    "device": "Device", "specimen": "Specimen", "recorder": "Practitioner",
    "asserter": "Practitioner", "requester": "Practitioner",
    "sender": "Organization", "receiver": "Organization",
    "author": "Practitioner", "source": "Patient", "focus": "Resource",
    "basedOn": "ServiceRequest", "partOf": "Resource",
    "reasonReference": "Condition", "hasMember": "Observation",
    "derivedFrom": "Observation", "generalPractitioner": "Practitioner",
    "referral": "ServiceRequest", "prescription": "MedicationRequest",
    "originalPrescription": "MedicationRequest", "enterer": "Practitioner",
    "appointment": "Appointment", "episodeOfCare": "EpisodeOfCare",
    "account": "Account", "manufacturer": "Organization",
}

# Don't resolve to these -- they're abstract/non-entity
_SKIP_TARGETS = {"Resource"}


def _local_name(dotted: str) -> str:
    """Extract local property name from a dotted FHIR name like 'Observation.subject'."""
    return dotted.rsplit(".", 1)[-1]


def _resolve(local: str, entity_names: set) -> str | None:
    """Return the resolved entity type or None if unresolvable."""
    target = _FHIR_REFERENCE_TARGETS.get(local)
    if target and target not in _SKIP_TARGETS and target in entity_names:
        return target
    return None


def resolve_bundle(data: Dict[str, Any], dry_run: bool = False) -> Dict[str, int]:
    """Resolve Reference targets in-place. Returns resolution stats."""
    ontology = data.get("ontology", {})
    definitions = ontology.get("entities", {}).get("definitions", {})
    edge_catalog = ontology.get("edge_catalog", {})

    entity_names = set(definitions.keys())
    stats = {"relationships_resolved": 0, "relationships_kept": 0,
             "catalog_resolved": 0, "catalog_kept": 0}

    # Resolve in entity relationships
    for ent_name, ent_def in definitions.items():
        rels = ent_def.get("relationships", {})
        for rel_name, rel_info in rels.items():
            if not isinstance(rel_info, dict) or rel_info.get("target") != "Reference":
                continue
            local = _local_name(rel_name)
            resolved = _resolve(local, entity_names)
            if resolved:
                if not dry_run:
                    rel_info["target"] = resolved
                stats["relationships_resolved"] += 1
            else:
                stats["relationships_kept"] += 1

    # Resolve in edge_catalog
    for edge_name, edge_info in edge_catalog.items():
        if not isinstance(edge_info, dict):
            continue
        target_key = "target" if "target" in edge_info else ("range" if "range" in edge_info else None)
        if not target_key or edge_info[target_key] != "Reference":
            continue
        local = _local_name(edge_name)
        resolved = _resolve(local, entity_names)
        if resolved:
            if not dry_run:
                edge_info[target_key] = resolved
            stats["catalog_resolved"] += 1
        else:
            stats["catalog_kept"] += 1

    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    parser.add_argument("--bundle", default="fhir_r4", help="Bundle name (default: fhir_r4)")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    bundle_path = repo_root / "configurations" / "ontology_bundles" / f"{args.bundle}.yaml"
    if not bundle_path.is_file():
        print(f"ERROR: {bundle_path} not found", file=sys.stderr)
        sys.exit(1)

    data = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
    stats = resolve_bundle(data, dry_run=args.dry_run)

    print(f"Bundle: {args.bundle}")
    print(f"  Relationships resolved: {stats['relationships_resolved']}")
    print(f"  Relationships kept as Reference: {stats['relationships_kept']}")
    print(f"  Edge catalog resolved: {stats['catalog_resolved']}")
    print(f"  Edge catalog kept as Reference: {stats['catalog_kept']}")

    if args.dry_run:
        print("\n  (dry-run -- no files written)")
    else:
        bundle_path.write_text(
            yaml.dump(data, default_flow_style=False, allow_unicode=True, width=120, sort_keys=False),
            encoding="utf-8",
        )
        print(f"\n  Wrote {bundle_path}")


if __name__ == "__main__":
    main()
