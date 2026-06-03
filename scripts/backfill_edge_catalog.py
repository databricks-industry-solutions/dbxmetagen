#!/usr/bin/env python3
"""Backfill edge_catalog from entity relationships in custom bundle YAMLs.

For each entity's ``relationships`` block, generates a corresponding
``edge_catalog`` entry (if missing) with inferred domain, range, inverse,
and category.  Also generates inverse entries when the inverse target entity
exists in the bundle.

Usage:
    python scripts/backfill_edge_catalog.py [--dry-run] [--bundle general healthcare ...]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Set

import yaml

BUNDLE_DIR = Path(__file__).resolve().parent.parent / "configurations" / "ontology_bundles"

DEFAULT_BUNDLES = ["general", "healthcare", "financial_services", "retail_cpg"]

# Common inverse name heuristics
_INVERSE_MAP = {
    "has_patient": "patient_of",
    "has_encounter": "encounter_of",
    "has_provider": "provider_of",
    "has_diagnosis": "diagnosis_of",
    "has_procedure": "procedure_of",
    "has_medication": "medication_of",
    "has_observation": "observation_of",
    "has_claim": "claim_of",
    "has_coverage": "coverage_of",
    "has_practitioner": "practitioner_of",
    "has_organization": "organization_of",
    "has_location": "location_of",
    "has_member": "member_of",
    "has_employee": "employee_of",
    "has_department": "department_of",
    "has_product": "product_of",
    "has_order": "order_of",
    "has_customer": "customer_of",
    "has_supplier": "supplier_of",
    "has_transaction": "transaction_of",
    "has_account": "account_of",
    "has_holding": "holding_of",
    "has_instrument": "instrument_of",
    "has_portfolio": "portfolio_of",
    "belongs_to": "contains",
    "contains": "belongs_to",
    "member_of": "has_member",
    "part_of": "has_part",
    "has_part": "part_of",
    "references": "referenced_by",
    "referenced_by": "references",
    "associated_with": "associated_with",
    "prescribed_for": "has_prescription",
    "performed_for": "has_procedure",
    "ordered_by": "has_order",
    "supplied_by": "has_supplier",
    "manufactured_by": "has_manufacturer",
    "located_in": "has_location",
    "employs": "employed_by",
    "employed_by": "employs",
    "manages": "managed_by",
    "managed_by": "manages",
}


def _infer_inverse(rel_name: str) -> str | None:
    """Infer inverse relationship name from a relationship name."""
    if rel_name in _INVERSE_MAP:
        return _INVERSE_MAP[rel_name]
    # has_X -> X_of, X_of -> has_X
    if rel_name.startswith("has_"):
        return rel_name[4:] + "_of"
    if rel_name.endswith("_of"):
        return "has_" + rel_name[:-3]
    return None


def _infer_category(rel_name: str) -> str:
    """Infer edge category from relationship name."""
    structural = {"contains", "belongs_to", "part_of", "has_part", "member_of", "has_member",
                  "instance_of", "subclass_of"}
    if rel_name in structural:
        return "structural"
    return "business"


def backfill_bundle(data: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """Backfill edge_catalog entries from entity relationships. Returns stats."""
    ontology = data.get("ontology", {})
    definitions = ontology.get("entities", {}).get("definitions", {})
    edge_catalog = ontology.setdefault("edge_catalog", {})

    entity_names = set(definitions.keys())
    stats = {"added": 0, "inverse_added": 0, "skipped_existing": 0}
    new_entries: List[Dict[str, Any]] = []

    for ent_name, ent_def in definitions.items():
        rels = ent_def.get("relationships", {})
        for rel_name, rel_info in rels.items():
            if rel_name in edge_catalog:
                stats["skipped_existing"] += 1
                continue

            target = rel_info.get("target", "Any") if isinstance(rel_info, dict) else "Any"
            inverse = _infer_inverse(rel_name)
            category = _infer_category(rel_name)

            entry = {
                "domain": ent_name,
                "range": target,
                "category": category,
            }
            if inverse:
                entry["inverse"] = inverse

            new_entries.append((rel_name, entry, inverse, target, ent_name))

    # Apply new entries
    for rel_name, entry, inverse, target, domain in new_entries:
        if rel_name not in edge_catalog:
            if not dry_run:
                edge_catalog[rel_name] = entry
            stats["added"] += 1

            # Generate inverse entry if the target entity exists and inverse doesn't exist
            if inverse and inverse not in edge_catalog and target in entity_names:
                inv_entry = {
                    "domain": target,
                    "range": domain,
                    "inverse": rel_name,
                    "category": entry.get("category", "business"),
                }
                if not dry_run:
                    edge_catalog[inverse] = inv_entry
                stats["inverse_added"] += 1

    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bundle", nargs="*", default=DEFAULT_BUNDLES,
                        help="Bundle names to process (default: all custom bundles)")
    args = parser.parse_args()

    for bundle_name in args.bundle:
        path = BUNDLE_DIR / f"{bundle_name}.yaml"
        if not path.is_file():
            print(f"SKIP: {path} not found", file=sys.stderr)
            continue

        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        stats = backfill_bundle(data, dry_run=args.dry_run)

        print(f"\n{bundle_name}:")
        print(f"  Added:            {stats['added']}")
        print(f"  Inverse added:    {stats['inverse_added']}")
        print(f"  Skipped existing: {stats['skipped_existing']}")

        if args.dry_run:
            print("  (dry-run -- no files written)")
        else:
            path.write_text(
                yaml.dump(data, default_flow_style=False, allow_unicode=True, width=120, sort_keys=False),
                encoding="utf-8",
            )
            print(f"  Wrote {path}")


if __name__ == "__main__":
    main()
