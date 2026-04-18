#!/usr/bin/env python3
"""Generate `properties:` blocks for ontology bundle entities.

Two strategies:
  - OWL-type: for formal_ontology bundles (fhir_r4, omop_cdm, schema_org).
    Maps relationship target types to property roles via source-specific type maps.
  - Pattern: for curated bundles (healthcare, financial_services, retail_cpg, general).
    Infers roles from entity-level typical_attributes using column-name patterns.

Both strategies preserve existing hand-curated properties and sync property_roles
from the canonical registry.

Usage:
    python scripts/generate_bundle_properties.py --bundle healthcare        # dry-run
    python scripts/generate_bundle_properties.py --bundle healthcare --write
    python scripts/generate_bundle_properties.py --all --write
"""

import argparse
import copy
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from dbxmetagen.ontology_roles import (
    infer_role_from_column_name,
    property_roles_for_yaml,
    resolve_fk_target,
)

logger = logging.getLogger(__name__)

BUNDLES_DIR = Path(__file__).resolve().parent.parent / "configurations" / "ontology_bundles"

ALL_BUNDLES = [
    "healthcare", "financial_services", "retail_cpg", "general",
    "fhir_r4", "omop_cdm", "schema_org",
]

# ---------------------------------------------------------------------------
# camelCase -> snake_case
# ---------------------------------------------------------------------------

def _camel_to_snake(name: str) -> str:
    s1 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s1).lower()


# ===================================================================
# OWL-type strategy: formal ontology bundles
# ===================================================================

# -- FHIR type maps (imported from generate_fhir_properties.py logic) --

_FHIR_TEMPORAL = {"date", "dateTime", "instant", "Period", "Timing"}
_FHIR_DIMENSION = {"code", "CodeableConcept", "Coding", "boolean"}
_FHIR_MEASURE = {
    "Money", "Quantity", "integer", "decimal", "positiveInt", "unsignedInt",
    "Age", "Duration", "Count", "Distance", "SimpleQuantity",
}
_FHIR_PII = {"HumanName", "Address", "ContactPoint"}
_FHIR_LABEL = {"string", "markdown", "Annotation", "Attachment", "Narrative"}

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

FHIR_ABBREVIATION_MAP: Dict[str, List[str]] = {
    "birthDate": ["birth_date", "birthdate", "dob", "date_of_birth"],
    "gender": ["gender", "sex", "gender_code"],
    "name": ["name", "full_name", "first_name", "last_name", "patient_name"],
    "address": ["address", "street_address", "addr"],
    "telecom": ["phone", "email", "telecom", "phone_number", "contact"],
    "status": ["status"], "active": ["active", "is_active"],
    "code": ["code"], "category": ["category"], "type": ["type"],
    "subject": ["subject_id", "patient_id"],
    "patient": ["patient_id", "pat_id", "member_id"],
    "encounter": ["encounter_id", "visit_id"],
    "performer": ["performer_id", "provider_id"],
    "provider": ["provider_id", "attending_id"],
    "practitioner": ["practitioner_id", "provider_id", "npi"],
    "organization": ["organization_id", "org_id"],
    "location": ["location_id", "facility_id"],
    "device": ["device_id"], "specimen": ["specimen_id", "sample_id"],
    "insurer": ["insurer_id", "payer_id"],
    "facility": ["facility_id", "location_id"],
    "created": ["created", "created_date", "created_at"],
    "issued": ["issued", "issued_date", "issue_date"],
    "recorded": ["recorded_date", "recorded_at"],
    "recordedDate": ["recorded_date", "record_date"],
    "effectiveDateTime": ["effective_date", "effective_date_time", "observation_date"],
    "effectivePeriod": ["effective_period", "effective_start", "effective_end"],
    "onsetDateTime": ["onset_date", "onset_date_time"],
    "abatementDateTime": ["abatement_date", "resolution_date"],
    "billablePeriod": ["billable_period", "service_period", "billing_period"],
    "period": ["period", "start_date", "end_date"],
    "note": ["note", "notes", "comment"],
    "description": ["description", "desc"],
    "severity": ["severity", "severity_code"],
    "priority": ["priority", "priority_code"],
    "bodySite": ["body_site", "body_site_code", "anatomical_site"],
    "method": ["method", "method_code"],
    "interpretation": ["interpretation", "interpretation_code"],
    "maritalStatus": ["marital_status", "marital_status_code"],
    "communication": ["communication", "language", "preferred_language"],
    "clinicalStatus": ["clinical_status", "clinical_status_code"],
    "verificationStatus": ["verification_status", "verification_status_code"],
    "valueQuantity": ["value_quantity", "value", "result_value", "numeric_value"],
    "valueCodeableConcept": ["value_code", "value_codeable_concept"],
    "valueString": ["value_string", "value_text"],
    "valueBoolean": ["value_boolean", "value_flag"],
    "valueInteger": ["value_integer", "value_int"],
    "valueDateTime": ["value_date_time", "value_date"],
    "use": ["use", "use_code"], "intent": ["intent", "intent_code"],
    "total": ["total", "total_amount"],
    "amount": ["amount", "total_charge", "billed_amount"],
}

# -- OMOP type maps --

_OMOP_DIMENSION = {"Concept", "Domain", "Vocabulary"}
_OMOP_LABEL = {"Thing"}
_OMOP_TEMPORAL: Set[str] = set()
_OMOP_MEASURE: Set[str] = set()

# -- Schema.org type maps --

_SCHEMAORG_TEMPORAL = {"Date", "DateTime", "Time"}
_SCHEMAORG_MEASURE = {"Integer", "Number", "Float", "QuantitativeValue"}
_SCHEMAORG_LABEL = {"Text", "URL"}
_SCHEMAORG_DIMENSION = {"Boolean"}


def _map_type_to_role(target: str, source: str, all_entities: Set[str]) -> Optional[str]:
    """Map a relationship target type to a property role, source-aware."""
    if source == "fhir_r4":
        if target == "Identifier":
            return "business_key"
        if target == "Reference":
            return "object_property"
        if target in _FHIR_PII:
            return "pii"
        if target in _FHIR_TEMPORAL:
            return "temporal"
        if target in _FHIR_DIMENSION:
            return "dimension"
        if target in _FHIR_MEASURE:
            return "measure"
        if target in _FHIR_LABEL:
            return "label"
        # Component types
        if target.endswith("Component") or "." in target:
            return None
        if target in all_entities:
            return "object_property"
        return "label"

    if source == "omop_cdm":
        if target in _OMOP_DIMENSION:
            return "dimension"
        if target in _OMOP_LABEL:
            return "label"
        if target in all_entities:
            return "object_property"
        return "label"

    if source == "schema_org":
        if target in _SCHEMAORG_TEMPORAL:
            return "temporal"
        if target in _SCHEMAORG_MEASURE:
            return "measure"
        if target in _SCHEMAORG_LABEL:
            return "label"
        if target in _SCHEMAORG_DIMENSION:
            return "dimension"
        if target in all_entities:
            return "object_property"
        return "label"

    return "label"


def _owl_typical_attrs(
    attr_name: str, entity_name: str, role: str, source: str,
) -> List[str]:
    """Generate column-name variants for a formal-ontology attribute."""
    ent_snake = _camel_to_snake(entity_name)

    if source == "fhir_r4" and attr_name in FHIR_ABBREVIATION_MAP:
        base = [v.replace("{entity}", ent_snake) for v in FHIR_ABBREVIATION_MAP[attr_name]]
        if role == "object_property":
            snake = _camel_to_snake(attr_name)
            if not snake.endswith("_id") and f"{snake}_id" not in base:
                base.append(f"{snake}_id")
        return base

    snake = _camel_to_snake(attr_name)
    variants = [snake]
    joined = attr_name.lower()
    if joined != snake and joined not in variants:
        variants.append(joined)

    if role in ("primary_key", "business_key"):
        if "id" not in variants:
            variants.insert(0, "id")
        variants.append(f"{ent_snake}_id")
    elif role == "object_property":
        if not snake.endswith("_id"):
            variants.append(f"{snake}_id")

    return variants


def _owl_resolve_target(attr_name: str, target_type: str, source: str, all_entities: Set[str]) -> Optional[str]:
    """Resolve object_property target entity for formal bundles."""
    if source == "fhir_r4":
        return _FHIR_REFERENCE_TARGETS.get(attr_name)
    if target_type in all_entities:
        return target_type
    return None


def generate_owl_properties(
    entity_name: str,
    defn: Dict[str, Any],
    source: str,
    all_entities: Set[str],
) -> Dict[str, Dict[str, Any]]:
    """Generate properties for one entity in a formal-ontology bundle."""
    relationships = defn.get("relationships", {})
    typical_attrs = defn.get("typical_attributes", [])

    # For FHIR, iterate over typical_attributes (dotted paths) + relationships
    # For OMOP/schema.org, iterate over relationships directly
    props: Dict[str, Dict[str, Any]] = {}
    seen_identifier = False

    if source == "fhir_r4":
        for dotted in typical_attrs:
            parts = dotted.split(".", 1)
            attr_name = parts[1] if len(parts) == 2 else parts[0]
            rel = relationships.get(dotted, {})
            target_type = rel.get("target", "")
            role = _map_type_to_role(target_type, source, all_entities)
            if role is None:
                continue
            if role == "business_key" and not seen_identifier:
                role = "primary_key"
                seen_identifier = True
            kind = "object_property" if role == "object_property" else "data_property"
            entry: Dict[str, Any] = {
                "kind": kind, "role": role,
                "typical_attributes": _owl_typical_attrs(attr_name, entity_name, role, source),
            }
            if role == "object_property":
                entry["edge"] = _camel_to_snake(attr_name)
                tgt = _owl_resolve_target(attr_name, target_type, source, all_entities)
                if tgt:
                    entry["target_entity"] = tgt
            key = attr_name if attr_name not in props else f"{attr_name}_{role}"
            props[key] = entry
    else:
        # OMOP / schema.org: iterate over relationships
        for rel_name, rel_info in relationships.items():
            target_type = rel_info.get("target", "")
            role = _map_type_to_role(target_type, source, all_entities)
            if role is None:
                continue
            kind = "object_property" if role == "object_property" else "data_property"
            entry = {
                "kind": kind, "role": role,
                "typical_attributes": _owl_typical_attrs(rel_name, entity_name, role, source),
            }
            if role == "object_property":
                entry["edge"] = _camel_to_snake(rel_name)
                tgt = _owl_resolve_target(rel_name, target_type, source, all_entities)
                if tgt:
                    entry["target_entity"] = tgt
            key = rel_name if rel_name not in props else f"{rel_name}_{role}"
            props[key] = entry

        # Also process typical_attributes not covered by relationships
        rel_covered = set(relationships.keys())
        for attr in typical_attrs:
            if attr in rel_covered:
                continue
            snake = _camel_to_snake(attr)
            variants = [snake]
            joined = attr.lower()
            if joined != snake:
                variants.append(joined)
            role = infer_role_from_column_name(snake, entity_name, all_entities) or "label"
            kind = "object_property" if role == "object_property" else "data_property"
            entry = {"kind": kind, "role": role, "typical_attributes": variants}
            if role == "object_property":
                entry["edge"] = snake.replace("_id", "").replace("_key", "")
                tgt = resolve_fk_target(snake, all_entities)
                if tgt:
                    entry["target_entity"] = tgt
            props[attr] = entry

    return props


# ===================================================================
# Pattern strategy: curated bundles
# ===================================================================

def generate_pattern_properties(
    entity_name: str,
    defn: Dict[str, Any],
    all_entity_names: frozenset,
    edge_catalog: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """Generate properties for one entity in a curated bundle using column-name patterns."""
    typical_attrs = defn.get("typical_attributes", [])
    relationships = defn.get("relationships", {})

    props: Dict[str, Dict[str, Any]] = {}

    for attr in typical_attrs:
        role = infer_role_from_column_name(attr, entity_name, all_entity_names)
        if role is None:
            role = "label"

        kind = "object_property" if role == "object_property" else "data_property"
        entry: Dict[str, Any] = {
            "kind": kind, "role": role,
            "typical_attributes": [attr],
        }

        if role == "object_property":
            snake = attr.lower()
            edge_name = snake.replace("_id", "").replace("_key", "")
            target = resolve_fk_target(attr, all_entity_names)
            if target:
                entry["target_entity"] = target
            # Try to find a matching edge in edge_catalog
            matched_edge = _find_edge(entity_name, target, edge_catalog, relationships)
            entry["edge"] = matched_edge or f"has_{edge_name}"

        props[attr] = entry

    return props


def _find_edge(
    src_entity: str,
    target_entity: Optional[str],
    edge_catalog: Dict[str, Any],
    relationships: Dict[str, Any],
) -> Optional[str]:
    """Find an appropriate edge name from edge_catalog or per-entity relationships."""
    if not target_entity:
        return None
    # Check per-entity relationships first
    for rel_name, rel_info in relationships.items():
        tgt = rel_info.get("target", "")
        if tgt == target_entity:
            return rel_name
    # Check edge_catalog
    for edge_name, edge_info in edge_catalog.items():
        domain = edge_info.get("domain", "Any")
        rng = edge_info.get("range", "Any")
        if isinstance(domain, list):
            domain_match = src_entity in domain
        else:
            domain_match = domain in (src_entity, "Any")
        if isinstance(rng, list):
            range_match = target_entity in rng
        else:
            range_match = rng in (target_entity, "Any")
        if domain_match and range_match:
            return edge_name
    return None


# ===================================================================
# Common logic
# ===================================================================

def detect_source(bundle: Dict[str, Any]) -> str:
    """Detect the bundle source for strategy selection."""
    meta = bundle.get("metadata", {})
    name = meta.get("name", "").lower()
    bundle_type = meta.get("bundle_type", "")
    if bundle_type == "formal_ontology":
        if "fhir" in name:
            return "fhir_r4"
        if "omop" in name:
            return "omop_cdm"
        if "schema" in name:
            return "schema_org"
        return "formal_unknown"
    return "curated"


def process_bundle(bundle: Dict[str, Any], bundle_name: str = "") -> Tuple[int, int, int]:
    """Generate properties for all entities. Returns (entity_count, new_props, preserved)."""
    source = detect_source(bundle)
    defs = bundle.get("ontology", {}).get("entities", {}).get("definitions", {})
    edge_catalog = bundle.get("ontology", {}).get("edge_catalog", {})
    all_entity_names = frozenset(defs.keys())

    # Sync property_roles from canonical registry
    bundle.setdefault("ontology", {})["property_roles"] = property_roles_for_yaml()

    total_new = 0
    total_preserved = 0

    for ent_name, defn in defs.items():
        existing = defn.get("properties", {})
        if isinstance(existing, list):
            existing = {}

        if source == "curated":
            generated = generate_pattern_properties(ent_name, defn, all_entity_names, edge_catalog)
        else:
            generated = generate_owl_properties(ent_name, defn, source, set(all_entity_names))

        # Merge: existing hand-curated properties take precedence
        merged = copy.deepcopy(generated)
        for k, v in existing.items():
            merged[k] = v
            total_preserved += 1

        new_count = len(merged) - len(existing)
        total_new += new_count

        if merged:
            defn["properties"] = merged

    return len(defs), total_new, total_preserved


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
