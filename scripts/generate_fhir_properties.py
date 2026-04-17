#!/usr/bin/env python3
"""Generate nested `properties:` blocks for every entity in fhir_r4.yaml.

Reads FHIR relationship target types to deterministically assign property roles
and generates snake_case column-name variants for bundle_match classification.

Usage:
    python scripts/generate_fhir_properties.py            # dry-run (stdout summary)
    python scripts/generate_fhir_properties.py --write     # overwrite fhir_r4.yaml in place
"""

import argparse
import copy
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FHIR target-type -> role mapping
# ---------------------------------------------------------------------------

_TEMPORAL_TYPES: Set[str] = {
    "date", "dateTime", "instant", "Period", "Timing",
}
_DIMENSION_TYPES: Set[str] = {
    "code", "CodeableConcept", "Coding", "boolean",
}
_MEASURE_TYPES: Set[str] = {
    "Money", "Quantity", "integer", "decimal", "positiveInt", "unsignedInt",
    "Age", "Duration", "Count", "Distance", "SimpleQuantity",
}
_PII_TYPES: Set[str] = {
    "HumanName", "Address", "ContactPoint",
}
_LABEL_TYPES: Set[str] = {
    "string", "markdown", "Annotation", "Attachment", "Narrative",
}
_IDENTIFIER_TYPE = "Identifier"
_REFERENCE_TYPE = "Reference"

# Component types that represent FHIR backbone elements (skip as properties)
_COMPONENT_SUFFIXES = ("Component",)


def _fhir_type_to_role(target_type: str) -> Optional[str]:
    if target_type == _IDENTIFIER_TYPE:
        return "business_key"
    if target_type == _REFERENCE_TYPE:
        return "object_property"
    if target_type in _PII_TYPES:
        return "pii"
    if target_type in _TEMPORAL_TYPES:
        return "temporal"
    if target_type in _DIMENSION_TYPES:
        return "dimension"
    if target_type in _MEASURE_TYPES:
        return "measure"
    if target_type in _LABEL_TYPES:
        return "label"
    return None


# ---------------------------------------------------------------------------
# camelCase -> snake_case conversion
# ---------------------------------------------------------------------------

def _camel_to_snake(name: str) -> str:
    s1 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    s2 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s1)
    return s2.lower()


# ---------------------------------------------------------------------------
# Well-known FHIR attribute abbreviation overrides
# ---------------------------------------------------------------------------

ABBREVIATION_MAP: Dict[str, List[str]] = {
    "birthDate": ["birth_date", "birthdate", "dob", "date_of_birth"],
    "gender": ["gender", "sex", "gender_code"],
    "name": ["name", "full_name", "first_name", "last_name", "patient_name"],
    "address": ["address", "street_address", "addr"],
    "telecom": ["phone", "email", "telecom", "phone_number", "contact"],
    "status": ["status"],
    "active": ["active", "is_active"],
    "code": ["code"],
    "category": ["category"],
    "type": ["type"],
    "subject": ["subject_id", "patient_id"],
    "patient": ["patient_id", "pat_id", "member_id"],
    "encounter": ["encounter_id", "visit_id"],
    "performer": ["performer_id", "provider_id"],
    "provider": ["provider_id", "attending_id"],
    "practitioner": ["practitioner_id", "provider_id", "npi"],
    "organization": ["organization_id", "org_id"],
    "location": ["location_id", "facility_id"],
    "device": ["device_id"],
    "specimen": ["specimen_id", "sample_id"],
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
    "photo": ["photo", "image"],
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
    "valuePeriod": ["value_period"],
    "use": ["use", "use_code"],
    "intent": ["intent", "intent_code"],
    "total": ["total", "total_amount"],
    "amount": ["amount", "total_charge", "billed_amount"],
}


def _generate_typical_attrs(
    attr_name: str, entity_name: str, role: str,
) -> List[str]:
    """Generate column-name variants for a FHIR attribute."""
    ent_snake = _camel_to_snake(entity_name).lower()

    if attr_name in ABBREVIATION_MAP:
        base = list(ABBREVIATION_MAP[attr_name])
        expanded = [v.replace("{entity}", ent_snake) for v in base]
        if role == "object_property":
            snake = _camel_to_snake(attr_name)
            if not snake.endswith("_id"):
                snake_id = f"{snake}_id"
                if snake_id not in expanded:
                    expanded.append(snake_id)
        return expanded

    snake = _camel_to_snake(attr_name)
    variants = [snake]
    joined = attr_name.lower()
    if joined != snake:
        variants.append(joined)

    if role in ("primary_key", "business_key"):
        variants.append(f"{ent_snake}_id")
        if "id" not in variants:
            variants.insert(0, "id")
        if "identifier" not in variants:
            variants.append("identifier")
    elif role == "object_property":
        if not snake.endswith("_id"):
            variants.append(f"{snake}_id")

    return variants


# ---------------------------------------------------------------------------
# Property roles block (same 12 roles as healthcare.yaml)
# ---------------------------------------------------------------------------

PROPERTY_ROLES = {
    "primary_key": {
        "description": "Surrogate or technical row identifier",
        "maps_to_kind": "data_property",
        "semantic_role": "identifier",
    },
    "business_key": {
        "description": "Natural key meaningful to users (MRN, NPI)",
        "maps_to_kind": "data_property",
        "semantic_role": "identifier",
    },
    "measure": {
        "description": "Numeric value to aggregate",
        "maps_to_kind": "data_property",
        "semantic_role": "metric",
    },
    "dimension": {
        "description": "Categorical filter or grouping attribute",
        "maps_to_kind": "data_property",
        "semantic_role": "dimension",
    },
    "temporal": {
        "description": "Date or timestamp of event",
        "maps_to_kind": "data_property",
        "semantic_role": "temporal",
    },
    "geographic": {
        "description": "Location-based dimension (country, city, region, coordinates)",
        "maps_to_kind": "data_property",
        "semantic_role": "geographic",
    },
    "object_property": {
        "description": "FK or reference to another entity's identifier",
        "maps_to_kind": "object_property",
        "semantic_role": "relationship",
    },
    "composite_component": {
        "description": "Part of a multi-column value object",
        "maps_to_kind": "data_property",
        "semantic_role": "component",
    },
    "label": {
        "description": "Human-readable name or title",
        "maps_to_kind": "data_property",
        "semantic_role": "label",
    },
    "pii": {
        "description": "Personally identifiable information",
        "maps_to_kind": "data_property",
        "semantic_role": "pii",
        "governance_flag": True,
    },
    "audit": {
        "description": "System-generated provenance column",
        "maps_to_kind": "data_property",
        "semantic_role": "provenance",
    },
    "derived": {
        "description": "Computed from other columns",
        "maps_to_kind": "data_property",
        "semantic_role": "derived",
    },
}


# ---------------------------------------------------------------------------
# Reference target inference from FHIR relationship names
# ---------------------------------------------------------------------------

_KNOWN_REFERENCE_TARGETS: Dict[str, str] = {
    "subject": "Patient",
    "patient": "Patient",
    "encounter": "Encounter",
    "performer": "Practitioner",
    "provider": "Practitioner",
    "practitioner": "Practitioner",
    "organization": "Organization",
    "managingOrganization": "Organization",
    "serviceProvider": "Organization",
    "location": "Location",
    "insurer": "Organization",
    "facility": "Location",
    "device": "Device",
    "specimen": "Specimen",
    "recorder": "Practitioner",
    "asserter": "Practitioner",
    "requester": "Practitioner",
    "sender": "Organization",
    "receiver": "Organization",
    "author": "Practitioner",
    "source": "Patient",
    "focus": "Resource",
    "basedOn": "ServiceRequest",
    "partOf": "Resource",
    "reasonReference": "Condition",
    "hasMember": "Observation",
    "derivedFrom": "Observation",
    "generalPractitioner": "Practitioner",
    "referral": "ServiceRequest",
    "prescription": "MedicationRequest",
    "originalPrescription": "MedicationRequest",
    "enterer": "Practitioner",
    "appointment": "Appointment",
    "episodeOfCare": "EpisodeOfCare",
    "account": "Account",
    "manufacturer": "Organization",
}


def _infer_reference_target(attr_name: str) -> Optional[str]:
    return _KNOWN_REFERENCE_TARGETS.get(attr_name)


# ---------------------------------------------------------------------------
# Core generation logic
# ---------------------------------------------------------------------------

def _is_component_type(target: str) -> bool:
    return any(target.endswith(s) for s in _COMPONENT_SUFFIXES) or "." in target


def generate_properties_for_entity(
    entity_name: str,
    defn: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """Generate a properties dict for one FHIR entity definition."""
    typical_attrs = defn.get("typical_attributes", [])
    relationships = defn.get("relationships", {})

    props: Dict[str, Dict[str, Any]] = {}
    seen_identifier = False

    for dotted in typical_attrs:
        parts = dotted.split(".", 1)
        attr_name = parts[1] if len(parts) == 2 else parts[0]

        rel_key = dotted
        rel = relationships.get(rel_key, {})
        target_type = rel.get("target", "")

        if _is_component_type(target_type):
            continue

        role = _fhir_type_to_role(target_type)
        if role is None:
            role = "label"

        if role == "business_key" and not seen_identifier:
            role = "primary_key"
            seen_identifier = True

        kind = "object_property" if role == "object_property" else "data_property"
        col_variants = _generate_typical_attrs(attr_name, entity_name, role)

        prop_entry: Dict[str, Any] = {
            "kind": kind,
            "role": role,
            "typical_attributes": col_variants,
        }

        if role == "object_property":
            edge_name = _camel_to_snake(attr_name)
            ref_target = _infer_reference_target(attr_name)
            prop_entry["edge"] = edge_name
            if ref_target:
                prop_entry["target_entity"] = ref_target

        prop_key = attr_name
        if prop_key in props:
            prop_key = f"{attr_name}_{role}"
        props[prop_key] = prop_entry

    return props


def process_bundle(bundle: Dict[str, Any]) -> Tuple[int, int]:
    """Add properties blocks to all entities in the bundle. Returns (entity_count, prop_count)."""
    defs = bundle.get("ontology", {}).get("entities", {}).get("definitions", {})

    if "property_roles" not in bundle.get("ontology", {}):
        bundle["ontology"]["property_roles"] = copy.deepcopy(PROPERTY_ROLES)

    total_props = 0
    for ent_name, defn in defs.items():
        props = generate_properties_for_entity(ent_name, defn)
        if props:
            defn["properties"] = props
            total_props += sum(len(p.get("typical_attributes", [])) for p in props.values())

    return len(defs), total_props


def main():
    parser = argparse.ArgumentParser(description="Generate nested properties for fhir_r4.yaml")
    parser.add_argument("--write", action="store_true", help="Write changes back to fhir_r4.yaml")
    parser.add_argument("--bundle", default="fhir_r4", help="Bundle name (default: fhir_r4)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    repo_root = Path(__file__).resolve().parent.parent
    yaml_path = repo_root / "configurations" / "ontology_bundles" / f"{args.bundle}.yaml"

    if not yaml_path.is_file():
        logger.error("Bundle YAML not found: %s", yaml_path)
        return

    logger.info("Reading %s", yaml_path)
    raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))

    ent_count, prop_count = process_bundle(raw)
    logger.info("Generated properties for %d entities (%d total attribute variants)", ent_count, prop_count)

    defs = raw["ontology"]["entities"]["definitions"]
    roles_hist: Dict[str, int] = {}
    for defn in defs.values():
        for p in defn.get("properties", {}).values():
            r = p.get("role", "unknown")
            roles_hist[r] = roles_hist.get(r, 0) + 1
    print("\nRole distribution:")
    for r, c in sorted(roles_hist.items(), key=lambda x: -x[1]):
        print(f"  {r}: {c}")

    sample_entities = ["Patient", "Encounter", "Claim", "Observation", "Condition", "Practitioner"]
    for ent in sample_entities:
        defn = defs.get(ent)
        if defn and defn.get("properties"):
            print(f"\n--- {ent} ({len(defn['properties'])} properties) ---")
            for pname, pinfo in list(defn["properties"].items())[:5]:
                print(f"  {pname}: role={pinfo['role']}, attrs={pinfo['typical_attributes']}")
            if len(defn["properties"]) > 5:
                print(f"  ... and {len(defn['properties']) - 5} more")

    if args.write:
        yaml_path.write_text(
            yaml.dump(raw, default_flow_style=False, sort_keys=False, allow_unicode=True, width=120),
            encoding="utf-8",
        )
        logger.info("Wrote updated YAML to %s", yaml_path)
    else:
        print("\nDry run complete. Use --write to save changes.")


if __name__ == "__main__":
    main()
