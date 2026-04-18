"""Canonical property-role vocabulary for ontology bundles.

Single source of truth for:
- PROPERTY_ROLES: the 12 recognised roles with metadata
- ROLE_PATTERNS: column-name / dtype patterns for build-time inference
- VALID_ROLE_NAMES: set of legal role strings

Imported by ontology.py (runtime heuristic), the property generator (build-time),
and validate_ontology_bundles.py (validation).
"""

from __future__ import annotations

PROPERTY_ROLES: dict[str, dict] = {
    "primary_key": {
        "description": "Surrogate or technical row identifier",
        "kind": "data_property",
        "semantic_role": "identifier",
    },
    "business_key": {
        "description": "Natural key meaningful to users (MRN, NPI)",
        "kind": "data_property",
        "semantic_role": "identifier",
    },
    "object_property": {
        "description": "FK or reference to another entity's identifier",
        "kind": "object_property",
        "semantic_role": "relationship",
    },
    "measure": {
        "description": "Numeric value to aggregate",
        "kind": "data_property",
        "semantic_role": "metric",
    },
    "dimension": {
        "description": "Categorical filter or grouping attribute",
        "kind": "data_property",
        "semantic_role": "dimension",
    },
    "temporal": {
        "description": "Date or timestamp of event",
        "kind": "data_property",
        "semantic_role": "temporal",
    },
    "geographic": {
        "description": "Location-based dimension (country, city, region, coordinates)",
        "kind": "data_property",
        "semantic_role": "geographic",
    },
    "label": {
        "description": "Human-readable name or title",
        "kind": "data_property",
        "semantic_role": "label",
    },
    "pii": {
        "description": "Personally identifiable information",
        "kind": "data_property",
        "semantic_role": "pii",
        "governance_flag": True,
    },
    "audit": {
        "description": "System-generated provenance column",
        "kind": "data_property",
        "semantic_role": "provenance",
    },
    "derived": {
        "description": "Computed from other columns",
        "kind": "data_property",
        "semantic_role": "derived",
    },
    "composite_component": {
        "description": "Part of a multi-column value object",
        "kind": "data_property",
        "semantic_role": "component",
    },
}

VALID_ROLE_NAMES: frozenset[str] = frozenset(PROPERTY_ROLES)

# ---------------------------------------------------------------------------
# Build-time patterns: used by the curated-bundle property generator to infer
# roles from entity-level typical_attributes (flat column name lists).
# ---------------------------------------------------------------------------

PII_COLUMN_NAMES: frozenset[str] = frozenset({
    "email", "phone", "phone_number", "ssn", "social_security_number",
    "first_name", "last_name", "full_name", "date_of_birth", "dob",
    "address", "street_address", "home_address", "mrn", "npi",
    "drivers_license", "passport_number", "credit_card_number",
    "bank_account_number", "tax_id", "national_id",
})

TEMPORAL_SUFFIXES: tuple[str, ...] = (
    "_date", "_time", "_at", "_timestamp", "_ts", "_datetime",
)

TEMPORAL_COLUMN_NAMES: frozenset[str] = frozenset({
    "date", "time", "timestamp", "created", "updated", "modified",
    "effective_date", "start_date", "end_date", "birth_date",
})

MEASURE_SUFFIXES: tuple[str, ...] = (
    "_amount", "_count", "_total", "_value", "_qty", "_quantity",
    "_rate", "_price", "_cost", "_balance", "_score", "_weight",
    "_percent", "_pct", "_ratio",
)

DIMENSION_SUFFIXES: tuple[str, ...] = (
    "_type", "_status", "_code", "_category", "_class", "_group",
    "_flag", "_level", "_tier", "_mode", "_kind",
)

AUDIT_COLUMN_NAMES: frozenset[str] = frozenset({
    "ingest_ts", "ingestion_timestamp", "batch_id", "row_hash",
    "source_system", "etl_timestamp", "etl_batch_id", "load_date",
    "load_ts", "created_by_etl", "upload_ts", "_rescued_data",
    "processing_timestamp",
})

GEO_COLUMN_NAMES: frozenset[str] = frozenset({
    "country", "country_code", "state", "state_code", "city",
    "postal_code", "zip_code", "zipcode", "zip", "latitude",
    "longitude", "lat", "lon", "geo_region", "region", "county",
    "province", "address",
})

LABEL_COLUMN_NAMES: frozenset[str] = frozenset({
    "name", "title", "label", "description", "note_text", "notes",
    "clinical_summary", "summary", "comment", "text_content", "body",
    "narrative", "remarks", "free_text", "memo", "abstract",
})


def infer_role_from_column_name(
    col: str,
    entity_name: str,
    all_entity_names: frozenset[str],
) -> str | None:
    """Infer a property role from a column name using pattern matching.

    Returns the role string or None if no confident match.
    Used by the curated-bundle property generator.
    """
    c = col.lower()
    ent_lower = entity_name.lower().rstrip("s")

    # PK: {entity}_id or bare "id" for this entity
    if c == "id" or c == f"{ent_lower}_id" or c == f"{ent_lower}_key":
        return "primary_key"

    # FK / object_property: {other_entity}_id where other entity exists
    if c.endswith(("_id", "_key")):
        stem = c.rsplit("_", 1)[0]
        for other in all_entity_names:
            other_lower = other.lower()
            # Match singular or plural stem
            if stem == other_lower or stem == other_lower.rstrip("s") or stem + "s" == other_lower:
                return "object_property"
        return "object_property"  # still an FK even if target unknown

    if c in PII_COLUMN_NAMES:
        return "pii"
    if c in AUDIT_COLUMN_NAMES or c.startswith("etl_"):
        return "audit"
    if c in GEO_COLUMN_NAMES or c.startswith("geo_"):
        return "geographic"
    if any(c.endswith(s) for s in TEMPORAL_SUFFIXES) or c in TEMPORAL_COLUMN_NAMES:
        return "temporal"
    if any(c.endswith(s) for s in MEASURE_SUFFIXES):
        return "measure"
    if any(c.endswith(s) for s in DIMENSION_SUFFIXES):
        return "dimension"
    if c in LABEL_COLUMN_NAMES:
        return "label"
    return None


def resolve_fk_target(
    col: str,
    all_entity_names: frozenset[str],
) -> str | None:
    """Given an _id/_key column, resolve the target entity name from the bundle's entity set."""
    c = col.lower()
    if not c.endswith(("_id", "_key")):
        return None
    stem = c.rsplit("_", 1)[0]
    for ent in all_entity_names:
        el = ent.lower()
        if stem == el or stem == el.rstrip("s") or stem + "s" == el:
            return ent
    return None


def property_roles_for_yaml() -> dict[str, dict]:
    """Return PROPERTY_ROLES formatted for embedding in bundle YAML property_roles section."""
    out = {}
    for role, meta in PROPERTY_ROLES.items():
        entry: dict = {
            "description": meta["description"],
            "maps_to_kind": meta["kind"],
            "semantic_role": meta["semantic_role"],
        }
        if meta.get("governance_flag"):
            entry["governance_flag"] = True
        out[role] = entry
    return out
