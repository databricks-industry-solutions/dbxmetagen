"""
Ontology module for entity and metric management.

Discovers and manages business entities from the knowledge base,
creating an ontology layer on top of the data catalog.

Includes flexible keyword matching, AI-powered structured classification,
value enforcement, domain-aware scoring, and deduplication.
"""

import logging
import yaml
import os
import re
import json
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import uuid
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    MapType,
    DoubleType,
    BooleanType,
    TimestampType,
)
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

DEFAULT_CLASSIFICATION_MODEL = "databricks-claude-sonnet-4-6"


# ==============================================================================
# Pydantic structured output models for AI classification
# ==============================================================================


class EntityClassificationResult(BaseModel):
    """Structured output from LLM entity classification."""

    entity_type: str = Field(description="Entity type from the provided list")
    secondary_entity_type: Optional[str] = Field(
        default=None,
        description="Second entity type if the table clearly represents a relationship between two entities (e.g. patient_encounters -> Patient, Encounter). Omit if only one entity type applies.",
    )
    confidence: float = Field(
        ge=0.0, le=1.0, description="Confidence score between 0.0 and 1.0"
    )
    recommended_entity: Optional[str] = Field(
        default=None,
        description="If confidence is below 0.5, suggest what entity type name this should be, even if it is not in the provided list",
    )
    reasoning: str = Field(description="Brief reasoning for the classification")


class ColumnClassificationItem(BaseModel):
    """Single column classification within a batch response."""
    column_name: str = Field(description="Exact column name as provided in input")
    entity_type: str = Field(description="Entity type from the provided list")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence 0.0-1.0")


class BatchColumnClassificationResult(BaseModel):
    """Batch response for classifying all columns of a single table."""
    classifications: List[ColumnClassificationItem] = Field(
        description="One classification per input column"
    )


class TableClassificationItem(BaseModel):
    """Single table classification within a batch response."""
    table_name: str = Field(description="Short table name as provided in input")
    entity_type: str = Field(description="Entity type from the provided list")
    secondary_entity_type: Optional[str] = Field(
        default=None,
        description="Second entity type if the table represents a relationship between two entities",
    )
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence 0.0-1.0")
    recommended_entity: Optional[str] = Field(
        default=None,
        description="If confidence is below 0.5, suggest a better entity name",
    )
    reasoning: str = Field(description="Brief reasoning for the classification")


class BatchTableClassificationResult(BaseModel):
    """Batch response for classifying multiple tables at once."""
    classifications: List[TableClassificationItem] = Field(
        description="One classification per input table"
    )


# ==============================================================================
# Domain-to-entity affinity map (loaded from config, with embedded fallback)
# ==============================================================================

_DEFAULT_DOMAIN_ENTITY_AFFINITY: Dict[str, List[str]] = {
    "healthcare": [
        "Patient",
        "Provider",
        "Encounter",
        "Condition",
        "Procedure",
        "Medication",
        "Observation",
        "Claim",
        "Coverage",
        "Person",
        "Organization",
        "Location",
    ],
    "clinical": [
        "Patient",
        "Provider",
        "Encounter",
        "Condition",
        "Procedure",
        "Medication",
        "Observation",
        "Claim",
        "Coverage",
        "Person",
        "Organization",
        "Location",
    ],
    "diagnostics": [
        "Patient",
        "Observation",
        "Condition",
        "Procedure",
        "Person",
        "Reference",
    ],
    "payer": [
        "Claim",
        "Coverage",
        "Person",
        "Organization",
        "Transaction",
        "Reference",
    ],
    "pharmaceutical": [
        "Patient",
        "Observation",
        "Medication",
        "Condition",
        "Procedure",
        "Person",
        "Document",
        "Reference",
        "Organization",
    ],
    "quality_safety": [
        "Patient",
        "Observation",
        "Encounter",
        "Condition",
        "Metric",
        "Event",
    ],
    "research": [
        "Patient",
        "Observation",
        "Medication",
        "Condition",
        "Procedure",
        "Person",
        "Document",
        "Reference",
    ],
    "finance": [
        "Transaction",
        "Person",
        "Organization",
        "Product",
        "Reference",
        "Event",
        "Metric",
    ],
    "operations": [
        "Product",
        "Organization",
        "Location",
        "Event",
        "Metric",
        "Reference",
    ],
    "workforce": ["Person", "Organization", "Location", "Event", "Reference"],
    "customer": [
        "Person",
        "Organization",
        "Product",
        "Transaction",
        "Event",
        "Metric",
        "Location",
    ],
    "technology": ["Event", "Metric", "Reference", "Organization"],
    "governance": ["Document", "Reference", "Organization", "Person"],
}


def load_domain_entity_affinity(ontology_config: Dict[str, Any]) -> Dict[str, set]:
    """Load domain_entity_affinity from ontology config, falling back to defaults."""
    raw = ontology_config.get("domain_entity_affinity")
    if raw and isinstance(raw, dict):
        return {k: set(v) for k, v in raw.items()}
    return {k: set(v) for k, v in _DEFAULT_DOMAIN_ENTITY_AFFINITY.items()}


# Backward-compat: module-level reference used by tests / external callers
DOMAIN_ENTITY_AFFINITY: Dict[str, set] = {
    k: set(v) for k, v in _DEFAULT_DOMAIN_ENTITY_AFFINITY.items()
}


# ==============================================================================
# Value enforcement (ported from domain_classifier)
# ==============================================================================


def _enforce_entity_value(
    predicted: str,
    allowed: List[str],
    fallback: str = "DataTable",
) -> Tuple[str, bool]:
    """Snap a predicted entity type to the nearest allowed value.

    Returns (value, was_exact_match). Confidence should be penalized
    by the caller when was_exact_match is False.
    """
    low = predicted.lower().strip()
    allowed_map = {a.lower(): a for a in allowed}
    if low in allowed_map:
        return allowed_map[low], True
    for a_low, a_orig in allowed_map.items():
        if a_low in low or low in a_low:
            return a_orig, False
    return fallback, False


# ==============================================================================
# EMBEDDED DEFAULT ENTITY DEFINITIONS
# These are used when the config file cannot be loaded
# ==============================================================================
DEFAULT_ENTITY_DEFINITIONS = {
    # General entities
    "Person": {
        "description": "People, customers, contacts, users, or employees",
        "keywords": ["customer", "employee", "user", "contact", "person"],
        "typical_attributes": [
            "id",
            "name",
            "email",
            "phone",
            "address",
            "created_date",
        ],
    },
    "Organization": {
        "description": "Companies, departments, facilities, or business units",
        "keywords": ["company", "organization", "department", "facility", "tenant"],
        "typical_attributes": ["id", "name", "type", "address", "parent_id"],
    },
    "Product": {
        "description": "Goods, services, SKUs, or catalog items",
        "keywords": ["product", "item", "sku", "catalog", "offering"],
        "typical_attributes": [
            "id",
            "name",
            "description",
            "price",
            "category",
            "status",
        ],
    },
    "Transaction": {
        "description": "Orders, purchases, payments, invoices, or financial events",
        "keywords": ["transaction", "order", "payment", "invoice", "purchase"],
        "typical_attributes": [
            "id",
            "customer_id",
            "amount",
            "date",
            "status",
            "currency",
        ],
    },
    "Location": {
        "description": "Addresses, sites, regions, facilities, or geographies",
        "keywords": ["location", "address", "site", "region", "facility"],
        "typical_attributes": [
            "id",
            "name",
            "address",
            "city",
            "state",
            "country",
            "zip_code",
        ],
    },
    "Event": {
        "description": "Activities, logs, sessions, audits, or system events",
        "keywords": ["event", "activity", "log", "audit", "session"],
        "typical_attributes": ["id", "type", "timestamp", "user_id", "details"],
    },
    "Reference": {
        "description": "Lookup tables, codes, categories, dimensions, or configuration",
        "keywords": ["reference", "lookup", "code", "category", "dim_"],
        "typical_attributes": ["id", "code", "name", "description", "active"],
    },
    "Metric": {
        "description": "KPIs, aggregates, facts, summaries, or analytic measures",
        "keywords": ["metric", "kpi", "aggregate", "fact_", "summary"],
        "typical_attributes": ["id", "name", "value", "period", "dimension"],
    },
    "Document": {
        "description": "Unstructured content, notes, files, or text records",
        "keywords": ["document", "note", "file", "content", "text"],
        "typical_attributes": ["id", "title", "body", "author", "created_date", "type"],
    },
    # Healthcare / Life Sciences entities
    "Patient": {
        "description": "Individuals receiving healthcare services (FHIR Patient)",
        "keywords": ["patient", "mrn", "subject", "participant", "beneficiary"],
        "typical_attributes": ["id", "mrn", "name", "dob", "gender", "address"],
    },
    "Provider": {
        "description": "Practitioners, clinicians, or care team members (FHIR Practitioner)",
        "keywords": ["provider", "physician", "practitioner", "clinician", "npi"],
        "typical_attributes": ["id", "npi", "name", "specialty", "organization"],
    },
    "Encounter": {
        "description": "Visits, admissions, or episodes of care (FHIR Encounter)",
        "keywords": ["encounter", "visit", "admission", "episode", "appointment"],
        "typical_attributes": [
            "id",
            "patient_id",
            "date",
            "type",
            "provider",
            "location",
        ],
    },
    "Condition": {
        "description": "Diagnoses, problems, or diseases (FHIR Condition / OMOP CONDITION_OCCURRENCE)",
        "keywords": ["diagnosis", "condition", "icd", "disease", "problem"],
        "typical_attributes": [
            "id",
            "code",
            "description",
            "patient_id",
            "date",
            "status",
        ],
    },
    "Procedure": {
        "description": "Medical procedures, surgeries, or interventions (FHIR Procedure)",
        "keywords": ["procedure", "surgery", "cpt", "intervention", "operation"],
        "typical_attributes": [
            "id",
            "code",
            "description",
            "date",
            "provider",
            "status",
        ],
    },
    "Medication": {
        "description": "Drugs, prescriptions, or medication orders (FHIR MedicationRequest)",
        "keywords": ["medication", "drug", "prescription", "pharmacy", "ndc"],
        "typical_attributes": ["id", "name", "ndc", "dose", "frequency", "route"],
    },
    "Observation": {
        "description": "Lab results, vitals, or clinical measurements (FHIR Observation / OMOP MEASUREMENT)",
        "keywords": ["lab", "result", "observation", "vital", "specimen"],
        "typical_attributes": [
            "id",
            "test_code",
            "value",
            "unit",
            "reference_range",
            "date",
        ],
    },
    "Claim": {
        "description": "Insurance claims or billing submissions (FHIR Claim)",
        "keywords": ["claim", "billing", "adjudication", "remittance", "eob"],
        "typical_attributes": [
            "id",
            "patient_id",
            "amount",
            "service_date",
            "status",
            "payer_id",
        ],
    },
    "Coverage": {
        "description": "Insurance plans, benefits, or member coverage (FHIR Coverage)",
        "keywords": ["coverage", "insurance", "benefit", "plan", "enrollment"],
        "typical_attributes": [
            "id",
            "member_id",
            "payer",
            "plan_name",
            "start_date",
            "end_date",
        ],
    },
    "DataTable": {
        "description": "Generic data table (fallback type)",
        "keywords": [],
        "typical_attributes": ["id"],
    },
}


@dataclass
class OntologyConfig:
    """Configuration for ontology management."""

    catalog_name: str
    schema_name: str
    config_path: str = "configurations/ontology_config.yaml"
    ontology_bundle: str = ""
    entities_table: str = "ontology_entities"
    metrics_table: str = "ontology_metrics"
    column_properties_table: str = "ontology_column_properties"
    relationships_table: str = "ontology_relationships"
    discovery_diff_table: str = "discovery_diff_report"
    kb_table: str = "table_knowledge_base"
    column_kb_table: str = "column_knowledge_base"
    nodes_table: str = "graph_nodes"
    incremental: bool = True
    entity_tag_key: str = "ontology.entity_type"
    metadata_cols_per_chunk: int = 120

    @property
    def fully_qualified_entities(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.entities_table}"

    @property
    def fully_qualified_metrics(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.metrics_table}"

    @property
    def fully_qualified_column_properties(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.column_properties_table}"

    @property
    def fully_qualified_relationships(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.relationships_table}"

    @property
    def fully_qualified_discovery_diff(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.discovery_diff_table}"

    @property
    def fully_qualified_kb(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.kb_table}"

    @property
    def fully_qualified_column_kb(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.column_kb_table}"

    @property
    def fully_qualified_nodes(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.nodes_table}"


@dataclass
class PropertyDefinition:
    """Formal property definition from ontology bundle."""

    name: str
    kind: str  # "data_property" or "object_property"
    role: str  # references property_roles taxonomy
    typical_attributes: List[str] = field(default_factory=list)
    edge: Optional[str] = None  # edge catalog name for object_properties
    target_entity: Optional[Any] = None  # str or list of str
    datatype: Optional[str] = None
    composite_columns: Optional[List[str]] = None


@dataclass
class EdgeCatalogEntry:
    """Entry in the global edge catalog."""

    name: str
    inverse: Optional[str] = None
    domain: Optional[Any] = None  # str, list of str, or "Any"
    range: Optional[Any] = None
    symmetric: bool = False
    category: str = "business"

    def _matches(self, constraint: Any, entity_type: str) -> bool:
        if constraint is None or constraint == "Any":
            return True
        if isinstance(constraint, list):
            return entity_type in constraint
        return constraint == entity_type

    def matches_domain(self, entity_type: str) -> bool:
        return self._matches(self.domain, entity_type)

    def matches_range(self, entity_type: str) -> bool:
        return self._matches(self.range, entity_type)

    def validate_edge(self, src_entity: str, dst_entity: str) -> bool:
        return self.matches_domain(src_entity) and self.matches_range(dst_entity)


class EdgeCatalog:
    """Utility wrapper around a dict of EdgeCatalogEntry objects."""

    def __init__(self, entries: Dict[str, "EdgeCatalogEntry"]):
        self._entries = entries

    def get(self, name: str) -> Optional[EdgeCatalogEntry]:
        return self._entries.get(name)

    def get_inverse(self, name: str) -> Optional[str]:
        entry = self._entries.get(name)
        return entry.inverse if entry else None

    def find_edge(self, src_entity: str, dst_entity: str) -> Optional[EdgeCatalogEntry]:
        """Find the best matching edge for a (src, dst) entity pair."""
        for entry in self._entries.values():
            if entry.validate_edge(src_entity, dst_entity):
                return entry
        return None

    def validate(self, edge_name: str, src_entity: str, dst_entity: str) -> Tuple[bool, str]:
        """Validate an edge. Returns (valid, message)."""
        entry = self._entries.get(edge_name)
        if not entry:
            return False, f"Edge '{edge_name}' not in catalog"
        if not entry.validate_edge(src_entity, dst_entity):
            return False, (
                f"Edge '{edge_name}' domain/range violation: "
                f"{src_entity}->{dst_entity} (expected {entry.domain}->{entry.range})"
            )
        return True, "ok"

    @property
    def entries(self) -> Dict[str, EdgeCatalogEntry]:
        return self._entries


@dataclass
class EntityDefinition:
    """Definition of a business entity."""

    name: str
    description: str
    keywords: List[str] = field(default_factory=list)
    typical_attributes: List[str] = field(default_factory=list)
    relationships: Dict[str, Dict[str, str]] = field(default_factory=dict)
    synonyms: List[str] = field(default_factory=list)
    business_questions: List[str] = field(default_factory=list)
    parent: Optional[str] = None
    properties: List[PropertyDefinition] = field(default_factory=list)


BUNDLE_DIR = "configurations/ontology_bundles"


def resolve_bundle_path(bundle_name: str) -> str:
    """Resolve a bundle name (e.g. 'healthcare') to its YAML path.

    Searches ``configurations/ontology_bundles/`` relative to the package
    root, CWD, and Workspace.  Returns the first existing path, or falls
    back to ``<BUNDLE_DIR>/<bundle_name>.yaml`` (which OntologyLoader will
    then search with its own path resolution).
    """
    filename = (
        f"{bundle_name}.yaml" if not bundle_name.endswith(".yaml") else bundle_name
    )
    candidates = [
        os.path.join(BUNDLE_DIR, filename),
        os.path.join("..", BUNDLE_DIR, filename),
        os.path.join(os.path.dirname(__file__), "..", "..", BUNDLE_DIR, filename),
    ]
    try:
        cwd = os.getcwd()
        candidates.append(os.path.join(cwd, BUNDLE_DIR, filename))
        candidates.append(os.path.join(cwd, "..", BUNDLE_DIR, filename))
    except Exception:
        pass

    for path in candidates:
        if os.path.exists(path):
            return path
    return os.path.join(BUNDLE_DIR, filename)


def list_available_bundles() -> List[Dict[str, Any]]:
    """Return metadata dicts for every bundle YAML found in the bundles directory."""
    bundles: List[Dict[str, Any]] = []
    candidates = [
        BUNDLE_DIR,
        os.path.join(os.path.dirname(__file__), "..", "..", BUNDLE_DIR),
    ]
    try:
        candidates.append(os.path.join(os.getcwd(), BUNDLE_DIR))
    except Exception:
        pass

    seen: set = set()
    for base in candidates:
        if not os.path.isdir(base):
            continue
        for fname in sorted(os.listdir(base)):
            if not fname.endswith(".yaml"):
                continue
            bundle_key = fname.replace(".yaml", "")
            if bundle_key in seen:
                continue
            seen.add(bundle_key)
            try:
                with open(os.path.join(base, fname), "r") as f:
                    raw = yaml.safe_load(f)
                meta = raw.get("metadata", {})
                entity_count = len(
                    raw.get("ontology", {}).get("entities", {}).get("definitions", {})
                )
                domain_count = len(raw.get("domains", {}))
                bundles.append(
                    {
                        "key": bundle_key,
                        "name": meta.get("name", bundle_key),
                        "industry": meta.get("industry", "general"),
                        "description": meta.get("description", ""),
                        "standards_alignment": meta.get("standards_alignment", ""),
                        "entity_count": entity_count,
                        "domain_count": domain_count,
                    }
                )
            except Exception as e:
                logger.debug(f"Could not read bundle {fname}: {e}")
    return bundles


class OntologyLoader:
    """Loads and parses ontology configuration with robust fallbacks."""

    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        """Load ontology configuration from YAML file with workspace-aware paths.

        Supports both standalone ontology configs (with top-level ``ontology`` key)
        and bundle configs (which also contain ``domains``, ``metadata``, and
        ``ontology.domain_entity_affinity``).  In both cases the returned dict
        is the ``ontology`` section.  When a bundle is loaded the
        ``domain_entity_affinity`` map is preserved inside the returned config so
        that ``load_domain_entity_affinity()`` can extract it at runtime.
        """
        paths_to_try = [
            config_path,
            f"../{config_path}",
            f"../../{config_path}",
            os.path.join(os.path.dirname(__file__), "..", "..", config_path),
            f"/Workspace/{config_path}",
            f"/Workspace/Repos/{config_path}",
        ]
        try:
            cwd = os.getcwd()
            paths_to_try.append(os.path.join(cwd, config_path))
            paths_to_try.append(os.path.join(cwd, "..", config_path))
        except Exception:
            pass

        for path in paths_to_try:
            try:
                if os.path.exists(path):
                    with open(path, "r") as f:
                        config = yaml.safe_load(f)
                    if config and "ontology" in config:
                        loaded_config = config.get("ontology", {})
                        OntologyLoader._validate_config(loaded_config)
                        if "metadata" in config:
                            loaded_config["_bundle_metadata"] = config["metadata"]
                        entity_count = len(
                            loaded_config.get("entities", {}).get("definitions", {})
                        )
                        logger.info(
                            f"Loaded ontology config from {path} with {entity_count} entity definitions"
                        )
                        return loaded_config
            except ValueError as e:
                logger.error(f"Invalid ontology config at {path}: {e}")
                raise
            except Exception as e:
                logger.debug(f"Could not load from {path}: {e}")

        logger.warning("Could not load ontology config file, using embedded defaults")
        return OntologyLoader._default_config()

    @staticmethod
    def _default_config() -> Dict[str, Any]:
        """Return default ontology configuration with embedded entity definitions."""
        return {
            "version": "1.0",
            "entities": {
                "auto_discover": True,
                "discovery_confidence_threshold": 0.4,  # Lowered from 0.7
                "definitions": DEFAULT_ENTITY_DEFINITIONS,
            },
            "relationships": {
                "types": [
                    "owns",
                    "contains",
                    "references",
                    "derives_from",
                    "similar_to",
                ]
            },
            "metrics": {"auto_discover": False, "definitions": []},
        }

    @staticmethod
    def _validate_config(config: Dict[str, Any]) -> None:
        """Validate ontology config structure. Raises ValueError on critical issues."""
        entities = config.get("entities")
        if not entities or not isinstance(entities, dict):
            raise ValueError("ontology config missing required 'entities' section")

        definitions = entities.get("definitions")
        if not definitions or not isinstance(definitions, dict):
            raise ValueError(
                "ontology config missing required 'entities.definitions' (must be non-empty dict)"
            )

        for name, defn in definitions.items():
            if not isinstance(defn, dict):
                raise ValueError(
                    f"Entity '{name}' definition must be a dict, got {type(defn).__name__}"
                )
            if "description" not in defn:
                logger.warning(f"Entity '{name}' missing 'description'")
            if not isinstance(defn.get("keywords", []), list):
                raise ValueError(f"Entity '{name}'.keywords must be a list")
            if not isinstance(defn.get("typical_attributes", []), list):
                raise ValueError(f"Entity '{name}'.typical_attributes must be a list")

        validation = config.get("validation", {})
        if validation:
            for key in ("min_entity_confidence", "max_entities_per_table"):
                val = validation.get(key)
                if val is not None and not isinstance(val, (int, float)):
                    raise ValueError(
                        f"validation.{key} must be numeric, got {type(val).__name__}"
                    )

    @staticmethod
    def get_entity_definitions(config: Dict[str, Any]) -> List[EntityDefinition]:
        """Extract entity definitions from config, using defaults if empty.

        Supports both the new ``relationships`` map format and the legacy
        ``typical_relationships`` flat list.  When the new format is present
        it takes precedence; otherwise the flat list is converted into a
        dict keyed by relationship name with empty target/cardinality.

        Also parses the ``properties`` block per entity into
        ``PropertyDefinition`` objects for bundle-driven classification.
        """
        definitions = config.get("entities", {}).get("definitions", {})

        if not definitions:
            logger.info("No entity definitions in config, using embedded defaults")
            definitions = DEFAULT_ENTITY_DEFINITIONS

        entities = []
        for name, details in definitions.items():
            rels: Dict[str, Dict[str, str]] = {}
            if "relationships" in details and isinstance(details["relationships"], dict):
                for rel_name, rel_info in details["relationships"].items():
                    if isinstance(rel_info, dict):
                        rels[rel_name] = {k: str(v) for k, v in rel_info.items()}
                    else:
                        rels[rel_name] = {"target": str(rel_info)}
            elif "typical_relationships" in details:
                for rel_name in details["typical_relationships"]:
                    rels[str(rel_name)] = {}

            props: List[PropertyDefinition] = []
            raw_props = details.get("properties", {})
            if isinstance(raw_props, dict):
                for prop_name, prop_info in raw_props.items():
                    if not isinstance(prop_info, dict):
                        continue
                    props.append(
                        PropertyDefinition(
                            name=prop_name,
                            kind=prop_info.get("kind", "data_property"),
                            role=prop_info.get("role", "attribute"),
                            typical_attributes=prop_info.get("typical_attributes", []),
                            edge=prop_info.get("edge"),
                            target_entity=prop_info.get("target_entity"),
                            datatype=prop_info.get("datatype"),
                            composite_columns=prop_info.get("composite_columns"),
                        )
                    )

            entities.append(
                EntityDefinition(
                    name=name,
                    description=details.get("description", f"{name} entity"),
                    keywords=details.get("keywords", []),
                    typical_attributes=details.get("typical_attributes", []),
                    relationships=rels,
                    synonyms=details.get("synonyms", []),
                    business_questions=details.get("business_questions", []),
                    parent=details.get("parent"),
                    properties=props,
                )
            )

        logger.info(f"Loaded {len(entities)} entity definitions")
        return entities

    @staticmethod
    def get_property_roles(config: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        """Extract the global property_roles taxonomy from config."""
        return config.get("property_roles", {})

    @staticmethod
    def get_edge_catalog(config: Dict[str, Any]) -> Dict[str, EdgeCatalogEntry]:
        """Extract the global edge catalog from config."""
        raw = config.get("edge_catalog", {})
        catalog: Dict[str, EdgeCatalogEntry] = {}
        for edge_name, info in raw.items():
            if not isinstance(info, dict):
                continue
            catalog[edge_name] = EdgeCatalogEntry(
                name=edge_name,
                inverse=info.get("inverse"),
                domain=info.get("domain"),
                range=info.get("range"),
                symmetric=info.get("symmetric", False),
                category=info.get("category", "business"),
            )
        return catalog


class EntityDiscoverer:
    """Discovers entities from knowledge base tables with flexible matching.

    Uses structured LLM output (ChatDatabricks), keyword scoring with
    domain-aware boosting, value enforcement, deduplication, and multi-entity
    support for relationship tables.
    """

    CONFIDENCE_PENALTY_SNAP = 0.1

    def __init__(
        self,
        spark: SparkSession,
        config: OntologyConfig,
        ontology_config: Dict[str, Any],
        domain_entity_affinity: Optional[Dict[str, set]] = None,
    ):
        self.spark = spark
        self.config = config
        self.ontology_config = ontology_config
        self.entity_definitions = OntologyLoader.get_entity_definitions(ontology_config)
        self._domain_entity_affinity = (
            domain_entity_affinity or load_domain_entity_affinity(ontology_config)
        )
        self._validation_cfg = ontology_config.get("validation", {})
        self._relationship_types = {
            r["name"] if isinstance(r, dict) else r
            for r in ontology_config.get("relationships", {}).get("types", [])
        }
        self._model_endpoint = self._validation_cfg.get(
            "classification_model",
            DEFAULT_CLASSIFICATION_MODEL,
        )
        self._entity_names = [
            e.name for e in self.entity_definitions if e.name != "DataTable"
        ]
        self._property_roles = OntologyLoader.get_property_roles(ontology_config)
        self._edge_catalog_entries = OntologyLoader.get_edge_catalog(ontology_config)
        self._bundle_geo_patterns = self._build_bundle_geo_patterns()
        self._edge_catalog = EdgeCatalog(self._edge_catalog_entries)
        self._entity_def_map: Dict[str, EntityDefinition] = {
            e.name: e for e in self.entity_definitions
        }
        self._stats = {
            "keyword_matches": 0,
            "ai_classifications": 0,
            "fallback_generic": 0,
            "column_keyword_matches": 0,
            "column_ai_classifications": 0,
            "column_fallback": 0,
        }

    def _build_bundle_geo_patterns(self) -> frozenset:
        """Collect keywords + typical_attributes from Geographic entities in the bundle."""
        geo_names = set()
        for e in self.entity_definitions:
            if e.name.lower() in ("geographic", "geocoordinate", "adminregion",
                                  "postalcode", "timezone", "address", "location"):
                geo_names.add(e.name)
            elif e.parent and e.parent.lower() in ("geographic", "location"):
                geo_names.add(e.name)
        if not geo_names:
            return frozenset()
        patterns: set = set()
        for e in self.entity_definitions:
            if e.name in geo_names:
                patterns.update(k.lower() for k in e.keywords)
                patterns.update(a.lower() for a in e.typical_attributes)
        return frozenset(patterns)

    # ------------------------------------------------------------------
    # LLM helpers
    # ------------------------------------------------------------------

    def _get_structured_llm(self):
        from databricks_langchain import ChatDatabricks

        llm = ChatDatabricks(
            endpoint=self._model_endpoint, temperature=0.0, max_tokens=512, max_retries=2
        )
        return llm.with_structured_output(EntityClassificationResult)

    def _get_batch_column_llm(self):
        from databricks_langchain import ChatDatabricks

        llm = ChatDatabricks(
            endpoint=self._model_endpoint, temperature=0.0, max_tokens=2048, max_retries=2
        )
        return llm.with_structured_output(BatchColumnClassificationResult)

    def _get_batch_table_llm(self):
        from databricks_langchain import ChatDatabricks

        llm = ChatDatabricks(
            endpoint=self._model_endpoint, temperature=0.0, max_tokens=2048, max_retries=2
        )
        return llm.with_structured_output(BatchTableClassificationResult)

    @staticmethod
    def _format_entity_desc(e: "EntityDefinition") -> str:
        """Format an entity definition for use in classification prompts."""
        line = f"- {e.name}: {e.description}"
        if e.synonyms:
            line += f" (also known as: {', '.join(e.synonyms)})"
        return line

    def _get_column_summary(self, table_names: List[str]) -> Dict[str, str]:
        """Get top column names per table for enriching classification prompts."""
        if not table_names:
            return {}
        tbl_clause = ", ".join(f"'{t}'" for t in table_names)
        try:
            rows = self.spark.sql(
                f"SELECT table_name, column_name, data_type "
                f"FROM {self.config.fully_qualified_column_kb} "
                f"WHERE table_name IN ({tbl_clause}) "
                f"ORDER BY table_name, column_name"
            ).collect()
        except Exception:
            return {}
        by_table: Dict[str, List[str]] = {}
        for r in rows:
            by_table.setdefault(r.table_name, []).append(f"{r.column_name} {r.data_type}")
        return {t: ", ".join(cols[:10]) for t, cols in by_table.items()}

    # ------------------------------------------------------------------
    # Keyword scoring + domain-aware boosting
    # ------------------------------------------------------------------

    def _keyword_prefilter(
        self,
        name: str,
        comment: str,
        domain: str,
        top_n: int = 0,
    ) -> List[str]:
        """Score entity definitions by keyword overlap and domain affinity,
        returning the top-N candidate entity names for the LLM prompt.
        top_n=0 (default) returns all entity types, ranked by relevance."""
        name_lower = name.lower()
        comment_lower = (comment or "").lower()
        domain_lower = (domain or "").lower()

        affinity_set = self._domain_entity_affinity.get(domain_lower, set())

        scored: List[Tuple[str, float]] = []
        max_kw_only = 0.0
        for edef in self.entity_definitions:
            if edef.name == "DataTable":
                continue
            kw_score = 0.0
            all_terms = list(edef.keywords) + list(edef.synonyms)
            for kw in all_terms:
                kw_l = kw.lower()
                if kw_l in name_lower:
                    kw_score += 2.0
                elif kw_l in comment_lower:
                    kw_score += 1.0
            max_kw_only = max(max_kw_only, kw_score)
            has_affinity = edef.name in affinity_set
            scored.append((edef.name, kw_score, has_affinity))

        # If a strong keyword match exists for some entity, reduce the boost
        # for entities that scored purely from domain affinity (no keyword hit).
        has_strong_kw_competitor = max_kw_only >= 2.0
        final: List[Tuple[str, float]] = []
        for ename, kw_score, has_affinity in scored:
            total = kw_score
            if has_affinity:
                if kw_score == 0.0 and has_strong_kw_competitor:
                    total += 0.75  # halved affinity when outcompeted by keywords
                else:
                    total += 1.5
            final.append((ename, total))

        final.sort(key=lambda x: x[1], reverse=True)
        if top_n <= 0 or top_n >= len(final):
            return [s[0] for s in final]
        return [s[0] for s in final[:top_n]]

    # ------------------------------------------------------------------
    # Table-level discovery
    # ------------------------------------------------------------------

    def discover_entities_from_tables(self) -> List[Dict[str, Any]]:
        """Discover entities by matching tables to entity definitions."""
        current_bv = self._get_bundle_version()
        try:
            if self.config.incremental:
                try:
                    tables_df = self.spark.sql(
                        f"""
                        SELECT kb.table_name, kb.table_short_name, kb.comment, kb.domain
                        FROM {self.config.fully_qualified_kb} kb
                        LEFT JOIN (
                            SELECT src_table, MAX(created_at) AS last_classified,
                                   MAX(bundle_version) AS last_bundle_version
                            FROM (
                                SELECT EXPLODE(source_tables) AS src_table, created_at, bundle_version
                                FROM {self.config.fully_qualified_entities}
                                WHERE COALESCE(attributes['granularity'], 'table') = 'table'
                            )
                            GROUP BY src_table
                        ) oe ON kb.table_name = oe.src_table
                        WHERE kb.table_name IS NOT NULL
                          AND (oe.last_classified IS NULL
                               OR kb.updated_at > oe.last_classified
                               OR COALESCE(oe.last_bundle_version, '') != '{current_bv}')
                    """
                    )
                    table_count = tables_df.count()
                    total = self.spark.sql(f"SELECT COUNT(*) AS n FROM {self.config.fully_qualified_kb}").collect()[0].n
                    logger.info(f"Incremental mode: {table_count} tables need classification out of {total}")
                except Exception as e:
                    logger.warning(f"Incremental filtering failed ({e}), falling back to full scan")
                    tables_df = self.spark.sql(
                        f"""
                        SELECT table_name, table_short_name, comment, domain
                        FROM {self.config.fully_qualified_kb}
                    """
                    )
                    table_count = tables_df.count()
            else:
                tables_df = self.spark.sql(
                    f"""
                    SELECT table_name, table_short_name, comment, domain
                    FROM {self.config.fully_qualified_kb}
                """
                )
                table_count = tables_df.count()
        except Exception as e:
            logger.error(f"Could not read from knowledge base: {e}")
            return []

        if table_count == 0:
            logger.warning(
                f"Knowledge base table {self.config.fully_qualified_kb} is empty "
                "or all tables are up-to-date (incremental mode). "
                "Run build_knowledge_base first to populate it."
            )
            return []

        logger.info(f"Evaluating {table_count} tables for entity classification")

        discovered = []
        tables_without_matches = []

        for row in tables_df.collect():
            matches = self._match_table_to_entities(row)
            if matches:
                discovered.extend(matches)
                self._stats["keyword_matches"] += 1
            else:
                tables_without_matches.append(row)

        if tables_without_matches:
            n = len(tables_without_matches)
            batch_size = 15
            chunks = [
                tables_without_matches[i : i + batch_size]
                for i in range(0, n, batch_size)
            ]
            logger.info(
                f"Using AI to classify {n} unmatched tables in {len(chunks)} batches of <={batch_size}"
            )

            from concurrent.futures import ThreadPoolExecutor, as_completed

            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {
                    executor.submit(self._ai_classify_tables_batch, chunk): chunk
                    for chunk in chunks
                }
                for future in as_completed(futures):
                    for table_entities in future.result():
                        discovered.extend(table_entities)

        logger.info(
            f"Entity discovery complete: {self._stats['keyword_matches']} keyword matches, "
            f"{self._stats['ai_classifications']} AI classifications, "
            f"{self._stats['fallback_generic']} generic fallbacks"
        )
        return discovered

    def _normalize_name(self, name: str) -> List[str]:
        """Generate name variations for flexible matching."""
        if not name:
            return []
        name = name.lower()
        variations = [name, name.replace("_", " "), name.replace("_", "")]
        if name.endswith("ies"):
            variations.append(name[:-3] + "y")
        elif name.endswith("es"):
            variations.append(name[:-2])
        elif name.endswith("s"):
            variations.append(name[:-1])
        variations.append(name + "s")
        return list(set(variations))

    def _match_table_to_entities(self, table_row) -> List[Dict[str, Any]]:
        """Match a table to potential entity definitions with flexible matching."""
        matches = []
        table_name = table_row.table_name
        short_name = (table_row.table_short_name or "").lower()
        comment = (table_row.comment or "").lower()
        name_variations = self._normalize_name(short_name)

        threshold = self.ontology_config.get("entities", {}).get(
            "discovery_confidence_threshold", 0.4
        )

        for entity_def in self.entity_definitions:
            confidence = self._calculate_match_confidence(
                name_variations, short_name, comment, entity_def
            )
            if confidence >= threshold:
                matches.append(
                    {
                        "entity_id": str(uuid.uuid4()),
                        "entity_name": entity_def.name,
                        "entity_type": entity_def.name,
                        "description": entity_def.description,
                        "source_tables": [table_name],
                        "source_columns": [],
                        "attributes": {
                            "expected_attributes": str(entity_def.typical_attributes),
                            "discovery_method": "keyword",
                        },
                        "confidence": confidence,
                        "auto_discovered": True,
                        "validated": False,
                        "validation_notes": None,
                    }
                )
        return matches

    def _calculate_match_confidence(
        self,
        name_variations: List[str],
        original_name: str,
        comment: str,
        entity_def: EntityDefinition,
    ) -> float:
        """Calculate confidence score with flexible matching."""
        score = 0.0
        max_score = 0.0
        for keyword in entity_def.keywords:
            keyword_lower = keyword.lower()
            keyword_variations = self._normalize_name(keyword)
            max_score += 2.0
            if keyword_lower == original_name:
                score += 2.0
                continue
            matched = False
            for kw_var in keyword_variations:
                for name_var in name_variations:
                    if kw_var in name_var or name_var in kw_var:
                        score += 1.5
                        matched = True
                        break
                if matched:
                    break
            if not matched and keyword_lower in comment:
                score += 0.8
        for attr in entity_def.typical_attributes:
            max_score += 0.3
            if attr.lower() in comment:
                score += 0.3
        if max_score == 0:
            return 0.0
        return min(1.0, score / max_score)

    # ------------------------------------------------------------------
    # AI classification for tables (structured output + enforce + multi-entity)
    # ------------------------------------------------------------------

    def _ai_classify_tables_batch(
        self, table_rows: List, batch_size: int = 15
    ) -> List[List[Dict[str, Any]]]:
        """Classify a batch of tables in one LLM call.

        Returns a list parallel to table_rows, each element being the
        list of entity dicts for that table (same format as _ai_classify_table).
        Falls back to per-table classification on failure.
        """
        candidate_set: set = set()
        for row in table_rows:
            short = row.table_short_name or row.table_name.split(".")[-1]
            comment = row.comment or ""
            domain = getattr(row, "domain", None) or "unknown"
            candidate_set.update(self._keyword_prefilter(short, comment, domain))

        entity_descriptions = "\n".join(
            self._format_entity_desc(e)
            for e in self.entity_definitions
            if e.name in candidate_set
        )

        col_summaries = self._get_column_summary([r.table_name for r in table_rows])
        table_lines = []
        for i, row in enumerate(table_rows):
            short = row.table_short_name or row.table_name.split(".")[-1]
            comment = (row.comment or "No description")[:400]
            domain = getattr(row, "domain", None) or "unknown"
            line = f"{i+1}. {short} | Domain: {domain} | {comment}"
            cols_text = col_summaries.get(row.table_name, "")
            if cols_text:
                line += f" | Columns: {cols_text}"
            table_lines.append(line)
        tables_text = "\n".join(table_lines)

        system_prompt = (
            "You are an entity classifier for database tables. "
            "For each table, classify it into an entity type. "
            "If a table clearly represents a relationship between two entities, "
            "also populate secondary_entity_type. "
            "If your confidence is below 0.5, populate recommended_entity. "
            "Tables with similar names, structures, or descriptions should receive similar confidence scores. "
            "If two tables contain essentially the same data (e.g., different sample sizes of the same source), "
            "they should get the same entity type and similar confidence. "
            "Return one classification per table, using the exact table_name provided."
        )
        user_prompt = (
            f"Tables:\n{tables_text}\n\n"
            f"Available entity types:\n{entity_descriptions}\n\n"
            "Classify every table listed above."
        )

        try:
            batch_llm = self._get_batch_table_llm()
            response: BatchTableClassificationResult = batch_llm.invoke(
                [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ]
            )

            short_to_row = {}
            for row in table_rows:
                short = row.table_short_name or row.table_name.split(".")[-1]
                short_to_row[short.lower()] = row

            all_results: List[List[Dict[str, Any]]] = [[] for _ in table_rows]
            row_index = {id(r): i for i, r in enumerate(table_rows)}

            for item in response.classifications:
                matched_row = short_to_row.get(item.table_name.lower())
                if matched_row is None:
                    continue
                idx = row_index.get(id(matched_row))
                if idx is None:
                    continue

                table_name = matched_row.table_name
                enforced, exact = _enforce_entity_value(item.entity_type, self._entity_names)
                confidence = item.confidence
                if not exact:
                    confidence = max(0.0, confidence - self.CONFIDENCE_PENALTY_SNAP)

                edef = next((e for e in self.entity_definitions if e.name == enforced), None)
                attrs: Dict[str, str] = {"discovery_method": "ai_batch"}
                if item.reasoning:
                    attrs["reasoning"] = item.reasoning
                if item.recommended_entity:
                    attrs["recommended_entity"] = item.recommended_entity

                all_results[idx].append({
                    "entity_id": str(uuid.uuid4()),
                    "entity_name": enforced,
                    "entity_type": enforced,
                    "description": edef.description if edef else f"Auto-classified as {enforced}",
                    "source_tables": [table_name],
                    "source_columns": [],
                    "attributes": attrs,
                    "confidence": round(confidence, 3),
                    "auto_discovered": True,
                    "validated": False,
                    "validation_notes": item.reasoning,
                })
                self._stats["ai_classifications"] += 1

                if item.secondary_entity_type:
                    sec_enforced, sec_exact = _enforce_entity_value(
                        item.secondary_entity_type, self._entity_names
                    )
                    sec_conf = item.confidence * 0.85
                    if not sec_exact:
                        sec_conf = max(0.0, sec_conf - self.CONFIDENCE_PENALTY_SNAP)
                    min_conf = self._validation_cfg.get("min_entity_confidence", 0.5)
                    if sec_conf >= min_conf and sec_enforced != enforced:
                        sec_edef = next(
                            (e for e in self.entity_definitions if e.name == sec_enforced), None
                        )
                        all_results[idx].append({
                            "entity_id": str(uuid.uuid4()),
                            "entity_name": sec_enforced,
                            "entity_type": sec_enforced,
                            "description": sec_edef.description if sec_edef else f"Secondary: {sec_enforced}",
                            "source_tables": [table_name],
                            "source_columns": [],
                            "attributes": {"discovery_method": "ai_batch_secondary", "reasoning": item.reasoning or ""},
                            "confidence": round(sec_conf, 3),
                            "auto_discovered": True,
                            "validated": False,
                            "validation_notes": f"Secondary entity from batch classification of {table_name}",
                        })

            for i, row in enumerate(table_rows):
                if not all_results[i]:
                    all_results[i] = self._ai_classify_table(row)

            return all_results

        except Exception as e:
            logger.warning(
                f"Batch table classification failed ({len(table_rows)} tables): {e}. "
                "Falling back to per-table classification."
            )
            return [self._ai_classify_table(row) for row in table_rows]

    def _ai_classify_table(self, table_row) -> List[Dict[str, Any]]:
        """Classify a table using structured LLM output. May return 1-2 entities
        (primary + optional secondary for relationship tables)."""
        table_name = table_row.table_name
        short_name = table_row.table_short_name or table_name.split(".")[-1]
        comment = table_row.comment or "No description available"
        domain = getattr(table_row, "domain", None) or "unknown"

        candidates = self._keyword_prefilter(short_name, comment, domain)
        entity_descriptions = "\n".join(
            self._format_entity_desc(e)
            for e in self.entity_definitions
            if e.name in candidates
        )

        system_prompt = (
            "You are an entity classifier for database tables. "
            "Classify each table into an entity type. "
            "If the table clearly represents a relationship between two entities "
            "(e.g. patient_encounters links Patient and Encounter), also populate secondary_entity_type. "
            "If your confidence is below 0.5, populate recommended_entity with what you think "
            "the entity should really be called, even if it is not in the provided list."
        )
        col_summary = self._get_column_summary([table_row.table_name])
        cols_text = col_summary.get(table_row.table_name, "")
        user_prompt = (
            f"Table name: {short_name}\n"
            f"Description: {comment[:500]}\n"
            f"Domain: {domain}\n"
        )
        if cols_text:
            user_prompt += f"Columns: {cols_text}\n"
        user_prompt += (
            f"\nAvailable entity types:\n{entity_descriptions}\n\n"
            "Classify this table."
        )

        results = []
        try:
            structured_llm = self._get_structured_llm()
            response: EntityClassificationResult = structured_llm.invoke(
                [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ]
            )

            # --- primary entity ---
            enforced, exact = _enforce_entity_value(
                response.entity_type, self._entity_names
            )
            confidence = response.confidence
            if not exact:
                confidence = max(0.0, confidence - self.CONFIDENCE_PENALTY_SNAP)
                logger.debug(
                    f"Snapped '{response.entity_type}' -> '{enforced}' for {short_name}"
                )

            attrs: Dict[str, str] = {"discovery_method": "ai"}
            if response.reasoning:
                attrs["reasoning"] = response.reasoning
            if response.recommended_entity:
                attrs["recommended_entity"] = response.recommended_entity

            edef = next(
                (e for e in self.entity_definitions if e.name == enforced), None
            )
            results.append(
                {
                    "entity_id": str(uuid.uuid4()),
                    "entity_name": enforced,
                    "entity_type": enforced,
                    "description": (
                        edef.description if edef else f"Auto-classified as {enforced}"
                    ),
                    "source_tables": [table_name],
                    "source_columns": [],
                    "attributes": attrs,
                    "confidence": round(confidence, 3),
                    "auto_discovered": True,
                    "validated": False,
                    "validation_notes": response.reasoning,
                }
            )
            self._stats["ai_classifications"] += 1

            # --- secondary entity (multi-entity support) ---
            if response.secondary_entity_type:
                sec_enforced, sec_exact = _enforce_entity_value(
                    response.secondary_entity_type,
                    self._entity_names,
                )
                sec_conf = response.confidence * 0.85
                if not sec_exact:
                    sec_conf = max(0.0, sec_conf - self.CONFIDENCE_PENALTY_SNAP)
                min_conf = self._validation_cfg.get("min_entity_confidence", 0.5)
                if sec_conf >= min_conf and sec_enforced != enforced:
                    sec_edef = next(
                        (e for e in self.entity_definitions if e.name == sec_enforced),
                        None,
                    )
                    results.append(
                        {
                            "entity_id": str(uuid.uuid4()),
                            "entity_name": sec_enforced,
                            "entity_type": sec_enforced,
                            "description": (
                                sec_edef.description
                                if sec_edef
                                else f"Secondary: {sec_enforced}"
                            ),
                            "source_tables": [table_name],
                            "source_columns": [],
                            "attributes": {
                                "discovery_method": "ai_secondary",
                                "reasoning": response.reasoning or "",
                            },
                            "confidence": round(sec_conf, 3),
                            "auto_discovered": True,
                            "validated": False,
                            "validation_notes": f"Secondary entity from relationship table {short_name}",
                        }
                    )

        except Exception as e:
            logger.warning(
                f"AI classification failed for {table_name}, using fallback: {e}"
            )
            self._stats["fallback_generic"] += 1
            results.append(
                {
                    "entity_id": str(uuid.uuid4()),
                    "entity_name": "DataTable",
                    "entity_type": "DataTable",
                    "description": "Generic data table (AI classification failed)",
                    "source_tables": [table_name],
                    "source_columns": [],
                    "attributes": {"discovery_method": "ai_fallback"},
                    "confidence": 0.3,
                    "auto_discovered": True,
                    "validated": False,
                    "validation_notes": str(e),
                }
            )
        return results

    # ------------------------------------------------------------------
    # Column-level entity discovery
    # ------------------------------------------------------------------

    def discover_entities_from_columns(self) -> List[Dict[str, Any]]:
        """Discover entities by matching columns to entity definitions."""
        current_bv = self._get_bundle_version()
        try:
            if self.config.incremental:
                try:
                    cols_df = self.spark.sql(
                        f"""
                        SELECT ckb.column_name, ckb.table_name, ckb.table_short_name, ckb.comment,
                               ckb.data_type, ckb.classification, ckb.classification_type
                        FROM {self.config.fully_qualified_column_kb} ckb
                        INNER JOIN {self.config.fully_qualified_kb} kb ON ckb.table_name = kb.table_name
                        LEFT JOIN (
                            SELECT src_table, MAX(created_at) AS last_classified,
                                   MAX(bundle_version) AS last_bundle_version
                            FROM (
                                SELECT EXPLODE(source_tables) AS src_table, created_at, bundle_version
                                FROM {self.config.fully_qualified_entities}
                                WHERE attributes['granularity'] = 'column'
                            )
                            GROUP BY src_table
                        ) oe ON ckb.table_name = oe.src_table
                        WHERE oe.last_classified IS NULL
                          OR kb.updated_at > oe.last_classified
                          OR COALESCE(oe.last_bundle_version, '') != '{current_bv}'
                    """
                    )
                    col_count = cols_df.count()
                    total = self.spark.sql(f"SELECT COUNT(*) AS n FROM {self.config.fully_qualified_column_kb}").collect()[0].n
                    logger.info(f"Incremental mode: {col_count} columns need classification out of {total}")
                except Exception as e:
                    logger.warning(f"Incremental column filtering failed ({e}), falling back to full scan")
                    cols_df = self.spark.sql(
                        f"""
                        SELECT column_name, table_name, table_short_name, comment,
                               data_type, classification, classification_type
                        FROM {self.config.fully_qualified_column_kb}
                    """
                    )
                    col_count = cols_df.count()
            else:
                cols_df = self.spark.sql(
                    f"""
                    SELECT column_name, table_name, table_short_name, comment,
                           data_type, classification, classification_type
                    FROM {self.config.fully_qualified_column_kb}
                """
                )
                col_count = cols_df.count()
        except Exception as e:
            logger.warning(
                f"Could not read column_knowledge_base, skipping column discovery: {e}"
            )
            return []

        if col_count == 0:
            logger.info("column_knowledge_base is empty or all up-to-date, skipping column discovery")
            return []

        logger.info(f"Evaluating {col_count} columns for entity classification")

        min_confidence = self._validation_cfg.get("min_entity_confidence", 0.5)
        max_per_table = self._validation_cfg.get("max_entities_per_table", 3)

        table_entity_map: Dict[tuple, Dict[str, Any]] = {}
        unmatched = []

        for row in cols_df.collect():
            matches = self._match_column_to_entities(row)
            if matches:
                for entity_type, conf in matches:
                    key = (row.table_name, entity_type)
                    if key not in table_entity_map:
                        table_entity_map[key] = {
                            "columns": [],
                            "conf_sum": 0.0,
                            "count": 0,
                            "table_short_name": row.table_short_name,
                        }
                    table_entity_map[key]["columns"].append(row.column_name)
                    table_entity_map[key]["conf_sum"] += conf
                    table_entity_map[key]["count"] += 1
                self._stats["column_keyword_matches"] += 1
            else:
                unmatched.append(row)

        if unmatched:
            logger.info(f"Using AI to classify {len(unmatched)} unmatched columns")
            from collections import defaultdict

            grouped: Dict[str, List] = defaultdict(list)
            for row in unmatched:
                grouped[row.table_name].append(row)

            logger.info(
                f"Batching {len(unmatched)} columns across {len(grouped)} tables"
            )

            def _classify_table_cols(table_name, rows):
                short = rows[0].table_short_name or table_name.split(".")[-1]
                return self._ai_classify_columns_for_table(table_name, short, rows)

            from concurrent.futures import ThreadPoolExecutor, as_completed

            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {
                    executor.submit(_classify_table_cols, tbl, rows): tbl
                    for tbl, rows in grouped.items()
                }
                for future in as_completed(futures):
                    tbl = futures[future]
                    short_name = grouped[tbl][0].table_short_name
                    for col_name, entity_type, conf in future.result():
                        key = (tbl, entity_type)
                        if key not in table_entity_map:
                            table_entity_map[key] = {
                                "columns": [],
                                "conf_sum": 0.0,
                                "count": 0,
                                "table_short_name": short_name,
                            }
                        table_entity_map[key]["columns"].append(col_name)
                        table_entity_map[key]["conf_sum"] += conf
                        table_entity_map[key]["count"] += 1

        discovered = []
        table_groups: Dict[str, list] = {}
        for (tbl, etype), info in table_entity_map.items():
            avg_conf = info["conf_sum"] / info["count"]
            if avg_conf < min_confidence:
                continue
            entity_def = next(
                (e for e in self.entity_definitions if e.name == etype), None
            )
            bindings = []
            if entity_def:
                for col in info["columns"]:
                    attr = self._best_attribute_for_column(col, entity_def)
                    bindings.append({"attribute_name": attr, "bound_table": tbl, "bound_column": col})
            entry = {
                "entity_id": str(uuid.uuid4()),
                "entity_name": etype,
                "entity_type": etype,
                "description": (
                    entity_def.description if entity_def else f"Column-derived {etype}"
                ),
                "source_tables": [tbl],
                "source_columns": info["columns"],
                "column_bindings": bindings,
                "attributes": {
                    "granularity": "column",
                    "discovery_method": "column_keyword",
                },
                "confidence": round(avg_conf, 3),
                "auto_discovered": True,
                "validated": False,
                "validation_notes": None,
                "_avg_conf": avg_conf,
            }
            table_groups.setdefault(tbl, []).append(entry)

        for tbl, entries in table_groups.items():
            entries.sort(key=lambda e: e["_avg_conf"], reverse=True)
            for entry in entries[:max_per_table]:
                entry.pop("_avg_conf", None)
                discovered.append(entry)

        logger.info(
            f"Column entity discovery: {self._stats['column_keyword_matches']} keyword, "
            f"{self._stats['column_ai_classifications']} AI, "
            f"{self._stats['column_fallback']} fallback. "
            f"{len(discovered)} column entities kept after thresholds."
        )
        return discovered

    def _match_column_to_entities(self, col_row) -> List[tuple]:
        """Match a column to entity definitions. Returns list of (entity_type, confidence)."""
        col_name = (col_row.column_name or "").lower()
        comment = (col_row.comment or "").lower()
        classification = (col_row.classification or "").lower()
        col_variations = self._normalize_name(col_name)

        matches = []
        for entity_def in self.entity_definitions:
            if entity_def.name == "DataTable":
                continue
            score = self._column_match_score(
                col_variations, col_name, comment, classification, entity_def
            )
            if score >= 0.4:
                matches.append((entity_def.name, score))
        return matches

    def _column_match_score(
        self,
        col_variations: List[str],
        col_name: str,
        comment: str,
        classification: str,
        entity_def: EntityDefinition,
    ) -> float:
        """Score how well a column matches an entity definition."""
        score = 0.0
        max_score = 0.0
        for attr in entity_def.typical_attributes:
            attr_lower = attr.lower()
            max_score += 2.0
            if attr_lower == col_name:
                score += 2.0
            elif attr_lower in col_name or col_name in attr_lower:
                score += 1.2
            elif any(attr_lower in v for v in col_variations):
                score += 0.8
        for kw in entity_def.keywords:
            kw_lower = kw.lower()
            max_score += 1.0
            if kw_lower in col_name:
                score += 1.0
            elif kw_lower in comment:
                score += 0.5
        entity_lower = entity_def.name.lower()
        if re.match(rf"{entity_lower}[_\s]?id$", col_name):
            score += 2.0
            max_score += 2.0
        healthcare_entities = {
            "patient",
            "provider",
            "encounter",
            "condition",
            "procedure",
            "medication",
            "observation",
            "claim",
            "coverage",
        }
        if classification in ("phi", "pii") and entity_lower in healthcare_entities:
            score += 0.5
            max_score += 0.5
        if max_score == 0:
            return 0.0
        return min(1.0, score / max_score)

    @staticmethod
    def _best_attribute_for_column(col_name: str, entity_def: EntityDefinition) -> str:
        """Derive the best-matching business attribute for a physical column.

        Strips entity-name prefixes (e.g. ``patient_mrn`` -> ``mrn``) and
        matches against ``typical_attributes``.  Falls back to the raw
        column name when no attribute matches.
        """
        cn = col_name.lower()
        prefix = entity_def.name.lower() + "_"
        stripped = cn[len(prefix):] if cn.startswith(prefix) else cn
        for attr in entity_def.typical_attributes:
            al = attr.lower()
            if al == stripped or al == cn:
                return attr
        for attr in entity_def.typical_attributes:
            al = attr.lower()
            if al in stripped or stripped in al:
                return attr
        return stripped

    def compute_conformance(
        self,
        table_entities: List[Dict[str, Any]],
        column_properties: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Compare discovered columns vs bundle declared properties per table.

        Returns per-table conformance metrics: declared_properties, matched_properties,
        extra_columns, missing_properties, conformance_score, missing, extras.
        """
        results: List[Dict[str, Any]] = []
        table_entity: Dict[str, str] = {}
        for r in table_entities:
            for t in r.get("source_tables") or []:
                if r.get("entity_role") == "primary":
                    table_entity[t] = r.get("entity_type", "")

        cols_by_table: Dict[str, set] = {}
        matched_by_table: Dict[str, set] = {}
        for cp in column_properties:
            tbl = cp.get("table_name")
            col = cp.get("column_name")
            if not tbl or not col:
                continue
            cols_by_table.setdefault(tbl, set()).add(col)
            dm = cp.get("discovery_method") or ""
            if "bundle" in dm.lower() or dm == "bundle_match":
                matched_by_table.setdefault(tbl, set()).add(col)

        for tbl, entity_type in table_entity.items():
            if not entity_type:
                continue
            edef = self._entity_def_map.get(entity_type)
            declared_attrs: set = set()
            if edef and edef.properties:
                for prop in edef.properties:
                    for attr in prop.typical_attributes:
                        declared_attrs.add(attr.lower())
            declared_count = len(declared_attrs)
            discovered = cols_by_table.get(tbl, set())
            matched = matched_by_table.get(tbl, set())
            matched_count = len(matched)
            missing = [a for a in declared_attrs if not any(
                c.lower() == a or a in c.lower() for c in discovered
            )]
            extras = [c for c in discovered if c.lower() not in declared_attrs]
            score = matched_count / declared_count if declared_count > 0 else 1.0
            results.append({
                "table_name": tbl,
                "entity_type": entity_type,
                "declared_properties": declared_count,
                "matched_properties": matched_count,
                "extra_columns": len(extras),
                "missing_properties": len(missing),
                "conformance_score": round(score, 2),
                "missing": missing[:20],
                "extras": sorted(extras)[:20],
            })
        return results

    # ------------------------------------------------------------------
    # AI classification for columns (structured output + enforce)
    # ------------------------------------------------------------------

    def _classify_column_chunk(
        self, short_name: str, columns: List
    ) -> List[Tuple[str, str, float]]:
        """Classify a chunk of columns via a single LLM call.

        Returns list of (column_name, entity_type, confidence).
        """
        entity_descriptions = "\n".join(
            self._format_entity_desc(e)
            for e in self.entity_definitions
            if e.name != "DataTable"
        )

        col_lines = []
        for row in columns:
            cname = row.column_name or "unknown"
            dtype = row.data_type or "unknown"
            desc = (row.comment or "")[:200]
            col_lines.append(f"  - {cname} ({dtype}): {desc}")
        columns_text = "\n".join(col_lines)

        system_prompt = (
            "You are an entity classifier for database columns. "
            "For each column, classify it into the entity type it most likely belongs to. "
            "Classify based on what the column itself represents, not the table's domain. "
            "Tables in specialized domains (e.g. healthcare) often contain commercial, "
            "supply-chain, or financial columns (product_name, supplier_id, unit_price) "
            "that should map to Product, Supplier, or Transaction -- not to domain-specific "
            "entities like Procedure or Medication. "
            "Return one classification per column, using the exact column_name provided."
        )
        user_prompt = (
            f"Table: {short_name}\n\n"
            f"Columns:\n{columns_text}\n\n"
            f"Entity types:\n{entity_descriptions}\n\n"
            "Classify every column listed above."
        )

        batch_llm = self._get_batch_column_llm()
        response: BatchColumnClassificationResult = batch_llm.invoke(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        )
        results = []
        for item in response.classifications:
            enforced, exact = _enforce_entity_value(item.entity_type, self._entity_names)
            conf = item.confidence
            if not exact:
                conf = max(0.0, conf - self.CONFIDENCE_PENALTY_SNAP)
            results.append((item.column_name, enforced, round(conf, 3)))
        self._stats["column_ai_classifications"] += len(results)
        return results

    def _ai_query_classify_columns(
        self, columns: List
    ) -> List[Tuple[str, str, float]]:
        """Classify columns via Spark SQL ai_query -- batch fallback for failed chunks."""
        entity_descriptions = "\n".join(
            self._format_entity_desc(e)
            for e in self.entity_definitions
            if e.name != "DataTable"
        )
        entity_names_csv = ", ".join(self._entity_names)
        escaped_desc = entity_descriptions.replace("'", "''")
        escaped_names = entity_names_csv.replace("'", "''")

        rows = [
            {
                "column_name": r.column_name or "unknown",
                "table_short_name": getattr(r, "table_short_name", None) or r.table_name or "unknown",
                "data_type": r.data_type or "unknown",
                "comment": (r.comment or "")[:200],
            }
            for r in columns
        ]
        df = self.spark.createDataFrame(rows)
        view_name = f"_ontology_ai_classify_cols_{uuid.uuid4().hex[:8]}"
        df.createOrReplaceTempView(view_name)

        sql = f"""
            SELECT
                column_name,
                ai_query(
                    '{self._model_endpoint}',
                    CONCAT(
                        'You are an entity classifier for database columns. ',
                        'Classify this column into the entity type it most likely belongs to. ',
                        'Classify based on what the column itself represents, not the table domain. ',
                        'Valid entity types: {escaped_names}. ',
                        'Entity descriptions:\\n{escaped_desc}\\n\\n',
                        'Column: ', column_name,
                        '\\nTable: ', table_short_name,
                        '\\nData type: ', data_type,
                        '\\nDescription: ', comment,
                        '\\n\\nReturn JSON with entity_type and confidence (0-1).'
                    ),
                    responseFormat => 'STRUCT<result: STRUCT<entity_type: STRING, confidence: DOUBLE>>',
                    failOnError => false
                ) AS classification
            FROM {view_name}
        """
        try:
            result_rows = self.spark.sql(sql).collect()
        except Exception as e:
            logger.warning(f"ai_query batch classification failed: {e}. Using DataTable fallback.")
            self.spark.catalog.dropTempView(view_name)
            return [(r["column_name"], "DataTable", 0.2) for r in rows]

        self.spark.catalog.dropTempView(view_name)

        results = []
        for rr in result_rows:
            col_name = rr.column_name
            cls = rr.classification
            if cls and cls.result and cls.result.entity_type:
                enforced, exact = _enforce_entity_value(cls.result.entity_type, self._entity_names)
                conf = cls.result.confidence or 0.5
                if not exact:
                    conf = max(0.0, conf - self.CONFIDENCE_PENALTY_SNAP)
                results.append((col_name, enforced, round(conf, 3)))
                self._stats["column_ai_classifications"] += 1
            else:
                results.append((col_name, "DataTable", 0.2))
                self._stats["column_fallback"] += 1
        return results

    def _ai_classify_columns_for_table(
        self, table_name: str, short_name: str, columns: List
    ) -> List[Tuple[str, str, float]]:
        """Classify all columns for a single table, chunking if needed.

        Returns list of (column_name, entity_type, confidence).
        Falls back to ai_query batch classification on chunk failure.
        """
        n = self.config.metadata_cols_per_chunk
        # Merge small remainder into last chunk to avoid wasteful splits
        if len(columns) <= n or (len(columns) <= n * 1.25):
            try:
                return self._classify_column_chunk(short_name, columns)
            except Exception as e:
                logger.warning(
                    f"Batch column classification failed for {table_name} ({len(columns)} cols): {e}. "
                    "Retrying with sub-chunks before ai_query fallback."
                )
                # Sub-chunk retry: split in half
                mid = len(columns) // 2
                results = []
                for sub in (columns[:mid], columns[mid:]):
                    try:
                        results.extend(self._classify_column_chunk(short_name, sub))
                    except Exception:
                        logger.warning(
                            f"Sub-chunk ({len(sub)} cols) also failed for {table_name}. "
                            "Falling back to ai_query batch."
                        )
                        results.extend(self._ai_query_classify_columns(sub))
                return results

        num_chunks = -(-len(columns) // n)
        all_results = []
        for i in range(0, len(columns), n):
            chunk = columns[i : i + n]
            chunk_idx = i // n + 1
            logger.info(f"Classifying {table_name}: chunk {chunk_idx}/{num_chunks} ({len(chunk)} cols)")
            try:
                all_results.extend(self._classify_column_chunk(short_name, chunk))
            except Exception as e:
                logger.warning(f"Chunk {chunk_idx} failed for {table_name}, using ai_query fallback: {e}")
                all_results.extend(self._ai_query_classify_columns(chunk))
        return all_results

    def _ai_classify_column(self, col_row) -> Tuple[str, float]:
        """AI classification for a single column using structured output."""
        col_name = col_row.column_name or "unknown"
        table_name = col_row.table_short_name or col_row.table_name or "unknown"
        comment = col_row.comment or "No description"
        data_type = col_row.data_type or "unknown"

        entity_descriptions = "\n".join(
            self._format_entity_desc(e)
            for e in self.entity_definitions
            if e.name != "DataTable"
        )

        system_prompt = (
            "You are an entity classifier for database columns. "
            "Classify each column into the entity type it most likely belongs to. "
            "If your confidence is below 0.5, populate recommended_entity with a better name."
        )
        user_prompt = (
            f"Column: {col_name}\nTable: {table_name}\n"
            f"Data type: {data_type}\nDescription: {comment[:300]}\n\n"
            f"Entity types:\n{entity_descriptions}\n\nClassify this column."
        )

        try:
            structured_llm = self._get_structured_llm()
            response: EntityClassificationResult = structured_llm.invoke(
                [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ]
            )
            enforced, exact = _enforce_entity_value(
                response.entity_type, self._entity_names
            )
            conf = response.confidence
            if not exact:
                conf = max(0.0, conf - self.CONFIDENCE_PENALTY_SNAP)
            self._stats["column_ai_classifications"] += 1
            return (enforced, round(conf, 3))
        except Exception as e:
            logger.warning(f"AI column classification failed for {col_name}: {e}")
            self._stats["column_fallback"] += 1
            return ("DataTable", 0.2)

    # ------------------------------------------------------------------
    # Deduplication / merge
    # ------------------------------------------------------------------

    @staticmethod
    def deduplicate_entities(entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge duplicate entities from different discovery methods.

        Groups by (source_table, entity_type, granularity) and merges
        source_columns, takes max confidence, and combines attributes.
        """
        groups: Dict[tuple, Dict[str, Any]] = {}
        for ent in entities:
            granularity = ent.get("attributes", {}).get("granularity", "table")
            for tbl in ent.get("source_tables", ["unknown"]):
                key = (tbl, ent["entity_type"], granularity)
                if key not in groups:
                    groups[key] = {
                        **ent,
                        "source_tables": [tbl],
                        "source_columns": list(ent.get("source_columns", [])),
                        "attributes": dict(ent.get("attributes", {})),
                    }
                else:
                    existing = groups[key]
                    existing["confidence"] = max(
                        existing["confidence"], ent.get("confidence", 0)
                    )
                    for col in ent.get("source_columns", []):
                        if col not in existing["source_columns"]:
                            existing["source_columns"].append(col)
                    for k, v in ent.get("attributes", {}).items():
                        if k not in existing["attributes"]:
                            existing["attributes"][k] = v
                    if ent.get("validation_notes") and not existing.get(
                        "validation_notes"
                    ):
                        existing["validation_notes"] = ent["validation_notes"]
        return list(groups.values())


class OntologyBuilder:
    """
    Builder for creating and managing the ontology layer.

    Creates ontology_entities and ontology_metrics tables, discovers
    entities from the knowledge base, and manages relationships.
    """

    _BINDING_SCHEMA = ArrayType(
        StructType(
            [
                StructField("attribute_name", StringType(), True),
                StructField("bound_table", StringType(), True),
                StructField("bound_column", StringType(), True),
            ]
        )
    )

    ENTITIES_SCHEMA = StructType(
        [
            StructField("entity_id", StringType(), False),
            StructField("entity_name", StringType(), True),
            StructField("entity_type", StringType(), True),
            StructField("description", StringType(), True),
            StructField("source_tables", ArrayType(StringType()), True),
            StructField("source_columns", ArrayType(StringType()), True),
            StructField("attributes", MapType(StringType(), StringType()), True),
            StructField("confidence", DoubleType(), True),
            StructField("discovery_confidence", DoubleType(), True),
            StructField("entity_role", StringType(), True),
            StructField("is_canonical", BooleanType(), True),
            StructField("auto_discovered", BooleanType(), True),
            StructField("validated", BooleanType(), True),
            StructField("validation_notes", StringType(), True),
            StructField("ontology_bundle", StringType(), True),
            StructField("bundle_version", StringType(), True),
            StructField("discovery_timestamp", TimestampType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("column_bindings", _BINDING_SCHEMA, True),
        ]
    )

    def __init__(self, spark: SparkSession, config: OntologyConfig):
        self.spark = spark
        self.config = config
        self.ontology_config = OntologyLoader.load_config(config.config_path)
        bundle_tag_key = (
            self.ontology_config
            .get("_bundle_metadata", {})
            .get("tag_key")
        )
        if bundle_tag_key and config.entity_tag_key in ("entity_type", "ontology.entity_type"):
            config.entity_tag_key = bundle_tag_key
            logger.info(f"Using entity_tag_key '{bundle_tag_key}' from bundle metadata")
        affinity = load_domain_entity_affinity(self.ontology_config)
        self.discoverer = EntityDiscoverer(
            spark, config, self.ontology_config, domain_entity_affinity=affinity
        )

    def _get_bundle_version(self) -> str:
        """Get bundle version from ontology config (ontology.version or metadata.version)."""
        ont = self.ontology_config.get("ontology", {}) or {}
        meta = self.ontology_config.get("metadata", {}) or {}
        return str(ont.get("version") or meta.get("version") or "1.0")

    def _insert_edges_safe(self, df: DataFrame, table: str):
        """Schema-safe INSERT into graph_edges: align columns + named SQL INSERT."""
        from dbxmetagen.knowledge_graph import KnowledgeGraphBuilder
        cols = []
        for name, dtype in KnowledgeGraphBuilder._EDGE_SCHEMA:
            if name in df.columns:
                cols.append(F.col(name).cast(dtype).alias(name))
            else:
                cols.append(F.lit(None).cast(dtype).alias(name))
        aligned = df.select(*cols)
        view = f"_staged_ont_edges_{id(df) % 10000}"
        aligned.createOrReplaceTempView(view)
        col_list = ", ".join(c for c, _ in KnowledgeGraphBuilder._EDGE_SCHEMA)
        self.spark.sql(f"INSERT INTO {table} ({col_list}) SELECT {col_list} FROM {view}")

    def create_entities_table(self) -> None:
        """Create the ontology entities table."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_entities} (
            entity_id STRING NOT NULL,
            entity_name STRING,
            entity_type STRING,
            description STRING,
            source_tables ARRAY<STRING>,
            source_columns ARRAY<STRING>,
            attributes MAP<STRING, STRING>,
            confidence DOUBLE,
            auto_discovered BOOLEAN,
            validated BOOLEAN,
            validation_notes STRING,
            ontology_bundle STRING,
            column_bindings ARRAY<STRUCT<attribute_name: STRING, bound_table: STRING, bound_column: STRING>>,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        COMMENT 'Ontology entities discovered from knowledge base'
        """
        self.spark.sql(ddl)
        existing = {f.name for f in self.spark.table(self.config.fully_qualified_entities).schema.fields}
        new_columns = {
            "ontology_bundle": "STRING",
            "column_bindings": "ARRAY<STRUCT<attribute_name: STRING, bound_table: STRING, bound_column: STRING>>",
            "entity_role": "STRING",
            "is_canonical": "BOOLEAN",
            "discovery_confidence": "DOUBLE",
            "bundle_version": "STRING",
            "discovery_timestamp": "TIMESTAMP",
        }
        for col_name, col_type in new_columns.items():
            if col_name not in existing:
                try:
                    self.spark.sql(
                        f"ALTER TABLE {self.config.fully_qualified_entities} ADD COLUMN {col_name} {col_type}"
                    )
                    logger.info("Added column %s to entities table", col_name)
                except Exception as e:
                    logger.warning("Could not add column %s: %s", col_name, e)
        try:
            self.spark.sql(
                f"UPDATE {self.config.fully_qualified_entities} "
                f"SET confidence = GREATEST(0.0, LEAST(1.0, confidence)) "
                f"WHERE confidence < 0.0 OR confidence > 1.0"
            )
        except Exception as e:
            logger.warning("Confidence cleanup failed (table may be empty): %s", e)
        logger.info(f"Entities table {self.config.fully_qualified_entities} ready")

    def create_metrics_table(self) -> None:
        """Create the ontology metrics table.

        NOTE: This table is intentionally a stub for future Unity Catalog
        metric views integration. It is created but not populated. When UC
        metric views become generally available, this table will hold
        entity-level metric definitions that map to UC metric views.
        """
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_metrics} (
            metric_id STRING NOT NULL,
            metric_name STRING,
            description STRING,
            entity_id STRING,
            sql_definition STRING,
            uc_view_name STRING,
            aggregation_type STRING,
            source_field STRING,
            filter_condition STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        COMMENT 'Ontology metrics linked to entities (stub for UC metric views)'
        """
        self.spark.sql(ddl)
        logger.info(f"Metrics table {self.config.fully_qualified_metrics} ready")

    def create_column_properties_table(self) -> None:
        """Create the ontology column properties table for property-level classification."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_column_properties} (
            property_id STRING NOT NULL,
            table_name STRING,
            column_name STRING,
            property_name STRING,
            property_role STRING,
            owning_entity_id STRING,
            owning_entity_type STRING,
            linked_entity_type STRING,
            confidence DOUBLE,
            auto_discovered BOOLEAN,
            discovery_method STRING,
            is_sensitive BOOLEAN,
            is_surrogate_key BOOLEAN,
            is_semi_structured BOOLEAN,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        COMMENT 'Column-level property roles and orthogonal flags'
        """
        self.spark.sql(ddl)
        existing = set()
        try:
            existing = {f.name for f in self.spark.table(self.config.fully_qualified_column_properties).schema.fields}
        except Exception:
            pass
        for col_name, col_type in [("bundle_version", "STRING"), ("discovery_timestamp", "TIMESTAMP")]:
            if col_name not in existing:
                try:
                    self.spark.sql(
                        f"ALTER TABLE {self.config.fully_qualified_column_properties} ADD COLUMN {col_name} {col_type}"
                    )
                    logger.info("Added column %s to column properties table", col_name)
                except Exception as e:
                    logger.warning("Could not add column %s: %s", col_name, e)
        logger.info("Column properties table %s ready", self.config.fully_qualified_column_properties)

    def create_discovery_diff_table(self) -> None:
        """Create the discovery diff report table for storing run diffs."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_discovery_diff} (
            report_id STRING NOT NULL,
            catalog STRING,
            schema STRING,
            bundle_version STRING,
            previous_version STRING,
            timestamp TIMESTAMP,
            diff_json STRING,
            created_at TIMESTAMP
        )
        COMMENT 'Discovery diff reports from ontology runs'
        """
        self.spark.sql(ddl)
        logger.info("Discovery diff table %s ready", self.config.fully_qualified_discovery_diff)

    def create_relationships_table(self) -> None:
        """Create the ontology relationships table for named entity-to-entity relationships."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.fully_qualified_relationships} (
            relationship_id STRING NOT NULL,
            src_entity_type STRING,
            relationship_name STRING,
            dst_entity_type STRING,
            cardinality STRING,
            evidence_column STRING,
            evidence_table STRING,
            source STRING,
            confidence DOUBLE,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        COMMENT 'Named relationships between ontology entity types'
        """
        self.spark.sql(ddl)
        logger.info("Relationships table %s ready", self.config.fully_qualified_relationships)

    def _purge_stale_bundle_entities(self, current_bundle: str) -> int:
        """Remove auto-discovered, unvalidated entities from a different bundle."""
        if not current_bundle:
            return 0
        esc = current_bundle.replace("'", "''")
        result = self.spark.sql(
            f"""
            DELETE FROM {self.config.fully_qualified_entities}
            WHERE ontology_bundle IS NOT NULL
              AND ontology_bundle != '{esc}'
              AND auto_discovered = TRUE
              AND validated = FALSE
            """
        )
        count = result.first()[0] if result.first() else 0
        if count:
            logger.info("Purged %d stale entities from previous bundle (keeping validated)", count)
        return count

    def _store_entities(
        self,
        entities: List[Dict[str, Any]],
        view_name: str = "new_entities",
        bundle_version: Optional[str] = None,
        discovery_timestamp: Optional[datetime] = None,
    ) -> int:
        """Store entity dicts via MERGE (insert new, update existing)."""
        if not entities:
            return 0

        bundle_name = self.config.ontology_bundle or None
        now = datetime.now()
        version = bundle_version or self._get_bundle_version()
        ts = discovery_timestamp or now
        rows = []
        for entity in entities:
            attributes = entity.get("attributes", {})
            if isinstance(attributes, dict):
                attributes = {k: str(v) for k, v in attributes.items()}
            else:
                attributes = {}
            bindings = entity.get("column_bindings") or []
            rows.append(
                (
                    entity["entity_id"],
                    entity.get("entity_name"),
                    entity.get("entity_type"),
                    entity.get("description"),
                    entity.get("source_tables", []),
                    entity.get("source_columns", []),
                    attributes,
                    max(0.0, min(1.0, float(entity.get("confidence", 0.0)))),
                    max(0.0, min(1.0, float(entity.get("discovery_confidence") or entity.get("confidence", 0.0)))),
                    entity.get("entity_role"),
                    bool(entity.get("is_canonical", False)),
                    bool(entity.get("auto_discovered", True)),
                    bool(entity.get("validated", False)),
                    entity.get("validation_notes"),
                    bundle_name,
                    version,
                    ts,
                    now,
                    now,
                    bindings,
                )
            )

        df = self.spark.createDataFrame(rows, schema=self.ENTITIES_SCHEMA)
        df.createOrReplaceTempView(view_name)

        self.spark.sql(
            f"""
        MERGE INTO {self.config.fully_qualified_entities} AS target
        USING {view_name} AS source
        ON target.entity_name = source.entity_name
           AND array_join(target.source_tables, ',') = array_join(source.source_tables, ',')
           AND COALESCE(target.attributes['granularity'], 'table') = COALESCE(source.attributes['granularity'], 'table')
        WHEN MATCHED AND target.auto_discovered = TRUE AND target.validated = FALSE THEN UPDATE SET
            target.confidence = source.confidence,
            target.source_columns = source.source_columns,
            target.attributes = source.attributes,
            target.column_bindings = source.column_bindings,
            target.bundle_version = source.bundle_version,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
        """
        )
        return len(entities)

    def discover_and_store_entities(self) -> int:
        """Discover table-level entities, deduplicate, and store."""
        bundle_name = self.config.ontology_bundle or None
        if bundle_name:
            self._purge_stale_bundle_entities(bundle_name)

        entities = self.discoverer.discover_entities_from_tables()
        if not entities:
            logger.warning(
                "No entities discovered. Check that table_knowledge_base has data."
            )
            return 0
        entities = EntityDiscoverer.deduplicate_entities(entities)
        now = datetime.now()
        stored = self._store_entities(
            entities, "new_table_entities",
            bundle_version=self._get_bundle_version(),
            discovery_timestamp=now,
        )
        logger.info(f"Stored {stored} table-level entities (after dedup)")
        return stored

    def discover_and_store_column_entities(self) -> int:
        """Discover column-level entities, deduplicate, and store."""
        entities = self.discoverer.discover_entities_from_columns()
        if not entities:
            logger.info("No column-level entities discovered")
            return 0
        entities = EntityDiscoverer.deduplicate_entities(entities)
        now = datetime.now()
        stored = self._store_entities(
            entities, "new_column_entities",
            bundle_version=self._get_bundle_version(),
            discovery_timestamp=now,
        )
        logger.info(f"Stored {stored} column-level entities (after dedup)")
        return stored

    def backfill_source_columns(self) -> int:
        """Update table-level entities' source_columns and column_bindings from column-level matches."""
        try:
            self.spark.sql(
                f"ALTER TABLE {self.config.fully_qualified_entities} "
                "ADD COLUMN IF NOT EXISTS column_bindings "
                "ARRAY<STRUCT<attribute_name: STRING, bound_table: STRING, bound_column: STRING>>"
            )
        except Exception:
            pass
        try:
            self.spark.sql(
                f"""
            MERGE INTO {self.config.fully_qualified_entities} AS tbl
            USING (
                SELECT entity_name,
                       array_distinct(flatten(collect_list(source_columns))) AS cols,
                       flatten(collect_list(COALESCE(column_bindings, array()))) AS bindings,
                       source_tables[0] AS tbl_name
                FROM {self.config.fully_qualified_entities}
                WHERE attributes['granularity'] = 'column'
                  AND SIZE(source_columns) > 0
                GROUP BY entity_name, source_tables[0]
            ) AS col_agg
            ON tbl.entity_name = col_agg.entity_name
               AND array_contains(tbl.source_tables, col_agg.tbl_name)
               AND COALESCE(tbl.attributes['granularity'], 'table') = 'table'
            WHEN MATCHED AND SIZE(tbl.source_columns) = 0 THEN
                UPDATE SET source_columns = col_agg.cols,
                           column_bindings = col_agg.bindings,
                           updated_at = current_timestamp()
            """
            )
            logger.info("Backfilled source_columns and column_bindings on table-level entities")
            return 1
        except Exception as e:
            logger.warning(f"Could not backfill source_columns: {e}")
            return 0

    def populate_entity_metrics(
        self, model_endpoint: str = "databricks-gpt-oss-120b"
    ) -> int:
        """Generate entity-level metric suggestions and populate ontology_metrics."""
        ents = self.spark.sql(
            f"""
            SELECT entity_id, entity_name, entity_type, description,
                   source_tables, source_columns
            FROM {self.config.fully_qualified_entities}
            WHERE confidence >= 0.5 AND COALESCE(attributes['granularity'], 'table') = 'table'
        """
        ).collect()
        if not ents:
            return 0

        # Get column metadata for the source tables
        all_tables = set()
        for e in ents:
            for t in e["source_tables"] or []:
                all_tables.add(t)
        tbl_clause = ", ".join(f"'{t}'" for t in all_tables)
        col_meta = {}
        if tbl_clause:
            try:
                col_rows = self.spark.sql(
                    f"SELECT table_name, column_name, data_type FROM {self.config.fully_qualified_column_kb} "
                    f"WHERE table_name IN ({tbl_clause})"
                ).collect()
                for c in col_rows:
                    col_meta.setdefault(c["table_name"], []).append(
                        f"{c['column_name']} {c['data_type']}"
                    )
            except Exception:
                pass

        stored = 0
        for e in ents:
            tables = e["source_tables"] or []
            cols_context = ""
            for t in tables:
                c_list = col_meta.get(t, [])
                if c_list:
                    cols_context += f"  {t}: {', '.join(c_list)}\n"
            prompt = (
                f"Entity: {e['entity_name']} ({e['entity_type']})\n"
                f"Description: {e['description']}\n"
                f"Tables and columns:\n{cols_context}\n"
                "Generate 3-5 business metrics for this entity as JSON array. "
                'Each: {{"metric_name": "...", "description": "...", '
                '"aggregation_type": "SUM|COUNT|AVG|MIN|MAX", '
                '"source_field": "column_name", "filter_condition": "optional WHERE clause"}}. '
                "Only output JSON array."
            )
            escaped = prompt.replace("'", "''")
            try:
                result = self.spark.sql(
                    f"SELECT AI_QUERY('{model_endpoint}', '{escaped}') as response"
                ).collect()[0]["response"]
                text = result.strip()
                text = re.sub(r"^```(?:json)?\s*", "", text)
                text = re.sub(r"\s*```$", "", text)
                s, end = text.find("["), text.rfind("]")
                if s != -1 and end != -1:
                    metrics = json.loads(text[s : end + 1])
                else:
                    continue
            except Exception as ex:
                logger.warning(
                    "AI metric generation failed for %s: %s", e["entity_name"], ex
                )
                continue

            now = datetime.now().isoformat()
            for m in metrics:
                mid = str(uuid.uuid4())
                vals = {
                    "metric_id": mid,
                    "metric_name": m.get("metric_name", ""),
                    "description": m.get("description", ""),
                    "entity_id": e["entity_id"],
                    "aggregation_type": m.get("aggregation_type", ""),
                    "source_field": m.get("source_field", ""),
                    "filter_condition": m.get("filter_condition", ""),
                    "created_at": now,
                }
                for k in vals:
                    vals[k] = str(vals[k]).replace("'", "''")
                self.spark.sql(
                    f"INSERT INTO {self.config.fully_qualified_metrics} VALUES ("
                    f"'{vals['metric_id']}', '{vals['metric_name']}', '{vals['description']}', "
                    f"'{vals['entity_id']}', NULL, NULL, '{vals['aggregation_type']}', "
                    f"'{vals['source_field']}', '{vals['filter_condition']}', "
                    f"'{vals['created_at']}', NULL)"
                )
                stored += 1

        logger.info("Populated %d ontology metrics", stored)
        return stored

    def compute_ontology_metrics(self) -> int:
        """Compute aggregate ontology health metrics and write to ontology_metrics."""
        ent = self.config.fully_qualified_entities
        edges = f"{self.config.catalog_name}.{self.config.schema_name}.graph_edges"
        kb = self.config.fully_qualified_kb
        metrics_tbl = self.config.fully_qualified_metrics
        now = datetime.now().isoformat()

        try:
            rows = self.spark.sql(f"""
                WITH ent AS (SELECT * FROM {ent}),
                kb AS (SELECT DISTINCT table_name FROM {kb}),
                covered AS (
                    SELECT DISTINCT t.table_name
                    FROM ent e LATERAL VIEW EXPLODE(e.source_tables) t AS table_name
                    WHERE COALESCE(e.attributes['granularity'], 'table') = 'table'
                ),
                edge_counts AS (
                    SELECT relationship, COUNT(*) as cnt FROM {edges}
                    WHERE relationship IN ('instance_of','has_attribute','is_a','references')
                    GROUP BY relationship
                )
                SELECT
                    COUNT(DISTINCT e.entity_id) as total_entities,
                    COUNT(DISTINCT e.entity_type) as total_entity_types,
                    ROUND(AVG(e.confidence), 4) as avg_confidence,
                    ROUND(COALESCE(
                        (SELECT COUNT(*) FROM covered) * 100.0 / NULLIF((SELECT COUNT(*) FROM kb), 0)
                    , 0), 1) as table_coverage_pct,
                    SUM(CASE WHEN SIZE(COALESCE(e.column_bindings, ARRAY())) > 0 THEN 1 ELSE 0 END) as entities_with_bindings,
                    SUM(CASE WHEN COALESCE(e.attributes['discovery_method'],'') LIKE '%keyword%' THEN 1 ELSE 0 END) as keyword_discovered,
                    SUM(CASE WHEN COALESCE(e.attributes['discovery_method'],'') LIKE '%ai%' THEN 1 ELSE 0 END) as ai_discovered,
                    SUM(CASE WHEN e.confidence < 0.5 THEN 1 ELSE 0 END) as low_confidence_count,
                    (SELECT COALESCE(SUM(cnt),0) FROM edge_counts) as ontology_edges_total,
                    (SELECT COALESCE(MAX(cnt),0) FROM edge_counts WHERE relationship='instance_of') as instance_of_edges,
                    (SELECT COALESCE(MAX(cnt),0) FROM edge_counts WHERE relationship='has_attribute') as has_attribute_edges,
                    (SELECT COALESCE(MAX(cnt),0) FROM edge_counts WHERE relationship='is_a') as is_a_edges,
                    (SELECT COALESCE(MAX(cnt),0) FROM edge_counts WHERE relationship='references') as references_edges
                FROM ent e
            """).collect()[0]

            metrics = [
                ("total_entities", "Total discovered entities", str(rows.total_entities)),
                ("total_entity_types", "Distinct entity types", str(rows.total_entity_types)),
                ("avg_confidence", "Average entity confidence", str(rows.avg_confidence)),
                ("table_coverage_pct", "% of KB tables with an entity", str(rows.table_coverage_pct)),
                ("binding_completeness_pct", "% of entities with column bindings",
                 str(round(rows.entities_with_bindings * 100.0 / max(rows.total_entities, 1), 1))),
                ("keyword_discovered", "Entities discovered by keyword", str(rows.keyword_discovered)),
                ("ai_discovered", "Entities discovered by AI", str(rows.ai_discovered)),
                ("low_confidence_count", "Entities with confidence < 0.5", str(rows.low_confidence_count)),
                ("ontology_edges_total", "Total ontology relationship edges", str(rows.ontology_edges_total)),
                ("instance_of_edges", "Table-to-entity edges", str(rows.instance_of_edges)),
                ("has_attribute_edges", "Entity-to-column edges", str(rows.has_attribute_edges)),
                ("is_a_edges", "Hierarchy edges", str(rows.is_a_edges)),
                ("references_edges", "Entity cross-reference edges", str(rows.references_edges)),
            ]

            for name, desc, val in metrics:
                mid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"ontology_metric_{name}"))
                escaped_desc = desc.replace("'", "''")
                self.spark.sql(f"""
                    MERGE INTO {metrics_tbl} t
                    USING (SELECT '{mid}' as metric_id) s ON t.metric_id = s.metric_id
                    WHEN MATCHED THEN UPDATE SET
                        sql_definition = '{val}', updated_at = '{now}'
                    WHEN NOT MATCHED THEN INSERT (
                        metric_id, metric_name, description, entity_id,
                        sql_definition, uc_view_name, aggregation_type,
                        source_field, filter_condition, created_at, updated_at
                    ) VALUES (
                        '{mid}', '{name}', '{escaped_desc}', NULL,
                        '{val}', NULL, 'COMPUTED', NULL, NULL, '{now}', '{now}'
                    )
                """)

            logger.info("Computed %d ontology metrics", len(metrics))
            return len(metrics)
        except Exception as e:
            logger.warning("Failed to compute ontology metrics: %s", e)
            return 0

    def apply_entity_tags(self) -> int:
        """Apply entity tags to UC tables and columns from discovered entities.

        Also writes an audit log to ``{schema}.entity_tag_audit_log`` for
        compliance traceability.
        """
        from datetime import datetime as _dt

        min_conf = self._validation_cfg.get("min_entity_confidence", 0.5)
        tag_key = self.config.entity_tag_key
        bundle_ver = self._get_bundle_version()
        tagged = 0
        audit_rows: list = []
        now = _dt.utcnow().isoformat()

        # Table-level tags
        try:
            table_ents = self.spark.sql(
                f"""
                SELECT entity_type, confidence,
                       COALESCE(attributes['discovery_method'], 'unknown') AS discovery_method,
                       EXPLODE(source_tables) AS table_name
                FROM {self.config.fully_qualified_entities}
                WHERE COALESCE(attributes['granularity'], 'table') = 'table'
                  AND confidence >= {min_conf}
            """
            ).collect()
            for row in table_ents:
                action = "failed"
                try:
                    self.spark.sql(
                        f"ALTER TABLE {row.table_name} SET TAGS ("
                        f"'{tag_key}' = '{row.entity_type}', "
                        f"'ontology.bundle_version' = '{bundle_ver}')"
                    )
                    tagged += 1
                    action = "applied"
                except Exception as e:
                    logger.warning("Failed to tag table %s: %s", row.table_name, e)
                audit_rows.append((
                    now, row.table_name, None, tag_key, row.entity_type,
                    float(row.confidence), bundle_ver, row.discovery_method, action,
                ))
        except Exception as e:
            logger.warning("Could not read table-level entities for tagging: %s", e)

        # Column-level tags
        try:
            col_ents = self.spark.sql(
                f"""
                SELECT entity_type, confidence,
                       COALESCE(attributes['discovery_method'], 'unknown') AS discovery_method,
                       source_tables[0] AS table_name,
                       EXPLODE(source_columns) AS col_name
                FROM {self.config.fully_qualified_entities}
                WHERE attributes['granularity'] = 'column'
                  AND confidence >= {min_conf}
                  AND SIZE(source_columns) > 0
            """
            ).collect()
            for row in col_ents:
                action = "failed"
                try:
                    self.spark.sql(
                        f"ALTER TABLE {row.table_name} ALTER COLUMN {row.col_name} "
                        f"SET TAGS ('{tag_key}' = '{row.entity_type}', "
                        f"'ontology.bundle_version' = '{bundle_ver}')"
                    )
                    tagged += 1
                    action = "applied"
                except Exception as e:
                    logger.warning(
                        "Failed to tag column %s.%s: %s",
                        row.table_name, row.col_name, e,
                    )
                audit_rows.append((
                    now, row.table_name, row.col_name, tag_key, row.entity_type,
                    float(row.confidence), bundle_ver, row.discovery_method, action,
                ))
        except Exception as e:
            logger.warning("Could not read column-level entities for tagging: %s", e)

        # Write audit log
        if audit_rows:
            audit_tbl = f"{self.config.catalog_name}.{self.config.schema_name}.entity_tag_audit_log"
            try:
                self.spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {audit_tbl} (
                        timestamp STRING, table_name STRING, column_name STRING,
                        tag_key STRING, tag_value STRING, confidence DOUBLE,
                        bundle_version STRING, discovery_method STRING, action STRING
                    ) USING DELTA
                """)
                cols = ["timestamp", "table_name", "column_name", "tag_key",
                        "tag_value", "confidence", "bundle_version",
                        "discovery_method", "action"]
                audit_df = self.spark.createDataFrame(audit_rows, cols)
                audit_df.write.mode("append").saveAsTable(audit_tbl)
                logger.info("Wrote %d rows to %s", len(audit_rows), audit_tbl)
            except Exception as e:
                logger.warning("Failed to write tag audit log: %s", e)

        logger.info("Applied %d entity tags to UC objects", tagged)
        return tagged

    def get_entity_summary(self) -> DataFrame:
        """Get summary of discovered entities."""
        return self.spark.sql(
            f"""
            SELECT 
                entity_type,
                COUNT(*) as entity_count,
                ROUND(AVG(confidence), 2) as avg_confidence,
                SUM(CASE WHEN validated THEN 1 ELSE 0 END) as validated_count
            FROM {self.config.fully_qualified_entities}
            GROUP BY entity_type
            ORDER BY entity_count DESC
        """
        )

    def get_geographic_coverage_report(self) -> DataFrame:
        """Column-level geographic classification completeness report.

        Joins column_properties, entities, and column_knowledge_base to produce
        a per-column view showing whether each column has been classified as
        geographic, non_geographic, or remains unreviewed.
        """
        cp = self.config.fully_qualified_column_properties
        ent = self.config.fully_qualified_entities
        ckb = f"{self.config.catalog_name}.{self.config.schema_name}.{self.config.column_kb_table}"

        return self.spark.sql(f"""
            WITH all_cols AS (
                SELECT DISTINCT table_name, column_name FROM {ckb}
                WHERE table_name IS NOT NULL AND column_name IS NOT NULL
            ),
            cp_roles AS (
                SELECT table_name, column_name, property_role, confidence
                FROM {cp}
            ),
            geo_entities AS (
                SELECT EXPLODE(source_columns) AS col_name,
                       source_tables[0] AS table_name,
                       entity_type AS geo_entity_subtype,
                       confidence AS entity_confidence
                FROM {ent}
                WHERE attributes['granularity'] = 'column'
                  AND (entity_type IN ('Geographic','Address','GeoCoordinate',
                       'AdminRegion','PostalCode','Timezone','Location')
                       OR LOWER(entity_type) LIKE '%geo%'
                       OR LOWER(entity_type) LIKE '%location%')
            )
            SELECT
                ac.table_name,
                ac.column_name,
                cp.property_role,
                cp.confidence AS property_confidence,
                ge.geo_entity_subtype,
                ge.entity_confidence,
                CASE
                    WHEN cp.property_role = 'geographic' OR ge.geo_entity_subtype IS NOT NULL
                        THEN 'geographic'
                    WHEN cp.property_role IS NOT NULL
                        THEN 'non_geographic'
                    ELSE 'unreviewed'
                END AS classification_status
            FROM all_cols ac
            LEFT JOIN cp_roles cp
                ON ac.table_name = cp.table_name AND ac.column_name = cp.column_name
            LEFT JOIN geo_entities ge
                ON ac.table_name = ge.table_name AND ac.column_name = ge.col_name
            ORDER BY classification_status, ac.table_name, ac.column_name
        """)

    def reconcile_geographic_classifications(self, dry_run: bool = True) -> DataFrame:
        """Find columns where property_role and entity classification disagree on geographic status.

        Args:
            dry_run: If True (default), only report conflicts. If False, update
                     column_properties.property_role to 'geographic' for columns
                     bound to geographic entities.

        Returns:
            DataFrame of conflicting rows.
        """
        cp = self.config.fully_qualified_column_properties
        ent = self.config.fully_qualified_entities

        conflicts = self.spark.sql(f"""
            WITH geo_bound AS (
                SELECT EXPLODE(source_columns) AS col_name,
                       source_tables[0] AS table_name,
                       entity_type
                FROM {ent}
                WHERE attributes['granularity'] = 'column'
                  AND (entity_type IN ('Geographic','Address','GeoCoordinate',
                       'AdminRegion','PostalCode','Timezone','Location')
                       OR LOWER(entity_type) LIKE '%geo%'
                       OR LOWER(entity_type) LIKE '%location%')
            )
            SELECT cp.table_name, cp.column_name, cp.property_role,
                   gb.entity_type AS geo_entity_type,
                   CASE
                       WHEN gb.entity_type IS NOT NULL AND cp.property_role != 'geographic'
                           THEN 'entity_says_geo_but_role_disagrees'
                       WHEN gb.entity_type IS NULL AND cp.property_role = 'geographic'
                           THEN 'role_says_geo_but_no_entity'
                   END AS conflict_type
            FROM {cp} cp
            LEFT JOIN geo_bound gb
                ON cp.table_name = gb.table_name AND cp.column_name = gb.col_name
            WHERE (gb.entity_type IS NOT NULL AND cp.property_role != 'geographic')
               OR (gb.entity_type IS NULL AND cp.property_role = 'geographic')
        """)

        conflict_count = conflicts.count()
        logger.info("Geographic reconciliation: %d conflicts found (dry_run=%s)",
                     conflict_count, dry_run)

        if not dry_run and conflict_count > 0:
            fix_rows = conflicts.filter("conflict_type = 'entity_says_geo_but_role_disagrees'").collect()
            updated = 0
            for row in fix_rows:
                try:
                    self.spark.sql(f"""
                        UPDATE {cp}
                        SET property_role = 'geographic'
                        WHERE table_name = '{row.table_name}'
                          AND column_name = '{row.column_name}'
                    """)
                    updated += 1
                except Exception as e:
                    logger.warning("Failed to reconcile %s.%s: %s",
                                   row.table_name, row.column_name, e)
            logger.info("Reconciliation updated %d column property roles to 'geographic'", updated)

        return conflicts

    def discover_inter_entity_relationships(self) -> Dict[str, Any]:
        """Discover typed relationships between entities using declared relationships.

        Builds a lookup from entity definitions' ``relationships`` maps.
        For each FK-linked table pair whose entities match a declared
        relationship, the declared name and cardinality are used.
        Falls back to ``"references"`` when no declaration matches.

        Returns a summary dict with ``edges_added`` and
        ``undiscovered_declared`` (declared relationships not found via FKs).
        """
        edges_table = f"{self.config.catalog_name}.{self.config.schema_name}.graph_edges"
        fk_table = f"{self.config.catalog_name}.{self.config.schema_name}.fk_predictions"
        ent_table = self.config.fully_qualified_entities
        result: Dict[str, Any] = {"edges_added": 0, "undiscovered_declared": []}

        # Build declared relationship lookup: (src_type, dst_type) -> (rel_name, cardinality)
        declared_rels: Dict[Tuple[str, str], Tuple[str, str]] = {}
        for edef in self.discoverer.entity_definitions:
            for rel_name, rel_info in edef.relationships.items():
                target = rel_info.get("target", "")
                cardinality = rel_info.get("cardinality", "")
                if target:
                    declared_rels[(edef.name, target)] = (rel_name, cardinality)

        # Get entity-table mapping
        try:
            ent_rows = self.spark.sql(
                f"SELECT entity_id, entity_name, entity_type, source_tables "
                f"FROM {ent_table} WHERE COALESCE(attributes['granularity'], 'table') = 'table' "
                f"AND confidence >= 0.4"
            ).collect()
        except Exception:
            return result

        table_to_entity: Dict[str, List[Dict]] = {}
        for r in ent_rows:
            for t in r.source_tables or []:
                table_to_entity.setdefault(t, []).append(
                    {"entity_id": r.entity_id, "entity_type": r.entity_type}
                )

        # Get FK predictions linking tables
        fk_links: List[Tuple[str, str]] = []
        try:
            fk_rows = self.spark.sql(
                f"SELECT DISTINCT src_table, dst_table FROM {fk_table} WHERE final_confidence >= 0.5"
            ).collect()
            fk_links = [(r.src_table, r.dst_table) for r in fk_rows]
        except Exception:
            logger.debug("No FK predictions available for relationship discovery")

        new_edges = []
        seen = set()
        discovered_pairs: set = set()

        for src_tbl, dst_tbl in fk_links:
            src_ents = table_to_entity.get(src_tbl, [])
            dst_ents = table_to_entity.get(dst_tbl, [])
            for se in src_ents:
                for de in dst_ents:
                    if se["entity_id"] == de["entity_id"]:
                        continue
                    key = (se["entity_id"], de["entity_id"])
                    if key in seen:
                        continue
                    seen.add(key)

                    pair = (se["entity_type"], de["entity_type"])
                    decl = declared_rels.get(pair)
                    if decl:
                        rel_type, cardinality = decl
                        discovered_pairs.add(pair)
                    else:
                        rel_type, cardinality = "references", ""

                    new_edges.append(
                        (se["entity_id"], de["entity_id"], rel_type, 0.8 if decl else 0.6, cardinality)
                    )

        # Identify declared relationships that were not discovered
        undiscovered = [
            {"source": s, "target": t, "relationship": declared_rels[(s, t)][0]}
            for s, t in declared_rels
            if (s, t) not in discovered_pairs
        ]
        result["undiscovered_declared"] = undiscovered
        if undiscovered:
            logger.info(
                "Declared relationships not found via FK: %s",
                ", ".join(f"{u['source']}->{u['relationship']}->{u['target']}" for u in undiscovered),
            )

        if not new_edges:
            logger.info("No inter-entity relationships discovered")
            return result

        # Remove prior inter-entity ontology edges to prevent duplicates
        try:
            self.spark.sql(
                f"DELETE FROM {edges_table} "
                f"WHERE source_system = 'ontology' AND edge_type IN ('references', 'inter_entity')"
            )
        except Exception:
            pass

        now = datetime.now()
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
        edge_schema = StructType([
            StructField("src", StringType()), StructField("dst", StringType()),
            StructField("relationship", StringType()), StructField("weight", DoubleType()),
            StructField("edge_id", StringType()), StructField("edge_type", StringType()),
            StructField("direction", StringType()), StructField("join_expression", StringType()),
            StructField("join_confidence", DoubleType()), StructField("ontology_rel", StringType()),
            StructField("source_system", StringType()), StructField("status", StringType()),
            StructField("created_at", TimestampType()), StructField("updated_at", TimestampType()),
        ])
        edge_df = self.spark.createDataFrame(
            [
                (src, dst, rel, float(w),
                 f"{src}::{dst}::{rel}", rel, "out",
                 None, None, rel,
                 "ontology", "candidate", now, now)
                for src, dst, rel, w, _ in new_edges
            ],
            schema=edge_schema,
        )
        self._insert_edges_safe(edge_df, edges_table)
        logger.info("Added %d inter-entity relationship edges", len(new_edges))
        result["edges_added"] = len(new_edges)
        return result

    def add_hierarchy_edges(self) -> int:
        """Add is_a edges from child entity types to their parent types based on config."""
        entity_defs = self.ontology_config.get("entities", {}).get("definitions", {})
        parent_map: Dict[str, str] = {}
        for name, defn in entity_defs.items():
            parent = defn.get("parent")
            if parent:
                parent_map[name] = parent

        if not parent_map:
            return 0

        edges_table = f"{self.config.catalog_name}.{self.config.schema_name}.graph_edges"
        ent_table = self.config.fully_qualified_entities

        try:
            ent_rows = self.spark.sql(
                f"SELECT entity_id, entity_type FROM {ent_table} "
                f"WHERE COALESCE(attributes['granularity'], 'table') = 'table'"
            ).collect()
        except Exception:
            return 0

        type_to_ids: Dict[str, List[str]] = {}
        for r in ent_rows:
            type_to_ids.setdefault(r.entity_type, []).append(r.entity_id)

        new_edges = []
        for child_type, parent_type in parent_map.items():
            child_ids = type_to_ids.get(child_type, [])
            parent_ids = type_to_ids.get(parent_type, [])
            if not parent_ids:
                continue
            parent_id = parent_ids[0]
            for cid in child_ids:
                new_edges.append((cid, parent_id, "is_a", 1.0))

        if not new_edges:
            return 0

        now = datetime.now()
        _hierarchy_schema = StructType([
            StructField("src", StringType()),
            StructField("dst", StringType()),
            StructField("relationship", StringType()),
            StructField("weight", DoubleType()),
            StructField("edge_id", StringType()),
            StructField("edge_type", StringType()),
            StructField("direction", StringType()),
            StructField("join_expression", StringType(), nullable=True),
            StructField("join_confidence", DoubleType(), nullable=True),
            StructField("ontology_rel", StringType()),
            StructField("source_system", StringType()),
            StructField("status", StringType()),
            StructField("created_at", TimestampType()),
            StructField("updated_at", TimestampType()),
        ])
        edge_df = self.spark.createDataFrame(
            [
                (s, d, "is_a", w,
                 f"{s}::{d}::is_a", "is_a", "out",
                 None, None, "is_a",
                 "ontology", "candidate", now, now)
                for s, d, _, w in new_edges
            ],
            schema=_hierarchy_schema,
        )
        self._insert_edges_safe(edge_df, edges_table)
        logger.info("Added %d hierarchy (is_a) edges", len(new_edges))
        return len(new_edges)

    def _sync_entity_nodes_to_graph(self) -> int:
        """Merge ontology entities into graph_nodes so edges have valid targets.

        Uses MERGE to keep existing entity nodes up-to-date when confidence,
        entity_type, or description change across runs.
        """
        nodes_table = f"{self.config.catalog_name}.{self.config.schema_name}.{self.config.nodes_table}"
        ent_table = self.config.fully_qualified_entities
        try:
            self.spark.sql(f"""
                MERGE INTO {nodes_table} AS target
                USING (
                    SELECT entity_id, entity_name, entity_type,
                           COALESCE(entity_role, 'primary') AS entity_role,
                           description, confidence, created_at, updated_at
                    FROM {ent_table}
                ) AS source
                ON target.id = source.entity_id

                WHEN MATCHED THEN UPDATE SET
                    target.table_short_name = source.entity_name,
                    target.comment = COALESCE(source.description, target.comment),
                    target.quality_score = source.confidence,
                    target.ontology_type = source.entity_type,
                    target.display_name = source.entity_name,
                    target.short_description = COALESCE(source.description, target.short_description),
                    target.status = source.entity_role,
                    target.updated_at = source.updated_at

                WHEN NOT MATCHED THEN INSERT (
                    id, table_name, catalog, `schema`, table_short_name,
                    domain, subdomain, has_pii, has_phi, security_level,
                    comment, node_type, parent_id, data_type,
                    quality_score, embedding,
                    ontology_id, ontology_type, display_name, short_description,
                    sensitivity, status, source_system, keywords,
                    created_at, updated_at
                ) VALUES (
                    source.entity_id, NULL, NULL, NULL, source.entity_name,
                    NULL, NULL, FALSE, FALSE, 'PUBLIC',
                    source.description, 'entity', NULL, NULL,
                    source.confidence, NULL,
                    source.entity_id, source.entity_type, source.entity_name, source.description,
                    'public', source.entity_role, 'ontology', NULL,
                    source.created_at, source.updated_at
                )
            """)
            count = self.spark.sql(
                f"SELECT COUNT(*) as cnt FROM {nodes_table} WHERE node_type = 'entity'"
            ).collect()[0].cnt
            logger.info("Entity nodes in graph: %d", count)
            return count
        except Exception as e:
            logger.warning("Could not sync entity nodes to graph: %s", e)
            return 0

    def _clear_ontology_edges(self) -> None:
        """Delete existing ontology-typed edges to prevent accumulation on re-runs.

        Scoped to avoid deleting FK-based reference edges (source_system='fk_predictions').
        """
        edges_table = f"{self.config.catalog_name}.{self.config.schema_name}.graph_edges"
        try:
            self.spark.sql(
                f"DELETE FROM {edges_table} "
                f"WHERE source_system = 'ontology' "
                f"   OR (relationship IN ('instance_of', 'has_attribute', 'has_property', 'is_a') "
                f"       AND (source_system IS NULL OR source_system = 'ontology'))"
            )
            logger.info("Cleared existing ontology edges before regeneration")
        except Exception as e:
            logger.warning("Could not clear ontology edges (table may not exist): %s", e)

    def add_entity_relationships_to_graph(self) -> int:
        """Add entity relationships to the knowledge graph edges.

        Uses the redesigned model:
        - instance_of: only from table to its PRIMARY entity
        - has_property: from primary entity to columns with their role
        - Named relationships from ontology_relationships with ontology_rel
        """
        edges_table = (
            f"{self.config.catalog_name}.{self.config.schema_name}.graph_edges"
        )
        self._clear_ontology_edges()
        total = 0

        def _add_edge_columns(df, rel: str, etype: str, ont_rel: str = None):
            return df.select(
                F.col("src"), F.col("dst"),
                F.lit(rel).alias("relationship"),
                F.col("weight") if "weight" in df.columns else F.lit(1.0).alias("weight"),
                F.concat_ws("::", F.col("src"), F.col("dst"), F.lit(rel)).alias("edge_id"),
                F.lit(etype).alias("edge_type"),
                F.lit("out").alias("direction"),
                F.lit(None).cast("string").alias("join_expression"),
                F.lit(None).cast("double").alias("join_confidence"),
                F.lit(ont_rel).alias("ontology_rel"),
                F.lit("ontology").alias("source_system"),
                F.lit("candidate").alias("status"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )

        try:
            table_entities = self.spark.sql(f"""
                SELECT entity_id, entity_name, EXPLODE(source_tables) as table_name
                FROM {self.config.fully_qualified_entities}
                WHERE source_tables IS NOT NULL AND SIZE(source_tables) > 0
                  AND COALESCE(entity_role, 'primary') = 'primary'
            """)
            if table_entities.count() > 0:
                raw = table_entities.select(
                    F.col("table_name").alias("src"),
                    F.col("entity_id").alias("dst"),
                    F.lit(1.0).alias("weight"),
                )
                instance_edges = _add_edge_columns(raw, "instance_of", "instance_of")
                self._insert_edges_safe(instance_edges, edges_table)
                total += instance_edges.count()
        except Exception as e:
            logger.warning("Could not add instance_of edges: %s", e)

        try:
            cp_table = self.config.fully_qualified_column_properties
            raw = self.spark.sql(f"""
                SELECT owning_entity_id AS src,
                       CONCAT(table_name, '.', column_name) AS dst,
                       CAST(1.0 AS DOUBLE) AS weight
                FROM {cp_table}
            """)
            prop_count = raw.count()
            if prop_count > 0:
                prop_edges = _add_edge_columns(raw, "has_property", "has_property")
                self._insert_edges_safe(prop_edges, edges_table)
                total += prop_count
        except Exception as e:
            logger.warning("Could not add has_property edges: %s", e)

        try:
            rels_table = self.config.fully_qualified_relationships
            ent_table = self.config.fully_qualified_entities
            raw = self.spark.sql(f"""
                SELECT DISTINCT se.entity_id AS src, de.entity_id AS dst,
                       r.relationship_name AS rel_name,
                       r.confidence AS weight
                FROM {rels_table} r
                JOIN {ent_table} se ON se.entity_type = r.src_entity_type
                    AND COALESCE(se.entity_role, 'primary') = 'primary'
                JOIN {ent_table} de ON de.entity_type = r.dst_entity_type
                    AND COALESCE(de.entity_role, 'primary') = 'primary'
                WHERE se.entity_id != de.entity_id
            """)
            ref_count = raw.count()
            if ref_count > 0:
                ref_edges = raw.select(
                    F.col("src"), F.col("dst"),
                    F.col("rel_name").alias("relationship"),
                    F.col("weight"),
                    F.concat_ws("::", F.col("src"), F.col("dst"), F.col("rel_name")).alias("edge_id"),
                    F.col("rel_name").alias("edge_type"),
                    F.lit("out").alias("direction"),
                    F.lit(None).cast("string").alias("join_expression"),
                    F.lit(None).cast("double").alias("join_confidence"),
                    F.col("rel_name").alias("ontology_rel"),
                    F.lit("ontology").alias("source_system"),
                    F.lit("candidate").alias("status"),
                    F.current_timestamp().alias("created_at"),
                    F.current_timestamp().alias("updated_at"),
                )
                self._insert_edges_safe(ref_edges, edges_table)
                total += ref_count
        except Exception as e:
            logger.warning("Could not add ontology relationship edges: %s", e)

        logger.info("Added %d entity edges to graph", total)
        return total

    def validate_ontology_completeness(self) -> Dict[str, Any]:
        """Compare discovered entity types against the bundle's definitions."""
        defined_types = {e.name for e in self.discoverer.entity_definitions if e.name != "DataTable"}
        try:
            discovered = self.spark.sql(
                f"SELECT DISTINCT entity_type FROM {self.config.fully_qualified_entities}"
            ).collect()
            discovered_types = {r.entity_type for r in discovered}
        except Exception:
            discovered_types = set()

        missing = defined_types - discovered_types
        extra = discovered_types - defined_types

        # Count per type
        try:
            counts_rows = self.spark.sql(
                f"SELECT entity_type, COUNT(*) as cnt FROM {self.config.fully_qualified_entities} "
                f"GROUP BY entity_type ORDER BY cnt DESC"
            ).collect()
            counts = {r.entity_type: r.cnt for r in counts_rows}
        except Exception:
            counts = {}

        total = sum(counts.values())
        over_represented = [
            t for t, c in counts.items() if total > 0 and c / total > 0.4
        ]

        # Low confidence entities
        try:
            low_conf = self.spark.sql(
                f"SELECT COUNT(*) AS n FROM {self.config.fully_qualified_entities} WHERE confidence < 0.5"
            ).collect()[0].n
        except Exception:
            low_conf = 0

        if missing:
            logger.warning(f"Ontology completeness: {len(missing)} defined entity types not discovered: {missing}")
        if over_represented:
            logger.warning(f"Ontology completeness: over-represented types (>40%%): {over_represented}")
        if low_conf > 0:
            logger.info(f"Ontology completeness: {low_conf} entities with confidence < 0.5")

        result = {
            "defined_types": len(defined_types),
            "discovered_types": len(discovered_types),
            "missing_types": sorted(missing),
            "extra_types": sorted(extra),
            "over_represented": over_represented,
            "low_confidence_count": low_conf,
        }
        logger.info(f"Ontology completeness: {result['discovered_types']}/{result['defined_types']} types found")
        return result

    # ------------------------------------------------------------------
    # Phase 2: New classification steps for the redesigned ontology model
    # ------------------------------------------------------------------

    def classify_entity_roles(self) -> int:
        """Mark each entity as 'primary' or 'referenced' per table.

        For each source table, the highest-confidence table-granularity entity
        becomes 'primary'. All column-granularity entities are 'referenced'.
        If no table-granularity entity exists, promote the highest-confidence
        column entity to primary.
        """
        ent_table = self.config.fully_qualified_entities
        try:
            rows = self.spark.sql(f"""
                SELECT entity_id, entity_type, source_tables,
                       COALESCE(attributes['granularity'], 'table') AS granularity,
                       confidence
                FROM {ent_table}
            """).collect()
        except Exception as e:
            logger.warning("classify_entity_roles: cannot read entities: %s", e)
            return 0

        table_entities: Dict[str, List[Dict]] = {}
        for r in rows:
            for t in r.source_tables or []:
                table_entities.setdefault(t, []).append({
                    "entity_id": r.entity_id,
                    "granularity": r.granularity,
                    "confidence": float(r.confidence or 0),
                })

        primary_ids: set = set()
        referenced_ids: set = set()

        for tbl, ents in table_entities.items():
            table_level = [e for e in ents if e["granularity"] == "table"]
            col_level = [e for e in ents if e["granularity"] == "column"]

            if table_level:
                best = max(table_level, key=lambda e: e["confidence"])
                primary_ids.add(best["entity_id"])
                for e in table_level:
                    if e["entity_id"] != best["entity_id"]:
                        referenced_ids.add(e["entity_id"])
                for e in col_level:
                    referenced_ids.add(e["entity_id"])
            elif col_level:
                best = max(col_level, key=lambda e: e["confidence"])
                primary_ids.add(best["entity_id"])
                for e in col_level:
                    if e["entity_id"] != best["entity_id"]:
                        referenced_ids.add(e["entity_id"])

        referenced_ids -= primary_ids
        updated = 0

        if primary_ids:
            id_list = ",".join(f"'{eid}'" for eid in primary_ids)
            self.spark.sql(f"""
                UPDATE {ent_table}
                SET entity_role = 'primary',
                    discovery_confidence = COALESCE(discovery_confidence, confidence),
                    updated_at = CURRENT_TIMESTAMP()
                WHERE entity_id IN ({id_list})
            """)
            updated += len(primary_ids)

        if referenced_ids:
            id_list = ",".join(f"'{eid}'" for eid in referenced_ids)
            self.spark.sql(f"""
                UPDATE {ent_table}
                SET entity_role = 'referenced',
                    discovery_confidence = COALESCE(discovery_confidence, confidence),
                    updated_at = CURRENT_TIMESTAMP()
                WHERE entity_id IN ({id_list})
            """)
            updated += len(referenced_ids)

        logger.info("classify_entity_roles: %d primary, %d referenced",
                     len(primary_ids), len(referenced_ids))
        return updated

    def _build_bundle_property_index(self, entity_type: str) -> Dict[str, Tuple[str, str, Optional[str], Optional[Any]]]:
        """Build column_name_lower -> (role, property_name, edge, target_entity) from bundle properties."""
        edef = self.discoverer._entity_def_map.get(entity_type)
        if not edef or not edef.properties:
            return {}
        index: Dict[str, Tuple[str, str, Optional[str], Optional[Any]]] = {}
        for prop in edef.properties:
            for attr in prop.typical_attributes:
                index[attr.lower()] = (prop.role, prop.name, prop.edge, prop.target_entity)
        return index

    def _heuristic_classify(
        self,
        col_lower: str,
        dtype: str,
        is_pk_column: bool,
        linked_entity: Optional[str],
        cls_type: str,
        table_name: str = "",
    ) -> Tuple[str, str, float]:
        """Heuristic fallback classification. Returns (role, discovery_method, confidence)."""
        _GEO_PATTERNS_DEFAULT = frozenset({
            "country", "country_code", "state", "state_code", "city", "postal_code",
            "zip_code", "zipcode", "zip", "latitude", "longitude", "lat", "lon",
            "geo_region", "region", "county", "province", "address",
        })
        _bundle_geo = getattr(self.discoverer, "_bundle_geo_patterns", frozenset())
        _GEO_PATTERNS = _bundle_geo or _GEO_PATTERNS_DEFAULT
        _HIERARCHY_PATTERNS = frozenset({
            "year", "quarter", "month", "week", "day", "fiscal_year", "fiscal_quarter",
            "product_category", "product_subcategory", "category", "subcategory",
            "department", "division",
        })
        _TEXT_PATTERNS = frozenset({
            "note_text", "notes", "clinical_summary", "summary", "comment",
            "description", "text_content", "body", "narrative", "remarks",
            "free_text", "memo", "abstract",
        })
        _SYSTEM_PATTERNS = frozenset({
            "ingest_ts", "ingestion_timestamp", "batch_id", "row_hash", "source_system",
            "etl_timestamp", "etl_batch_id", "load_date", "load_ts", "created_by_etl",
            "upload_ts", "_rescued_data", "processing_timestamp",
        })

        if linked_entity:
            return "object_property", "heuristic_strong", 0.80
        if is_pk_column:
            return "primary_key", "heuristic_strong", 0.85
        if re.search(r'_(id|key)$', col_lower):
            # Self-referencing PK detection: e.g. order_id on the orders table
            table_short = table_name.rsplit(".", 1)[-1].lower().rstrip("s") if table_name else ""
            if table_short and (col_lower == f"{table_short}_id" or col_lower == f"{table_short}_key"):
                return "primary_key", "heuristic_strong", 0.80
            return "object_property", "heuristic_weak", 0.55
        if cls_type in ("PII", "PHI"):
            return "pii", "heuristic_strong", 0.85
        if dtype == "BOOLEAN" or col_lower.startswith(("is_", "has_", "flag_")):
            return "dimension", "heuristic_weak", 0.60
        _is_geo = col_lower in _GEO_PATTERNS or col_lower.startswith("geo_")
        _is_code = re.search(r'_(code|icd|cpt|loinc|ndc|drg)$', col_lower) or col_lower.endswith("_code")
        if _bundle_geo:
            if _is_geo:
                return "geographic", "heuristic_strong", 0.75
            if _is_code:
                return "dimension", "heuristic_strong", 0.75
        else:
            if _is_code:
                return "dimension", "heuristic_strong", 0.75
            if _is_geo:
                return "geographic", "heuristic_strong", 0.75
        if col_lower in _SYSTEM_PATTERNS or col_lower.startswith("etl_"):
            return "audit", "heuristic_strong", 0.80
        if dtype in ("DATE", "TIMESTAMP", "TIMESTAMP_NTZ"):
            return "temporal", "heuristic_strong", 0.80
        if col_lower in _HIERARCHY_PATTERNS:
            return "dimension", "heuristic_weak", 0.60
        if dtype == "STRING" and col_lower in _TEXT_PATTERNS:
            return "label", "heuristic_weak", 0.55
        if dtype in ("INT", "INTEGER", "BIGINT", "LONG", "DOUBLE", "FLOAT", "DECIMAL"):
            return "measure", "heuristic_weak", 0.60
        return "dimension", "fallback", 0.40

    def classify_column_properties(self) -> int:
        """Populate ontology_column_properties for every column in tables with a primary entity.

        Uses a two-tier classification strategy:
        1. **Bundle match**: check column name against entity's formal property
           definitions (typical_attributes). High confidence (0.95).
        2. **Heuristic fallback**: regex/type-based cascade for unmatched columns.
           Variable confidence (0.40-0.85).
        """
        ent_table = self.config.fully_qualified_entities
        col_kb = self.config.fully_qualified_column_kb
        cp_table = self.config.fully_qualified_column_properties

        self.create_column_properties_table()

        try:
            primary_ents = self.spark.sql(f"""
                SELECT entity_id, entity_type, source_tables, source_columns
                FROM {ent_table}
                WHERE entity_role = 'primary'
            """).collect()
        except Exception as e:
            logger.warning("classify_column_properties: cannot read primary entities: %s", e)
            return 0

        table_primary: Dict[str, Dict] = {}
        for r in primary_ents:
            for t in r.source_tables or []:
                table_primary[t] = {
                    "entity_id": r.entity_id,
                    "entity_type": r.entity_type,
                    "source_columns": set(r.source_columns or []),
                }

        try:
            ref_ents = self.spark.sql(f"""
                SELECT entity_type, source_tables, source_columns
                FROM {ent_table}
                WHERE entity_role = 'referenced' AND source_columns IS NOT NULL AND SIZE(source_columns) > 0
            """).collect()
        except Exception:
            ref_ents = []

        col_entity_map: Dict[tuple, str] = {}
        for r in ref_ents:
            for t in r.source_tables or []:
                for c in r.source_columns or []:
                    col_entity_map[(t, c)] = r.entity_type

        if not table_primary:
            logger.info("classify_column_properties: no primary entities found")
            return 0

        # Pre-build bundle property indices per entity type
        bundle_indices: Dict[str, Dict] = {}
        for pe_info in table_primary.values():
            etype = pe_info["entity_type"]
            if etype not in bundle_indices:
                bundle_indices[etype] = self._build_bundle_property_index(etype)

        tbl_list = ",".join(f"'{t}'" for t in table_primary)
        try:
            columns = self.spark.sql(f"""
                SELECT table_name, column_name, data_type,
                       classification_type
                FROM {col_kb}
                WHERE table_name IN ({tbl_list})
            """).collect()
        except Exception as e:
            logger.warning("classify_column_properties: cannot read column KB: %s", e)
            return 0

        now = datetime.utcnow()
        props = []
        bundle_match_count = 0
        for c in columns:
            tbl = c.table_name
            col = c.column_name
            dtype = (c.data_type or "").upper()
            pe = table_primary.get(tbl)
            if not pe:
                continue

            linked = col_entity_map.get((tbl, col))
            col_lower = col.lower()
            cls_type = (getattr(c, "classification_type", None) or "").upper()
            is_sensitive = cls_type in ("PII", "PHI", "PCI")
            is_surrogate_key = bool(
                re.search(r'_(sk|surrogate)$', col_lower)
                or (col_lower.endswith("_hash") and re.search(r'_(id|key)$', col_lower))
            )
            is_semi_structured = dtype in ("MAP", "STRUCT", "ARRAY", "VARIANT")

            # Tier 1: Bundle property match
            bindex = bundle_indices.get(pe["entity_type"], {})
            bundle_hit = bindex.get(col_lower)

            if bundle_hit:
                role, prop_name, edge, target_entity = bundle_hit
                linked_from_bundle = target_entity
                if isinstance(linked_from_bundle, list):
                    linked_from_bundle = linked_from_bundle[0] if linked_from_bundle else None
                confidence = 0.95
                discovery_method = "bundle_match"
                if role == "object_property" and linked_from_bundle:
                    linked = str(linked_from_bundle)
                bundle_match_count += 1
            else:
                # Tier 2: Heuristic fallback
                is_pk = col_lower in {cc.lower() for cc in pe["source_columns"]}
                role, discovery_method, confidence = self._heuristic_classify(
                    col_lower, dtype, is_pk, linked, cls_type, table_name=tbl,
                )
                prop_name = col

            props.append({
                "property_id": str(uuid.uuid4()),
                "table_name": tbl,
                "column_name": col,
                "property_name": prop_name,
                "property_role": role,
                "owning_entity_id": pe["entity_id"],
                "owning_entity_type": pe["entity_type"],
                "linked_entity_type": linked,
                "confidence": confidence,
                "auto_discovered": True,
                "discovery_method": discovery_method,
                "is_sensitive": is_sensitive,
                "is_surrogate_key": is_surrogate_key,
                "is_semi_structured": is_semi_structured,
                "bundle_version": self._get_bundle_version(),
                "discovery_timestamp": now,
                "created_at": now,
                "updated_at": now,
            })

        if not props:
            return 0

        df = self.spark.createDataFrame(props)
        df.write.mode("overwrite").saveAsTable(cp_table)
        logger.info(
            "classify_column_properties: wrote %d properties (%d bundle_match, %d heuristic/fallback)",
            len(props), bundle_match_count, len(props) - bundle_match_count,
        )
        return len(props)

    def _resolve_edge_name(self, src_type: str, dst_type: str, column_name: Optional[str] = None) -> str:
        """Resolve the best edge name for a (src, dst) entity pair using edge catalog.

        Priority:
        1. Bundle property definition (if column_name matches a typed object_property)
        2. Entity relationship config (legacy relationships block)
        3. Edge catalog domain/range match
        4. Fallback to 'references'
        """
        catalog = self.discoverer._edge_catalog
        edef = self.discoverer._entity_def_map.get(src_type)

        # Check bundle property definitions for column-level edge name
        if edef and column_name:
            col_lower = (column_name or "").lower()
            for prop in edef.properties:
                if prop.kind == "object_property" and prop.edge:
                    if col_lower in [a.lower() for a in prop.typical_attributes]:
                        entry = catalog.get(prop.edge)
                        if entry and entry.validate_edge(src_type, dst_type):
                            return prop.edge

        # Check legacy relationships block
        if edef:
            for rn, ri in edef.relationships.items():
                if ri.get("target") == dst_type:
                    if catalog.get(rn):
                        return rn
                    return rn

        # Search edge catalog by domain/range match
        match = catalog.find_edge(src_type, dst_type)
        if match:
            return match.name

        return "references"

    def discover_named_relationships(self) -> int:
        """Populate ontology_relationships from FK predictions and column link patterns.

        Uses the edge catalog for named relationship resolution and domain/range validation.
        """
        ent_table = self.config.fully_qualified_entities
        fk_table = f"{self.config.catalog_name}.{self.config.schema_name}.fk_predictions"
        rels_table = self.config.fully_qualified_relationships

        self.create_relationships_table()

        try:
            primary_rows = self.spark.sql(f"""
                SELECT entity_type, EXPLODE(source_tables) AS table_name
                FROM {ent_table}
                WHERE entity_role = 'primary'
            """).collect()
        except Exception as e:
            logger.warning("discover_named_relationships: cannot read entities: %s", e)
            return 0

        table_to_primary: Dict[str, str] = {}
        for r in primary_rows:
            table_to_primary[r.table_name] = r.entity_type

        rels: List[Dict] = []
        seen: set = set()
        now = datetime.utcnow()
        catalog = self.discoverer._edge_catalog
        validation_warnings: List[str] = []

        # From FK predictions
        try:
            fk_rows = self.spark.sql(f"""
                SELECT src_table, dst_table, src_column, dst_column, final_confidence
                FROM {fk_table}
                WHERE final_confidence >= 0.5
            """).collect()
        except Exception:
            fk_rows = []

        for fk in fk_rows:
            src_type = table_to_primary.get(fk.src_table)
            dst_type = table_to_primary.get(fk.dst_table)
            if not src_type or not dst_type or src_type == dst_type:
                continue
            key = (src_type, dst_type)
            if key in seen:
                continue
            seen.add(key)

            rel_name = self._resolve_edge_name(src_type, dst_type, fk.src_column)
            valid, msg = catalog.validate(rel_name, src_type, dst_type)
            if not valid and rel_name != "references":
                validation_warnings.append(msg)

            rels.append({
                "relationship_id": str(uuid.uuid4()),
                "src_entity_type": src_type,
                "relationship_name": rel_name,
                "dst_entity_type": dst_type,
                "cardinality": "1:N",
                "evidence_column": fk.src_column,
                "evidence_table": fk.src_table,
                "source": "fk_inferred",
                "confidence": float(fk.final_confidence or 0.5),
                "created_at": now,
                "updated_at": now,
            })

        # From column_properties with object_property role
        cp_table = self.config.fully_qualified_column_properties
        try:
            link_rows = self.spark.sql(f"""
                SELECT DISTINCT owning_entity_type, linked_entity_type, column_name, table_name
                FROM {cp_table}
                WHERE property_role = 'object_property' AND linked_entity_type IS NOT NULL
            """).collect()
        except Exception:
            link_rows = []

        for lr in link_rows:
            src_type = lr.owning_entity_type
            dst_type = lr.linked_entity_type
            if not src_type or not dst_type or src_type == dst_type:
                continue
            key = (src_type, dst_type)
            if key in seen:
                continue
            seen.add(key)

            rel_name = self._resolve_edge_name(src_type, dst_type, lr.column_name)
            valid, msg = catalog.validate(rel_name, src_type, dst_type)
            if not valid and rel_name != "references":
                validation_warnings.append(msg)

            rels.append({
                "relationship_id": str(uuid.uuid4()),
                "src_entity_type": src_type,
                "relationship_name": rel_name,
                "dst_entity_type": dst_type,
                "cardinality": "1:N",
                "evidence_column": lr.column_name,
                "evidence_table": lr.table_name,
                "source": "discovered",
                "confidence": 0.7,
                "created_at": now,
                "updated_at": now,
            })

        # From config hierarchy (subclass_of)
        for edef in self.discoverer.entity_definitions:
            parent = edef.parent
            if parent:
                key = (edef.name, parent)
                if key not in seen:
                    seen.add(key)
                    rels.append({
                        "relationship_id": str(uuid.uuid4()),
                        "src_entity_type": edef.name,
                        "relationship_name": "subclass_of",
                        "dst_entity_type": parent,
                        "cardinality": "1:1",
                        "evidence_column": None,
                        "evidence_table": None,
                        "source": "configured",
                        "confidence": 1.0,
                        "created_at": now,
                        "updated_at": now,
                    })

        # Auto-generate inverse edges from EdgeCatalog metadata
        inverse_rels: List[Dict] = []
        for rel in rels:
            rel_name = rel["relationship_name"]
            if rel_name == "subclass_of":
                continue
            inv_name = catalog.get_inverse(rel_name)
            if not inv_name:
                continue
            inv_key = (rel["dst_entity_type"], rel["src_entity_type"])
            if inv_key in seen:
                continue
            seen.add(inv_key)
            fwd_card = rel.get("cardinality", "1:N")
            inv_card = "N:1" if fwd_card == "1:N" else ("1:N" if fwd_card == "N:1" else fwd_card)
            inverse_rels.append({
                "relationship_id": str(uuid.uuid4()),
                "src_entity_type": rel["dst_entity_type"],
                "relationship_name": inv_name,
                "dst_entity_type": rel["src_entity_type"],
                "cardinality": inv_card,
                "evidence_column": rel["evidence_column"],
                "evidence_table": rel["evidence_table"],
                "source": "auto_inverse",
                "confidence": rel["confidence"],
                "created_at": now,
                "updated_at": now,
            })
        if inverse_rels:
            rels.extend(inverse_rels)
            logger.info("Auto-generated %d inverse edges", len(inverse_rels))

        if validation_warnings:
            for w in validation_warnings[:10]:
                logger.warning("edge_catalog validation: %s", w)

        if not rels:
            logger.info("discover_named_relationships: no relationships found")
            return 0

        _rels_schema = StructType([
            StructField("relationship_id", StringType()),
            StructField("src_entity_type", StringType()),
            StructField("relationship_name", StringType()),
            StructField("dst_entity_type", StringType()),
            StructField("cardinality", StringType()),
            StructField("evidence_column", StringType(), nullable=True),
            StructField("evidence_table", StringType(), nullable=True),
            StructField("source", StringType()),
            StructField("confidence", DoubleType()),
            StructField("created_at", TimestampType()),
            StructField("updated_at", TimestampType()),
        ])
        df = self.spark.createDataFrame(rels, schema=_rels_schema)
        df.write.mode("overwrite").saveAsTable(rels_table)
        logger.info("discover_named_relationships: wrote %d relationships", len(rels))
        return len(rels)

    def validate_entity_conformance(self) -> Dict[str, Any]:
        """Check how well discovered column properties cover each entity's declared schema.

        Compares bundle-defined properties against actual columns in ontology_column_properties.
        Returns a dict of conformance scores per entity-table pair and logs warnings for
        low coverage. Does not write back to any table -- purely informational.
        """
        results: Dict[str, Any] = {}
        ent_table = self.config.fully_qualified_entities
        cp_table = self.config.fully_qualified_column_properties

        try:
            primary_ents = self.spark.sql(f"""
                SELECT entity_type, EXPLODE(source_tables) AS table_name
                FROM {ent_table}
                WHERE entity_role = 'primary'
            """).collect()
        except Exception as e:
            logger.debug("validate_entity_conformance: cannot read entities: %s", e)
            return results

        try:
            cp_rows = self.spark.sql(f"""
                SELECT table_name, column_name, property_role
                FROM {cp_table}
            """).collect()
        except Exception as e:
            logger.debug("validate_entity_conformance: cannot read column_properties: %s", e)
            return results

        # Build lookup: table_name -> set of column names
        table_columns: Dict[str, set] = {}
        for r in cp_rows:
            table_columns.setdefault(r.table_name, set()).add(r.column_name.lower())

        for row in primary_ents:
            et = row.entity_type
            tbl = row.table_name
            edef = self.discoverer._entity_def_map.get(et)
            if not edef or not edef.properties:
                continue

            expected_attrs = set()
            for prop in edef.properties:
                for attr in prop.typical_attributes:
                    expected_attrs.add(attr.lower())

            actual_cols = table_columns.get(tbl, set())
            matched = expected_attrs & actual_cols
            total = len(expected_attrs)
            score = len(matched) / total if total > 0 else 1.0
            missing = sorted(expected_attrs - actual_cols)

            results[f"{et}:{tbl}"] = {
                "conformance_score": round(score, 3),
                "matched": len(matched),
                "total_declared": total,
                "missing_properties": missing[:10],
            }
            if score < 0.5 and total > 2:
                logger.warning(
                    "Low conformance for %s on %s: %.0f%% (%d/%d), missing: %s",
                    et, tbl, score * 100, len(matched), total, ", ".join(missing[:5]),
                )

        if results:
            logger.info(
                "Entity conformance: %d entity-table pairs checked, avg score %.0f%%",
                len(results),
                sum(r["conformance_score"] for r in results.values()) / len(results) * 100,
            )
        return results

    def _snapshot_ontology_state(self) -> Dict[str, Any]:
        """Read current ontology tables into Python dicts for diff comparison."""
        snapshot: Dict[str, Any] = {"entities": {}, "columns": {}, "relationships": set()}
        ent_tbl = self.config.fully_qualified_entities
        cp_tbl = self.config.fully_qualified_column_properties
        rel_tbl = self.config.fully_qualified_relationships
        try:
            for r in self.spark.sql(f"SELECT entity_name, entity_type, source_tables, confidence, "
                                    f"COALESCE(attributes['granularity'], 'table') AS granularity "
                                    f"FROM {ent_tbl}").collect():
                key = (r.entity_name, ",".join(sorted(r.source_tables or [])), r.granularity)
                snapshot["entities"][key] = {"name": r.entity_name, "type": r.entity_type, "confidence": float(r.confidence or 0)}
        except Exception as e:
            logger.debug("Snapshot entities failed: %s", e)
        try:
            for r in self.spark.sql(f"SELECT table_name, column_name, property_role, confidence "
                                    f"FROM {cp_tbl}").collect():
                key = (r.table_name, r.column_name)
                snapshot["columns"][key] = {"role": r.property_role, "confidence": float(r.confidence or 0)}
        except Exception as e:
            logger.debug("Snapshot columns failed: %s", e)
        try:
            for r in self.spark.sql(f"SELECT src_entity_type, relationship_name, dst_entity_type "
                                    f"FROM {rel_tbl}").collect():
                snapshot["relationships"].add((r.src_entity_type, r.relationship_name, r.dst_entity_type))
        except Exception as e:
            logger.debug("Snapshot relationships failed: %s", e)
        return snapshot

    def generate_discovery_diff(
        self,
        before: Dict[str, Any],
        after: Dict[str, Any],
        bundle_version: str,
        previous_version: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Compare before/after ontology snapshots and return a diff report."""
        ts = datetime.now().isoformat()
        entity_changes: Dict[str, List] = {"added": [], "removed": [], "changed": []}
        col_changes: Dict[str, List] = {"role_changed": [], "new_columns": [], "removed_columns": []}
        rel_changes: Dict[str, List] = {"added": [], "removed": []}

        before_ents = before.get("entities", {})
        after_ents = after.get("entities", {})
        for key in set(after_ents) - set(before_ents):
            entity_changes["added"].append(after_ents[key]["name"])
        for key in set(before_ents) - set(after_ents):
            entity_changes["removed"].append(before_ents[key]["name"])
        entity_changes["added"] = sorted(set(entity_changes["added"]))
        entity_changes["removed"] = sorted(set(entity_changes["removed"]))
        for key in set(before_ents) & set(after_ents):
            b, a = before_ents[key], after_ents[key]
            if abs((b.get("confidence") or 0) - (a.get("confidence") or 0)) > 0.001:
                entity_changes["changed"].append({
                    "name": a["name"], "field": "confidence",
                    "old": b.get("confidence"), "new": a.get("confidence"),
                })

        before_cols = before.get("columns", {})
        after_cols = after.get("columns", {})
        for key in set(after_cols) - set(before_cols):
            col = after_cols[key]
            col_changes["new_columns"].append({"table": key[0], "column": key[1], "role": col.get("role")})
        for key in set(before_cols) - set(after_cols):
            col = before_cols[key]
            col_changes["removed_columns"].append({"table": key[0], "column": key[1], "role": col.get("role")})
        for key in set(before_cols) & set(after_cols):
            if (before_cols[key].get("role") or "") != (after_cols[key].get("role") or ""):
                col_changes["role_changed"].append({
                    "table": key[0], "column": key[1],
                    "old_role": before_cols[key].get("role"), "new_role": after_cols[key].get("role"),
                })

        before_rels = before.get("relationships", set())
        after_rels = after.get("relationships", set())
        rel_changes["added"] = [{"src": r[0], "rel": r[1], "dst": r[2]} for r in after_rels - before_rels]
        rel_changes["removed"] = [{"src": r[0], "rel": r[1], "dst": r[2]} for r in before_rels - after_rels]

        return {
            "bundle_version": bundle_version,
            "previous_version": previous_version,
            "timestamp": ts,
            "entity_changes": entity_changes,
            "column_changes": col_changes,
            "relationship_changes": rel_changes,
        }

    def _store_discovery_diff(self, diff: Dict[str, Any]) -> None:
        """Store discovery diff report in discovery_diff_report table."""
        self.create_discovery_diff_table()
        report_id = str(uuid.uuid4())
        diff_json = json.dumps(diff).replace("'", "''")
        esc_bv = (diff.get("bundle_version") or "").replace("'", "''")
        esc_pv = (str(diff.get("previous_version") or "")).replace("'", "''")
        self.spark.sql(f"""
            INSERT INTO {self.config.fully_qualified_discovery_diff}
            (report_id, catalog, schema, bundle_version, previous_version, timestamp, diff_json, created_at)
            VALUES ('{report_id}', '{self.config.catalog_name}', '{self.config.schema_name}',
                    '{esc_bv}', '{esc_pv}', current_timestamp(), '{diff_json}', current_timestamp())
        """)
        logger.info("Stored discovery diff report %s", report_id)

    def run(self, apply_tags=False):
        """Execute the ontology building pipeline.

        Args:
            apply_tags: If True, write entity_type tags to UC tables/columns via ALTER TABLE SET TAGS.
        """
        import time as _time

        def _step(name, fn, *args, **kwargs):
            t0 = _time.time()
            result = fn(*args, **kwargs)
            elapsed = _time.time() - t0
            logger.info(f"[timing] {name}: {elapsed:.1f}s")
            return result

        logger.info("Starting ontology build")
        pipeline_start = _time.time()

        before_snapshot = self._snapshot_ontology_state()
        previous_version = None
        try:
            row = self.spark.sql(
                f"SELECT MAX(bundle_version) AS v FROM {self.config.fully_qualified_entities} "
                "WHERE bundle_version IS NOT NULL"
            ).collect()
            if row and row[0].v:
                previous_version = str(row[0].v)
        except Exception:
            pass

        _step("create_entities_table", self.create_entities_table)
        _step("create_metrics_table", self.create_metrics_table)

        table_discovered = _step("discover_table_entities", self.discover_and_store_entities)
        logger.info(f"Discovered {table_discovered} table-level entities")

        column_discovered = _step("discover_column_entities", self.discover_and_store_column_entities)
        logger.info(f"Discovered {column_discovered} column-level entities")

        if column_discovered > 0:
            _step("backfill_source_columns", self.backfill_source_columns)

        roles_classified = _step("classify_entity_roles", self.classify_entity_roles)
        logger.info(f"Classified {roles_classified} entity roles (primary/referenced)")

        col_props = _step("classify_column_properties", self.classify_column_properties)
        logger.info(f"Classified {col_props} column properties")

        _step("validate_entity_conformance", self.validate_entity_conformance)

        named_rels = _step("discover_named_relationships", self.discover_named_relationships)
        logger.info(f"Discovered {named_rels} named relationships")

        _step("sync_entity_nodes_to_graph", self._sync_entity_nodes_to_graph)

        edges_added = _step("add_entity_relationships_to_graph", self.add_entity_relationships_to_graph)

        hierarchy_edges = _step("add_hierarchy_edges", self.add_hierarchy_edges)
        edges_added += hierarchy_edges

        rel_result = _step("discover_inter_entity_relationships", self.discover_inter_entity_relationships)
        edges_added += rel_result.get("edges_added", 0)

        _step("validate_ontology_completeness", self.validate_ontology_completeness)
        _step("compute_ontology_metrics", self.compute_ontology_metrics)

        tags_applied = 0
        if apply_tags:
            tags_applied = _step("apply_entity_tags", self.apply_entity_tags)

        try:
            summary = self.get_entity_summary()
            entity_types = summary.count()
            logger.info(f"Ontology summary: {entity_types} entity types")
            for row in summary.collect():
                logger.info(
                    f"  {row['entity_type']}: {row['entity_count']} entities (avg confidence: {row['avg_confidence']})"
                )
        except Exception:
            entity_types = 0

        total_elapsed = _time.time() - pipeline_start
        logger.info(f"[timing] ontology_build_total: {total_elapsed:.1f}s")

        after_snapshot = self._snapshot_ontology_state()
        bundle_version = self._get_bundle_version()
        diff = self.generate_discovery_diff(
            before_snapshot, after_snapshot, bundle_version, previous_version
        )
        try:
            self._store_discovery_diff(diff)
        except Exception as e:
            logger.warning("Could not store discovery diff: %s", e)

        return {
            "entities_discovered": table_discovered,
            "column_entities_discovered": column_discovered,
            "entity_types": entity_types,
            "edges_added": edges_added,
            "tags_applied": tags_applied,
            "roles_classified": roles_classified,
            "column_properties": col_props,
            "named_relationships": named_rels,
            "discovery_diff": diff,
        }


def build_ontology(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    config_path: str = "configurations/ontology_config.yaml",
    apply_tags: bool = False,
    ontology_bundle: str = "",
    incremental: bool = True,
    entity_tag_key: str = "ontology.entity_type",
) -> Dict[str, Any]:
    """Convenience function to build the ontology.

    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for tables
        schema_name: Schema name for tables
        config_path: Path to ontology configuration YAML
        apply_tags: If True, write entity tags to UC tables/columns
        ontology_bundle: Optional bundle name (stored alongside entities)
        incremental: Only classify tables/columns changed since last run
        entity_tag_key: UC tag key used when apply_tags is True
    """
    config = OntologyConfig(
        catalog_name=catalog_name,
        schema_name=schema_name,
        config_path=config_path,
        ontology_bundle=ontology_bundle,
        incremental=incremental,
        entity_tag_key=entity_tag_key,
    )
    builder = OntologyBuilder(spark, config)
    return builder.run(apply_tags=apply_tags)
