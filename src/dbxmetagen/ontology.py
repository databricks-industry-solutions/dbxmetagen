"""
Ontology module for entity and metric management.

Discovers and manages business entities from the knowledge base,
creating an ontology layer on top of the data catalog.

Includes flexible keyword matching, AI-powered structured classification,
value enforcement, domain-aware prefiltering, and deduplication.
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
    kb_table: str = "table_knowledge_base"
    column_kb_table: str = "column_knowledge_base"
    nodes_table: str = "graph_nodes"
    incremental: bool = True
    entity_tag_key: str = "entity_type"

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
    def fully_qualified_kb(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.kb_table}"

    @property
    def fully_qualified_column_kb(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.column_kb_table}"

    @property
    def fully_qualified_nodes(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.nodes_table}"


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
                )
            )

        logger.info(f"Loaded {len(entities)} entity definitions")
        return entities


class EntityDiscoverer:
    """Discovers entities from knowledge base tables with flexible matching.

    Uses structured LLM output (ChatDatabricks), keyword prefiltering with
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
        self._stats = {
            "keyword_matches": 0,
            "ai_classifications": 0,
            "fallback_generic": 0,
            "column_keyword_matches": 0,
            "column_ai_classifications": 0,
            "column_fallback": 0,
        }

    # ------------------------------------------------------------------
    # LLM helpers
    # ------------------------------------------------------------------

    def _get_structured_llm(self):
        from databricks_langchain import ChatDatabricks

        llm = ChatDatabricks(
            endpoint=self._model_endpoint, temperature=0.0, max_tokens=512
        )
        return llm.with_structured_output(EntityClassificationResult)

    def _get_batch_column_llm(self):
        from databricks_langchain import ChatDatabricks

        llm = ChatDatabricks(
            endpoint=self._model_endpoint, temperature=0.0, max_tokens=2048
        )
        return llm.with_structured_output(BatchColumnClassificationResult)

    def _get_batch_table_llm(self):
        from databricks_langchain import ChatDatabricks

        llm = ChatDatabricks(
            endpoint=self._model_endpoint, temperature=0.0, max_tokens=2048
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
    # Keyword prefilter + domain-aware boosting
    # ------------------------------------------------------------------

    def _keyword_prefilter(
        self,
        name: str,
        comment: str,
        domain: str,
        top_n: int = 8,
    ) -> List[str]:
        """Score entity definitions by keyword overlap and domain affinity,
        returning the top-N candidate entity names for the LLM prompt."""
        name_lower = name.lower()
        comment_lower = (comment or "").lower()
        domain_lower = (domain or "").lower()

        affinity_set = self._domain_entity_affinity.get(domain_lower, set())

        scored: List[Tuple[str, float]] = []
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
            if edef.name in affinity_set:
                kw_score += 1.5
            scored.append((edef.name, kw_score))

        scored.sort(key=lambda x: x[1], reverse=True)
        if top_n <= 0 or top_n >= len(scored):
            return [s[0] for s in scored]
        return [s[0] for s in scored[:top_n]]

    # ------------------------------------------------------------------
    # Table-level discovery
    # ------------------------------------------------------------------

    def discover_entities_from_tables(self) -> List[Dict[str, Any]]:
        """Discover entities by matching tables to entity definitions."""
        try:
            if self.config.incremental:
                try:
                    tables_df = self.spark.sql(
                        f"""
                        SELECT kb.table_name, kb.table_short_name, kb.comment, kb.domain
                        FROM {self.config.fully_qualified_kb} kb
                        LEFT JOIN (
                            SELECT src_table, MAX(created_at) AS last_classified
                            FROM (
                                SELECT EXPLODE(source_tables) AS src_table, created_at
                                FROM {self.config.fully_qualified_entities}
                                WHERE COALESCE(attributes['granularity'], 'table') = 'table'
                            )
                            GROUP BY src_table
                        ) oe ON kb.table_name = oe.src_table
                        WHERE kb.table_name IS NOT NULL
                          AND (oe.last_classified IS NULL OR kb.updated_at > oe.last_classified)
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
                            SELECT src_table, MAX(created_at) AS last_classified
                            FROM (
                                SELECT EXPLODE(source_tables) AS src_table, created_at
                                FROM {self.config.fully_qualified_entities}
                                WHERE attributes['granularity'] = 'column'
                            )
                            GROUP BY src_table
                        ) oe ON ckb.table_name = oe.src_table
                        WHERE oe.last_classified IS NULL OR kb.updated_at > oe.last_classified
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

    # ------------------------------------------------------------------
    # AI classification for columns (structured output + enforce)
    # ------------------------------------------------------------------

    def _ai_classify_columns_for_table(
        self, table_name: str, short_name: str, columns: List
    ) -> List[Tuple[str, str, float]]:
        """Classify all columns for a single table in one LLM call.

        Returns list of (column_name, entity_type, confidence).
        Falls back to per-column classification on failure.
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
            "Return one classification per column, using the exact column_name provided."
        )
        user_prompt = (
            f"Table: {short_name}\n\n"
            f"Columns:\n{columns_text}\n\n"
            f"Entity types:\n{entity_descriptions}\n\n"
            "Classify every column listed above."
        )

        try:
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
        except Exception as e:
            logger.warning(
                f"Batch column classification failed for {table_name} ({len(columns)} cols): {e}. "
                "Falling back to per-column classification."
            )
            results = []
            for row in columns:
                etype, conf = self._ai_classify_column(row)
                results.append((row.column_name or "unknown", etype, conf))
            return results

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
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("column_bindings", _BINDING_SCHEMA, True),
        ]
    )

    def __init__(self, spark: SparkSession, config: OntologyConfig):
        self.spark = spark
        self.config = config
        self.ontology_config = OntologyLoader.load_config(config.config_path)
        affinity = load_domain_entity_affinity(self.ontology_config)
        self.discoverer = EntityDiscoverer(
            spark, config, self.ontology_config, domain_entity_affinity=affinity
        )

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
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        COMMENT 'Column-level property classifications: identifier, attribute, measure, link, timestamp'
        """
        self.spark.sql(ddl)
        logger.info("Column properties table %s ready", self.config.fully_qualified_column_properties)

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

    def _store_entities(
        self, entities: List[Dict[str, Any]], view_name: str = "new_entities"
    ) -> int:
        """Common logic to store a list of entity dicts via MERGE."""
        if not entities:
            return 0

        bundle_name = self.config.ontology_bundle or None
        now = datetime.now()
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
                    float(entity.get("confidence", 0.0)),
                    float(entity.get("discovery_confidence") or entity.get("confidence", 0.0)),
                    entity.get("entity_role"),
                    bool(entity.get("is_canonical", False)),
                    bool(entity.get("auto_discovered", True)),
                    bool(entity.get("validated", False)),
                    entity.get("validation_notes"),
                    bundle_name,
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
        WHEN NOT MATCHED THEN INSERT *
        """
        )
        return len(entities)

    def discover_and_store_entities(self) -> int:
        """Discover table-level entities, deduplicate, and store."""
        entities = self.discoverer.discover_entities_from_tables()
        if not entities:
            logger.warning(
                "No entities discovered. Check that table_knowledge_base has data."
            )
            return 0
        entities = EntityDiscoverer.deduplicate_entities(entities)
        stored = self._store_entities(entities, "new_table_entities")
        logger.info(f"Stored {stored} table-level entities (after dedup)")
        return stored

    def discover_and_store_column_entities(self) -> int:
        """Discover column-level entities, deduplicate, and store."""
        entities = self.discoverer.discover_entities_from_columns()
        if not entities:
            logger.info("No column-level entities discovered")
            return 0
        entities = EntityDiscoverer.deduplicate_entities(entities)
        stored = self._store_entities(entities, "new_column_entities")
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
        """Apply entity tags to UC tables and columns from discovered entities."""
        min_conf = self._validation_cfg.get("min_entity_confidence", 0.5)
        tag_key = self.config.entity_tag_key
        tagged = 0

        # Table-level tags
        try:
            table_ents = self.spark.sql(
                f"""
                SELECT entity_type, EXPLODE(source_tables) AS table_name
                FROM {self.config.fully_qualified_entities}
                WHERE COALESCE(attributes['granularity'], 'table') = 'table'
                  AND confidence >= {min_conf}
            """
            ).collect()
            for row in table_ents:
                try:
                    self.spark.sql(
                        f"ALTER TABLE {row.table_name} SET TAGS ('{tag_key}' = '{row.entity_type}')"
                    )
                    tagged += 1
                except Exception as e:
                    logger.warning("Failed to tag table %s: %s", row.table_name, e)
        except Exception as e:
            logger.warning("Could not read table-level entities for tagging: %s", e)

        # Column-level tags
        try:
            col_ents = self.spark.sql(
                f"""
                SELECT entity_type, source_tables[0] AS table_name,
                       EXPLODE(source_columns) AS col_name
                FROM {self.config.fully_qualified_entities}
                WHERE attributes['granularity'] = 'column'
                  AND confidence >= {min_conf}
                  AND SIZE(source_columns) > 0
            """
            ).collect()
            for row in col_ents:
                try:
                    self.spark.sql(
                        f"ALTER TABLE {row.table_name} ALTER COLUMN {row.col_name} "
                        f"SET TAGS ('{tag_key}' = '{row.entity_type}')"
                    )
                    tagged += 1
                except Exception as e:
                    logger.warning(
                        "Failed to tag column %s.%s: %s",
                        row.table_name,
                        row.col_name,
                        e,
                    )
        except Exception as e:
            logger.warning("Could not read column-level entities for tagging: %s", e)

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

        now = datetime.now()
        edge_df = self.spark.createDataFrame(
            [
                (src, dst, rel, w, now, now)
                for src, dst, rel, w, _ in new_edges
            ],
            ["src", "dst", "relationship", "weight", "created_at", "updated_at"],
        )
        edge_df.write.mode("append").saveAsTable(edges_table)
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

        edge_df = self.spark.createDataFrame(
            [(s, d, r, w, datetime.now(), datetime.now()) for s, d, r, w in new_edges],
            ["src", "dst", "relationship", "weight", "created_at", "updated_at"],
        )
        edge_df.write.mode("append").saveAsTable(edges_table)
        logger.info("Added %d hierarchy (is_a) edges", len(new_edges))
        return len(new_edges)

    def _sync_entity_nodes_to_graph(self) -> int:
        """Insert ontology entities into graph_nodes so edges have valid targets.

        Includes entity_role in node metadata via the data_type column.
        """
        nodes_table = f"{self.config.catalog_name}.{self.config.schema_name}.{self.config.nodes_table}"
        ent_table = self.config.fully_qualified_entities
        try:
            self.spark.sql(f"""
                INSERT INTO {nodes_table}
                    (id, table_name, catalog, `schema`, table_short_name,
                     domain, subdomain, has_pii, has_phi, security_level,
                     comment, node_type, parent_id, data_type,
                     quality_score, embedding, created_at, updated_at)
                SELECT entity_id, NULL, NULL, NULL, entity_name,
                    NULL, NULL, FALSE, FALSE, 'PUBLIC',
                    description, 'entity', entity_type,
                    COALESCE(entity_role, 'primary'),
                    confidence, NULL, created_at, updated_at
                FROM {ent_table}
                WHERE entity_id NOT IN (SELECT id FROM {nodes_table})
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
        """Delete existing ontology-typed edges to prevent accumulation on re-runs."""
        edges_table = f"{self.config.catalog_name}.{self.config.schema_name}.graph_edges"
        try:
            self.spark.sql(
                f"DELETE FROM {edges_table} "
                f"WHERE relationship IN ('instance_of', 'has_attribute', 'has_property', 'references', 'is_a')"
            )
            logger.info("Cleared existing ontology edges before regeneration")
        except Exception as e:
            logger.warning("Could not clear ontology edges (table may not exist): %s", e)

    def add_entity_relationships_to_graph(self) -> int:
        """Add entity relationships to the knowledge graph edges.

        Uses the redesigned model:
        - instance_of: only from table to its PRIMARY entity
        - has_property: from primary entity to columns with their role
        - references: uses named relationships from ontology_relationships
        """
        edges_table = (
            f"{self.config.catalog_name}.{self.config.schema_name}.graph_edges"
        )
        self._clear_ontology_edges()
        total = 0

        try:
            # instance_of edges: table -> PRIMARY entity only
            table_entities = self.spark.sql(f"""
                SELECT entity_id, entity_name, EXPLODE(source_tables) as table_name
                FROM {self.config.fully_qualified_entities}
                WHERE source_tables IS NOT NULL AND SIZE(source_tables) > 0
                  AND COALESCE(entity_role, 'primary') = 'primary'
            """)
            if table_entities.count() > 0:
                instance_edges = table_entities.select(
                    F.col("table_name").alias("src"),
                    F.col("entity_id").alias("dst"),
                    F.lit("instance_of").alias("relationship"),
                    F.lit(1.0).alias("weight"),
                    F.current_timestamp().alias("created_at"),
                    F.current_timestamp().alias("updated_at"),
                )
                instance_edges.write.mode("append").saveAsTable(edges_table)
                total += instance_edges.count()
        except Exception as e:
            logger.warning(f"Could not add instance_of edges: {e}")

        try:
            # has_property edges: primary entity -> column (from column_properties)
            cp_table = self.config.fully_qualified_column_properties
            prop_edges_df = self.spark.sql(f"""
                SELECT owning_entity_id AS src,
                       CONCAT(table_name, '.', column_name) AS dst,
                       'has_property' AS relationship,
                       CAST(1.0 AS DOUBLE) AS weight,
                       CURRENT_TIMESTAMP() AS created_at,
                       CURRENT_TIMESTAMP() AS updated_at
                FROM {cp_table}
            """)
            prop_count = prop_edges_df.count()
            if prop_count > 0:
                prop_edges_df.write.mode("append").saveAsTable(edges_table)
                total += prop_count
        except Exception as e:
            logger.warning(f"Could not add has_property edges: {e}")

        try:
            # Named relationship edges from ontology_relationships
            rels_table = self.config.fully_qualified_relationships
            ent_table = self.config.fully_qualified_entities
            ref_edges_df = self.spark.sql(f"""
                SELECT DISTINCT se.entity_id AS src, de.entity_id AS dst,
                       r.relationship_name AS relationship,
                       r.confidence AS weight,
                       CURRENT_TIMESTAMP() AS created_at,
                       CURRENT_TIMESTAMP() AS updated_at
                FROM {rels_table} r
                JOIN {ent_table} se ON se.entity_type = r.src_entity_type
                    AND COALESCE(se.entity_role, 'primary') = 'primary'
                JOIN {ent_table} de ON de.entity_type = r.dst_entity_type
                    AND COALESCE(de.entity_role, 'primary') = 'primary'
                WHERE se.entity_id != de.entity_id
            """)
            ref_count = ref_edges_df.count()
            if ref_count > 0:
                ref_edges_df.write.mode("append").saveAsTable(edges_table)
                total += ref_count
        except Exception as e:
            logger.warning(f"Could not add column entity edges: {e}")

        logger.info(f"Added {total} entity edges to graph")
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

    def classify_column_properties(self) -> int:
        """Populate ontology_column_properties for every column in tables with a primary entity."""
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

        # Build lookup: table_name -> (entity_id, entity_type, source_columns)
        table_primary: Dict[str, Dict] = {}
        for r in primary_ents:
            for t in r.source_tables or []:
                table_primary[t] = {
                    "entity_id": r.entity_id,
                    "entity_type": r.entity_type,
                    "source_columns": set(r.source_columns or []),
                }

        # Build lookup of column-level (referenced) entities for link detection
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

        tbl_list = ",".join(f"'{t}'" for t in table_primary)
        try:
            columns = self.spark.sql(f"""
                SELECT table_name, column_name, data_type
                FROM {col_kb}
                WHERE table_name IN ({tbl_list})
            """).collect()
        except Exception as e:
            logger.warning("classify_column_properties: cannot read column KB: %s", e)
            return 0

        now = datetime.utcnow()
        props = []
        for c in columns:
            tbl = c.table_name
            col = c.column_name
            dtype = (c.data_type or "").upper()
            pe = table_primary.get(tbl)
            if not pe:
                continue

            linked = col_entity_map.get((tbl, col))
            col_lower = col.lower()

            if linked:
                role = "link"
            elif re.search(r'_(id|key)$', col_lower) and col_lower in {cc.lower() for cc in pe["source_columns"]}:
                role = "identifier"
            elif re.search(r'_(id|key|code)$', col_lower) and linked is None:
                # FK-like column but no entity match -- still a link candidate
                role = "foreign_key"
            elif dtype in ("INT", "INTEGER", "BIGINT", "LONG", "DOUBLE", "FLOAT", "DECIMAL"):
                role = "measure"
            elif dtype in ("DATE", "TIMESTAMP", "TIMESTAMP_NTZ"):
                role = "timestamp"
            else:
                role = "attribute"

            props.append({
                "property_id": str(uuid.uuid4()),
                "table_name": tbl,
                "column_name": col,
                "property_name": col,
                "property_role": role,
                "owning_entity_id": pe["entity_id"],
                "owning_entity_type": pe["entity_type"],
                "linked_entity_type": linked,
                "confidence": 1.0 if role in ("identifier", "link") else 0.8,
                "auto_discovered": True,
                "discovery_method": "heuristic",
                "created_at": now,
                "updated_at": now,
            })

        if not props:
            return 0

        df = self.spark.createDataFrame(props)
        # Overwrite existing data for these tables
        df.write.mode("overwrite").saveAsTable(cp_table)
        logger.info("classify_column_properties: wrote %d column properties", len(props))
        return len(props)

    def discover_named_relationships(self) -> int:
        """Populate ontology_relationships from FK predictions and column link patterns."""
        ent_table = self.config.fully_qualified_entities
        fk_table = f"{self.config.catalog_name}.{self.config.schema_name}.fk_predictions"
        rels_table = self.config.fully_qualified_relationships

        self.create_relationships_table()

        # Build entity lookup: table -> primary entity type
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

            col_name = (fk.src_column or "").lower().replace("_id", "").replace("_key", "")
            rel_name = f"has_{col_name}" if col_name else "references"

            # Check declared relationships from config
            for edef in self.discoverer.entity_definitions:
                if edef.name == src_type:
                    for rn, ri in edef.relationships.items():
                        if ri.get("target") == dst_type:
                            rel_name = rn
                            break

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

        # From column_properties link columns
        cp_table = self.config.fully_qualified_column_properties
        try:
            link_rows = self.spark.sql(f"""
                SELECT DISTINCT owning_entity_type, linked_entity_type, column_name, table_name
                FROM {cp_table}
                WHERE property_role = 'link' AND linked_entity_type IS NOT NULL
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

            col_base = re.sub(r'_(id|key|code)$', '', lr.column_name.lower())
            rel_name = f"has_{col_base}" if col_base else "references"

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

        # From config hierarchy (is_a)
        for edef in self.discoverer.entity_definitions:
            parent = edef.parent
            if parent:
                key = (edef.name, parent)
                if key not in seen:
                    seen.add(key)
                    rels.append({
                        "relationship_id": str(uuid.uuid4()),
                        "src_entity_type": edef.name,
                        "relationship_name": "is_a",
                        "dst_entity_type": parent,
                        "cardinality": "1:1",
                        "evidence_column": None,
                        "evidence_table": None,
                        "source": "configured",
                        "confidence": 1.0,
                        "created_at": now,
                        "updated_at": now,
                    })

        if not rels:
            logger.info("discover_named_relationships: no relationships found")
            return 0

        df = self.spark.createDataFrame(rels)
        df.write.mode("overwrite").saveAsTable(rels_table)
        logger.info("discover_named_relationships: wrote %d relationships", len(rels))
        return len(rels)

    def run(self, apply_tags: bool = False) -> Dict[str, Any]:
        """Execute the ontology building pipeline.

        Args:
            apply_tags: If True, write entity_type tags to UC tables/columns via ALTER TABLE SET TAGS.
        """
        logger.info("Starting ontology build")

        self.create_entities_table()
        self.create_metrics_table()

        # Table-level entity discovery
        table_discovered = self.discover_and_store_entities()
        logger.info(f"Discovered {table_discovered} table-level entities")

        # Column-level entity discovery
        column_discovered = self.discover_and_store_column_entities()
        logger.info(f"Discovered {column_discovered} column-level entities")

        # Backfill source_columns on table entities from column matches
        if column_discovered > 0:
            self.backfill_source_columns()

        # --- New classification steps (Phase 2) ---
        roles_classified = self.classify_entity_roles()
        logger.info(f"Classified {roles_classified} entity roles (primary/referenced)")

        col_props = self.classify_column_properties()
        logger.info(f"Classified {col_props} column properties")

        named_rels = self.discover_named_relationships()
        logger.info(f"Discovered {named_rels} named relationships")

        # Sync entity nodes into graph_nodes so edges have valid targets
        self._sync_entity_nodes_to_graph()

        # Add relationships to graph
        edges_added = self.add_entity_relationships_to_graph()

        # Add hierarchy edges (is_a) from config parent fields
        hierarchy_edges = self.add_hierarchy_edges()
        edges_added += hierarchy_edges

        # Discover inter-entity relationships from FK predictions
        rel_result = self.discover_inter_entity_relationships()
        edges_added += rel_result.get("edges_added", 0)

        # Validate ontology completeness
        self.validate_ontology_completeness()

        # Compute and store aggregate ontology metrics
        self.compute_ontology_metrics()

        # Apply UC tags if requested
        tags_applied = 0
        if apply_tags:
            tags_applied = self.apply_entity_tags()

        # Get summary
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

        return {
            "entities_discovered": table_discovered,
            "column_entities_discovered": column_discovered,
            "entity_types": entity_types,
            "edges_added": edges_added,
            "tags_applied": tags_applied,
            "roles_classified": roles_classified,
            "column_properties": col_props,
            "named_relationships": named_rels,
        }


def build_ontology(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    config_path: str = "configurations/ontology_config.yaml",
    apply_tags: bool = False,
    ontology_bundle: str = "",
    incremental: bool = True,
    entity_tag_key: str = "entity_type",
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
