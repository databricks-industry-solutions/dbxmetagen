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
    StructType, StructField, StringType, ArrayType, MapType,
    DoubleType, BooleanType, TimestampType
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
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence score between 0.0 and 1.0")
    recommended_entity: Optional[str] = Field(
        default=None,
        description="If confidence is below 0.5, suggest what entity type name this should be, even if it is not in the provided list",
    )
    reasoning: str = Field(description="Brief reasoning for the classification")


# ==============================================================================
# Domain-to-entity affinity map (loaded from config, with embedded fallback)
# ==============================================================================

_DEFAULT_DOMAIN_ENTITY_AFFINITY: Dict[str, List[str]] = {
    "healthcare": ["Patient", "Provider", "Encounter", "Condition", "Procedure",
                   "Medication", "Observation", "Claim", "Coverage", "Person",
                   "Organization", "Location"],
    "clinical": ["Patient", "Provider", "Encounter", "Condition", "Procedure",
                 "Medication", "Observation", "Claim", "Coverage", "Person",
                 "Organization", "Location"],
    "diagnostics": ["Patient", "Observation", "Condition", "Procedure", "Person", "Reference"],
    "payer": ["Claim", "Coverage", "Person", "Organization", "Transaction", "Reference"],
    "pharmaceutical": ["Patient", "Observation", "Medication", "Condition", "Procedure",
                       "Person", "Document", "Reference", "Organization"],
    "quality_safety": ["Patient", "Observation", "Encounter", "Condition", "Metric", "Event"],
    "research": ["Patient", "Observation", "Medication", "Condition", "Procedure",
                 "Person", "Document", "Reference"],
    "finance": ["Transaction", "Person", "Organization", "Product", "Reference", "Event", "Metric"],
    "operations": ["Product", "Organization", "Location", "Event", "Metric", "Reference"],
    "workforce": ["Person", "Organization", "Location", "Event", "Reference"],
    "customer": ["Person", "Organization", "Product", "Transaction", "Event", "Metric", "Location"],
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
DOMAIN_ENTITY_AFFINITY: Dict[str, set] = {k: set(v) for k, v in _DEFAULT_DOMAIN_ENTITY_AFFINITY.items()}


# ==============================================================================
# Value enforcement (ported from domain_classifier)
# ==============================================================================

def _enforce_entity_value(
    predicted: str, allowed: List[str], fallback: str = "DataTable",
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
        "typical_attributes": ["id", "name", "email", "phone", "address", "created_date"],
    },
    "Organization": {
        "description": "Companies, departments, facilities, or business units",
        "keywords": ["company", "organization", "department", "facility", "tenant"],
        "typical_attributes": ["id", "name", "type", "address", "parent_id"],
    },
    "Product": {
        "description": "Goods, services, SKUs, or catalog items",
        "keywords": ["product", "item", "sku", "catalog", "offering"],
        "typical_attributes": ["id", "name", "description", "price", "category", "status"],
    },
    "Transaction": {
        "description": "Orders, purchases, payments, invoices, or financial events",
        "keywords": ["transaction", "order", "payment", "invoice", "purchase"],
        "typical_attributes": ["id", "customer_id", "amount", "date", "status", "currency"],
    },
    "Location": {
        "description": "Addresses, sites, regions, facilities, or geographies",
        "keywords": ["location", "address", "site", "region", "facility"],
        "typical_attributes": ["id", "name", "address", "city", "state", "country", "zip_code"],
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
        "typical_attributes": ["id", "patient_id", "date", "type", "provider", "location"],
    },
    "Condition": {
        "description": "Diagnoses, problems, or diseases (FHIR Condition / OMOP CONDITION_OCCURRENCE)",
        "keywords": ["diagnosis", "condition", "icd", "disease", "problem"],
        "typical_attributes": ["id", "code", "description", "patient_id", "date", "status"],
    },
    "Procedure": {
        "description": "Medical procedures, surgeries, or interventions (FHIR Procedure)",
        "keywords": ["procedure", "surgery", "cpt", "intervention", "operation"],
        "typical_attributes": ["id", "code", "description", "date", "provider", "status"],
    },
    "Medication": {
        "description": "Drugs, prescriptions, or medication orders (FHIR MedicationRequest)",
        "keywords": ["medication", "drug", "prescription", "pharmacy", "ndc"],
        "typical_attributes": ["id", "name", "ndc", "dose", "frequency", "route"],
    },
    "Observation": {
        "description": "Lab results, vitals, or clinical measurements (FHIR Observation / OMOP MEASUREMENT)",
        "keywords": ["lab", "result", "observation", "vital", "specimen"],
        "typical_attributes": ["id", "test_code", "value", "unit", "reference_range", "date"],
    },
    "Claim": {
        "description": "Insurance claims or billing submissions (FHIR Claim)",
        "keywords": ["claim", "billing", "adjudication", "remittance", "eob"],
        "typical_attributes": ["id", "patient_id", "amount", "service_date", "status", "payer_id"],
    },
    "Coverage": {
        "description": "Insurance plans, benefits, or member coverage (FHIR Coverage)",
        "keywords": ["coverage", "insurance", "benefit", "plan", "enrollment"],
        "typical_attributes": ["id", "member_id", "payer", "plan_name", "start_date", "end_date"],
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
    kb_table: str = "table_knowledge_base"
    column_kb_table: str = "column_knowledge_base"
    nodes_table: str = "graph_nodes"
    
    @property
    def fully_qualified_entities(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.entities_table}"
    
    @property
    def fully_qualified_metrics(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.metrics_table}"
    
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


BUNDLE_DIR = "configurations/ontology_bundles"


def resolve_bundle_path(bundle_name: str) -> str:
    """Resolve a bundle name (e.g. 'healthcare') to its YAML path.

    Searches ``configurations/ontology_bundles/`` relative to the package
    root, CWD, and Workspace.  Returns the first existing path, or falls
    back to ``<BUNDLE_DIR>/<bundle_name>.yaml`` (which OntologyLoader will
    then search with its own path resolution).
    """
    filename = f"{bundle_name}.yaml" if not bundle_name.endswith(".yaml") else bundle_name
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
                entity_count = len(raw.get("ontology", {}).get("entities", {}).get("definitions", {}))
                domain_count = len(raw.get("domains", {}))
                bundles.append({
                    "key": bundle_key,
                    "name": meta.get("name", bundle_key),
                    "industry": meta.get("industry", "general"),
                    "description": meta.get("description", ""),
                    "standards_alignment": meta.get("standards_alignment", ""),
                    "entity_count": entity_count,
                    "domain_count": domain_count,
                })
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
                    with open(path, 'r') as f:
                        config = yaml.safe_load(f)
                    if config and 'ontology' in config:
                        loaded_config = config.get('ontology', {})
                        OntologyLoader._validate_config(loaded_config)
                        entity_count = len(loaded_config.get('entities', {}).get('definitions', {}))
                        logger.info(f"Loaded ontology config from {path} with {entity_count} entity definitions")
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
                "definitions": DEFAULT_ENTITY_DEFINITIONS
            },
            "relationships": {
                "types": ["owns", "contains", "references", "derives_from", "similar_to"]
            },
            "metrics": {
                "auto_discover": False,
                "definitions": []
            }
        }
    
    @staticmethod
    def _validate_config(config: Dict[str, Any]) -> None:
        """Validate ontology config structure. Raises ValueError on critical issues."""
        entities = config.get('entities')
        if not entities or not isinstance(entities, dict):
            raise ValueError("ontology config missing required 'entities' section")
        
        definitions = entities.get('definitions')
        if not definitions or not isinstance(definitions, dict):
            raise ValueError("ontology config missing required 'entities.definitions' (must be non-empty dict)")
        
        for name, defn in definitions.items():
            if not isinstance(defn, dict):
                raise ValueError(f"Entity '{name}' definition must be a dict, got {type(defn).__name__}")
            if 'description' not in defn:
                logger.warning(f"Entity '{name}' missing 'description'")
            if not isinstance(defn.get('keywords', []), list):
                raise ValueError(f"Entity '{name}'.keywords must be a list")
            if not isinstance(defn.get('typical_attributes', []), list):
                raise ValueError(f"Entity '{name}'.typical_attributes must be a list")
        
        validation = config.get('validation', {})
        if validation:
            for key in ('min_entity_confidence', 'max_entities_per_table'):
                val = validation.get(key)
                if val is not None and not isinstance(val, (int, float)):
                    raise ValueError(f"validation.{key} must be numeric, got {type(val).__name__}")
    
    @staticmethod
    def get_entity_definitions(config: Dict[str, Any]) -> List[EntityDefinition]:
        """Extract entity definitions from config, using defaults if empty."""
        definitions = config.get('entities', {}).get('definitions', {})
        
        # If no definitions in config, use embedded defaults
        if not definitions:
            logger.info("No entity definitions in config, using embedded defaults")
            definitions = DEFAULT_ENTITY_DEFINITIONS
        
        entities = []
        for name, details in definitions.items():
            entities.append(EntityDefinition(
                name=name,
                description=details.get('description', f'{name} entity'),
                keywords=details.get('keywords', []),
                typical_attributes=details.get('typical_attributes', [])
            ))
        
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
        self, spark: SparkSession, config: OntologyConfig, ontology_config: Dict[str, Any],
        domain_entity_affinity: Optional[Dict[str, set]] = None,
    ):
        self.spark = spark
        self.config = config
        self.ontology_config = ontology_config
        self.entity_definitions = OntologyLoader.get_entity_definitions(ontology_config)
        self._domain_entity_affinity = domain_entity_affinity or load_domain_entity_affinity(ontology_config)
        self._validation_cfg = ontology_config.get('validation', {})
        self._relationship_types = {
            r['name'] if isinstance(r, dict) else r
            for r in ontology_config.get('relationships', {}).get('types', [])
        }
        self._model_endpoint = self._validation_cfg.get(
            'classification_model',
            DEFAULT_CLASSIFICATION_MODEL,
        )
        self._entity_names = [e.name for e in self.entity_definitions if e.name != "DataTable"]
        self._stats = {
            "keyword_matches": 0, "ai_classifications": 0, "fallback_generic": 0,
            "column_keyword_matches": 0, "column_ai_classifications": 0, "column_fallback": 0,
        }

    # ------------------------------------------------------------------
    # LLM helpers
    # ------------------------------------------------------------------

    def _get_structured_llm(self):
        from databricks_langchain import ChatDatabricks
        llm = ChatDatabricks(endpoint=self._model_endpoint, temperature=0.0, max_tokens=512)
        return llm.with_structured_output(EntityClassificationResult)

    # ------------------------------------------------------------------
    # Keyword prefilter + domain-aware boosting
    # ------------------------------------------------------------------

    def _keyword_prefilter(
        self, name: str, comment: str, domain: str, top_n: int = 8,
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
            for kw in edef.keywords:
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
            tables_df = self.spark.sql(f"""
                SELECT table_name, table_short_name, comment, domain
                FROM {self.config.fully_qualified_kb}
            """)
            table_count = tables_df.count()
        except Exception as e:
            logger.error(f"Could not read from knowledge base: {e}")
            return []

        if table_count == 0:
            logger.warning(f"Knowledge base table {self.config.fully_qualified_kb} is empty. "
                          "Run build_knowledge_base first to populate it.")
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
            logger.info(f"Using AI to classify {len(tables_without_matches)} unmatched tables")
            for row in tables_without_matches:
                entities = self._ai_classify_table(row)
                discovered.extend(entities)

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
        variations = [name, name.replace('_', ' '), name.replace('_', '')]
        if name.endswith('ies'):
            variations.append(name[:-3] + 'y')
        elif name.endswith('es'):
            variations.append(name[:-2])
        elif name.endswith('s'):
            variations.append(name[:-1])
        variations.append(name + 's')
        return list(set(variations))

    def _match_table_to_entities(self, table_row) -> List[Dict[str, Any]]:
        """Match a table to potential entity definitions with flexible matching."""
        matches = []
        table_name = table_row.table_name
        short_name = (table_row.table_short_name or '').lower()
        comment = (table_row.comment or '').lower()
        name_variations = self._normalize_name(short_name)

        threshold = self.ontology_config.get('entities', {}).get(
            'discovery_confidence_threshold', 0.4
        )

        for entity_def in self.entity_definitions:
            confidence = self._calculate_match_confidence(
                name_variations, short_name, comment, entity_def
            )
            if confidence >= threshold:
                matches.append({
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
                })
        return matches

    def _calculate_match_confidence(
        self, name_variations: List[str], original_name: str,
        comment: str, entity_def: EntityDefinition,
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

    def _ai_classify_table(self, table_row) -> List[Dict[str, Any]]:
        """Classify a table using structured LLM output. May return 1-2 entities
        (primary + optional secondary for relationship tables)."""
        table_name = table_row.table_name
        short_name = table_row.table_short_name or table_name.split('.')[-1]
        comment = table_row.comment or "No description available"
        domain = getattr(table_row, 'domain', None) or "unknown"

        candidates = self._keyword_prefilter(short_name, comment, domain)
        entity_descriptions = "\n".join(
            f"- {e.name}: {e.description}"
            for e in self.entity_definitions if e.name in candidates
        )

        system_prompt = (
            "You are an entity classifier for database tables. "
            "Classify each table into an entity type. "
            "If the table clearly represents a relationship between two entities "
            "(e.g. patient_encounters links Patient and Encounter), also populate secondary_entity_type. "
            "If your confidence is below 0.5, populate recommended_entity with what you think "
            "the entity should really be called, even if it is not in the provided list."
        )
        user_prompt = (
            f"Table name: {short_name}\n"
            f"Description: {comment[:500]}\n"
            f"Domain: {domain}\n\n"
            f"Available entity types:\n{entity_descriptions}\n\n"
            "Classify this table."
        )

        results = []
        try:
            structured_llm = self._get_structured_llm()
            response: EntityClassificationResult = structured_llm.invoke([
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ])

            # --- primary entity ---
            enforced, exact = _enforce_entity_value(response.entity_type, self._entity_names)
            confidence = response.confidence
            if not exact:
                confidence = max(0.0, confidence - self.CONFIDENCE_PENALTY_SNAP)
                logger.debug(f"Snapped '{response.entity_type}' -> '{enforced}' for {short_name}")

            attrs: Dict[str, str] = {"discovery_method": "ai"}
            if response.reasoning:
                attrs["reasoning"] = response.reasoning
            if response.recommended_entity:
                attrs["recommended_entity"] = response.recommended_entity

            edef = next((e for e in self.entity_definitions if e.name == enforced), None)
            results.append({
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
                "validation_notes": response.reasoning,
            })
            self._stats["ai_classifications"] += 1

            # --- secondary entity (multi-entity support) ---
            if response.secondary_entity_type:
                sec_enforced, sec_exact = _enforce_entity_value(
                    response.secondary_entity_type, self._entity_names,
                )
                sec_conf = response.confidence * 0.85
                if not sec_exact:
                    sec_conf = max(0.0, sec_conf - self.CONFIDENCE_PENALTY_SNAP)
                min_conf = self._validation_cfg.get('min_entity_confidence', 0.5)
                if sec_conf >= min_conf and sec_enforced != enforced:
                    sec_edef = next((e for e in self.entity_definitions if e.name == sec_enforced), None)
                    results.append({
                        "entity_id": str(uuid.uuid4()),
                        "entity_name": sec_enforced,
                        "entity_type": sec_enforced,
                        "description": sec_edef.description if sec_edef else f"Secondary: {sec_enforced}",
                        "source_tables": [table_name],
                        "source_columns": [],
                        "attributes": {"discovery_method": "ai_secondary", "reasoning": response.reasoning or ""},
                        "confidence": round(sec_conf, 3),
                        "auto_discovered": True,
                        "validated": False,
                        "validation_notes": f"Secondary entity from relationship table {short_name}",
                    })

        except Exception as e:
            logger.warning(f"AI classification failed for {table_name}, using fallback: {e}")
            self._stats["fallback_generic"] += 1
            results.append({
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
            })
        return results

    # ------------------------------------------------------------------
    # Column-level entity discovery
    # ------------------------------------------------------------------

    def discover_entities_from_columns(self) -> List[Dict[str, Any]]:
        """Discover entities by matching columns to entity definitions."""
        try:
            cols_df = self.spark.sql(f"""
                SELECT column_name, table_name, table_short_name, comment,
                       data_type, classification, classification_type
                FROM {self.config.fully_qualified_column_kb}
            """)
            col_count = cols_df.count()
        except Exception as e:
            logger.warning(f"Could not read column_knowledge_base, skipping column discovery: {e}")
            return []

        if col_count == 0:
            logger.info("column_knowledge_base is empty, skipping column discovery")
            return []

        logger.info(f"Evaluating {col_count} columns for entity classification")

        min_confidence = self._validation_cfg.get('min_entity_confidence', 0.5)
        max_per_table = self._validation_cfg.get('max_entities_per_table', 3)

        table_entity_map: Dict[tuple, Dict[str, Any]] = {}
        unmatched = []

        for row in cols_df.collect():
            matches = self._match_column_to_entities(row)
            if matches:
                for entity_type, conf in matches:
                    key = (row.table_name, entity_type)
                    if key not in table_entity_map:
                        table_entity_map[key] = {
                            "columns": [], "conf_sum": 0.0, "count": 0,
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
            for row in unmatched:
                entity_type, conf = self._ai_classify_column(row)
                key = (row.table_name, entity_type)
                if key not in table_entity_map:
                    table_entity_map[key] = {
                        "columns": [], "conf_sum": 0.0, "count": 0,
                        "table_short_name": row.table_short_name,
                    }
                table_entity_map[key]["columns"].append(row.column_name)
                table_entity_map[key]["conf_sum"] += conf
                table_entity_map[key]["count"] += 1

        discovered = []
        table_groups: Dict[str, list] = {}
        for (tbl, etype), info in table_entity_map.items():
            avg_conf = info["conf_sum"] / info["count"]
            if avg_conf < min_confidence:
                continue
            entity_def = next((e for e in self.entity_definitions if e.name == etype), None)
            entry = {
                "entity_id": str(uuid.uuid4()),
                "entity_name": etype,
                "entity_type": etype,
                "description": entity_def.description if entity_def else f"Column-derived {etype}",
                "source_tables": [tbl],
                "source_columns": info["columns"],
                "attributes": {"granularity": "column", "discovery_method": "column_keyword"},
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
        col_name = (col_row.column_name or '').lower()
        comment = (col_row.comment or '').lower()
        classification = (col_row.classification or '').lower()
        col_variations = self._normalize_name(col_name)

        matches = []
        for entity_def in self.entity_definitions:
            if entity_def.name == "DataTable":
                continue
            score = self._column_match_score(col_variations, col_name, comment, classification, entity_def)
            if score >= 0.4:
                matches.append((entity_def.name, score))
        return matches

    def _column_match_score(
        self, col_variations: List[str], col_name: str, comment: str,
        classification: str, entity_def: EntityDefinition,
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
        if re.match(rf'{entity_lower}[_\s]?id$', col_name):
            score += 2.0
            max_score += 2.0
        healthcare_entities = {"patient", "provider", "encounter", "condition",
                               "procedure", "medication", "observation", "claim", "coverage"}
        if classification in ('phi', 'pii') and entity_lower in healthcare_entities:
            score += 0.5
            max_score += 0.5
        if max_score == 0:
            return 0.0
        return min(1.0, score / max_score)

    # ------------------------------------------------------------------
    # AI classification for columns (structured output + enforce)
    # ------------------------------------------------------------------

    def _ai_classify_column(self, col_row) -> Tuple[str, float]:
        """AI classification for a single column using structured output."""
        col_name = col_row.column_name or "unknown"
        table_name = col_row.table_short_name or col_row.table_name or "unknown"
        comment = col_row.comment or "No description"
        data_type = col_row.data_type or "unknown"

        entity_descriptions = "\n".join(
            f"- {e.name}: {e.description}"
            for e in self.entity_definitions if e.name != "DataTable"
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
            response: EntityClassificationResult = structured_llm.invoke([
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ])
            enforced, exact = _enforce_entity_value(response.entity_type, self._entity_names)
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
                    existing["confidence"] = max(existing["confidence"], ent.get("confidence", 0))
                    for col in ent.get("source_columns", []):
                        if col not in existing["source_columns"]:
                            existing["source_columns"].append(col)
                    for k, v in ent.get("attributes", {}).items():
                        if k not in existing["attributes"]:
                            existing["attributes"][k] = v
                    if ent.get("validation_notes") and not existing.get("validation_notes"):
                        existing["validation_notes"] = ent["validation_notes"]
        return list(groups.values())


class OntologyBuilder:
    """
    Builder for creating and managing the ontology layer.
    
    Creates ontology_entities and ontology_metrics tables, discovers
    entities from the knowledge base, and manages relationships.
    """
    
    ENTITIES_SCHEMA = StructType([
        StructField("entity_id", StringType(), False),
        StructField("entity_name", StringType(), True),
        StructField("entity_type", StringType(), True),
        StructField("description", StringType(), True),
        StructField("source_tables", ArrayType(StringType()), True),
        StructField("source_columns", ArrayType(StringType()), True),
        StructField("attributes", MapType(StringType(), StringType()), True),
        StructField("confidence", DoubleType(), True),
        StructField("auto_discovered", BooleanType(), True),
        StructField("validated", BooleanType(), True),
        StructField("validation_notes", StringType(), True),
        StructField("ontology_bundle", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ])
    
    def __init__(self, spark: SparkSession, config: OntologyConfig):
        self.spark = spark
        self.config = config
        self.ontology_config = OntologyLoader.load_config(config.config_path)
        affinity = load_domain_entity_affinity(self.ontology_config)
        self.discoverer = EntityDiscoverer(spark, config, self.ontology_config, domain_entity_affinity=affinity)
    
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
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        COMMENT 'Ontology entities discovered from knowledge base'
        """
        self.spark.sql(ddl)
        # Add column if table already exists (ALTER TABLE ADD COLUMN is idempotent for existing columns in Delta)
        try:
            self.spark.sql(f"ALTER TABLE {self.config.fully_qualified_entities} ADD COLUMN IF NOT EXISTS ontology_bundle STRING")
        except Exception:
            pass
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
    
    def _store_entities(self, entities: List[Dict[str, Any]], view_name: str = "new_entities") -> int:
        """Common logic to store a list of entity dicts via MERGE."""
        if not entities:
            return 0

        bundle_name = self.config.ontology_bundle or None
        now = datetime.now()
        rows = []
        for entity in entities:
            attributes = entity.get('attributes', {})
            if isinstance(attributes, dict):
                attributes = {k: str(v) for k, v in attributes.items()}
            else:
                attributes = {}
            rows.append((
                entity['entity_id'],
                entity.get('entity_name'),
                entity.get('entity_type'),
                entity.get('description'),
                entity.get('source_tables', []),
                entity.get('source_columns', []),
                attributes,
                float(entity.get('confidence', 0.0)),
                bool(entity.get('auto_discovered', True)),
                bool(entity.get('validated', False)),
                entity.get('validation_notes'),
                bundle_name,
                now, now,
            ))

        df = self.spark.createDataFrame(rows, schema=self.ENTITIES_SCHEMA)
        df.createOrReplaceTempView(view_name)

        self.spark.sql(f"""
        MERGE INTO {self.config.fully_qualified_entities} AS target
        USING {view_name} AS source
        ON target.entity_name = source.entity_name
           AND array_join(target.source_tables, ',') = array_join(source.source_tables, ',')
           AND COALESCE(target.attributes['granularity'], 'table') = COALESCE(source.attributes['granularity'], 'table')
        WHEN NOT MATCHED THEN INSERT *
        """)
        return len(entities)

    def discover_and_store_entities(self) -> int:
        """Discover table-level entities, deduplicate, and store."""
        entities = self.discoverer.discover_entities_from_tables()
        if not entities:
            logger.warning("No entities discovered. Check that table_knowledge_base has data.")
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
        """Update table-level entities' source_columns from column-level matches."""
        try:
            self.spark.sql(f"""
            MERGE INTO {self.config.fully_qualified_entities} AS tbl
            USING (
                SELECT entity_name,
                       array_distinct(flatten(collect_list(source_columns))) AS cols,
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
                UPDATE SET source_columns = col_agg.cols, updated_at = current_timestamp()
            """)
            logger.info("Backfilled source_columns on table-level entities")
            return 1
        except Exception as e:
            logger.warning(f"Could not backfill source_columns: {e}")
            return 0

    def populate_entity_metrics(self, model_endpoint: str = "databricks-gpt-oss-120b") -> int:
        """Generate entity-level metric suggestions and populate ontology_metrics."""
        ents = self.spark.sql(f"""
            SELECT entity_id, entity_name, entity_type, description,
                   source_tables, source_columns
            FROM {self.config.fully_qualified_entities}
            WHERE confidence >= 0.5 AND COALESCE(attributes['granularity'], 'table') = 'table'
        """).collect()
        if not ents:
            return 0

        # Get column metadata for the source tables
        all_tables = set()
        for e in ents:
            for t in (e["source_tables"] or []):
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
                "Each: {{\"metric_name\": \"...\", \"description\": \"...\", "
                "\"aggregation_type\": \"SUM|COUNT|AVG|MIN|MAX\", "
                "\"source_field\": \"column_name\", \"filter_condition\": \"optional WHERE clause\"}}. "
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
                    metrics = json.loads(text[s:end + 1])
                else:
                    continue
            except Exception as ex:
                logger.warning("AI metric generation failed for %s: %s", e["entity_name"], ex)
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

    def apply_entity_tags(self) -> int:
        """Apply entity_type tags to UC tables and columns from discovered entities."""
        min_conf = self._validation_cfg.get('min_entity_confidence', 0.5)
        tagged = 0

        # Table-level tags
        try:
            table_ents = self.spark.sql(f"""
                SELECT entity_type, EXPLODE(source_tables) AS table_name
                FROM {self.config.fully_qualified_entities}
                WHERE COALESCE(attributes['granularity'], 'table') = 'table'
                  AND confidence >= {min_conf}
            """).collect()
            for row in table_ents:
                try:
                    self.spark.sql(
                        f"ALTER TABLE {row.table_name} SET TAGS ('entity_type' = '{row.entity_type}')"
                    )
                    tagged += 1
                except Exception as e:
                    logger.warning("Failed to tag table %s: %s", row.table_name, e)
        except Exception as e:
            logger.warning("Could not read table-level entities for tagging: %s", e)

        # Column-level tags
        try:
            col_ents = self.spark.sql(f"""
                SELECT entity_type, source_tables[0] AS table_name,
                       EXPLODE(source_columns) AS col_name
                FROM {self.config.fully_qualified_entities}
                WHERE attributes['granularity'] = 'column'
                  AND confidence >= {min_conf}
                  AND SIZE(source_columns) > 0
            """).collect()
            for row in col_ents:
                try:
                    self.spark.sql(
                        f"ALTER TABLE {row.table_name} ALTER COLUMN {row.col_name} "
                        f"SET TAGS ('entity_type' = '{row.entity_type}')"
                    )
                    tagged += 1
                except Exception as e:
                    logger.warning("Failed to tag column %s.%s: %s", row.table_name, row.col_name, e)
        except Exception as e:
            logger.warning("Could not read column-level entities for tagging: %s", e)

        logger.info("Applied %d entity tags to UC objects", tagged)
        return tagged
    
    def get_entity_summary(self) -> DataFrame:
        """Get summary of discovered entities."""
        return self.spark.sql(f"""
            SELECT 
                entity_type,
                COUNT(*) as entity_count,
                ROUND(AVG(confidence), 2) as avg_confidence,
                SUM(CASE WHEN validated THEN 1 ELSE 0 END) as validated_count
            FROM {self.config.fully_qualified_entities}
            GROUP BY entity_type
            ORDER BY entity_count DESC
        """)
    
    def add_entity_relationships_to_graph(self) -> int:
        """Add entity relationships to the knowledge graph edges."""
        edges_table = f"{self.config.catalog_name}.{self.config.schema_name}.graph_edges"
        total = 0

        try:
            # instance_of edges: table -> entity (table-level entities)
            table_entities = self.spark.sql(f"""
                SELECT entity_id, entity_name, EXPLODE(source_tables) as table_name
                FROM {self.config.fully_qualified_entities}
                WHERE COALESCE(attributes['granularity'], 'table') = 'table'
            """)
            if table_entities.count() > 0:
                instance_edges = table_entities.select(
                    F.col("table_name").alias("src"),
                    F.col("entity_id").alias("dst"),
                    F.lit("instance_of").alias("relationship"),
                    F.lit(1.0).alias("weight"),
                    F.current_timestamp().alias("created_at"),
                    F.current_timestamp().alias("updated_at")
                )
                instance_edges.write.mode("append").saveAsTable(edges_table)
                total += instance_edges.count()
        except Exception as e:
            logger.warning(f"Could not add instance_of edges: {e}")

        try:
            # has_attribute edges: entity -> column (column-level entities)
            col_entities = self.spark.sql(f"""
                SELECT entity_id, entity_name, source_tables[0] as table_name,
                       EXPLODE(source_columns) as col_name
                FROM {self.config.fully_qualified_entities}
                WHERE attributes['granularity'] = 'column'
                  AND SIZE(source_columns) > 0
            """)
            if col_entities.count() > 0:
                attr_edges = col_entities.select(
                    F.col("entity_id").alias("src"),
                    F.concat_ws(".", F.col("table_name"), F.col("col_name")).alias("dst"),
                    F.lit("has_attribute").alias("relationship"),
                    F.lit(1.0).alias("weight"),
                    F.current_timestamp().alias("created_at"),
                    F.current_timestamp().alias("updated_at")
                )
                attr_edges.write.mode("append").saveAsTable(edges_table)
                total += attr_edges.count()

                # references edges: if column like <entity>_id on a different entity's table
                # e.g., patient_id on encounters table -> references Patient entity
                ref_edges = col_entities.alias("c").join(
                    self.spark.sql(f"""
                        SELECT entity_id as target_entity_id, entity_name as target_entity
                        FROM {self.config.fully_qualified_entities}
                        WHERE COALESCE(attributes['granularity'], 'table') = 'table'
                    """).alias("t"),
                    F.lower(F.col("c.col_name")).contains(F.lower(F.col("t.target_entity")))
                    & (F.col("c.entity_name") != F.col("t.target_entity")),
                    "inner"
                ).select(
                    F.col("c.entity_id").alias("src"),
                    F.col("t.target_entity_id").alias("dst"),
                    F.lit("references").alias("relationship"),
                    F.lit(0.8).alias("weight"),
                    F.current_timestamp().alias("created_at"),
                    F.current_timestamp().alias("updated_at")
                ).dropDuplicates(["src", "dst"])

                if ref_edges.count() > 0:
                    ref_edges.write.mode("append").saveAsTable(edges_table)
                    total += ref_edges.count()
        except Exception as e:
            logger.warning(f"Could not add column entity edges: {e}")

        logger.info(f"Added {total} entity edges to graph")
        return total
    
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
        
        # Add relationships to graph
        edges_added = self.add_entity_relationships_to_graph()

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
                logger.info(f"  {row['entity_type']}: {row['entity_count']} entities (avg confidence: {row['avg_confidence']})")
        except Exception:
            entity_types = 0
        
        return {
            "entities_discovered": table_discovered,
            "column_entities_discovered": column_discovered,
            "entity_types": entity_types,
            "edges_added": edges_added,
            "tags_applied": tags_applied,
        }


def build_ontology(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    config_path: str = "configurations/ontology_config.yaml",
    apply_tags: bool = False,
    ontology_bundle: str = "",
) -> Dict[str, Any]:
    """Convenience function to build the ontology.

    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for tables
        schema_name: Schema name for tables
        config_path: Path to ontology configuration YAML
        apply_tags: If True, write entity_type tags to UC tables/columns
        ontology_bundle: Optional bundle name (stored alongside entities)
    """
    config = OntologyConfig(
        catalog_name=catalog_name,
        schema_name=schema_name,
        config_path=config_path,
        ontology_bundle=ontology_bundle,
    )
    builder = OntologyBuilder(spark, config)
    return builder.run(apply_tags=apply_tags)
