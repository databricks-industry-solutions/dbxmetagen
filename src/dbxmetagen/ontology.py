"""
Ontology module for entity and metric management.

Discovers and manages business entities from the knowledge base,
creating an ontology layer on top of the data catalog.

Includes flexible keyword matching, AI-powered fallback classification,
and robust config loading with embedded defaults.
"""

import logging
import yaml
import os
import re
import json
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, MapType,
    DoubleType, BooleanType, TimestampType
)

logger = logging.getLogger(__name__)


# ==============================================================================
# EMBEDDED DEFAULT ENTITY DEFINITIONS
# These are used when the config file cannot be loaded
# ==============================================================================
DEFAULT_ENTITY_DEFINITIONS = {
    "Customer": {
        "description": "Customer, user, or account entities",
        "keywords": ["customer", "user", "client", "account", "member", "subscriber"],
        "typical_attributes": ["id", "name", "email", "phone", "address"]
    },
    "Product": {
        "description": "Product, item, or SKU entities",
        "keywords": ["product", "item", "sku", "inventory", "goods", "catalog"],
        "typical_attributes": ["id", "name", "description", "price", "category"]
    },
    "Transaction": {
        "description": "Transaction or order entities",
        "keywords": ["transaction", "order", "purchase", "sale", "payment", "invoice"],
        "typical_attributes": ["id", "customer_id", "amount", "date", "status"]
    },
    "Employee": {
        "description": "Employee or staff entities",
        "keywords": ["employee", "staff", "worker", "personnel", "hr"],
        "typical_attributes": ["id", "name", "email", "department", "role"]
    },
    "Location": {
        "description": "Location or geographic entities",
        "keywords": ["location", "address", "site", "facility", "store", "warehouse"],
        "typical_attributes": ["id", "name", "address", "city", "state"]
    },
    "Event": {
        "description": "Event or activity entities",
        "keywords": ["event", "activity", "log", "audit", "action", "session"],
        "typical_attributes": ["id", "type", "timestamp", "user_id"]
    },
    "Reference": {
        "description": "Reference or lookup data entities",
        "keywords": ["reference", "lookup", "code", "type", "category", "dim_", "config"],
        "typical_attributes": ["id", "code", "name", "description"]
    },
    "Metric": {
        "description": "Metric or aggregated data entities",
        "keywords": ["metric", "kpi", "aggregate", "summary", "stats", "fact_"],
        "typical_attributes": ["id", "name", "value", "period"]
    },
    "Patient": {
        "description": "Patient or healthcare individual",
        "keywords": ["patient", "person", "individual", "subject", "participant"],
        "typical_attributes": ["id", "mrn", "name", "dob", "gender"]
    },
    "ClinicalNote": {
        "description": "Clinical notes or medical documentation",
        "keywords": ["note", "ehr", "emr", "clinical", "documentation", "record", "chart"],
        "typical_attributes": ["id", "patient_id", "text", "date", "author"]
    },
    "Encounter": {
        "description": "Healthcare encounter or visit",
        "keywords": ["encounter", "visit", "admission", "episode", "appointment"],
        "typical_attributes": ["id", "patient_id", "date", "type", "provider"]
    },
    "Diagnosis": {
        "description": "Medical diagnosis or condition",
        "keywords": ["diagnosis", "condition", "icd", "disease", "disorder", "problem"],
        "typical_attributes": ["id", "code", "description", "patient_id", "date"]
    },
    "Medication": {
        "description": "Medication or prescription",
        "keywords": ["medication", "drug", "prescription", "rx", "pharmacy", "medicine"],
        "typical_attributes": ["id", "name", "ndc", "dose", "frequency"]
    },
    "Procedure": {
        "description": "Medical procedure or intervention",
        "keywords": ["procedure", "surgery", "intervention", "cpt", "operation"],
        "typical_attributes": ["id", "code", "description", "date", "provider"]
    },
    "LabResult": {
        "description": "Laboratory test results",
        "keywords": ["lab", "result", "test", "loinc", "specimen", "panel", "blood"],
        "typical_attributes": ["id", "test_code", "value", "unit", "date"]
    },
    "Provider": {
        "description": "Healthcare provider or practitioner",
        "keywords": ["provider", "physician", "doctor", "nurse", "practitioner", "clinician"],
        "typical_attributes": ["id", "npi", "name", "specialty"]
    },
    "DataTable": {
        "description": "Generic data table (fallback type)",
        "keywords": [],
        "typical_attributes": ["id"]
    }
}


@dataclass
class OntologyConfig:
    """Configuration for ontology management."""
    catalog_name: str
    schema_name: str
    config_path: str = "configurations/ontology_config.yaml"
    entities_table: str = "ontology_entities"
    metrics_table: str = "ontology_metrics"
    kb_table: str = "table_knowledge_base"
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
    def fully_qualified_nodes(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.nodes_table}"


@dataclass
class EntityDefinition:
    """Definition of a business entity."""
    name: str
    description: str
    keywords: List[str] = field(default_factory=list)
    typical_attributes: List[str] = field(default_factory=list)


class OntologyLoader:
    """Loads and parses ontology configuration with robust fallbacks."""
    
    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        """Load ontology configuration from YAML file with workspace-aware paths."""
        # Try multiple path variations
        paths_to_try = [
            config_path,
            f"../{config_path}",
            f"../../{config_path}",
            os.path.join(os.path.dirname(__file__), "..", "..", config_path),
            # Databricks workspace paths
            f"/Workspace/{config_path}",
            f"/Workspace/Repos/{config_path}",
        ]
        
        # Also try to find it relative to current working directory
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
                        entity_count = len(loaded_config.get('entities', {}).get('definitions', {}))
                        logger.info(f"Loaded ontology config from {path} with {entity_count} entity definitions")
                        return loaded_config
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
    """Discovers entities from knowledge base tables with flexible matching."""
    
    AI_MODEL = "databricks-gpt-oss-120b"
    
    def __init__(self, spark: SparkSession, config: OntologyConfig, ontology_config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.ontology_config = ontology_config
        self.entity_definitions = OntologyLoader.get_entity_definitions(ontology_config)
        # Track discovery stats
        self._stats = {"keyword_matches": 0, "ai_classifications": 0, "fallback_generic": 0}
    
    def discover_entities_from_tables(self) -> List[Dict[str, Any]]:
        """Discover entities by matching tables to entity definitions."""
        try:
            tables_df = self.spark.sql(f"""
                SELECT 
                    table_name,
                    table_short_name,
                    comment,
                    domain
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
        
        # AI-based fallback for ALL tables without keyword matches
        if tables_without_matches:
            logger.info(f"Using AI to classify {len(tables_without_matches)} unmatched tables")
            for row in tables_without_matches:
                entity = self._ai_classify_table_with_fallback(row)
                discovered.append(entity)
        
        # Summary logging
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
        variations = [name]
        
        # Snake_case to separate words
        variations.append(name.replace('_', ' '))
        variations.append(name.replace('_', ''))
        
        # Handle plurals (simple rules)
        if name.endswith('s'):
            variations.append(name[:-1])  # customers -> customer
        if name.endswith('ies'):
            variations.append(name[:-3] + 'y')  # categories -> category
        if name.endswith('es'):
            variations.append(name[:-2])  # boxes -> box
        
        # Add singular form
        variations.append(name + 's')  # customer -> customers
        
        return list(set(variations))
    
    def _match_table_to_entities(self, table_row) -> List[Dict[str, Any]]:
        """Match a table to potential entity definitions with flexible matching."""
        matches = []
        table_name = table_row.table_name
        short_name = (table_row.table_short_name or '').lower()
        comment = (table_row.comment or '').lower()
        
        # Generate name variations for flexible matching
        name_variations = self._normalize_name(short_name)
        
        for entity_def in self.entity_definitions:
            confidence = self._calculate_match_confidence(
                name_variations, short_name, comment, entity_def
            )
            
            # Use lower threshold (0.4 instead of 0.7)
            threshold = self.ontology_config.get('entities', {}).get(
                'discovery_confidence_threshold', 0.4
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
                        "discovery_method": "keyword"
                    },
                    "confidence": confidence,
                    "auto_discovered": True,
                    "validated": False,
                    "validation_notes": None
                })
        
        return matches
    
    def _calculate_match_confidence(
        self, 
        name_variations: List[str],
        original_name: str,
        comment: str, 
        entity_def: EntityDefinition
    ) -> float:
        """Calculate confidence score with flexible matching."""
        score = 0.0
        max_score = 0.0
        
        for keyword in entity_def.keywords:
            keyword_lower = keyword.lower()
            keyword_variations = self._normalize_name(keyword)
            max_score += 2.0
            
            # Check exact match in name (highest score)
            if keyword_lower == original_name:
                score += 2.0
                continue
            
            # Check keyword variations against name variations
            matched = False
            for kw_var in keyword_variations:
                for name_var in name_variations:
                    if kw_var in name_var or name_var in kw_var:
                        score += 1.5
                        matched = True
                        break
                if matched:
                    break
            
            # Check in comment (lower score)
            if not matched and keyword_lower in comment:
                score += 0.8
        
        # Check for typical attribute patterns in comment
        for attr in entity_def.typical_attributes:
            max_score += 0.3
            if attr.lower() in comment:
                score += 0.3
        
        if max_score == 0:
            return 0.0
        
        return min(1.0, score / max_score)
    
    def _ai_classify_table_with_fallback(self, table_row) -> Dict[str, Any]:
        """Use AI to classify a table, ALWAYS returning an entity (never None)."""
        table_name = table_row.table_name
        short_name = table_row.table_short_name or table_name.split('.')[-1]
        comment = table_row.comment or "No description available"
        domain = table_row.domain or "unknown"
        
        # Get available entity types for the prompt
        entity_types = [e.name for e in self.entity_definitions if e.name != "DataTable"]
        entity_list = ", ".join(entity_types)
        
        prompt = f"""Classify this database table into ONE entity type from the list.

Table name: {short_name}
Description: {comment[:500]}
Domain: {domain}

Available entity types: {entity_list}

You MUST choose one entity type from the list. If unsure, choose the closest match.
Respond with ONLY the entity type name, nothing else."""

        entity_type = "DataTable"  # Default fallback
        confidence = 0.3
        discovery_method = "ai_fallback"
        
        try:
            escaped_prompt = prompt.replace("'", "''")
            result = self.spark.sql(f"""
                SELECT AI_QUERY('{self.AI_MODEL}', '{escaped_prompt}') as entity_type
            """).collect()[0]['entity_type']
            
            # Clean up response
            ai_response = result.strip().split('\n')[0].strip()
            
            # Validate the response is one of our entity types
            valid_types = {e.name.lower(): e.name for e in self.entity_definitions}
            
            if ai_response.lower() in valid_types:
                entity_type = valid_types[ai_response.lower()]
                confidence = 0.6
                discovery_method = "ai"
                self._stats["ai_classifications"] += 1
                logger.debug(f"AI classified {short_name} as {entity_type}")
            else:
                # Try partial match
                for valid_lower, valid_name in valid_types.items():
                    if valid_lower in ai_response.lower() or ai_response.lower() in valid_lower:
                        entity_type = valid_name
                        confidence = 0.5
                        discovery_method = "ai_partial"
                        self._stats["ai_classifications"] += 1
                        break
                else:
                    self._stats["fallback_generic"] += 1
                    logger.debug(f"AI response '{ai_response}' not recognized, using DataTable for {short_name}")
                    
        except Exception as e:
            logger.warning(f"AI classification failed for {table_name}, using fallback: {e}")
            self._stats["fallback_generic"] += 1
        
        # Find matching definition for description
        description = f"Auto-classified as {entity_type}"
        for entity_def in self.entity_definitions:
            if entity_def.name == entity_type:
                description = entity_def.description
                break
        
        return {
            "entity_id": str(uuid.uuid4()),
            "entity_name": entity_type,
            "entity_type": entity_type,
            "description": description,
            "source_tables": [table_name],
            "source_columns": [],
            "attributes": {"discovery_method": discovery_method},
            "confidence": confidence,
            "auto_discovered": True,
            "validated": False,
            "validation_notes": None
        }


class OntologyBuilder:
    """
    Builder for creating and managing the ontology layer.
    
    Creates ontology_entities and ontology_metrics tables, discovers
    entities from the knowledge base, and manages relationships.
    """
    
    # Explicit schema for entities table
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
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])
    
    def __init__(self, spark: SparkSession, config: OntologyConfig):
        self.spark = spark
        self.config = config
        self.ontology_config = OntologyLoader.load_config(config.config_path)
        self.discoverer = EntityDiscoverer(spark, config, self.ontology_config)
    
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
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        COMMENT 'Ontology entities discovered from knowledge base'
        """
        self.spark.sql(ddl)
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
    
    def discover_and_store_entities(self) -> int:
        """Discover entities and store in the table."""
        entities = self.discoverer.discover_entities_from_tables()
        
        if not entities:
            logger.warning("No entities discovered. Check that table_knowledge_base has data.")
            return 0
        
        now = datetime.now()
        
        # Build rows as tuples matching schema order
        rows = []
        for entity in entities:
            # Convert attributes dict to map type properly
            attributes = entity.get('attributes', {})
            if isinstance(attributes, dict):
                attributes = {k: str(v) for k, v in attributes.items()}
            else:
                attributes = {}
            
            row = (
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
                now,
                now
            )
            rows.append(row)
        
        df = self.spark.createDataFrame(rows, schema=self.ENTITIES_SCHEMA)
        
        # Use MERGE to avoid duplicates based on entity_name + source_tables
        df.createOrReplaceTempView("new_entities")
        
        merge_sql = f"""
        MERGE INTO {self.config.fully_qualified_entities} AS target
        USING new_entities AS source
        ON target.entity_name = source.entity_name 
           AND array_join(target.source_tables, ',') = array_join(source.source_tables, ',')

        WHEN NOT MATCHED THEN INSERT *
        """
        
        self.spark.sql(merge_sql)
        
        logger.info(f"Stored {len(entities)} entities in {self.config.fully_qualified_entities}")
        return len(entities)
    
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
        try:
            # Get entities with their source tables
            entities_df = self.spark.sql(f"""
                SELECT entity_id, entity_name, entity_type, EXPLODE(source_tables) as table_name
                FROM {self.config.fully_qualified_entities}
            """)
            
            if entities_df.count() == 0:
                return 0
            
            # Create 'instance_of' edges from tables to entities
            edges_df = entities_df.select(
                F.col("table_name").alias("src"),
                F.col("entity_id").alias("dst"),
                F.lit("instance_of").alias("relationship"),
                F.lit(1.0).alias("weight"),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at")
            )
            
            # Append to edges table
            edges_df.write.mode("append").saveAsTable(
                f"{self.config.catalog_name}.{self.config.schema_name}.graph_edges"
            )
            
            count = edges_df.count()
            logger.info(f"Added {count} instance_of edges to graph")
            return count
        except Exception as e:
            logger.warning(f"Could not add entity relationships to graph: {e}")
            return 0
    
    def run(self) -> Dict[str, Any]:
        """Execute the ontology building pipeline."""
        logger.info("Starting ontology build")
        
        self.create_entities_table()
        self.create_metrics_table()
        
        # Discover entities
        discovered = self.discover_and_store_entities()
        logger.info(f"Discovered and stored {discovered} entities")
        
        # Add relationships to graph
        edges_added = self.add_entity_relationships_to_graph()
        
        # Get summary
        try:
            summary = self.get_entity_summary()
            entity_types = summary.count()
            
            # Log summary
            logger.info(f"Ontology summary: {entity_types} entity types")
            for row in summary.collect():
                logger.info(f"  {row['entity_type']}: {row['entity_count']} entities (avg confidence: {row['avg_confidence']})")
        except Exception:
            entity_types = 0
        
        return {
            "entities_discovered": discovered,
            "entity_types": entity_types,
            "edges_added": edges_added
        }


def build_ontology(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    config_path: str = "configurations/ontology_config.yaml"
) -> Dict[str, Any]:
    """
    Convenience function to build the ontology.
    
    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for tables
        schema_name: Schema name for tables
        config_path: Path to ontology configuration YAML
        
    Returns:
        Dict with execution statistics
    """
    config = OntologyConfig(
        catalog_name=catalog_name,
        schema_name=schema_name,
        config_path=config_path
    )
    builder = OntologyBuilder(spark, config)
    return builder.run()
