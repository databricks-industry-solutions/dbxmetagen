"""
Ontology module for entity and metric management.

Discovers and manages business entities from the knowledge base,
creating an ontology layer on top of the data catalog.
"""

import logging
import yaml
import os
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
import uuid
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


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
    """Loads and parses ontology configuration."""
    
    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        """Load ontology configuration from YAML file."""
        paths_to_try = [
            config_path,
            f"../{config_path}",
            f"../../{config_path}",
            os.path.join(os.path.dirname(__file__), "..", "..", config_path)
        ]
        
        for path in paths_to_try:
            try:
                if os.path.exists(path):
                    with open(path, 'r') as f:
                        config = yaml.safe_load(f)
                    logger.info(f"Loaded ontology config from {path}")
                    return config.get('ontology', {})
            except Exception as e:
                logger.debug(f"Could not load from {path}: {e}")
        
        logger.warning("Could not load ontology config, using defaults")
        return OntologyLoader._default_config()
    
    @staticmethod
    def _default_config() -> Dict[str, Any]:
        """Return default ontology configuration."""
        return {
            "version": "1.0",
            "entities": {
                "auto_discover": True,
                "definitions": {}
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
        """Extract entity definitions from config."""
        entities = []
        definitions = config.get('entities', {}).get('definitions', {})
        
        for name, details in definitions.items():
            entities.append(EntityDefinition(
                name=name,
                description=details.get('description', ''),
                keywords=details.get('keywords', []),
                typical_attributes=details.get('typical_attributes', [])
            ))
        
        return entities


class EntityDiscoverer:
    """Discovers entities from knowledge base tables."""
    
    def __init__(self, spark: SparkSession, config: OntologyConfig, ontology_config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.ontology_config = ontology_config
        self.entity_definitions = OntologyLoader.get_entity_definitions(ontology_config)
    
    def discover_entities_from_tables(self) -> List[Dict[str, Any]]:
        """Discover entities by matching tables to entity definitions."""
        tables_df = self.spark.sql(f"""
            SELECT 
                table_name,
                table_short_name,
                comment,
                domain
            FROM {self.config.fully_qualified_kb}
        """)
        
        discovered = []
        
        for row in tables_df.collect():
            matches = self._match_table_to_entities(row)
            discovered.extend(matches)
        
        return discovered
    
    def _match_table_to_entities(self, table_row) -> List[Dict[str, Any]]:
        """Match a table to potential entity definitions."""
        matches = []
        table_name = table_row.table_name
        short_name = (table_row.table_short_name or '').lower()
        comment = (table_row.comment or '').lower()
        
        for entity_def in self.entity_definitions:
            confidence = self._calculate_match_confidence(
                short_name, comment, entity_def
            )
            
            threshold = self.ontology_config.get('entities', {}).get(
                'discovery_confidence_threshold', 0.7
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
                        "expected_attributes": entity_def.typical_attributes
                    },
                    "confidence": confidence,
                    "auto_discovered": True
                })
        
        return matches
    
    def _calculate_match_confidence(
        self, 
        table_name: str, 
        comment: str, 
        entity_def: EntityDefinition
    ) -> float:
        """Calculate confidence score for entity match."""
        score = 0.0
        max_score = 0.0
        
        # Check keywords in table name (high weight)
        for keyword in entity_def.keywords:
            max_score += 2.0
            if keyword.lower() in table_name:
                score += 2.0
            elif keyword.lower() in comment:
                score += 1.0
        
        # Check for typical attribute patterns in comment
        for attr in entity_def.typical_attributes:
            max_score += 0.5
            if attr.lower() in comment:
                score += 0.5
        
        if max_score == 0:
            return 0.0
        
        return min(1.0, score / max_score)


class OntologyBuilder:
    """
    Builder for creating and managing the ontology layer.
    
    Creates ontology_entities and ontology_metrics tables, discovers
    entities from the knowledge base, and manages relationships.
    """
    
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
            validated BOOLEAN DEFAULT FALSE,
            validation_notes STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        COMMENT 'Ontology entities discovered from knowledge base'
        """
        self.spark.sql(ddl)
        logger.info(f"Entities table {self.config.fully_qualified_entities} ready")
    
    def create_metrics_table(self) -> None:
        """Create the ontology metrics table (stub for future)."""
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
            logger.info("No entities discovered")
            return 0
        
        # Add timestamps
        for entity in entities:
            entity['created_at'] = F.current_timestamp()
            entity['updated_at'] = F.current_timestamp()
            entity['validated'] = False
            entity['validation_notes'] = None
        
        # Convert attributes dict to map type properly
        entities_for_df = []
        for e in entities:
            e_copy = e.copy()
            if isinstance(e_copy.get('attributes'), dict):
                e_copy['attributes'] = {k: str(v) for k, v in e_copy['attributes'].items()}
            entities_for_df.append(e_copy)
        
        df = self.spark.createDataFrame(entities_for_df)
        df = (
            df
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )
        
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
        
        return len(entities)
    
    def get_entity_summary(self) -> DataFrame:
        """Get summary of discovered entities."""
        return self.spark.sql(f"""
            SELECT 
                entity_type,
                COUNT(*) as entity_count,
                AVG(confidence) as avg_confidence,
                SUM(CASE WHEN validated THEN 1 ELSE 0 END) as validated_count
            FROM {self.config.fully_qualified_entities}
            GROUP BY entity_type
            ORDER BY entity_count DESC
        """)
    
    def add_entity_relationships_to_graph(self) -> int:
        """Add entity relationships to the knowledge graph edges."""
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
        
        return edges_df.count()
    
    def run(self) -> Dict[str, Any]:
        """Execute the ontology building pipeline."""
        logger.info("Starting ontology build")
        
        self.create_entities_table()
        self.create_metrics_table()
        
        # Discover entities
        discovered = self.discover_and_store_entities()
        logger.info(f"Discovered {discovered} entities")
        
        # Add relationships to graph
        edges_added = self.add_entity_relationships_to_graph()
        logger.info(f"Added {edges_added} entity relationship edges")
        
        # Get summary
        summary = self.get_entity_summary()
        entity_types = summary.count()
        
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

