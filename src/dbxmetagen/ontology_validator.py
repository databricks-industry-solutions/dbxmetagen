"""
Ontology Validator module for AI-powered validation and suggestions.

Uses AI_QUERY to validate discovered entities against the knowledge base
and suggest improvements to the ontology configuration.
"""

import logging
import json
import yaml
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass
class OntologyValidatorConfig:
    """Configuration for ontology validation."""
    catalog_name: str
    schema_name: str
    model: str = "databricks-meta-llama-3-3-70b-instruct"
    entities_table: str = "ontology_entities"
    kb_table: str = "table_knowledge_base"
    column_kb_table: str = "column_knowledge_base"
    
    @property
    def fully_qualified_entities(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.entities_table}"
    
    @property
    def fully_qualified_kb(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.kb_table}"
    
    @property
    def fully_qualified_column_kb(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.column_kb_table}"


class OntologyValidator:
    """
    Validator for ontology entities using AI.
    
    Validates discovered entities, suggests missing entities,
    and generates updated ontology configuration recommendations.
    """
    
    VALIDATION_PROMPT = """You are an ontology expert validating business entity definitions.

Given the following discovered entity and its source tables/columns from a data catalog,
evaluate whether this entity classification is correct and provide recommendations.

Entity Information:
- Name: {entity_name}
- Type: {entity_type}
- Description: {description}
- Confidence: {confidence}
- Source Tables: {source_tables}

Table Metadata:
{table_metadata}

Please respond with a JSON object containing:
{{
    "is_valid": true/false,
    "confidence_adjustment": float between -0.5 and 0.5,
    "reasoning": "explanation of your assessment",
    "suggested_type": "better entity type if different from current",
    "missing_relationships": ["list of related entities that should be linked"],
    "suggested_attributes": ["list of attributes this entity should have"]
}}

Be concise and focus on data-driven insights."""

    DISCOVERY_PROMPT = """You are an ontology expert analyzing a data catalog.

Given the following tables and their metadata from a knowledge base,
suggest business entities that should be defined in the ontology.

Tables Summary:
{tables_summary}

Current Entity Types in Ontology:
{existing_types}

Please respond with a JSON array of suggested new entities:
[
    {{
        "entity_name": "name",
        "entity_type": "type", 
        "description": "description",
        "keywords": ["keyword1", "keyword2"],
        "source_tables": ["table names that represent this entity"],
        "typical_attributes": ["attr1", "attr2"],
        "confidence": 0.0-1.0
    }}
]

Focus on entities NOT already covered by existing types. Be conservative and suggest only clear patterns."""

    def __init__(self, spark: SparkSession, config: OntologyValidatorConfig):
        self.spark = spark
        self.config = config
    
    def validate_entity(self, entity_row) -> Dict[str, Any]:
        """Validate a single entity using AI."""
        try:
            # Get table metadata for the entity's source tables
            source_tables = entity_row.source_tables or []
            table_metadata = self._get_table_metadata(source_tables)
            
            # Build prompt
            prompt = self.VALIDATION_PROMPT.format(
                entity_name=entity_row.entity_name,
                entity_type=entity_row.entity_type,
                description=entity_row.description or "",
                confidence=entity_row.confidence,
                source_tables=", ".join(source_tables),
                table_metadata=table_metadata
            )
            
            # Call AI
            result = self._call_ai(prompt)
            
            if result:
                return {
                    "entity_id": entity_row.entity_id,
                    "validation_result": result
                }
        except Exception as e:
            logger.warning(f"Could not validate entity {entity_row.entity_id}: {e}")
        
        return None
    
    def _get_table_metadata(self, table_names: List[str]) -> str:
        """Get metadata summary for tables."""
        if not table_names:
            return "No tables"
        
        table_list = ", ".join(f"'{t}'" for t in table_names[:5])
        
        try:
            metadata_df = self.spark.sql(f"""
                SELECT 
                    table_name,
                    comment,
                    domain
                FROM {self.config.fully_qualified_kb}
                WHERE table_name IN ({table_list})
            """)
            
            rows = metadata_df.collect()
            summaries = []
            for row in rows:
                summary = f"- {row.table_name}"
                if row.comment:
                    summary += f": {row.comment[:200]}"
                if row.domain:
                    summary += f" [Domain: {row.domain}]"
                summaries.append(summary)
            
            return "\n".join(summaries) if summaries else "No metadata found"
        except Exception as e:
            return f"Error fetching metadata: {e}"
    
    def _call_ai(self, prompt: str) -> Optional[Dict[str, Any]]:
        """Call AI_QUERY and parse response."""
        try:
            escaped_prompt = prompt.replace("'", "''")
            
            result = self.spark.sql(f"""
                SELECT AI_QUERY(
                    '{self.config.model}',
                    '{escaped_prompt}'
                ) as response
            """).collect()[0]['response']
            
            # Try to parse JSON from response
            if isinstance(result, str):
                # Find JSON in response
                start = result.find('{')
                end = result.rfind('}') + 1
                if start >= 0 and end > start:
                    return json.loads(result[start:end])
            
            return None
        except Exception as e:
            logger.warning(f"AI call failed: {e}")
            return None
    
    def validate_all_entities(self) -> List[Dict[str, Any]]:
        """Validate all discovered entities."""
        entities_df = self.spark.sql(f"""
            SELECT * FROM {self.config.fully_qualified_entities}
            WHERE validated = FALSE OR validated IS NULL
        """)
        
        results = []
        for entity_row in entities_df.collect():
            result = self.validate_entity(entity_row)
            if result:
                results.append(result)
        
        return results
    
    def update_entity_validation(self, validation_results: List[Dict[str, Any]]) -> int:
        """Update entities with validation results."""
        updated = 0
        
        for result in validation_results:
            entity_id = result['entity_id']
            validation = result.get('validation_result', {})
            
            is_valid = validation.get('is_valid', True)
            confidence_adj = validation.get('confidence_adjustment', 0)
            reasoning = validation.get('reasoning', '')
            
            try:
                self.spark.sql(f"""
                    UPDATE {self.config.fully_qualified_entities}
                    SET 
                        validated = {is_valid},
                        confidence = confidence + {confidence_adj},
                        validation_notes = '{reasoning.replace("'", "''")}',
                        updated_at = current_timestamp()
                    WHERE entity_id = '{entity_id}'
                """)
                updated += 1
            except Exception as e:
                logger.warning(f"Could not update entity {entity_id}: {e}")
        
        return updated
    
    def suggest_new_entities(self) -> List[Dict[str, Any]]:
        """Use AI to suggest new entities from the knowledge base."""
        try:
            # Get tables summary
            tables_df = self.spark.sql(f"""
                SELECT 
                    table_short_name,
                    domain,
                    SUBSTRING(comment, 1, 100) as comment_preview
                FROM {self.config.fully_qualified_kb}
                LIMIT 50
            """)
            
            tables_list = []
            for row in tables_df.collect():
                summary = f"- {row.table_short_name}"
                if row.domain:
                    summary += f" [{row.domain}]"
                if row.comment_preview:
                    summary += f": {row.comment_preview}..."
                tables_list.append(summary)
            
            tables_summary = "\n".join(tables_list)
            
            # Get existing entity types
            existing_df = self.spark.sql(f"""
                SELECT DISTINCT entity_type 
                FROM {self.config.fully_qualified_entities}
            """)
            existing_types = [row.entity_type for row in existing_df.collect()]
            
            # Build prompt
            prompt = self.DISCOVERY_PROMPT.format(
                tables_summary=tables_summary,
                existing_types=", ".join(existing_types) if existing_types else "None"
            )
            
            result = self._call_ai(prompt)
            
            if result and isinstance(result, list):
                return result
            
            return []
        except Exception as e:
            logger.warning(f"Entity suggestion failed: {e}")
            return []
    
    def generate_ontology_recommendations(self) -> Dict[str, Any]:
        """Generate recommendations for ontology configuration updates."""
        recommendations = {
            "suggested_entities": [],
            "validation_summary": {},
            "config_updates": {}
        }
        
        # Get validation summary
        summary_df = self.spark.sql(f"""
            SELECT 
                entity_type,
                COUNT(*) as total,
                SUM(CASE WHEN validated THEN 1 ELSE 0 END) as validated,
                AVG(confidence) as avg_confidence
            FROM {self.config.fully_qualified_entities}
            GROUP BY entity_type
        """)
        
        recommendations["validation_summary"] = {
            row.entity_type: {
                "total": row.total,
                "validated": row.validated,
                "avg_confidence": row.avg_confidence
            }
            for row in summary_df.collect()
        }
        
        # Get suggested new entities
        recommendations["suggested_entities"] = self.suggest_new_entities()
        
        # Generate config updates
        if recommendations["suggested_entities"]:
            recommendations["config_updates"]["new_entity_definitions"] = {
                e["entity_name"]: {
                    "description": e.get("description", ""),
                    "keywords": e.get("keywords", []),
                    "typical_attributes": e.get("typical_attributes", [])
                }
                for e in recommendations["suggested_entities"]
            }
        
        return recommendations
    
    def run(self) -> Dict[str, Any]:
        """Execute the validation pipeline."""
        logger.info("Starting ontology validation")
        
        # Validate existing entities
        validation_results = self.validate_all_entities()
        updated = self.update_entity_validation(validation_results)
        logger.info(f"Validated and updated {updated} entities")
        
        # Generate recommendations
        recommendations = self.generate_ontology_recommendations()
        
        logger.info("Ontology validation complete")
        
        return {
            "entities_validated": updated,
            "recommendations": recommendations
        }


def validate_ontology(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str
) -> Dict[str, Any]:
    """
    Convenience function to validate the ontology.
    
    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for tables
        schema_name: Schema name for tables
        
    Returns:
        Dict with validation results and recommendations
    """
    config = OntologyValidatorConfig(
        catalog_name=catalog_name,
        schema_name=schema_name
    )
    validator = OntologyValidator(spark, config)
    return validator.run()


def export_ontology_yaml(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    output_path: str = "configurations/ontology_config_updated.yaml"
) -> str:
    """
    Export current ontology state to YAML configuration.
    
    Args:
        spark: SparkSession instance
        catalog_name: Catalog name
        schema_name: Schema name
        output_path: Path for output YAML
        
    Returns:
        Path to exported file
    """
    # Get entities
    entities_df = spark.sql(f"""
        SELECT entity_type, entity_name, description, confidence
        FROM {catalog_name}.{schema_name}.ontology_entities
        WHERE validated = TRUE OR confidence >= 0.7
    """)
    
    # Build config structure
    config = {
        "ontology": {
            "version": "1.1",
            "entities": {
                "auto_discover": True,
                "definitions": {}
            }
        }
    }
    
    for row in entities_df.collect():
        if row.entity_type not in config["ontology"]["entities"]["definitions"]:
            config["ontology"]["entities"]["definitions"][row.entity_type] = {
                "description": row.description or f"Auto-discovered {row.entity_type} entity",
                "keywords": [],
                "typical_attributes": []
            }
    
    # Write to file
    with open(output_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    logger.info(f"Exported ontology to {output_path}")
    return output_path

