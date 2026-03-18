"""
Ontology Validator module for AI-powered validation and suggestions.

Uses AI_QUERY to validate discovered entities against the knowledge base
and suggest improvements to the ontology configuration.
"""

import logging
import json
import re
import yaml
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Set
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass
class OntologyValidatorConfig:
    """Configuration for ontology validation."""
    catalog_name: str
    schema_name: str
    model: str = "databricks-gpt-oss-120b"
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
- Source Columns: {source_columns}
- Granularity: {granularity}

Table Metadata:
{table_metadata}

Column Metadata:
{column_metadata}

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
        self._stats_failed = 0
    
    def validate_entity(self, entity_row) -> Dict[str, Any]:
        """Validate a single entity using AI."""
        try:
            source_tables = entity_row.source_tables or []
            source_columns = entity_row.source_columns or []
            attributes = entity_row.attributes or {}
            granularity = attributes.get('granularity', 'table')

            table_metadata = self._get_table_metadata(source_tables)
            column_metadata = self._get_column_metadata(source_tables)

            if table_metadata is None or column_metadata is None:
                return {
                    "entity_id": entity_row.entity_id,
                    "validation_result": {
                        "validated": False,
                        "confidence": 0.0,
                        "reasoning": "Metadata unavailable -- skipped validation",
                    },
                }

            prompt = self.VALIDATION_PROMPT.format(
                entity_name=entity_row.entity_name,
                entity_type=entity_row.entity_type,
                description=entity_row.description or "",
                confidence=entity_row.confidence,
                source_tables=", ".join(source_tables),
                source_columns=", ".join(source_columns) if source_columns else "none",
                granularity=granularity,
                table_metadata=table_metadata,
                column_metadata=column_metadata,
            )

            result = self._call_ai(prompt)
            if result:
                result = self._detect_contradiction(
                    result, entity_row.entity_type
                )
                return {"entity_id": entity_row.entity_id, "validation_result": result}
        except Exception as e:
            logger.warning(f"Could not validate entity {entity_row.entity_id}: {e}")
        return None

    @staticmethod
    def _detect_contradiction(
        result: Dict[str, Any], current_type: str
    ) -> Dict[str, Any]:
        """Reject validation when the reasoning contradicts the classified type.

        If the validator suggests a different type AND its own reasoning text
        positively references that alternative, the validation is self-contradictory.
        """
        suggested = (result.get("suggested_type") or "").strip()
        reasoning = (result.get("reasoning") or "").lower()
        if (
            suggested
            and suggested.lower() != current_type.lower()
            and result.get("is_valid", False)
        ):
            alt = suggested.lower()
            contradiction_phrases = [
                f"aligns with a {alt}",
                f"aligns with {alt}",
                f"should be {alt}",
                f"better classified as {alt}",
                f"more accurately a {alt}",
                f"represents a {alt}",
                f"consistent with {alt}",
            ]
            if any(p in reasoning for p in contradiction_phrases):
                logger.info(
                    "Validator contradiction: reasoning favors %s over %s, rejecting",
                    suggested, current_type,
                )
                result = dict(result)
                result["is_valid"] = False
                adj = result.get("confidence_adjustment", 0)
                result["confidence_adjustment"] = min(adj, -0.3)
        return result
    
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
            logger.warning("Failed to fetch table metadata for %s: %s", table_names, e)
            return None
    
    def _get_column_metadata(self, table_names: List[str]) -> str:
        """Get column metadata summary for tables from column_knowledge_base."""
        if not table_names:
            return "No columns"
        table_list = ", ".join(f"'{t}'" for t in table_names[:5])
        try:
            df = self.spark.sql(f"""
                SELECT table_name, column_name, data_type, classification, comment
                FROM {self.config.fully_qualified_column_kb}
                WHERE table_name IN ({table_list})
                LIMIT 50
            """)
            rows = df.collect()
            if not rows:
                return "No column metadata found"
            summaries = []
            for r in rows:
                parts = [f"  - {r.column_name} ({r.data_type or '?'})"]
                if r.classification:
                    parts.append(f"[{r.classification}]")
                if r.comment:
                    parts.append(f": {r.comment[:100]}")
                summaries.append(" ".join(parts))
            return "\n".join(summaries)
        except Exception as e:
            logger.warning("Failed to fetch column metadata for %s: %s", table_names, e)
            return None

    def _call_ai(self, prompt: str) -> Optional[Dict[str, Any]]:
        """Call AI_QUERY and parse response, with 1 retry on parse failure."""
        escaped_prompt = prompt.replace("'", "''")
        for attempt in range(2):
            try:
                result = self.spark.sql(f"""
                    SELECT AI_QUERY(
                        '{self.config.model}',
                        '{escaped_prompt}'
                    ) as response
                """).collect()[0]['response']

                if isinstance(result, str):
                    cleaned = re.sub(r'```(?:json)?\s*', '', result).strip().rstrip('`')
                    start = cleaned.find('{')
                    end = cleaned.rfind('}') + 1
                    if start >= 0 and end > start:
                        return json.loads(cleaned[start:end])
                if attempt == 0:
                    continue
                self._stats_failed += 1
                return None
            except json.JSONDecodeError as e:
                if attempt == 0:
                    logger.info("JSON parse failed (attempt 1), retrying: %s", e)
                    continue
                logger.warning("AI call JSON parse failed after retry: %s", e)
                self._stats_failed += 1
                return None
            except Exception as e:
                logger.warning("AI call failed: %s", e)
                self._stats_failed += 1
                return None
    
    def validate_all_entities(self, granularity: str = None) -> Dict[str, Any]:
        """Validate discovered entities, optionally filtered by granularity.

        Returns dict with keys: results, total, failed.
        """
        filter_clause = "WHERE (validated = FALSE OR validated IS NULL)"
        if granularity:
            filter_clause += f" AND COALESCE(attributes['granularity'], 'table') = '{granularity}'"

        entities_df = self.spark.sql(
            f"SELECT * FROM {self.config.fully_qualified_entities} {filter_clause}"
        )

        from concurrent.futures import ThreadPoolExecutor, as_completed

        self._stats_failed = 0
        entity_rows = entities_df.collect()
        results = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(self.validate_entity, row): row
                for row in entity_rows
            }
            for future in as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)

        failed = self._stats_failed
        if failed:
            logger.warning(
                "%d of %d entities could not be validated (AI parse errors)",
                failed, len(entity_rows),
            )
        return {"results": results, "total": len(entity_rows), "failed": failed}
    
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
        """Use AI to suggest new entities from the knowledge base, including column patterns."""
        try:
            tables_df = self.spark.sql(f"""
                SELECT table_short_name, domain,
                       SUBSTRING(comment, 1, 100) as comment_preview
                FROM {self.config.fully_qualified_kb} LIMIT 50
            """)
            tables_list = []
            for row in tables_df.collect():
                s = f"- {row.table_short_name}"
                if row.domain:
                    s += f" [{row.domain}]"
                if row.comment_preview:
                    s += f": {row.comment_preview}..."
                tables_list.append(s)

            # Add frequent column name patterns
            col_patterns = ""
            try:
                freq_cols = self.spark.sql(f"""
                    SELECT column_name, COUNT(*) as cnt
                    FROM {self.config.fully_qualified_column_kb}
                    GROUP BY column_name HAVING cnt >= 3
                    ORDER BY cnt DESC LIMIT 20
                """).collect()
                if freq_cols:
                    col_patterns = "\n\nFrequent column names across tables:\n" + "\n".join(
                        f"- {r.column_name} (appears in {r.cnt} tables)" for r in freq_cols
                    )
            except Exception:
                pass

            existing_df = self.spark.sql(f"""
                SELECT DISTINCT entity_type FROM {self.config.fully_qualified_entities}
            """)
            existing_types = [row.entity_type for row in existing_df.collect()]

            prompt = self.DISCOVERY_PROMPT.format(
                tables_summary="\n".join(tables_list) + col_patterns,
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
    
    def check_consistency(self) -> List[Dict[str, Any]]:
        """Check cross-table consistency: same entity type should have consistent classification."""
        issues = []
        try:
            rows = self.spark.sql(f"""
                SELECT entity_type, source_tables, confidence, entity_name
                FROM {self.config.fully_qualified_entities}
                WHERE COALESCE(attributes['granularity'], 'table') = 'table'
            """).collect()

            type_tables: Dict[str, List] = {}
            for r in rows:
                for t in r.source_tables or []:
                    type_tables.setdefault(r.entity_type, []).append({
                        "table": t, "confidence": r.confidence
                    })

            for etype, entries in type_tables.items():
                confs = [e["confidence"] for e in entries]
                if len(confs) >= 2:
                    spread = max(confs) - min(confs)
                    if spread > 0.3:
                        issues.append({
                            "type": "consistency",
                            "entity_type": etype,
                            "message": f"Confidence spread {spread:.2f} across {len(entries)} tables",
                            "tables": [e["table"] for e in entries],
                        })
        except Exception as e:
            logger.warning("Consistency check failed: %s", e)
        return issues

    def check_coverage(self, ontology_config: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Check for entity types in config with zero matches -- potential gaps."""
        gaps = []
        try:
            discovered = self.spark.sql(f"""
                SELECT DISTINCT entity_type FROM {self.config.fully_qualified_entities}
            """).collect()
            discovered_types = {r.entity_type for r in discovered}

            if ontology_config:
                defined = set(ontology_config.get("entities", {}).get("definitions", {}).keys())
                missing = defined - discovered_types - {"DataTable"}
                for m in sorted(missing):
                    gaps.append({
                        "type": "coverage_gap",
                        "entity_type": m,
                        "message": f"Entity type '{m}' defined in config but no tables matched",
                    })
        except Exception as e:
            logger.warning("Coverage check failed: %s", e)
        return gaps

    def validate_relationships(self) -> List[Dict[str, Any]]:
        """Validate discovered inter-entity relationships make sense."""
        issues = []
        try:
            edges_table = f"{self.config.catalog_name}.{self.config.schema_name}.graph_edges"
            rels = self.spark.sql(f"""
                SELECT e.src, e.dst, e.relationship, e.weight,
                       s.entity_type AS src_type, d.entity_type AS dst_type
                FROM {edges_table} e
                JOIN {self.config.fully_qualified_entities} s ON e.src = s.entity_id
                JOIN {self.config.fully_qualified_entities} d ON e.dst = d.entity_id
                WHERE e.relationship NOT IN ('similar_embedding', 'instance_of', 'has_attribute', 'predicted_fk')
            """).collect()

            for r in rels:
                if r.src_type == r.dst_type and r.relationship != "similar_to":
                    issues.append({
                        "type": "relationship",
                        "message": f"Self-type relationship: {r.src_type} --{r.relationship}--> {r.dst_type}",
                        "src": r.src, "dst": r.dst,
                    })
        except Exception as e:
            logger.debug("Relationship validation skipped: %s", e)
        return issues

    def run(self, ontology_config: Optional[Dict] = None) -> Dict[str, Any]:
        """Execute the validation pipeline for both table and column entities."""
        logger.info("Starting ontology validation")

        # Validate table-level entities
        table_out = self.validate_all_entities(granularity="table")
        table_updated = self.update_entity_validation(table_out["results"])
        table_failed = table_out["failed"]
        logger.info(f"Validated {table_updated} table-level entities ({table_failed} parse failures)")

        # Validate column-level entities
        col_out = self.validate_all_entities(granularity="column")
        col_updated = self.update_entity_validation(col_out["results"])
        col_failed = col_out["failed"]
        logger.info(f"Validated {col_updated} column-level entities ({col_failed} parse failures)")

        total_failed = table_failed + col_failed

        # Cross-table consistency
        consistency_issues = self.check_consistency()
        if consistency_issues:
            logger.warning("Found %d consistency issues", len(consistency_issues))

        # Coverage gaps
        coverage_gaps = self.check_coverage(ontology_config)
        if coverage_gaps:
            logger.warning("Found %d coverage gaps: %s",
                           len(coverage_gaps),
                           [g["entity_type"] for g in coverage_gaps])

        # Relationship validation
        rel_issues = self.validate_relationships()
        if rel_issues:
            logger.warning("Found %d relationship issues", len(rel_issues))

        recommendations = self.generate_ontology_recommendations()
        recommendations["consistency_issues"] = consistency_issues
        recommendations["coverage_gaps"] = coverage_gaps
        recommendations["relationship_issues"] = rel_issues
        logger.info("Ontology validation complete")

        result = {
            "entities_validated": table_updated + col_updated,
            "table_entities_validated": table_updated,
            "column_entities_validated": col_updated,
            "ai_parse_failures": total_failed,
            "consistency_issues": len(consistency_issues),
            "coverage_gaps": len(coverage_gaps),
            "relationship_issues": len(rel_issues),
            "recommendations": recommendations,
        }
        if total_failed:
            result["message"] = (
                f"{total_failed} entities could not be validated due to "
                "AI response parsing errors"
            )
        return result


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

