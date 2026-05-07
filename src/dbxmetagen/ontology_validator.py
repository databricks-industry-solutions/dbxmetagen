"""
Ontology Validator module for AI-powered validation and suggestions.

Uses AI_QUERY to validate discovered entities against the knowledge base
and suggest improvements to the ontology configuration.
"""

import logging
import json
import re
import yaml
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Set, Tuple
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)

MAX_ENTITIES_PER_PROMPT = 20
RETRY_ENTITIES_PER_PROMPT = 10
MAX_REPARTITION = 16
CIRCUIT_BREAKER_THRESHOLD = 0.5


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

    @property
    def fully_qualified_edges(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.graph_edges"


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

    COLUMN_BATCH_VALIDATION_PROMPT = """Validate the following column-level entity classifications from a data catalog.
For EACH entity, evaluate whether the entity type is correct given its matched columns.

{entity_sections}

Return a JSON array with one entry per entity:
[{{"entity_id": "...", "is_valid": true/false, "confidence_adjustment": -0.5 to 0.5, "reasoning": "brief explanation", "suggested_type": "better type or empty string"}}]

Evaluate each entity independently. Be skeptical of low-confidence AI-discovered matches."""

    TABLE_BATCH_VALIDATION_PROMPT = """Validate the following table-level entity classifications from a data catalog.
For EACH entity, evaluate whether the entity type is correct given the table metadata and columns.

{entity_sections}

Return a JSON array with one entry per entity:
[{{"entity_id": "...", "is_valid": true/false, "confidence_adjustment": -0.5 to 0.5, "reasoning": "brief explanation", "suggested_type": "better type or empty string"}}]

Evaluate each entity independently. Consider whether table purpose, columns, and domain align with the assigned entity type."""

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
    
    @staticmethod
    def _parse_ai_array(
        response_text: str, expected_entity_ids: List[str]
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Parse a JSON array from AI response with truncation recovery.

        Returns (parsed_items, missing_entity_ids).
        """
        if not response_text:
            return [], list(expected_entity_ids)

        cleaned = re.sub(r'```(?:json)?\s*', '', response_text).strip().rstrip('`')
        start = cleaned.find('[')
        end = cleaned.rfind(']')
        if start < 0:
            return [], list(expected_entity_ids)

        if end > start:
            raw = cleaned[start:end + 1]
        else:
            raw = cleaned[start:]

        try:
            items = json.loads(raw)
            if isinstance(items, list):
                found_ids = {
                    item.get("entity_id") for item in items if isinstance(item, dict)
                }
                missing = [eid for eid in expected_entity_ids if eid not in found_ids]
                return [i for i in items if isinstance(i, dict)], missing
        except json.JSONDecodeError:
            pass

        # Truncation recovery: find last complete object
        last_brace = raw.rfind('}')
        if last_brace > 0:
            try:
                recovered = raw[:last_brace + 1] + ']'
                items = json.loads(recovered)
                if isinstance(items, list):
                    found_ids = {
                        item.get("entity_id") for item in items if isinstance(item, dict)
                    }
                    missing = [eid for eid in expected_entity_ids if eid not in found_ids]
                    valid = [i for i in items if isinstance(i, dict)]
                    logger.info(
                        "Recovered %d/%d entities from truncated response",
                        len(valid), len(expected_entity_ids),
                    )
                    return valid, missing
            except json.JSONDecodeError:
                pass

        return [], list(expected_entity_ids)

    @staticmethod
    def _build_entity_section(entity: Dict[str, Any]) -> str:
        """Build the prompt section for a single entity."""
        cols_lines = []
        bindings = entity.get("column_bindings") or []
        source_cols = entity.get("source_columns") or []
        if bindings:
            for b in bindings:
                attr = b.get("attribute_name", "?")
                col = b.get("bound_column", "?")
                cols_lines.append(f"    - {col} -> {entity['entity_type']}.{attr}")
        elif source_cols:
            for c in source_cols:
                cols_lines.append(f"    - {c}")

        method = entity.get("discovery_method", "unknown")
        lines = [
            f"Entity (id: {entity['entity_id']}):",
            f"  Type: {entity['entity_type']} | Confidence: {entity.get('confidence', '?')} | Method: {method}",
            "  Matched columns:",
        ]
        lines.extend(cols_lines or ["    (none)"])
        return "\n".join(lines)

    @staticmethod
    def _build_table_entity_section(entity: Dict[str, Any]) -> str:
        """Build the prompt section for a single table-level entity."""
        lines = [
            f"Entity (id: {entity['entity_id']}):",
            f"  Name: {entity.get('entity_name', '?')}",
            f"  Type: {entity['entity_type']} | Confidence: {entity.get('confidence', '?')}",
        ]
        desc = entity.get("description", "")
        if desc:
            lines.append(f"  Description: {desc[:200]}")
        tables = entity.get("source_tables") or []
        if tables:
            lines.append(f"  Source Tables: {', '.join(tables[:5])}")
        table_meta = entity.get("table_metadata", "")
        if table_meta:
            lines.append(f"  Table Metadata:\n{table_meta}")
        col_meta = entity.get("column_metadata", "")
        if col_meta:
            lines.append(f"  Columns:\n{col_meta}")
        return "\n".join(lines)

    def _build_table_batch_prompt(self, entities: List[Dict[str, Any]]) -> str:
        """Build a multi-entity batch prompt for table-level validation."""
        sections = [self._build_table_entity_section(e) for e in entities]
        return self.TABLE_BATCH_VALIDATION_PROMPT.format(
            entity_sections="\n---\n".join(sections)
        )

    def _build_batch_prompt(
        self, entities: List[Dict[str, Any]]
    ) -> str:
        """Build a multi-entity, multi-table batch prompt."""
        by_table: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for e in entities:
            by_table[e.get("table_name", "unknown")].append(e)

        sections = []
        for table_name, ents in by_table.items():
            table_comment = ents[0].get("table_comment", "") or ""
            domain = ents[0].get("domain", "") or ""
            header = f"---\nTable: {table_name}"
            if table_comment:
                header += f"\nDescription: {table_comment[:200]}"
            if domain:
                header += f"\nDomain: {domain}"
            header += "\n"
            entity_blocks = [self._build_entity_section(e) for e in ents]
            sections.append(header + "\n".join(entity_blocks))

        return self.COLUMN_BATCH_VALIDATION_PROMPT.format(
            entity_sections="\n".join(sections)
        )

    def _run_ai_query_batch(
        self, prompt_rows: List[Dict[str, Any]]
    ) -> Tuple[List[Row], List[Row]]:
        """Run AI_QUERY on a multi-row DataFrame. Returns (successes, failures).

        Raises RuntimeError if the circuit breaker trips (>50% failure rate).
        """
        if not prompt_rows:
            return [], []

        schema = StructType([
            StructField("chunk_id", StringType(), False),
            StructField("entity_ids", ArrayType(StringType()), False),
            StructField("prompt_text", StringType(), False),
        ])
        df = self.spark.createDataFrame(
            [(r["chunk_id"], r["entity_ids"], r["prompt_text"]) for r in prompt_rows],
            schema=schema,
        )
        n_parts = min(len(prompt_rows), MAX_REPARTITION)
        df = df.repartition(max(n_parts, 1))
        df.createOrReplaceTempView("__col_val_prompts")

        results_df = self.spark.sql(f"""
            SELECT chunk_id, entity_ids,
                   AI_QUERY('{self.config.model}', prompt_text) AS response
            FROM __col_val_prompts
        """)
        rows = results_df.collect()

        successes = [r for r in rows if r.response is not None]
        failures = [r for r in rows if r.response is None]

        total = len(rows)
        n_failed = len(failures)
        if total > 0 and n_failed == total:
            raise RuntimeError(
                f"Column validation failed: all {total} AI_QUERY calls returned errors. "
                f"Check that model '{self.config.model}' is accessible and the serving endpoint is healthy."
            )
        if total > 0 and n_failed / total > CIRCUIT_BREAKER_THRESHOLD:
            raise RuntimeError(
                f"Column validation failed: {n_failed}/{total} AI_QUERY calls errored. "
                f"This suggests a model serving issue, not individual prompt problems."
            )
        return successes, failures

    def validate_table_entities_batched(self) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Validate table-level entities in batches using Spark-native AI_QUERY.

        Returns (validation_results, unvalidated_entity_ids).
        """
        ent_table = self.config.fully_qualified_entities
        kb_table = self.config.fully_qualified_kb
        col_kb_table = self.config.fully_qualified_column_kb

        entities_df = self.spark.sql(f"""
            SELECT e.entity_id, e.entity_name, e.entity_type, e.description,
                   e.source_tables, e.source_columns, e.confidence,
                   kb.comment AS table_comment, kb.domain
            FROM {ent_table} e
            LEFT JOIN {kb_table} kb ON e.source_tables[0] = kb.table_name
            WHERE COALESCE(e.attributes['granularity'], 'table') = 'table'
              AND (e.validated = FALSE OR e.validated IS NULL)
        """)
        entity_rows = entities_df.collect()

        if not entity_rows:
            logger.info("No unvalidated table entities found")
            return [], []

        logger.info("Batched table validation: %d entities to validate", len(entity_rows))

        all_table_names = set()
        for r in entity_rows:
            for t in (r.source_tables or []):
                all_table_names.add(t)

        # Fetch column metadata once for all relevant tables
        col_meta_map: Dict[str, str] = {}
        if all_table_names:
            table_list = ", ".join(f"'{t}'" for t in list(all_table_names)[:200])
            try:
                col_rows = self.spark.sql(f"""
                    SELECT table_name, column_name, data_type, classification, comment
                    FROM {col_kb_table}
                    WHERE table_name IN ({table_list})
                """).collect()
                by_table: Dict[str, List[str]] = defaultdict(list)
                for cr in col_rows:
                    parts = [f"  - {cr.column_name} ({cr.data_type or '?'})"]
                    if cr.classification:
                        parts.append(f"[{cr.classification}]")
                    if cr.comment:
                        parts.append(f": {cr.comment[:80]}")
                    by_table[cr.table_name].append(" ".join(parts))
                for tbl, lines in by_table.items():
                    col_meta_map[tbl] = "\n".join(lines[:15])
            except Exception as e:
                logger.warning("Could not fetch column metadata for table batch: %s", e)

        entities = []
        for r in entity_rows:
            src_tables = list(r.source_tables or [])
            primary_table = src_tables[0] if src_tables else ""
            table_meta_parts = []
            if r.table_comment:
                table_meta_parts.append(f"  Comment: {r.table_comment[:200]}")
            if r.domain:
                table_meta_parts.append(f"  Domain: {r.domain}")

            entities.append({
                "entity_id": r.entity_id,
                "entity_name": r.entity_name,
                "entity_type": r.entity_type,
                "description": r.description or "",
                "source_tables": src_tables,
                "confidence": r.confidence,
                "table_metadata": "\n".join(table_meta_parts),
                "column_metadata": col_meta_map.get(primary_table, ""),
            })

        def _chunk_and_run(ents, chunk_size):
            chunks = [ents[i:i + chunk_size] for i in range(0, len(ents), chunk_size)]
            prompt_rows = []
            for idx, chunk in enumerate(chunks):
                prompt_text = self._build_table_batch_prompt(chunk)
                prompt_rows.append({
                    "chunk_id": str(idx),
                    "entity_ids": [e["entity_id"] for e in chunk],
                    "prompt_text": prompt_text,
                })
            return self._run_ai_query_batch(prompt_rows)

        entity_type_map = {e["entity_id"]: e["entity_type"] for e in entities}

        def _process_successes(success_rows):
            results = []
            missing_ids = []
            chunk_missing = {}
            for row in success_rows:
                parsed, missing = self._parse_ai_array(row.response, list(row.entity_ids))
                for item in parsed:
                    eid = item.get("entity_id", "")
                    checked = self._detect_contradiction(item, entity_type_map.get(eid, ""))
                    results.append({
                        "entity_id": eid,
                        "validation_result": {
                            "is_valid": checked.get("is_valid", True),
                            "confidence_adjustment": checked.get("confidence_adjustment", 0),
                            "reasoning": checked.get("reasoning", ""),
                        },
                    })
                if missing:
                    chunk_missing[row.chunk_id] = missing
                missing_ids.extend(missing)
            return results, missing_ids, chunk_missing

        successes, failures = _chunk_and_run(entities, MAX_ENTITIES_PER_PROMPT)
        all_results, unvalidated_ids, chunk_missing = _process_successes(successes)

        for row in failures:
            unvalidated_ids.extend(row.entity_ids)

        if unvalidated_ids:
            skip_ids = set()
            for chunk_id, missing in chunk_missing.items():
                if len(missing) <= 2:
                    skip_ids.update(missing)
            if skip_ids:
                logger.info(
                    "Skipping retry for %d table entities from chunks with <= 2 losses",
                    len(skip_ids),
                )
            retry_ids = set(unvalidated_ids) - skip_ids
            retry_entities = [e for e in entities if e["entity_id"] in retry_ids]
            if retry_entities:
                logger.info(
                    "Retrying %d table entities at chunk size %d",
                    len(retry_entities), RETRY_ENTITIES_PER_PROMPT,
                )
                unvalidated_ids = list(skip_ids)
                try:
                    r_successes, r_failures = _chunk_and_run(retry_entities, RETRY_ENTITIES_PER_PROMPT)
                    r_results, r_missing, _ = _process_successes(r_successes)
                    all_results.extend(r_results)
                    unvalidated_ids.extend(r_missing)
                    for row in r_failures:
                        unvalidated_ids.extend(row.entity_ids)
                except RuntimeError as e:
                    logger.warning("Table retry pass also failed: %s", e)
                    unvalidated_ids.extend(e_obj["entity_id"] for e_obj in retry_entities)
            else:
                unvalidated_ids = list(skip_ids)

        if unvalidated_ids:
            logger.warning(
                "%d table entities could not be validated: %s",
                len(unvalidated_ids), unvalidated_ids[:10],
            )

        logger.info(
            "Batched table validation complete: %d validated, %d unresolved",
            len(all_results), len(unvalidated_ids),
        )
        return all_results, unvalidated_ids

    def validate_column_entities_batched(self) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Validate column entities in batches using Spark-native AI_QUERY.

        Returns (validation_results, unvalidated_entity_ids).
        """
        ent_table = self.config.fully_qualified_entities
        kb_table = self.config.fully_qualified_kb

        has_bindings = True
        try:
            entities_df = self.spark.sql(f"""
                SELECT e.entity_id, e.entity_type, e.source_tables[0] AS table_name,
                       e.source_columns, e.column_bindings, e.confidence,
                       COALESCE(e.attributes['discovery_method'], 'unknown') AS discovery_method,
                       kb.comment AS table_comment, kb.domain
                FROM {ent_table} e
                LEFT JOIN {kb_table} kb ON e.source_tables[0] = kb.table_name
                WHERE e.attributes['granularity'] = 'column'
                  AND (e.validated = FALSE OR e.validated IS NULL)
            """)
            entity_rows = entities_df.collect()
        except AnalysisException:
            logger.warning("column_bindings not found in %s, falling back without it", ent_table)
            has_bindings = False
            entities_df = self.spark.sql(f"""
                SELECT e.entity_id, e.entity_type, e.source_tables[0] AS table_name,
                       e.source_columns, e.confidence,
                       COALESCE(e.attributes['discovery_method'], 'unknown') AS discovery_method,
                       kb.comment AS table_comment, kb.domain
                FROM {ent_table} e
                LEFT JOIN {kb_table} kb ON e.source_tables[0] = kb.table_name
                WHERE e.attributes['granularity'] = 'column'
                  AND (e.validated = FALSE OR e.validated IS NULL)
            """)
            entity_rows = entities_df.collect()

        if not entity_rows:
            logger.info("No unvalidated column entities found")
            return [], []

        logger.info("Batched column validation: %d entities to validate", len(entity_rows))

        entities = []
        for r in entity_rows:
            bindings = []
            if has_bindings and getattr(r, "column_bindings", None):
                for b in r.column_bindings:
                    bindings.append({
                        "attribute_name": b.attribute_name if hasattr(b, 'attribute_name') else b.get("attribute_name", "?"),
                        "bound_table": b.bound_table if hasattr(b, 'bound_table') else b.get("bound_table", ""),
                        "bound_column": b.bound_column if hasattr(b, 'bound_column') else b.get("bound_column", ""),
                    })
            entities.append({
                "entity_id": r.entity_id,
                "entity_type": r.entity_type,
                "table_name": r.table_name,
                "source_columns": list(r.source_columns or []),
                "column_bindings": bindings,
                "confidence": r.confidence,
                "discovery_method": r.discovery_method,
                "table_comment": r.table_comment,
                "domain": r.domain,
            })

        def _chunk_and_run(ents, chunk_size):
            chunks = [ents[i:i + chunk_size] for i in range(0, len(ents), chunk_size)]
            prompt_rows = []
            for idx, chunk in enumerate(chunks):
                prompt_text = self._build_batch_prompt(chunk)
                prompt_rows.append({
                    "chunk_id": str(idx),
                    "entity_ids": [e["entity_id"] for e in chunk],
                    "prompt_text": prompt_text,
                })
            return self._run_ai_query_batch(prompt_rows)

        entity_type_map = {e["entity_id"]: e["entity_type"] for e in entities}

        def _process_successes(success_rows):
            results = []
            missing_ids = []
            chunk_missing = {}
            for row in success_rows:
                parsed, missing = self._parse_ai_array(row.response, list(row.entity_ids))
                for item in parsed:
                    eid = item.get("entity_id", "")
                    checked = self._detect_contradiction(item, entity_type_map.get(eid, ""))
                    results.append({
                        "entity_id": eid,
                        "validation_result": {
                            "is_valid": checked.get("is_valid", True),
                            "confidence_adjustment": checked.get("confidence_adjustment", 0),
                            "reasoning": checked.get("reasoning", ""),
                        },
                    })
                if missing:
                    chunk_missing[row.chunk_id] = missing
                missing_ids.extend(missing)
            return results, missing_ids, chunk_missing

        successes, failures = _chunk_and_run(entities, MAX_ENTITIES_PER_PROMPT)

        all_results, unvalidated_ids, chunk_missing = _process_successes(successes)

        for row in failures:
            unvalidated_ids.extend(row.entity_ids)

        # Retry pass -- skip entities from chunks where only 1-2 were lost (truncation tail)
        if unvalidated_ids:
            skip_ids = set()
            for chunk_id, missing in chunk_missing.items():
                if len(missing) <= 2:
                    skip_ids.update(missing)
            if skip_ids:
                logger.info(
                    "Skipping retry for %d entities from chunks with <= 2 losses",
                    len(skip_ids),
                )

            retry_ids = set(unvalidated_ids) - skip_ids
            retry_entities = [e for e in entities if e["entity_id"] in retry_ids]
            if retry_entities:
                logger.info(
                    "Retrying %d entities at chunk size %d",
                    len(retry_entities), RETRY_ENTITIES_PER_PROMPT,
                )
                unvalidated_ids = list(skip_ids)
                try:
                    r_successes, r_failures = _chunk_and_run(retry_entities, RETRY_ENTITIES_PER_PROMPT)
                    r_results, r_missing, _ = _process_successes(r_successes)
                    all_results.extend(r_results)
                    unvalidated_ids.extend(r_missing)
                    for row in r_failures:
                        unvalidated_ids.extend(row.entity_ids)
                except RuntimeError as e:
                    logger.warning("Retry pass also failed: %s", e)
                    unvalidated_ids.extend(e_obj["entity_id"] for e_obj in retry_entities)
            else:
                unvalidated_ids = list(skip_ids)

        if unvalidated_ids:
            logger.warning(
                "%d column entities could not be validated: %s",
                len(unvalidated_ids),
                unvalidated_ids[:10],
            )

        logger.info(
            "Batched column validation complete: %d validated, %d unresolved",
            len(all_results), len(unvalidated_ids),
        )
        return all_results, unvalidated_ids

    def batch_update_entity_validation(self, validation_results: List[Dict[str, Any]]) -> int:
        """Update entities with validation results using a single MERGE."""
        if not validation_results:
            return 0

        rows = []
        for r in validation_results:
            v = r.get("validation_result", {})
            rows.append((
                r["entity_id"],
                bool(v.get("is_valid", True)),
                float(v.get("confidence_adjustment", 0)),
                str(v.get("reasoning", ""))[:500],
            ))

        schema = StructType([
            StructField("entity_id", StringType(), False),
            StructField("validated", StringType(), False),
            StructField("confidence_adjustment", StringType(), False),
            StructField("validation_notes", StringType(), True),
        ])
        results_df = self.spark.createDataFrame(
            [(r[0], str(r[1]).lower(), str(r[2]), r[3]) for r in rows],
            schema=schema,
        )
        results_df.createOrReplaceTempView("__col_val_results")

        try:
            # MERGE: Batch-update existing entities with validation results from temp view
            #   `__col_val_results` into `ontology_entities`, matching on `entity_id`.
            #   MATCHED-only (no INSERT) -- sets `validated`, adds `confidence_adjustment`
            #   to `confidence` (floored at 0.0), replaces `validation_notes`, and
            #   refreshes `updated_at`. Entities not in the staging view are left untouched.
            # WHY: Persist batch LLM/heuristic validation outcomes in one SQL statement so
            #   the ontology graph and dashboards reflect which entities passed review
            #   without N separate round-trips per entity. Update-only prevents phantom
            #   entities from being inserted by the validation pipeline.
            # TRADEOFFS: Fast and idiomatic for Delta; depends on MERGE support and the
            #   temp view. On failure the code falls back to per-row UPDATE (slower but
            #   more granular).
            self.spark.sql(f"""
                MERGE INTO {self.config.fully_qualified_entities} AS target
                USING __col_val_results AS source
                ON target.entity_id = source.entity_id
                WHEN MATCHED THEN UPDATE SET
                    target.validated = CAST(source.validated AS BOOLEAN),
                    target.confidence = GREATEST(0.0, target.confidence + CAST(source.confidence_adjustment AS DOUBLE)),
                    target.validation_notes = source.validation_notes,
                    target.updated_at = current_timestamp()
            """)
            logger.info("MERGE updated %d column entity validation results", len(rows))
            return len(rows)
        except Exception as e:
            logger.warning("MERGE failed (%s), falling back to per-entity UPDATE", e)
            return self.update_entity_validation(validation_results)

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
                # UPDATE: Single-row update on `ontology_entities` for `entity_id`, setting
                #   `validated`, incrementing `confidence` by `confidence_adj` (floored at 0.0),
                #   `validation_notes`, and `updated_at`.
                # WHY: Fallback path when batched MERGE fails (permissions, analyzer limits, etc.)
                #   so validation results still land without losing the whole batch.
                # TRADEOFFS: One statement per entity — higher latency and warehouse cost than a
                #   single MERGE; reasoning is inlined with SQL escaping which is brittle for edge
                #   characters but avoids parameterized-SQL gaps in this call site. Alternative:
                #   retry MERGE only — simpler but all-or-nothing.
                self.spark.sql(f"""
                    UPDATE {self.config.fully_qualified_entities}
                    SET 
                        validated = {is_valid},
                        confidence = GREATEST(0.0, confidence + {confidence_adj}),
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
            rels = self.spark.sql(f"""
                SELECT e.src, e.dst, e.relationship, e.weight,
                       s.entity_type AS src_type, d.entity_type AS dst_type
                FROM {self.config.fully_qualified_edges} e
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

    def prune_invalid_edges(self) -> int:
        """Delete edges referencing entities that were invalidated (validated=FALSE)."""
        edges = self.config.fully_qualified_edges
        ent = self.config.fully_qualified_entities
        try:
            before = self.spark.sql(f"SELECT COUNT(*) AS cnt FROM {edges}").collect()[0].cnt
            # DELETE: Remove rows from `graph_edges` (`fully_qualified_edges`) whose `src` or `dst`
            #   `entity_id` references an entity in `ontology_entities` with `validated = FALSE`.
            # WHY: Keep the knowledge graph consistent — invalidated entities should not retain
            #   structural or semantic edges that imply they are trusted endpoints.
            # TRADEOFFS: Hard delete loses historical edge provenance; subquery filters may scan
            #   large edge tables. Alternative: soft-delete columns or use `merge_edges`-style
            #   keyed merges per `source_system` — heavier but uniform with other graph writers.
            self.spark.sql(f"""
                DELETE FROM {edges}
                WHERE src IN (SELECT entity_id FROM {ent} WHERE validated = FALSE)
                   OR dst IN (SELECT entity_id FROM {ent} WHERE validated = FALSE)
            """)
            after = self.spark.sql(f"SELECT COUNT(*) AS cnt FROM {edges}").collect()[0].cnt
            pruned = before - after
            if pruned > 0:
                logger.info("Pruned %d edges referencing invalidated entities", pruned)
            return pruned
        except Exception as e:
            logger.debug("Edge pruning skipped: %s", e)
            return 0

    def run(
        self,
        ontology_config: Optional[Dict] = None,
        validate_columns: bool = False,
        force_revalidate: bool = False,
    ) -> Dict[str, Any]:
        """Execute the validation pipeline.

        Args:
            ontology_config: Optional ontology config dict for coverage checks.
            validate_columns: If True, validate column-level entities using batched
                AI_QUERY. Default False (opt-in) since column validation can be slow
                at scale.
            force_revalidate: If True, reset all previously-validated auto-discovered
                entities so they are re-validated from scratch.
        """
        logger.info("Starting ontology validation")

        if force_revalidate:
            # UPDATE: Bulk-reset `validated` to FALSE and clear `validation_notes` on
            #   `ontology_entities` rows that are `auto_discovered = TRUE` and currently
            #   `validated = TRUE` (leaves manually curated rows and already-unvalidated rows alone).
            # WHY: Lets operators rerun validation from a clean slate after ontology or model
            #   changes without re-ingesting entities from scratch.
            # TRADEOFFS: Does not reset `confidence` or other fields — faster and preserves scores
            #   while forcing re-validation. Alternative: truncate auto-discovered partition or
            #   delete rows — stricter cleanup but loses entity records and downstream keys.
            reset_df = self.spark.sql(f"""
                UPDATE {self.config.fully_qualified_entities}
                SET validated = FALSE, validation_notes = NULL
                WHERE auto_discovered = TRUE AND validated = TRUE
            """)
            try:
                reset_count = reset_df.collect()[0][0]
            except Exception:
                reset_count = 0
            logger.info("Force revalidate: reset %s previously-validated entities", reset_count)

        # Validate table-level entities via batched AI_QUERY
        table_updated = 0
        table_failed = 0
        try:
            table_results, table_unvalidated = self.validate_table_entities_batched()
            table_updated = self.batch_update_entity_validation(table_results)
            table_failed = len(table_unvalidated)
        except RuntimeError as e:
            logger.error("Table validation aborted: %s", e)
            table_failed = -1
        logger.info("Validated %d table-level entities (%d unresolved)", table_updated, max(table_failed, 0))

        col_updated = 0
        col_failed = 0

        if validate_columns:
            try:
                col_results, col_unvalidated = self.validate_column_entities_batched()
                col_updated = self.batch_update_entity_validation(col_results)
                col_failed = len(col_unvalidated)
            except RuntimeError as e:
                logger.error("Column validation aborted: %s", e)
                col_failed = -1
        else:
            logger.info(
                "Column-level validation skipped (opt-in via validate_columns=true). "
                "Best for smaller catalogs; can be slow at scale."
            )

        total_failed = max(table_failed, 0) + max(col_failed, 0)

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

        # Prune edges referencing invalidated entities
        pruned_edges = self.prune_invalid_edges()

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
            "pruned_edges": pruned_edges,
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
    schema_name: str,
    validate_columns: bool = False,
    force_revalidate: bool = False,
) -> Dict[str, Any]:
    """Convenience function to validate the ontology.

    Args:
        spark: SparkSession instance
        catalog_name: Catalog name for tables
        schema_name: Schema name for tables
        validate_columns: If True, also validate column-level entities using
            batched AI_QUERY. Default False (opt-in).
        force_revalidate: If True, reset all previously-validated entities and
            re-validate from scratch.

    Returns:
        Dict with validation results and recommendations
    """
    config = OntologyValidatorConfig(
        catalog_name=catalog_name,
        schema_name=schema_name
    )
    validator = OntologyValidator(spark, config)
    return validator.run(validate_columns=validate_columns, force_revalidate=force_revalidate)


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

