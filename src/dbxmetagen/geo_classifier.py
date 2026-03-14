"""Geographic column classification for dbxmetagen (LEGACY).

Classifies columns as geographic (suitable for location-based filtering) or
non-geographic using keyword matching with LLM fallback for ambiguous cases.

DEPRECATED: For new deployments, use the geo_doj ontology bundle via
EntityDiscoverer instead. This module is retained for backward compatibility
with existing customers who have a custom geo_config.yaml.
"""

import logging
import os
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import yaml
from pydantic import BaseModel, Field
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = None  # Legacy geo_config.yaml removed; pass explicit path or use geo_doj ontology bundle
_DEFAULT_TAG_KEY = "geo_classification"
_TABLE_NAME = "geo_classifications"


class GeoColumnResult(BaseModel):
    column_name: str
    classification: str = Field(description="'geographic' or 'non_geographic'")
    confidence: float = Field(ge=0.0, le=1.0)


class BatchGeoClassificationResult(BaseModel):
    classifications: List[GeoColumnResult]


@dataclass
class GeoConfig:
    catalog_name: str
    schema_name: str
    config_path: str = _DEFAULT_CONFIG_PATH
    tag_key: str = _DEFAULT_TAG_KEY
    model_endpoint: str = "databricks-meta-llama-3-3-70b-instruct"
    column_kb_table: str = "column_knowledge_base"

    @property
    def fq_results(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{_TABLE_NAME}"

    @property
    def fq_column_kb(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.column_kb_table}"

    def load_categories(self) -> Dict[str, Any]:
        """Load geo_config.yaml and return category definitions."""
        for candidate in [
            self.config_path,
            os.path.join(os.path.dirname(__file__), "../../configurations", os.path.basename(self.config_path)),
        ]:
            if os.path.isfile(candidate):
                with open(candidate) as f:
                    return yaml.safe_load(f)
        raise FileNotFoundError(f"Could not find geo config: {self.config_path}")


class GeoClassifier:
    """Classifies columns as geographic or non-geographic."""

    def __init__(self, spark: SparkSession, config: GeoConfig):
        self.spark = spark
        self.config = config
        raw = config.load_categories()
        categories = raw.get("categories", {})
        self._geo_keywords = set(categories.get("geographic", {}).get("keywords", []))
        self._non_geo_keywords = set(categories.get("non_geographic", {}).get("keywords", []))
        self._default = raw.get("default_category", "non_geographic")
        self._stats: Dict[str, int] = defaultdict(int)

    def _keyword_classify(self, col_name: str) -> Optional[str]:
        """Return 'geographic', 'non_geographic', or None (ambiguous)."""
        lower = col_name.lower()
        parts = re.split(r"[_\s]+", lower)
        if any(kw in lower or kw in parts for kw in self._non_geo_keywords):
            return "non_geographic"
        if any(kw in lower or kw in parts for kw in self._geo_keywords):
            return "geographic"
        return None

    def _get_llm(self):
        from databricks_langchain import ChatDatabricks
        llm = ChatDatabricks(
            endpoint=self.config.model_endpoint, temperature=0.0, max_tokens=2048
        )
        return llm.with_structured_output(BatchGeoClassificationResult)

    def _ai_classify_batch(
        self, table_name: str, columns: List[Tuple[str, str, str]]
    ) -> List[Tuple[str, str, float]]:
        """LLM classify a batch of columns for one table.

        Args:
            table_name: fully qualified table name
            columns: list of (column_name, data_type, comment)

        Returns:
            list of (column_name, classification, confidence)
        """
        col_lines = "\n".join(
            f"  - {cn} ({dt}): {desc[:200]}" for cn, dt, desc in columns
        )
        system_prompt = (
            "You are a geographic column classifier. For each column, decide if it "
            "represents a physical location (geographic) or not (non_geographic). "
            "Geographic columns include addresses, cities, states, zip codes, "
            "lat/lon, FIPS codes, regions, etc. Non-geographic columns include "
            "IDs, dates, amounts, names (unless place names), status fields, etc."
        )
        user_prompt = (
            f"Table: {table_name}\n\nColumns:\n{col_lines}\n\n"
            "Classify each column as 'geographic' or 'non_geographic'."
        )

        try:
            llm = self._get_llm()
            result: BatchGeoClassificationResult = llm.invoke([
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ])
            return [
                (r.column_name, r.classification, round(r.confidence, 3))
                for r in result.classifications
            ]
        except Exception as e:
            logger.warning("LLM geo classification failed for %s: %s", table_name, e)
            return [(cn, self._default, 0.3) for cn, _, _ in columns]

    def ensure_table(self) -> None:
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.config.fq_results} (
                table_name STRING NOT NULL,
                column_name STRING NOT NULL,
                classification STRING,
                confidence DOUBLE,
                tag_key STRING,
                method STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            COMMENT 'Geographic classification of columns'
        """)

    def classify(self, table_names: Optional[List[str]] = None) -> Dict[str, Any]:
        """Classify columns from the column_knowledge_base.

        Args:
            table_names: optional list of FQ table names to restrict to

        Returns:
            dict with counts of classified columns
        """
        self.ensure_table()

        where = ""
        if table_names:
            quoted = ", ".join(f"'{t}'" for t in table_names)
            where = f"WHERE table_name IN ({quoted})"

        cols_df = self.spark.sql(f"""
            SELECT column_name, table_name, data_type, comment
            FROM {self.config.fq_column_kb}
            {where}
        """)
        rows = cols_df.collect()
        if not rows:
            logger.info("No columns to classify")
            return {"classified": 0, "geographic": 0, "non_geographic": 0}

        logger.info("Classifying %d columns for geographic relevance", len(rows))

        results = []
        ambiguous_by_table: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)
        now = datetime.now()

        for row in rows:
            cn = row.column_name or ""
            kw_result = self._keyword_classify(cn)
            if kw_result:
                self._stats[f"keyword_{kw_result}"] += 1
                results.append((
                    row.table_name, cn, kw_result, 0.9, "keyword", now, now
                ))
            else:
                ambiguous_by_table[row.table_name].append(
                    (cn, row.data_type or "", row.comment or "")
                )

        if ambiguous_by_table:
            logger.info(
                "Using LLM for %d ambiguous columns across %d tables",
                sum(len(v) for v in ambiguous_by_table.values()),
                len(ambiguous_by_table),
            )

            def _classify_table(tbl, cols):
                return tbl, self._ai_classify_batch(tbl, cols)

            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {
                    executor.submit(_classify_table, tbl, cols): tbl
                    for tbl, cols in ambiguous_by_table.items()
                }
                for future in as_completed(futures):
                    tbl, ai_results = future.result()
                    for cn, cls, conf in ai_results:
                        self._stats[f"llm_{cls}"] += 1
                        results.append((tbl, cn, cls, conf, "llm", now, now))

        if results:
            schema = "table_name STRING, column_name STRING, classification STRING, confidence DOUBLE, method STRING, created_at TIMESTAMP, updated_at TIMESTAMP"
            df = self.spark.createDataFrame(results, schema=schema)
            df.createOrReplaceTempView("_geo_new")
            self.spark.sql(f"""
                MERGE INTO {self.config.fq_results} AS t
                USING _geo_new AS s
                ON t.table_name = s.table_name AND t.column_name = s.column_name
                WHEN MATCHED THEN UPDATE SET
                    t.classification = s.classification,
                    t.confidence = s.confidence,
                    t.method = s.method,
                    t.tag_key = '{self.config.tag_key}',
                    t.updated_at = s.updated_at
                WHEN NOT MATCHED THEN INSERT (
                    table_name, column_name, classification, confidence, tag_key, method, created_at, updated_at
                ) VALUES (
                    s.table_name, s.column_name, s.classification, s.confidence, '{self.config.tag_key}', s.method, s.created_at, s.updated_at
                )
            """)

        geo_count = sum(1 for r in results if r[2] == "geographic")
        non_geo_count = len(results) - geo_count
        logger.info("Classified %d columns: %d geographic, %d non-geographic", len(results), geo_count, non_geo_count)
        return {
            "classified": len(results),
            "geographic": geo_count,
            "non_geographic": non_geo_count,
            "stats": dict(self._stats),
        }

    def apply_tags(self) -> int:
        """Apply geo classification tags to UC columns from stored results."""
        tagged = 0
        try:
            rows = self.spark.sql(f"""
                SELECT table_name, column_name, classification, tag_key
                FROM {self.config.fq_results}
                WHERE classification IS NOT NULL AND confidence >= 0.5
            """).collect()
        except Exception as e:
            logger.warning("Could not read geo_classifications: %s", e)
            return 0

        tag_key = self.config.tag_key
        for row in rows:
            rk = row.tag_key or tag_key
            try:
                self.spark.sql(
                    f"ALTER TABLE {row.table_name} ALTER COLUMN `{row.column_name}` "
                    f"SET TAGS ('{rk}' = '{row.classification}')"
                )
                tagged += 1
            except Exception as e:
                logger.warning("Failed to tag %s.%s: %s", row.table_name, row.column_name, e)
        logger.info("Applied %d geo tags", tagged)
        return tagged

    def generate_ddl(self, tag_key: Optional[str] = None) -> List[str]:
        """Generate DDL statements for geo tags without executing them."""
        tk = tag_key or self.config.tag_key
        try:
            rows = self.spark.sql(f"""
                SELECT table_name, column_name, classification
                FROM {self.config.fq_results}
                WHERE classification IS NOT NULL AND confidence >= 0.5
            """).collect()
        except Exception as e:
            logger.warning("Could not read geo_classifications: %s", e)
            return []

        return [
            f"ALTER TABLE {r.table_name} ALTER COLUMN `{r.column_name}` SET TAGS ('{tk}' = '{r.classification}');"
            for r in rows
        ]


def classify_columns_geo(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    config_path: str = _DEFAULT_CONFIG_PATH,
    tag_key: str = _DEFAULT_TAG_KEY,
    table_names: Optional[List[str]] = None,
    apply_ddl: bool = False,
    model_endpoint: str = "databricks-meta-llama-3-3-70b-instruct",
) -> Dict[str, Any]:
    """Convenience function to classify columns as geographic/non-geographic.

    Args:
        spark: SparkSession
        catalog_name: UC catalog
        schema_name: UC schema containing the column_knowledge_base
        config_path: path to geo_config.yaml
        tag_key: UC tag key to use (e.g. 'doj_geo_filter')
        table_names: optional filter to specific tables
        apply_ddl: if True, apply tags to UC columns
        model_endpoint: LLM endpoint for ambiguous columns
    """
    config = GeoConfig(
        catalog_name=catalog_name,
        schema_name=schema_name,
        config_path=config_path,
        tag_key=tag_key,
        model_endpoint=model_endpoint,
    )
    classifier = GeoClassifier(spark, config)
    result = classifier.classify(table_names=table_names)
    if apply_ddl:
        result["tags_applied"] = classifier.apply_tags()
    return result
