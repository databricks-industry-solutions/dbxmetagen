"""Pre-compute LLM summaries for domain/subdomain communities.

Each community groups tables sharing the same (domain, subdomain). The summary
is a short paragraph describing the collective purpose and relationships of the
tables in that community. Summaries are indexed in the metadata VS for broad
questions that would otherwise require traversing many nodes.
"""

import hashlib
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

TABLE_NAME = "community_summaries"

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS {fq_table} (
  community_id STRING NOT NULL,
  domain STRING,
  subdomain STRING,
  summary STRING,
  table_count INT,
  table_names ARRAY<STRING>,
  generated_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
"""

_PROMPT_TEMPLATE = """You are a data catalog expert. Summarize the following group of tables that all belong to
domain "{domain}" / subdomain "{subdomain}".

Tables in this group:
{table_block}

Write a single paragraph (3-5 sentences) that:
1. Describes the collective purpose of these tables.
2. Notes key relationships or patterns.
3. Mentions any notable data types (PII, PHI, financial, etc.) if present.
Keep the summary factual and concise."""


def community_id(domain: str, subdomain: str) -> str:
    raw = f"{domain}::{subdomain}".lower()
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def discover_communities(spark, catalog_name: str, schema_name: str,
                         min_tables: int = 2) -> List[Dict[str, Any]]:
    """Group tables by (domain, subdomain) from table_knowledge_base."""
    fq = f"`{catalog_name}`.`{schema_name}`.table_knowledge_base"
    df = spark.sql(f"""
        SELECT domain, subdomain,
               COLLECT_LIST(table_name) AS table_names,
               COLLECT_LIST(COALESCE(comment, '')) AS comments,
               COUNT(*) AS table_count
        FROM {fq}
        WHERE domain IS NOT NULL AND domain != ''
        GROUP BY domain, subdomain
        HAVING COUNT(*) >= {min_tables}
        ORDER BY COUNT(*) DESC
    """)
    rows = df.collect()
    communities = []
    for r in rows:
        dom = r["domain"] or "unknown"
        sub = r["subdomain"] or "general"
        communities.append({
            "community_id": community_id(dom, sub),
            "domain": dom,
            "subdomain": sub,
            "table_names": list(r["table_names"]),
            "comments": list(r["comments"]),
            "table_count": r["table_count"],
        })
    logger.info("Discovered %d communities with >= %d tables", len(communities), min_tables)
    return communities


def build_prompt(comm: Dict[str, Any]) -> str:
    lines = []
    for name, comment in zip(comm["table_names"], comm["comments"]):
        desc = f": {comment[:200]}" if comment else ""
        lines.append(f"- {name}{desc}")
    return _PROMPT_TEMPLATE.format(
        domain=comm["domain"],
        subdomain=comm["subdomain"],
        table_block="\n".join(lines),
    )


def generate_summaries_batch(spark, catalog_name: str, schema_name: str,
                             model: str = "databricks-claude-sonnet-4",
                             min_tables: int = 2) -> int:
    """Discover communities and generate summaries via AI_QUERY batch SQL.

    Returns number of communities written.
    """
    communities = discover_communities(spark, catalog_name, schema_name, min_tables)
    if not communities:
        logger.info("No communities found, skipping summary generation")
        return 0

    fq_table = f"`{catalog_name}`.`{schema_name}`.{TABLE_NAME}"
    spark.sql(_CREATE_SQL.format(fq_table=fq_table))

    from pyspark.sql import Row
    from pyspark.sql.types import (
        ArrayType, IntegerType, StringType, StructField, StructType,
    )

    schema = StructType([
        StructField("community_id", StringType(), False),
        StructField("domain", StringType()),
        StructField("subdomain", StringType()),
        StructField("prompt", StringType()),
        StructField("table_count", IntegerType()),
        StructField("table_names", ArrayType(StringType())),
    ])

    rows = [
        Row(
            community_id=c["community_id"],
            domain=c["domain"],
            subdomain=c["subdomain"],
            prompt=build_prompt(c),
            table_count=c["table_count"],
            table_names=c["table_names"],
        )
        for c in communities
    ]
    df = spark.createDataFrame(rows, schema=schema)
    df.createOrReplaceTempView("_community_prompts")

    summary_sql = f"""
        SELECT
            community_id, domain, subdomain,
            AI_QUERY('{model}', prompt) AS summary,
            table_count, table_names,
            current_timestamp() AS generated_at
        FROM _community_prompts
    """
    result_df = spark.sql(summary_sql)

    result_df.write.format("delta").mode("overwrite").option(
        "mergeSchema", "true"
    ).saveAsTable(fq_table)

    count = len(communities)
    logger.info("Generated %d community summaries -> %s", count, fq_table)
    return count
