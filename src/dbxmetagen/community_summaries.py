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
                         min_tables: int = 2, table_names=None) -> List[Dict[str, Any]]:
    """Group tables by (domain, subdomain) from table_knowledge_base."""
    from dbxmetagen.table_filter import table_filter_sql
    fq = f"`{catalog_name}`.`{schema_name}`.table_knowledge_base"
    tn_clause = table_filter_sql(table_names or [])
    df = spark.sql(f"""
        SELECT domain, subdomain,
               SORT_ARRAY(COLLECT_LIST(STRUCT(table_name, COALESCE(comment, '') AS comment))) AS table_entries,
               COUNT(*) AS table_count
        FROM {fq}
        WHERE domain IS NOT NULL AND domain != ''
        {tn_clause}
        GROUP BY domain, subdomain
        HAVING COUNT(*) >= {min_tables}
        ORDER BY COUNT(*) DESC
    """)
    rows = df.collect()
    communities = []
    for r in rows:
        dom = r["domain"] or "unknown"
        sub = r["subdomain"] or "general"
        entries = r["table_entries"]
        communities.append({
            "community_id": community_id(dom, sub),
            "domain": dom,
            "subdomain": sub,
            "table_names": [e["table_name"] for e in entries],
            "comments": [e["comment"] for e in entries],
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


def _get_stale_communities(spark, communities: List[Dict[str, Any]],
                            catalog_name: str, schema_name: str) -> List[Dict[str, Any]]:
    """Filter communities to only those whose underlying tables have changed.

    Compares MAX(table_knowledge_base.updated_at) for each community's tables
    against the existing community_summaries.generated_at timestamp.
    """
    fq_kb = f"`{catalog_name}`.`{schema_name}`.table_knowledge_base"
    fq_cs = f"`{catalog_name}`.`{schema_name}`.{TABLE_NAME}"

    existing = {}
    try:
        rows = spark.sql(
            f"SELECT community_id, generated_at FROM {fq_cs}"
        ).collect()
        existing = {r.community_id: r.generated_at for r in rows}
    except Exception:
        pass

    stale = []
    for c in communities:
        gen_at = existing.get(c["community_id"])
        if gen_at is None:
            stale.append(c)
            continue
        tbl_list = ", ".join(f"'{t}'" for t in c["table_names"][:500])
        max_upstream = spark.sql(
            f"SELECT COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01') AS mu "
            f"FROM {fq_kb} WHERE table_name IN ({tbl_list})"
        ).collect()[0].mu
        if max_upstream > gen_at:
            stale.append(c)
    return stale


def generate_summaries_batch(spark, catalog_name: str, schema_name: str,
                             model: str = "databricks-claude-sonnet-4",
                             min_tables: int = 2, table_names=None,
                             incremental: bool = True) -> int:
    """Discover communities and generate summaries via AI_QUERY batch SQL.

    When incremental=True, only regenerates communities where table_knowledge_base
    rows are newer than the stored summary (see _get_stale_communities).

    Returns number of communities written.
    """
    communities = discover_communities(spark, catalog_name, schema_name, min_tables,
                                       table_names=table_names)
    if not communities:
        logger.info("No communities found, skipping summary generation")
        return 0

    fq_table = f"`{catalog_name}`.`{schema_name}`.{TABLE_NAME}"
    spark.sql(_CREATE_SQL.format(fq_table=fq_table))

    if incremental:
        communities = _get_stale_communities(spark, communities, catalog_name, schema_name)
        if not communities:
            logger.info("Incremental: all communities up-to-date, skipping AI calls")
            return 0
        logger.info("Incremental: %d communities need re-summarization", len(communities))

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
    result_df.createOrReplaceTempView("_community_results")

    # MERGE upserts this batch without truncating summaries for communities not in the batch.
    spark.sql(f"""
        MERGE INTO {fq_table} AS tgt
        USING _community_results AS src
        ON tgt.community_id = src.community_id
        WHEN MATCHED THEN UPDATE SET
            tgt.domain = src.domain,
            tgt.subdomain = src.subdomain,
            tgt.summary = src.summary,
            tgt.table_count = src.table_count,
            tgt.table_names = src.table_names,
            tgt.generated_at = src.generated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    spark.sql("DROP VIEW IF EXISTS _community_results")
    spark.sql("DROP VIEW IF EXISTS _community_prompts")

    count = len(communities)
    logger.info("Generated %d community summaries -> %s", count, fq_table)
    return count
