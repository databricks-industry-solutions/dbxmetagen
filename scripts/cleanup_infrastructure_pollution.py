"""Purge dbxmetagen infrastructure tables from knowledge bases, ontology, and graph.

Run as a Databricks notebook or via ``spark-submit``.

After running this script, a full knowledge-graph rebuild is recommended for
complete consistency (orphaned column nodes, similarity edges, etc.).

Usage (notebook):
    %run ./cleanup_infrastructure_pollution

Usage (local with Databricks Connect):
    python scripts/cleanup_infrastructure_pollution.py \
        --catalog my_catalog --schema my_schema --dry-run
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from dbxmetagen.table_filter import INFRASTRUCTURE_TABLE_NAMES

_INFRA_IN_CLAUSE = ", ".join(f"'{n}'" for n in sorted(INFRASTRUCTURE_TABLE_NAMES))
_DYNAMIC_RLIKE = "^(metadata_control_|temp_metadata_generation_log_)"


def _bare_expr(col: str) -> str:
    return f"substring_index({col}, '.', -1)"


def _is_infra_predicate(col: str) -> str:
    bare = _bare_expr(col)
    return f"({bare} IN ({_INFRA_IN_CLAUSE}) OR {bare} RLIKE '{_DYNAMIC_RLIKE}')"


def cleanup(catalog: str, schema: str, *, dry_run: bool = False) -> dict[str, int]:
    spark = SparkSession.builder.getOrCreate()
    fq = f"`{catalog}`.`{schema}`"
    counts: dict[str, int] = {}

    # --- KB and log tables ---
    targets = [
        ("table_knowledge_base", "table_name"),
        ("column_knowledge_base", "table_name"),
        ("metadata_generation_log", "table_name"),
    ]
    for table, col in targets:
        predicate = _is_infra_predicate(col)
        if dry_run:
            n = spark.sql(
                f"SELECT COUNT(*) AS n FROM {fq}.{table} WHERE {predicate}"
            ).collect()[0].n
            print(f"[DRY RUN] {table}: would delete {n} rows")
        else:
            spark.sql(f"DELETE FROM {fq}.{table} WHERE {predicate}")
            n = -1
            print(f"Cleaned {table}")
        counts[table] = n

    # --- ontology_entities: rows whose source_tables are ALL infra tables ---
    oe_where = (
        f"size(filter(source_tables, t -> "
        f"NOT {_is_infra_predicate('t')}"
        f")) = 0"
    )
    if dry_run:
        n = spark.sql(
            f"SELECT COUNT(*) AS n FROM {fq}.ontology_entities WHERE {oe_where}"
        ).collect()[0].n
        print(f"[DRY RUN] ontology_entities: would delete {n} rows")
    else:
        spark.sql(f"DELETE FROM {fq}.ontology_entities WHERE {oe_where}")
        n = -1
        print("Cleaned ontology_entities")
    counts["ontology_entities"] = n

    # --- graph_nodes: infra table nodes ---
    gn_pred = _is_infra_predicate("id")
    if dry_run:
        n = spark.sql(
            f"SELECT COUNT(*) AS n FROM {fq}.graph_nodes "
            f"WHERE node_type = 'table' AND {gn_pred}"
        ).collect()[0].n
        print(f"[DRY RUN] graph_nodes (table): would delete {n} rows")
    else:
        spark.sql(
            f"DELETE FROM {fq}.graph_nodes "
            f"WHERE node_type = 'table' AND {gn_pred}"
        )
        n = -1
        print("Cleaned graph_nodes (table)")
    counts["graph_nodes.table"] = n

    # --- graph_nodes: orphaned entity nodes whose ontology_id was deleted ---
    if dry_run:
        n = spark.sql(
            f"SELECT COUNT(*) AS n FROM {fq}.graph_nodes "
            f"WHERE node_type = 'entity' AND ontology_id IS NOT NULL "
            f"AND ontology_id NOT IN (SELECT entity_id FROM {fq}.ontology_entities)"
        ).collect()[0].n
        print(f"[DRY RUN] graph_nodes (orphaned entity): would delete {n} rows")
    else:
        spark.sql(
            f"DELETE FROM {fq}.graph_nodes "
            f"WHERE node_type = 'entity' AND ontology_id IS NOT NULL "
            f"AND ontology_id NOT IN (SELECT entity_id FROM {fq}.ontology_entities)"
        )
        n = -1
        print("Cleaned graph_nodes (orphaned entity)")
    counts["graph_nodes.entity"] = n

    # --- graph_edges: edges referencing deleted nodes ---
    for col in ("source_id", "target_id"):
        pred = _is_infra_predicate(col)
        if dry_run:
            n = spark.sql(
                f"SELECT COUNT(*) AS n FROM {fq}.graph_edges WHERE {pred}"
            ).collect()[0].n
            print(f"[DRY RUN] graph_edges ({col}): would delete {n} rows")
        else:
            spark.sql(f"DELETE FROM {fq}.graph_edges WHERE {pred}")
            n = -1
            print(f"Cleaned graph_edges ({col})")
        counts[f"graph_edges.{col}"] = n

    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Purge infrastructure table pollution")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--dry-run", action="store_true", help="Count without deleting")
    args = parser.parse_args()
    cleanup(args.catalog, args.schema, dry_run=args.dry_run)
