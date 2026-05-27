"""Semantic Knowledge Graph for metric view definitions.

Decomposes metric views into typed nodes (metric_view, measure, dimension,
source_table) and edges (has_measure, has_dimension, sourced_from, joins_to,
provides, shared_source, co_dimension).  Persists into ``semantic_nodes`` and
``semantic_edges`` Delta tables with incremental MERGE.
"""

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

_ALIAS_COL_RE = re.compile(r"(?<![.\w])(\w+)\.(\w+)(?!\w)")


@dataclass
class SemanticGraphConfig:
    catalog_name: str
    schema_name: str
    definitions_table: str = "metric_view_definitions"

    def fq(self, table: str) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{table}"

    @property
    def fq_nodes(self) -> str:
        return self.fq("semantic_nodes")

    @property
    def fq_edges(self) -> str:
        return self.fq("semantic_edges")


class SemanticGraphBuilder:
    """Builds a semantic knowledge graph from metric view definitions."""

    def __init__(self, spark: SparkSession, config: SemanticGraphConfig):
        self.spark = spark
        self.config = config

    # -- DDL ----------------------------------------------------------------

    def create_tables(self) -> None:
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.config.fq_nodes} (
                node_id STRING NOT NULL,
                node_type STRING,
                definition_id STRING,
                name STRING,
                display_name STRING,
                expr STRING,
                comment STRING,
                source_table STRING,
                status STRING,
                deployed_fqn STRING,
                filter_expr STRING,
                synonyms STRING,
                format_spec STRING,
                window_spec STRING,
                domain STRING,
                subdomain STRING,
                graph_node_id STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            ) USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.config.fq_edges} (
                edge_id STRING NOT NULL,
                src STRING,
                dst STRING,
                relationship STRING,
                direction STRING,
                weight DOUBLE,
                properties STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            ) USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)

    # -- Watermark ----------------------------------------------------------

    def _check_watermark(self) -> bool:
        """Return True if definitions are newer than semantic_nodes."""
        cfg = self.config
        try:
            up = self.spark.sql(
                f"SELECT MAX(created_at) AS mu FROM {cfg.fq(cfg.definitions_table)}"
            ).collect()[0].mu
        except Exception:
            return True
        if up is None:
            return False
        try:
            cur = self.spark.sql(
                f"SELECT COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01') AS mu FROM {cfg.fq_nodes}"
            ).collect()[0].mu
        except Exception:
            return True
        return up > cur

    # -- Domain lookup ------------------------------------------------------

    def _load_domains(self) -> Dict[str, Tuple[str, str]]:
        """Map source table FQN -> (domain, subdomain) from table_knowledge_base."""
        try:
            rows = self.spark.sql(
                f"SELECT table_name, domain, subdomain FROM {self.config.fq('table_knowledge_base')}"
            ).collect()
            return {r.table_name: (r.domain, r.subdomain) for r in rows}
        except Exception:
            return {}

    # -- Decompose a single definition --------------------------------------

    def _decompose(
        self, defn: dict, definition_id: str, domains: Dict[str, Tuple[str, str]]
    ) -> Tuple[List[dict], List[dict]]:
        """Parse one metric view definition into nodes and edges."""
        nodes: List[dict] = []
        edges: List[dict] = []
        now = datetime.utcnow()
        mv_name = defn.get("name", "")
        source = defn.get("source", "")
        dom, subdom = domains.get(source, (None, None))
        status = defn.get("_status")
        deployed_fqn = defn.get("_deployed_fqn")

        mv_nid = f"metric_view::{definition_id}::{mv_name}"
        nodes.append({
            "node_id": mv_nid, "node_type": "metric_view",
            "definition_id": definition_id, "name": mv_name,
            "comment": defn.get("comment"), "source_table": source,
            "status": status, "deployed_fqn": deployed_fqn,
            "filter_expr": defn.get("filter"),
            "domain": dom, "subdomain": subdom,
            "created_at": now, "updated_at": now,
        })

        src_nid = f"source_table::{source}"
        nodes.append({
            "node_id": src_nid, "node_type": "source_table",
            "name": source, "domain": dom, "subdomain": subdom,
            "created_at": now, "updated_at": now,
        })
        edges.append(self._edge(mv_nid, src_nid, "sourced_from", directed=True))

        alias_map: Dict[str, str] = {"source": source}
        self._walk_joins(defn.get("joins", []), source, "source", alias_map, nodes, edges, now)

        for m in defn.get("measures", []):
            m_nid = f"measure::{definition_id}::{m['name']}"
            nodes.append({
                "node_id": m_nid, "node_type": "measure",
                "definition_id": definition_id, "name": m["name"],
                "display_name": m.get("display_name"), "expr": m.get("expr"),
                "comment": m.get("comment"),
                "synonyms": json.dumps(m["synonyms"]) if m.get("synonyms") else None,
                "format_spec": json.dumps(m["format"]) if m.get("format") else None,
                "window_spec": json.dumps(m["window"]) if m.get("window") else None,
                "created_at": now, "updated_at": now,
            })
            edges.append(self._edge(mv_nid, m_nid, "has_measure", directed=True))
            self._add_provides_edges(m.get("expr", ""), m_nid, alias_map, edges)

        for d in defn.get("dimensions", []):
            d_nid = f"dimension::{definition_id}::{d['name']}"
            nodes.append({
                "node_id": d_nid, "node_type": "dimension",
                "definition_id": definition_id, "name": d["name"],
                "display_name": d.get("display_name"), "expr": d.get("expr"),
                "comment": d.get("comment"),
                "synonyms": json.dumps(d["synonyms"]) if d.get("synonyms") else None,
                "created_at": now, "updated_at": now,
            })
            edges.append(self._edge(mv_nid, d_nid, "has_dimension", directed=True))
            self._add_provides_edges(d.get("expr", ""), d_nid, alias_map, edges)

        return nodes, edges

    def _walk_joins(
        self, joins: list, parent_fqn: str, parent_alias: str,
        alias_map: dict, nodes: list, edges: list, now: datetime,
    ) -> None:
        for j in joins:
            j_source = j.get("source", "")
            j_alias = j.get("name", j_source.split(".")[-1] if j_source else "")
            alias_map[j_alias] = j_source
            j_nid = f"source_table::{j_source}"
            nodes.append({
                "node_id": j_nid, "node_type": "source_table",
                "name": j_source, "created_at": now, "updated_at": now,
            })
            parent_nid = f"source_table::{parent_fqn}"
            props = {"join_expression": j.get("on", ""), "join_type": j.get("type", "LEFT")}
            edges.append(self._edge(parent_nid, j_nid, "joins_to", directed=True, props=props))
            if j.get("joins"):
                self._walk_joins(j["joins"], j_source, j_alias, alias_map, nodes, edges, now)

    def _add_provides_edges(self, expr: str, target_nid: str, alias_map: dict, edges: list) -> None:
        if not expr:
            return
        resolved = set()
        for alias, col in _ALIAS_COL_RE.findall(expr):
            fqn = alias_map.get(alias)
            if fqn:
                resolved.add(fqn)
        if not resolved:
            src_fqn = alias_map.get("source", "")
            if src_fqn:
                resolved.add(src_fqn)
                edges.append(self._edge(
                    f"source_table::{src_fqn}", target_nid, "provides",
                    directed=True, weight=0.7,
                ))
                return
        for fqn in resolved:
            edges.append(self._edge(
                f"source_table::{fqn}", target_nid, "provides",
                directed=True, weight=1.0,
            ))

    @staticmethod
    def _edge(
        src: str, dst: str, rel: str, *, directed: bool = True,
        weight: float = None, props: dict = None,
    ) -> dict:
        now = datetime.utcnow()
        return {
            "edge_id": f"{src}::{dst}::{rel}",
            "src": src, "dst": dst, "relationship": rel,
            "direction": "directed" if directed else "undirected",
            "weight": weight,
            "properties": json.dumps(props) if props else None,
            "created_at": now, "updated_at": now,
        }

    # -- Inter-view edges ---------------------------------------------------

    def _compute_inter_view_edges(
        self, nodes: List[dict], edges: List[dict],
    ) -> List[dict]:
        inter: List[dict] = []

        mv_tables: Dict[str, set] = {}
        for e in edges:
            if e["relationship"] in ("sourced_from", "joins_to"):
                mv_nids = [n["node_id"] for n in nodes
                           if n["node_type"] == "metric_view" and n["node_id"] == e["src"]]
                if e["relationship"] == "sourced_from" and mv_nids:
                    mv_tables.setdefault(mv_nids[0], set()).add(e["dst"])
        for e in edges:
            if e["relationship"] == "joins_to":
                for mv_nid, tbl_set in mv_tables.items():
                    if e["src"] in tbl_set or e["dst"] in tbl_set:
                        tbl_set.add(e["src"])
                        tbl_set.add(e["dst"])

        mv_list = list(mv_tables.keys())
        for i, mv_a in enumerate(mv_list):
            for mv_b in mv_list[i + 1:]:
                if mv_tables[mv_a] & mv_tables[mv_b]:
                    inter.append(self._edge(mv_a, mv_b, "shared_source", directed=False))

        dims = [n for n in nodes if n["node_type"] == "dimension"]
        dim_by_sig: Dict[str, list] = {}
        for d in dims:
            norm_expr = self._normalize_expr(d.get("expr", ""))
            sig = f"{d['name']}||{norm_expr}"
            dim_by_sig.setdefault(sig, []).append(d["node_id"])
        for sig, nids in dim_by_sig.items():
            if len(nids) < 2:
                continue
            for i, a in enumerate(nids):
                for b in nids[i + 1:]:
                    inter.append(self._edge(a, b, "co_dimension", directed=False))

        return inter

    @staticmethod
    def _normalize_expr(expr: str) -> str:
        """Strip alias prefixes for co_dimension comparison."""
        return _ALIAS_COL_RE.sub(r"\2", expr).strip() if expr else ""

    # -- Build orchestrator -------------------------------------------------

    def build(self, incremental: bool = True) -> Dict[str, Any]:
        self.create_tables()
        if incremental and not self._check_watermark():
            logger.info("Semantic graph: no upstream changes, skipping rebuild")
            return {"nodes": 0, "edges": 0, "skipped": True}

        cfg = self.config
        rows = self.spark.sql(
            f"SELECT definition_id, metric_view_name, source_table, json_definition, "
            f"status, deployed_catalog, deployed_schema "
            f"FROM {cfg.fq(cfg.definitions_table)} "
            f"WHERE status = 'applied'"
        ).collect()

        if not rows:
            logger.info("No validated/applied definitions found")
            return {"nodes": 0, "edges": 0}

        domains = self._load_domains()
        all_nodes: List[dict] = []
        all_edges: List[dict] = []
        valid_def_ids: set = set()

        for r in rows:
            defn = json.loads(r.json_definition) if isinstance(r.json_definition, str) else r.json_definition
            defn["_status"] = r.status
            if r.deployed_catalog and r.deployed_schema:
                defn["_deployed_fqn"] = f"{r.deployed_catalog}.{r.deployed_schema}.{r.metric_view_name}"
            valid_def_ids.add(r.definition_id)
            n, e = self._decompose(defn, r.definition_id, domains)
            all_nodes.extend(n)
            all_edges.extend(e)

        seen_nodes: Dict[str, dict] = {}
        for n in all_nodes:
            nid = n["node_id"]
            if nid in seen_nodes:
                existing = seen_nodes[nid]
                if n.get("definition_id") and not existing.get("definition_id"):
                    seen_nodes[nid] = n
            else:
                seen_nodes[nid] = n
        deduped_nodes = list(seen_nodes.values())

        self._populate_graph_node_ids(deduped_nodes)

        inter = self._compute_inter_view_edges(deduped_nodes, all_edges)
        all_edges.extend(inter)

        seen_edges: Dict[str, dict] = {}
        for e in all_edges:
            seen_edges[e["edge_id"]] = e
        deduped_edges = list(seen_edges.values())

        node_count = self._merge_nodes(deduped_nodes)
        edge_count = self._merge_edges(deduped_edges)
        self._sweep_stale(valid_def_ids)

        logger.info("Semantic graph built: %d nodes, %d edges", node_count, edge_count)
        return {"nodes": node_count, "edges": edge_count}

    def _populate_graph_node_ids(self, nodes: List[dict]) -> None:
        """Cross-reference source_table nodes to graph_nodes."""
        st_nodes = [n for n in nodes if n["node_type"] == "source_table"]
        if not st_nodes:
            return
        try:
            gn_rows = self.spark.sql(
                f"SELECT id, table_name FROM {self.config.fq('graph_nodes')} WHERE node_type = 'table'"
            ).collect()
            gn_map = {r.table_name: r.id for r in gn_rows}
            for n in st_nodes:
                n["graph_node_id"] = gn_map.get(n["name"])
        except Exception:
            pass

    # -- MERGE helpers ------------------------------------------------------

    def _merge_nodes(self, nodes: List[dict]) -> int:
        if not nodes:
            return 0
        df = self.spark.createDataFrame(nodes)
        df = df.dropDuplicates(["node_id"])
        df.createOrReplaceTempView("_sg_staged_nodes")
        self.spark.sql(f"""
            MERGE INTO {self.config.fq_nodes} AS t
            USING _sg_staged_nodes AS s
            ON t.node_id = s.node_id
            WHEN MATCHED AND (
                COALESCE(t.expr, '') != COALESCE(s.expr, '')
                OR COALESCE(t.comment, '') != COALESCE(s.comment, '')
                OR COALESCE(t.source_table, '') != COALESCE(s.source_table, '')
                OR COALESCE(t.status, '') != COALESCE(s.status, '')
                OR COALESCE(t.deployed_fqn, '') != COALESCE(s.deployed_fqn, '')
            ) THEN UPDATE SET
                t.node_type = s.node_type, t.definition_id = s.definition_id,
                t.name = s.name, t.display_name = s.display_name,
                t.expr = s.expr, t.comment = s.comment,
                t.source_table = s.source_table, t.status = s.status,
                t.deployed_fqn = s.deployed_fqn, t.filter_expr = s.filter_expr,
                t.synonyms = s.synonyms, t.format_spec = s.format_spec,
                t.window_spec = s.window_spec, t.domain = s.domain,
                t.subdomain = s.subdomain, t.graph_node_id = s.graph_node_id,
                t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT *
        """)
        return df.count()

    def _merge_edges(self, edges: List[dict]) -> int:
        if not edges:
            return 0
        df = self.spark.createDataFrame(edges)
        df = df.dropDuplicates(["edge_id"])
        df.createOrReplaceTempView("_sg_staged_edges")
        self.spark.sql(f"""
            MERGE INTO {self.config.fq_edges} AS t
            USING _sg_staged_edges AS s
            ON t.edge_id = s.edge_id
            WHEN MATCHED THEN UPDATE SET
                t.src = s.src, t.dst = s.dst, t.relationship = s.relationship,
                t.direction = s.direction, t.weight = s.weight,
                t.properties = s.properties, t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT *
        """)
        return df.count()

    def _sweep_stale(self, valid_def_ids: set) -> None:
        """Remove nodes/edges for superseded or deleted definitions."""
        if not valid_def_ids:
            return
        id_list = ", ".join(f"'{d}'" for d in valid_def_ids)
        try:
            self.spark.sql(f"""
                DELETE FROM {self.config.fq_nodes}
                WHERE definition_id IS NOT NULL
                  AND definition_id NOT IN ({id_list})
            """)
            self.spark.sql(f"""
                DELETE FROM {self.config.fq_nodes}
                WHERE node_type = 'source_table'
                  AND node_id NOT IN (
                    SELECT DISTINCT dst FROM {self.config.fq_edges} WHERE relationship IN ('sourced_from', 'joins_to')
                    UNION
                    SELECT DISTINCT src FROM {self.config.fq_edges} WHERE relationship IN ('sourced_from', 'joins_to')
                  )
            """)
            self.spark.sql(f"""
                DELETE FROM {self.config.fq_edges}
                WHERE src NOT IN (SELECT node_id FROM {self.config.fq_nodes})
                   OR dst NOT IN (SELECT node_id FROM {self.config.fq_nodes})
            """)
        except Exception as e:
            logger.warning("Stale sweep error: %s", e)


def build_semantic_graph(spark: SparkSession, config) -> Dict[str, Any]:
    """Module-level entry point. Accepts either SemanticGraphConfig or any config with catalog_name/schema_name."""
    if not isinstance(config, SemanticGraphConfig):
        sg_config = SemanticGraphConfig(
            catalog_name=config.catalog_name,
            schema_name=config.schema_name,
        )
        if hasattr(config, "definitions_table"):
            sg_config.definitions_table = config.definitions_table
    else:
        sg_config = config
    builder = SemanticGraphBuilder(spark, sg_config)
    return builder.build()
