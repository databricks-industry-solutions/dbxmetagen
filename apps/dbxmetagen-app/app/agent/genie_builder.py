"""Deterministic context assembly for Genie space generation.

Gathers all pre-computed metadata from knowledge bases, FK predictions,
ontology entities, and metric view definitions, then packages it into
a structured context string for the ReAct agent.
"""

import json
import logging
import uuid
from typing import Any, Dict, List

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Format, Disposition

logger = logging.getLogger(__name__)


def _run_sql(ws: WorkspaceClient, warehouse_id: str, query: str) -> list[dict]:
    """Execute SQL via Statement Execution API, return list[dict]."""
    r = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=query,
        wait_timeout="30s",
        format=Format.JSON_ARRAY,
        disposition=Disposition.INLINE,
    )
    state = r.status.state.value if r.status and r.status.state else "UNKNOWN"
    if state not in ("SUCCEEDED", "CLOSED"):
        logger.warning("SQL state=%s for: %s", state, query[:120])
        return []
    if r.result and r.result.data_array:
        cols = [c.name for c in r.manifest.schema.columns]
        return [dict(zip(cols, row)) for row in r.result.data_array]
    return []


def _safe_sql(ws: WorkspaceClient, warehouse_id: str, query: str) -> list[dict]:
    """Like _run_sql but returns [] on any exception."""
    try:
        return _run_sql(ws, warehouse_id, query)
    except Exception as e:
        logger.info("Skipping query (table may not exist): %s", e)
        return []


class GenieContextAssembler:
    """Gathers metadata from knowledge bases and builds context for the Genie agent."""

    def __init__(
        self, ws: WorkspaceClient, warehouse_id: str, catalog: str, schema: str
    ):
        self.ws = ws
        self.wh = warehouse_id
        self.catalog = catalog
        self.schema = schema

    def _fq(self, table: str) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{table}`"

    def assemble(
        self, table_identifiers: List[str], questions: List[str] | None = None
    ) -> Dict[str, Any]:
        """Gather all context and pre-build deterministic sections.

        Returns dict with keys:
            context_text: str  -- full metadata context for the agent prompt
            join_specs: list   -- pre-built join_specs from FK predictions
            data_sources: dict -- pre-built data_sources section
            sql_snippets: dict -- pre-built measures, filters, expressions from metric views
            questions: list    -- user-provided sample questions
        """
        table_meta = self._get_table_metadata(table_identifiers)
        column_meta = self._get_column_metadata(table_identifiers)
        fk_rows = self._get_fk_predictions(table_identifiers)
        entity_rows = self._get_ontology_entities()
        metric_views = self._get_metric_views()
        value_samples = self._sample_categorical_values(column_meta)

        # Split metric views: applied -> data_sources, validated -> sql_snippets
        applied_mvs = [mv for mv in metric_views if mv.get("status") == "applied"]
        unapplied_mvs = [mv for mv in metric_views if mv.get("status") != "applied"]

        context_text = self._format_context(
            table_meta, column_meta, fk_rows, entity_rows, value_samples
        )
        join_specs = self._build_join_specs(fk_rows)
        data_sources = self._build_data_sources(
            table_identifiers, table_meta, applied_mvs
        )
        sql_snippets = self._build_sql_snippets(unapplied_mvs, value_samples)

        return {
            "context_text": context_text,
            "join_specs": join_specs,
            "data_sources": data_sources,
            "sql_snippets": sql_snippets,
            "questions": questions or [],
        }

    # -- Data gathering methods -----------------------------------------------

    def _get_table_metadata(self, tables: List[str]) -> list[dict]:
        table_list = ", ".join(f"'{t}'" for t in tables)
        return _safe_sql(
            self.ws,
            self.wh,
            f"""
            SELECT table_name, comment, domain, subdomain
            FROM {self._fq('table_knowledge_base')}
            WHERE table_name IN ({table_list})
        """,
        )

    def _get_column_metadata(self, tables: List[str]) -> list[dict]:
        table_list = ", ".join(f"'{t}'" for t in tables)
        return _safe_sql(
            self.ws,
            self.wh,
            f"""
            SELECT table_name, column_name, data_type, comment,
                   classification, classification_type
            FROM {self._fq('column_knowledge_base')}
            WHERE table_name IN ({table_list})
        """,
        )

    def _get_fk_predictions(self, tables: List[str]) -> list[dict]:
        table_list = ", ".join(f"'{t}'" for t in tables)
        return _safe_sql(
            self.ws,
            self.wh,
            f"""
            SELECT src_table, dst_table, src_column, dst_column, final_confidence
            FROM {self._fq('fk_predictions')}
            WHERE is_fk = true AND final_confidence >= 0.7
              AND src_table IN ({table_list}) AND dst_table IN ({table_list})
        """,
        )

    def _get_ontology_entities(self) -> list[dict]:
        return _safe_sql(
            self.ws,
            self.wh,
            f"""
            SELECT entity_type, entity_name, description, source_tables, source_columns, confidence
            FROM {self._fq('ontology_entities')}
            WHERE confidence >= 0.4
        """,
        )

    def _get_metric_views(self) -> list[dict]:
        return _safe_sql(
            self.ws,
            self.wh,
            f"""
            SELECT metric_view_name, source_table, json_definition, status
            FROM {self._fq('metric_view_definitions')}
            WHERE status IN ('applied', 'validated')
        """,
        )

    def _sample_categorical_values(
        self, columns: list[dict]
    ) -> dict[str, dict[str, list]]:
        """Sample distinct values for STRING columns (useful for filter suggestions)."""
        samples: dict[str, dict[str, list]] = {}
        string_cols = [
            c
            for c in columns
            if c.get("data_type", "").upper() in ("STRING", "VARCHAR")
        ]
        for col in string_cols[:50]:  # cap to avoid too many queries
            tbl = col["table_name"]
            cn = col["column_name"]
            rows = _safe_sql(
                self.ws,
                self.wh,
                f"SELECT DISTINCT `{cn}` AS val FROM {tbl} WHERE `{cn}` IS NOT NULL LIMIT 15",
            )
            if rows:
                samples.setdefault(tbl, {})[cn] = [r["val"] for r in rows]
        return samples

    # -- Formatting -----------------------------------------------------------

    def _format_context(
        self, table_meta, column_meta, fk_rows, entity_rows, value_samples
    ) -> str:
        parts = []
        col_by_table: dict[str, list] = {}
        for c in column_meta:
            col_by_table.setdefault(c["table_name"], []).append(c)

        entity_map: dict[str, str] = {}
        for e in entity_rows:
            src_tables = e.get("source_tables") or []
            if isinstance(src_tables, str):
                src_tables = [src_tables]
            for t in src_tables:
                entity_map[t] = e["entity_type"]

        for t in table_meta:
            tname = t["table_name"]
            ent = entity_map.get(tname, "")
            header = f"Table: {tname}"
            if t.get("comment"):
                header += f' (Comment: "{t["comment"]}")'
            if t.get("domain"):
                header += f" Domain: {t['domain']}/{t.get('subdomain', '')}"
            if ent:
                header += f" Entity: {ent}"

            cols = col_by_table.get(tname, [])
            col_lines = []
            for c in cols:
                line = f"  - {c['column_name']} {c.get('data_type', '')}"
                if c.get("comment"):
                    line += f" : {c['comment']}"
                if c.get("classification"):
                    line += f" [{c['classification']}]"
                col_lines.append(line)

            block = header
            if col_lines:
                block += "\n  Columns:\n" + "\n".join(col_lines)

            samples = value_samples.get(tname, {})
            if samples:
                sample_lines = [
                    f"    {cn}: {vals}" for cn, vals in list(samples.items())[:5]
                ]
                block += "\n  Sample values:\n" + "\n".join(sample_lines)

            parts.append(block)

        if fk_rows:
            parts.append("\nFOREIGN KEY RELATIONSHIPS:")
            for fk in fk_rows:
                parts.append(
                    f"  {fk['src_table']}.{fk['src_column']} -> "
                    f"{fk['dst_table']}.{fk['dst_column']} (confidence {fk['final_confidence']})"
                )

        return "\n\n".join(parts)

    def _build_join_specs(self, fk_rows: list[dict]) -> list[dict]:
        specs = []
        for fk in fk_rows:
            specs.append(
                {
                    "id": uuid.uuid4().hex[:32],
                    "left": {"identifier": fk["src_table"]},
                    "right": {"identifier": fk["dst_table"]},
                    "sql": [
                        f"{fk['src_table'].split('.')[-1]}.{fk['src_column']} = "
                        f"{fk['dst_table'].split('.')[-1]}.{fk['dst_column']}"
                    ],
                }
            )
        return specs

    def _build_data_sources(
        self, table_ids: List[str], table_meta: list[dict], metric_views: list[dict]
    ) -> dict:
        meta_map = {t["table_name"]: t for t in table_meta}
        tables = []
        for tid in table_ids:
            entry: dict[str, Any] = {"identifier": tid}
            meta = meta_map.get(tid, {})
            if meta.get("comment"):
                entry["description"] = [meta["comment"]]
            tables.append(entry)

        mvs = []
        for mv in metric_views:
            mvs.append(
                {
                    "identifier": f"{self.catalog}.{self.schema}.{mv['metric_view_name']}",
                    "description": [f"Metric view on {mv['source_table']}"],
                }
            )

        result: dict[str, Any] = {"tables": tables}
        if mvs:
            result["metric_views"] = mvs
        return result

    def _build_sql_snippets(
        self, metric_views: list[dict], value_samples: dict
    ) -> dict:
        """Decompose metric view definitions into Genie sql_snippets structure."""
        measures: list[dict] = []
        filters: list[dict] = []
        expressions: list[dict] = []

        for mv in metric_views:
            raw = mv.get("json_definition", "")
            try:
                defn = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                continue

            source = defn.get("source", mv.get("source_table", ""))
            tbl_alias = source.split(".")[-1] if source else ""

            for m in defn.get("measures", []):
                measures.append(
                    {
                        "alias": m.get("name", ""),
                        "display_name": m.get("name", ""),
                        "sql": [m.get("expr", "")],
                        "description": m.get("comment", f"From {tbl_alias}"),
                    }
                )

            for d in defn.get("dimensions", []):
                expr = d.get("expr", "")
                if expr and expr != d.get("name", ""):
                    expressions.append(
                        {
                            "alias": d.get("name", ""),
                            "display_name": d.get("name", ""),
                            "sql": [expr],
                            "description": d.get("comment", ""),
                        }
                    )

            if defn.get("filter"):
                filters.append(
                    {
                        "display_name": f"{tbl_alias} default filter",
                        "sql": [defn["filter"]],
                    }
                )

        for tbl, cols in value_samples.items():
            tbl_short = tbl.split(".")[-1]
            for col, vals in cols.items():
                if 2 <= len(vals) <= 10:
                    filters.append(
                        {
                            "display_name": f"Filter by {col} ({tbl_short})",
                            "sql": [f"`{tbl}`.`{col}` = '<value>'"],
                        }
                    )

        return {
            "measures": measures,
            "filters": filters[:20],
            "expressions": expressions,
        }
