"""Deterministic context assembly for Genie space generation.

Gathers all pre-computed metadata from knowledge bases, FK predictions,
ontology entities, and metric view definitions, then packages it into
a structured context string for the ReAct agent.
"""

import json
import logging
import os
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


_COMMON_ABBREVIATIONS = {
    "id": "identifier", "qty": "quantity", "amt": "amount", "dt": "date",
    "ts": "timestamp", "num": "number", "cnt": "count", "pct": "percent",
    "avg": "average", "desc": "description", "addr": "address",
    "acct": "account", "org": "organization", "dept": "department",
    "mgr": "manager", "emp": "employee", "prod": "product", "cat": "category",
    "inv": "invoice", "txn": "transaction", "bal": "balance", "ref": "reference",
    "src": "source", "dst": "destination", "fk": "foreign key", "pk": "primary key",
}


_TYPE_SYNONYMS = {
    "TIMESTAMP": ["date", "time", "datetime", "when"],
    "DATE": ["date", "day", "when"],
    "DECIMAL": ["amount", "value", "number"],
    "DOUBLE": ["amount", "value", "number"],
    "FLOAT": ["amount", "value", "number"],
    "INT": ["count", "number"],
    "BIGINT": ["count", "number", "identifier"],
    "BOOLEAN": ["flag", "is", "yes/no"],
}

_DOMAIN_SYNONYMS = {
    "customer": ["client", "buyer", "account holder"],
    "clinical": ["patient", "medical", "healthcare"],
    "diagnostics": ["lab", "test result", "observation"],
    "payer": ["insurance", "coverage", "plan"],
    "pharmaceutical": ["drug", "medication", "therapy"],
    "sales": ["revenue", "deal", "opportunity"],
    "marketing": ["campaign", "lead", "outreach"],
    "support": ["ticket", "case", "service request"],
}


def _generate_synonyms(name: str, data_type: str = "", comment: str = "", domain: str = "") -> list[str]:
    """Generate synonyms from a column/measure name by expanding abbreviations,
    data type hints, domain context, and KB comments."""
    tokens = name.replace("_", " ").split()
    expanded = [_COMMON_ABBREVIATIONS.get(t.lower(), t) for t in tokens]
    synonyms = set()
    readable = " ".join(expanded)
    if readable.lower() != name.lower():
        synonyms.add(readable)
    if len(tokens) > 1:
        synonyms.add(" ".join(tokens))
    if len(tokens) >= 3:
        synonyms.add(" ".join(reversed(tokens[:2])) + " " + " ".join(tokens[2:]))
    if comment:
        first_sentence = comment.split(".")[0].strip()
        if 3 < len(first_sentence) < 60:
            synonyms.add(first_sentence)
    dt_upper = data_type.upper().split("(")[0].strip() if data_type else ""
    for dt_key, dt_syns in _TYPE_SYNONYMS.items():
        if dt_upper == dt_key:
            for s in dt_syns:
                if s.lower() not in name.lower():
                    synonyms.add(f"{' '.join(tokens)} {s}")
                    break
    if domain:
        for ds in _DOMAIN_SYNONYMS.get(domain.lower(), []):
            if ds.lower() not in name.lower() and len(tokens) > 0:
                synonyms.add(f"{ds} {tokens[-1]}")
                break
    synonyms.discard(name)
    synonyms.discard("")
    return sorted(synonyms)[:5]


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
        self,
        table_identifiers: List[str],
        questions: List[str] | None = None,
        metric_view_names: List[str] | None = None,
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
        entity_rows = self._get_ontology_entities(table_identifiers)
        entity_rels = self._get_entity_relationships(table_identifiers)
        metric_views = self._get_metric_views(table_identifiers)
        if metric_view_names:
            names_set = set(metric_view_names)
            metric_views = [mv for mv in metric_views if mv.get("metric_view_name") in names_set]
        value_samples = self._sample_categorical_values(column_meta)

        # Split metric views: applied -> data_sources, validated -> sql_snippets
        applied_mvs = [mv for mv in metric_views if mv.get("status") == "applied"]
        unapplied_mvs = [mv for mv in metric_views if mv.get("status") != "applied"]

        context_text = self._format_context(
            table_meta, column_meta, fk_rows, entity_rows, value_samples, entity_rels
        )
        join_specs = self._build_join_specs(fk_rows)
        ontology_joins = self._get_ontology_join_specs(table_identifiers)
        existing_pairs = {(j["left"]["identifier"], j["right"]["identifier"]) for j in join_specs}
        for oj in ontology_joins:
            pair = (oj["left"]["identifier"], oj["right"]["identifier"])
            if pair not in existing_pairs:
                join_specs.append(oj)
                existing_pairs.add(pair)
        data_sources = self._build_data_sources(
            table_identifiers, table_meta, applied_mvs,
            column_meta=column_meta, entity_rows=entity_rows,
        )
        sql_snippets = self._build_sql_snippets(
            unapplied_mvs, value_samples, column_meta
        )

        reference_text = self._load_genie_reference()

        return {
            "context_text": context_text,
            "join_specs": join_specs,
            "data_sources": data_sources,
            "sql_snippets": sql_snippets,
            "questions": questions or [],
            "reference_text": reference_text,
        }

    def _load_genie_reference(self) -> str:
        """Load genie_reference.json and format into prompt text."""
        candidates = [
            os.path.join(os.path.dirname(__file__), "..", "..", "configurations", "agent_references", "genie_reference.json"),
            os.path.join("configurations", "agent_references", "genie_reference.json"),
        ]
        ref = {}
        for p in candidates:
            p = os.path.normpath(p)
            if os.path.isfile(p):
                try:
                    with open(p) as f:
                        ref = json.load(f)
                except Exception:
                    pass
                break
        if not ref:
            return ""
        parts = []
        for section_key in ["sql_snippet_patterns", "instruction_templates", "example_sql_patterns", "sample_question_patterns", "date_function_rules"]:
            val = ref.get(section_key)
            if not val:
                continue
            if isinstance(val, dict):
                desc = val.get("description", "")
                header = f"\n### {section_key}"
                if desc:
                    header += f"\n{desc}"
                parts.append(header + "\n" + json.dumps(
                    {k: v for k, v in val.items() if k != "description"}, indent=2
                ))
            elif isinstance(val, str):
                parts.append(f"\n### {section_key}\n{val}")
            else:
                parts.append(f"\n### {section_key}\n{json.dumps(val, indent=2)}")
        return "\n".join(parts) if parts else ""

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
            WHERE final_confidence >= 0.7
              AND src_table IN ({table_list}) AND dst_table IN ({table_list})
        """,
        )

    def _get_ontology_entities(self, tables: List[str] | None = None) -> list[dict]:
        base = (
            f"SELECT entity_type, entity_name, description, source_tables, "
            f"source_columns, confidence, COALESCE(entity_role, 'primary') AS entity_role "
            f"FROM {self._fq('ontology_entities')} "
            f"WHERE confidence >= 0.4"
        )
        rows = _safe_sql(self.ws, self.wh, base)
        if not tables:
            return rows
        table_set = set(tables) | {t.split(".")[-1] for t in tables}
        filtered = []
        for r in rows:
            src = r.get("source_tables") or []
            if isinstance(src, str):
                src = [src]
            if any(t in table_set or t.split(".")[-1] in table_set for t in src):
                filtered.append(r)
        return filtered

    def _get_column_properties(self, tables: List[str]) -> list[dict]:
        """Fetch column property classifications for given tables."""
        in_clause = ", ".join(f"'{t}'" for t in tables)
        return _safe_sql(
            self.ws, self.wh,
            f"SELECT table_name, column_name, property_role, linked_entity_type "
            f"FROM {self._fq('ontology_column_properties')} "
            f"WHERE table_name IN ({in_clause})",
        )

    def _get_entity_relationships(self, tables: List[str] | None = None) -> list[dict]:
        """Fetch named relationships from ontology_relationships table."""
        rows = _safe_sql(
            self.ws, self.wh,
            f"SELECT src_entity_type AS src_type, dst_entity_type AS dst_type, "
            f"relationship_name AS relationship, cardinality "
            f"FROM {self._fq('ontology_relationships')} "
            f"WHERE confidence >= 0.4",
        )
        if not tables:
            return rows
        entity_rows = self._get_ontology_entities(tables)
        relevant_types = {r.get("entity_type") for r in entity_rows}
        return [r for r in rows if r.get("src_type") in relevant_types or r.get("dst_type") in relevant_types]

    def _get_ontology_join_specs(self, tables: List[str]) -> list[dict]:
        """Extract join_path specs from ontology YAML config for selected tables.

        Returns list of join_spec dicts suitable for the Genie API.
        """
        import os, glob as _glob
        import yaml

        bundle_dirs = ["configurations/ontology_bundles", "../configurations/ontology_bundles"]
        yamls: list[str] = []
        for d in bundle_dirs:
            if os.path.isdir(d):
                yamls.extend(_glob.glob(os.path.join(d, "*.yaml")))
        if not yamls:
            return []

        entity_rows = self._get_ontology_entities(tables)
        type_to_tables: dict[str, set[str]] = {}
        for e in entity_rows:
            src = e.get("source_tables") or []
            if isinstance(src, str):
                src = [src]
            for t in src:
                type_to_tables.setdefault(e["entity_type"], set()).add(t)

        table_short_set = {t.split(".")[-1].lower() for t in tables}

        specs: list[dict] = []
        for yf in yamls:
            try:
                with open(yf) as f:
                    cfg = yaml.safe_load(f) or {}
            except Exception:
                continue
            defs = cfg.get("ontology", cfg).get("entities", {}).get("definitions", {})
            for ent_name, details in defs.items():
                rels = details.get("relationships") or {}
                if not isinstance(rels, dict):
                    continue
                for rel_name, rel_info in rels.items():
                    if not isinstance(rel_info, dict):
                        continue
                    join_path = rel_info.get("join_path")
                    target = rel_info.get("target")
                    if not join_path or not target:
                        continue
                    # Resolve entity types to actual tables
                    src_tables = type_to_tables.get(ent_name, set())
                    dst_tables = type_to_tables.get(target, set())
                    if not src_tables or not dst_tables:
                        continue
                    # Only emit if both sides are in the selected tables
                    for st in src_tables:
                        for dt in dst_tables:
                            if st.split(".")[-1].lower() in table_short_set and dt.split(".")[-1].lower() in table_short_set:
                                resolved_sql = join_path
                                for old, new in [(ent_name.lower(), st.split(".")[-1]), (target.lower(), dt.split(".")[-1])]:
                                    resolved_sql = resolved_sql.replace(old, new)
                                specs.append({
                                    "id": uuid.uuid4().hex[:32],
                                    "left": {"identifier": st},
                                    "right": {"identifier": dt},
                                    "sql": [resolved_sql],
                                })
        return specs

    def _get_metric_views(self, tables: List[str]) -> list[dict]:
        table_list = ", ".join(f"'{t}'" for t in tables)
        short_names = ", ".join(f"'{t.split('.')[-1]}'" for t in tables)
        return _safe_sql(
            self.ws,
            self.wh,
            f"""
            SELECT metric_view_name, source_table, json_definition, status
            FROM {self._fq('metric_view_definitions')}
            WHERE status IN ('applied', 'validated')
              AND (source_table IN ({table_list}) OR source_table IN ({short_names}))
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
            fq_tbl = self._qualify(tbl)
            rows = _safe_sql(
                self.ws,
                self.wh,
                f"SELECT DISTINCT `{cn}` AS val FROM {fq_tbl} WHERE `{cn}` IS NOT NULL LIMIT 15",
            )
            if rows:
                samples.setdefault(tbl, {})[cn] = [r["val"] for r in rows]
        return samples

    # -- Synonym helpers -------------------------------------------------------

    def _generate_table_synonyms(self, table_name: str, meta: dict, entity: dict | None) -> list[str]:
        """Generate synonyms for a table from its name, domain, and entity type."""
        parts = table_name.split(".")[-1].replace("_", " ").split()
        syns = set()
        syns.add(" ".join(parts))
        domain = meta.get("domain", "")
        if domain:
            for ds in _DOMAIN_SYNONYMS.get(domain.lower(), []):
                syns.add(f"{ds} {parts[-1]}" if parts else ds)
        if entity:
            etype = entity.get("entity_type", "")
            if etype and etype.lower() != " ".join(parts).lower():
                syns.add(etype.replace("_", " "))
        syns.discard(table_name)
        return sorted(syns)[:4]

    def _generate_column_synonyms(self, col: dict, domain: str = "") -> list[str]:
        """Generate synonyms for a single column using all available signals."""
        return _generate_synonyms(
            col["column_name"],
            data_type=col.get("data_type", ""),
            comment=col.get("comment", ""),
            domain=domain,
        )

    # -- Formatting -----------------------------------------------------------

    def _format_context(
        self, table_meta, column_meta, fk_rows, entity_rows, value_samples,
        entity_rels=None,
    ) -> str:
        parts = []
        col_by_table: dict[str, list] = {}
        for c in column_meta:
            col_by_table.setdefault(c["table_name"], []).append(c)

        entity_map: dict[str, dict] = {}
        for e in entity_rows:
            src_tables = e.get("source_tables") or []
            if isinstance(src_tables, str):
                src_tables = [src_tables]
            for t in src_tables:
                entity_map[t] = e
                entity_map[t.split(".")[-1]] = e

        # Build per-entity-type relationship summary
        rel_summary: dict[str, list[str]] = {}
        for r in (entity_rels or []):
            src, dst, rel = r.get("src_type", ""), r.get("dst_type", ""), r.get("relationship", "")
            card = r.get("cardinality", "")
            if src and dst and rel:
                rel_summary.setdefault(src, []).append(f"{rel} -> {dst}" + (f" ({card})" if card else ""))

        # Build FK lookup for join info per table
        fk_by_table: dict[str, list[str]] = {}
        for fk in fk_rows:
            src_short = fk["src_table"].split(".")[-1]
            dst_short = fk["dst_table"].split(".")[-1]
            fk_by_table.setdefault(fk["src_table"], []).append(
                f"-> {dst_short} via {src_short}.{fk['src_column']}"
            )
            fk_by_table.setdefault(fk["dst_table"], []).append(
                f"<- {src_short} via {src_short}.{fk['src_column']}"
            )

        for t in table_meta:
            tname = t["table_name"]
            ent_info = entity_map.get(tname) or entity_map.get(tname.split(".")[-1])
            ent_type = ent_info["entity_type"] if ent_info else ""

            cols = col_by_table.get(tname, [])
            id_cols = [c["column_name"] for c in cols if c["column_name"].endswith("_id") or c["column_name"] == "id"]
            date_cols = [c["column_name"] for c in cols if c.get("data_type", "").upper() in ("DATE", "TIMESTAMP", "DATETIME")]
            numeric_cols = [c["column_name"] for c in cols
                           if c.get("data_type", "").upper().split("(")[0] in ("DECIMAL", "DOUBLE", "FLOAT", "INT", "BIGINT")
                           and not c["column_name"].endswith("_id") and c["column_name"] != "id"]
            string_cols = [c["column_name"] for c in cols if c.get("data_type", "").upper() in ("STRING", "VARCHAR")]

            # Classify: fact (has numeric measure columns + FK refs) vs dimension (mostly descriptive)
            is_fact = len(numeric_cols) >= 2 and len(id_cols) >= 2
            tbl_role = "FACT" if is_fact else "DIMENSION"
            pk_col = id_cols[0] if id_cols else "?"
            grain = ent_type or tname.split(".")[-1].rstrip("s")

            header = f"Table: {tname} [{tbl_role}]"
            if t.get("comment"):
                header += f'\n  Description: {t["comment"]}'
            if t.get("domain"):
                header += f"\n  Domain: {t['domain']}/{t.get('subdomain', '')}"
            header += f"\n  Grain: one row per {grain} (PK: {pk_col})"
            if ent_type:
                desc = ent_info.get("description", "")
                if desc:
                    header += f"\n  Entity: {ent_type} -- {desc}"
                rels_str = ", ".join(rel_summary.get(ent_type, []))
                if rels_str:
                    header += f"\n  Relationships: {rels_str}"

            # Structured column summary
            summary_parts = []
            if numeric_cols:
                summary_parts.append(f"Aggregatable measures: {', '.join(numeric_cols[:6])}")
            if date_cols:
                summary_parts.append(f"Date columns: {', '.join(date_cols[:4])}")
            if id_cols:
                summary_parts.append(f"Keys: {', '.join(id_cols[:4])}")
            if string_cols:
                summary_parts.append(f"Categorical: {', '.join(string_cols[:4])}")
            if summary_parts:
                header += "\n  " + "; ".join(summary_parts)

            # Joins available
            joins_avail = fk_by_table.get(tname, [])
            if joins_avail:
                header += "\n  Joins: " + ", ".join(joins_avail[:4])

            tbl_syns = self._generate_table_synonyms(tname, t, ent_info)
            if tbl_syns:
                header += f"\n  Synonyms: {', '.join(tbl_syns)}"

            domain = t.get("domain", "")
            col_lines = []
            for c in cols:
                line = f"  - {c['column_name']} {c.get('data_type', '')}"
                if c.get("comment"):
                    line += f" : {c['comment']}"
                if c.get("classification"):
                    line += f" [{c['classification']}]"
                col_syns = self._generate_column_synonyms(c, domain=domain)
                if col_syns:
                    line += f" (synonyms: {', '.join(col_syns)})"
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

        if entity_rels:
            parts.append("\nENTITY RELATIONSHIPS:")
            for r in entity_rels:
                card = r.get("cardinality", "")
                parts.append(f"  {r.get('src_type', '')} --{r.get('relationship', '')}--> {r.get('dst_type', '')}" + (f" ({card})" if card else ""))

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
        self, table_ids: List[str], table_meta: list[dict], metric_views: list[dict],
        column_meta: list[dict] | None = None, entity_rows: list[dict] | None = None,
    ) -> dict:
        meta_map = {}
        for t in table_meta:
            name = t["table_name"]
            meta_map[name] = t
            meta_map[name.split(".")[-1]] = t

        cols_by_table: dict[str, list] = {}
        for c in (column_meta or []):
            cols_by_table.setdefault(c["table_name"], []).append(c)
            cols_by_table.setdefault(c["table_name"].split(".")[-1], []).append(c)

        entity_map: dict[str, dict] = {}
        for e in (entity_rows or []):
            for t in (e.get("source_tables") or []):
                if isinstance(t, str):
                    entity_map[t] = e
                    entity_map[t.split(".")[-1]] = e

        tables = []
        for tid in table_ids:
            entry: dict[str, Any] = {"identifier": tid}
            meta = meta_map.get(tid) or meta_map.get(tid.split(".")[-1], {})
            desc_parts = []
            if meta.get("comment"):
                desc_parts.append(meta["comment"])
            domain = meta.get("domain")
            if domain:
                sub = meta.get("subdomain", "")
                desc_parts.append(f"Domain: {domain}" + (f"/{sub}" if sub else ""))
            ent = entity_map.get(tid) or entity_map.get(tid.split(".")[-1])
            if ent:
                role = ent.get("entity_role", "primary")
                desc_parts.append(f"Entity: {ent.get('entity_type', '')} ({role})")
            cols = cols_by_table.get(tid, cols_by_table.get(tid.split(".")[-1], []))
            if cols:
                id_cols = [c["column_name"] for c in cols if c["column_name"].endswith("_id") or c["column_name"] == "id"]
                date_cols = [c["column_name"] for c in cols if c.get("data_type", "").upper() in ("DATE", "TIMESTAMP", "DATETIME")]
                numeric_cols = [c["column_name"] for c in cols if c.get("data_type", "").upper().split("(")[0] in ("DECIMAL", "DOUBLE", "FLOAT", "INT", "BIGINT")]
                summary_parts = []
                if id_cols:
                    summary_parts.append(f"IDs: {', '.join(id_cols[:4])}")
                if date_cols:
                    summary_parts.append(f"Dates: {', '.join(date_cols[:4])}")
                if numeric_cols:
                    summary_parts.append(f"Measures: {', '.join(numeric_cols[:4])}")
                desc_parts.append(f"Columns ({len(cols)}): {'; '.join(summary_parts) if summary_parts else ', '.join(c['column_name'] for c in cols[:6])}")
            if desc_parts:
                entry["description"] = desc_parts
            tables.append(entry)

        mvs = []
        for mv in metric_views:
            desc_lines = []
            try:
                defn = json.loads(mv["json_definition"]) if isinstance(mv.get("json_definition"), str) else mv.get("json_definition") or {}
                if defn.get("comment"):
                    desc_lines.append(defn["comment"])
                dims = defn.get("dimensions", [])
                measures = defn.get("measures", [])
                if dims or measures:
                    names = [d.get("name") for d in dims if d.get("name")] + [m.get("name") for m in measures if m.get("name")]
                    if names:
                        desc_lines.append(f"Dimensions/measures: {', '.join(names[:12])}{'...' if len(names) > 12 else ''}")
            except (json.JSONDecodeError, TypeError):
                pass
            if not desc_lines:
                desc_lines = [f"Metric view on {mv.get('source_table', '')}"]
            mvs.append(
                {
                    "identifier": f"{self.catalog}.{self.schema}.{mv['metric_view_name']}",
                    "description": desc_lines,
                }
            )

        result: dict[str, Any] = {"tables": tables}
        if mvs:
            result["metric_views"] = mvs
        return result

    def _qualify(self, name: str) -> str:
        """Ensure a table name is fully qualified (catalog.schema.table)."""
        if name.count(".") >= 2:
            return name
        return f"{self.catalog}.{self.schema}.{name}"

    @staticmethod
    def _qualify_columns_in_expr(
        expr: str, table_short: str, known_cols: set[str]
    ) -> str:
        """Prefix bare column references with table short name for Genie API."""
        import re

        def _replacer(m):
            token = m.group(0)
            if token.lower() in known_cols:
                return f"{table_short}.{token}"
            return token

        sql_kws = {
            "sum",
            "count",
            "avg",
            "min",
            "max",
            "distinct",
            "case",
            "when",
            "then",
            "else",
            "end",
            "and",
            "or",
            "not",
            "null",
            "true",
            "false",
            "in",
            "is",
            "like",
            "between",
            "as",
            "cast",
            "if",
            "coalesce",
            "nullif",
            "filter",
            "where",
            "date_trunc",
            "extract",
            "concat",
            "upper",
            "lower",
            "trim",
            "over",
            "partition",
            "by",
            "order",
            "asc",
            "desc",
            "timestampdiff",
            "timestampadd",
            "datediff",
            "year",
            "quarter",
            "month",
            "week",
            "day",
            "hour",
            "minute",
            "second",
        }
        known_cols_lower = {c.lower() for c in known_cols}

        def _replace_token(m):
            tok = m.group(0)
            if tok.lower() in sql_kws:
                return tok
            if tok.lower() in known_cols_lower:
                return f"{table_short}.{tok}"
            return tok

        return re.sub(r"(?<![`.\w])([A-Za-z_]\w*)(?![`.\w(])", _replace_token, expr)

    def _build_sql_snippets(
        self,
        metric_views: list[dict],
        value_samples: dict,
        column_meta: list[dict] | None = None,
    ) -> dict:
        """Decompose metric view definitions into Genie sql_snippets structure."""
        measures: list[dict] = []
        filters: list[dict] = []
        expressions: list[dict] = []

        cols_by_table: dict[str, set[str]] = {}
        for c in column_meta or []:
            tbl = c.get("table_name", "")
            cols_by_table.setdefault(tbl, set()).add(c["column_name"].lower())
            cols_by_table.setdefault(tbl.split(".")[-1], set()).add(
                c["column_name"].lower()
            )

        for mv in metric_views:
            raw = mv.get("json_definition", "")
            try:
                defn = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                continue

            source = defn.get("source", mv.get("source_table", ""))
            tbl_alias = source.split(".")[-1] if source else ""
            known_cols = cols_by_table.get(tbl_alias, cols_by_table.get(source, set()))

            for m in defn.get("measures", []):
                expr = m.get("expr", "")
                if expr.strip().upper() in ("COUNT(*)", "COUNT(1)"):
                    continue
                if known_cols and tbl_alias:
                    expr = self._qualify_columns_in_expr(expr, tbl_alias, known_cols)
                alias = m.get("name", "")
                syns = m.get("synonyms") or _generate_synonyms(alias, comment=m.get("comment", ""))
                measures.append(
                    {
                        "alias": alias,
                        "display_name": m.get("display_name") or alias,
                        "sql": [expr],
                        "description": m.get("comment", f"From {tbl_alias}"),
                        "synonyms": syns or None,
                    }
                )

            for d in defn.get("dimensions", []):
                expr = d.get("expr", "")
                if expr and expr != d.get("name", ""):
                    if known_cols and tbl_alias:
                        expr = self._qualify_columns_in_expr(
                            expr, tbl_alias, known_cols
                        )
                    d_alias = d.get("name", "")
                    d_syns = d.get("synonyms") or _generate_synonyms(d_alias, comment=d.get("comment", ""))
                    expressions.append(
                        {
                            "alias": d_alias,
                            "display_name": d.get("display_name") or d_alias,
                            "sql": [expr],
                            "description": d.get("comment", ""),
                            "synonyms": d_syns or None,
                        }
                    )

            if defn.get("filter"):
                filt_expr = defn["filter"]
                if known_cols and tbl_alias:
                    filt_expr = self._qualify_columns_in_expr(
                        filt_expr, tbl_alias, known_cols
                    )
                filters.append(
                    {
                        "display_name": f"{tbl_alias} default filter",
                        "sql": [filt_expr],
                    }
                )

        for tbl, cols in value_samples.items():
            tbl_short = tbl.split(".")[-1]
            for col, vals in cols.items():
                if 2 <= len(vals) <= 10:
                    escaped = [str(v).replace("'", "''") for v in vals[:5]]
                    if len(escaped) == 1:
                        sql_fragment = f"{tbl_short}.{col} = '{escaped[0]}'"
                    else:
                        in_list = ", ".join(f"'{v}'" for v in escaped)
                        sql_fragment = f"{tbl_short}.{col} IN ({in_list})"
                    filters.append(
                        {
                            "display_name": f"Filter by {col} ({tbl_short})",
                            "sql": [sql_fragment],
                        }
                    )

        return {
            "measures": measures,
            "filters": filters[:20],
            "expressions": expressions,
        }
