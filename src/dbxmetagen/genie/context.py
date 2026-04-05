"""Deterministic context assembly for Genie space generation.

Gathers all pre-computed metadata from knowledge bases, FK predictions,
ontology entities, and metric view definitions, then packages it into
a structured context string for the multi-phase LLM generation pipeline.
"""

import json
import logging
import os
import re
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
        err = str(e)
        if "TABLE_OR_VIEW_NOT_FOUND" in err or "SCHEMA_NOT_FOUND" in err:
            logger.info("Table not found (expected before pipeline runs): %s", query[:80])
        else:
            logger.warning("SQL query failed (non-404): %s — %s", query[:80], e)
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


def _translate_mv_aliases(expr: str, source_short: str, joins: list) -> str:
    """Replace metric-view 'source.' and join alias prefixes with real table names."""
    expr = re.sub(r'\bsource\.', f'{source_short}.', expr)
    for j in joins:
        alias = j.get("name", "")
        target = (j.get("source") or j.get("table", "")).split(".")[-1]
        if alias and target and alias.lower() != source_short.lower():
            expr = re.sub(rf'\b{re.escape(alias)}\.', f'{target}.', expr)
    return expr


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
        self._cached_entity_rows = entity_rows
        entity_rels = self._get_entity_relationships(table_identifiers)
        metric_views, mv_warnings = self._get_metric_views(table_identifiers)
        if metric_view_names is not None:
            names_set = set(metric_view_names)
            metric_views = [mv for mv in metric_views if mv.get("metric_view_name") in names_set]
        value_samples = self._sample_categorical_values(column_meta)

        applied_mvs = [mv for mv in metric_views if mv.get("status") == "applied"]
        unapplied_mvs = [mv for mv in metric_views if mv.get("status") != "applied"]
        if metric_view_names is not None and unapplied_mvs:
            unapplied_names = [mv["metric_view_name"] for mv in unapplied_mvs if mv.get("metric_view_name") in names_set]
            if unapplied_names:
                mv_warnings.append(
                    f"{len(unapplied_names)} metric view(s) not yet applied to UC (will contribute SQL snippets only, not Genie data sources): {', '.join(unapplied_names)}"
                )

        context_text = self._format_context(
            table_meta, column_meta, fk_rows, entity_rows, value_samples, entity_rels
        )
        join_specs = self._build_join_specs(fk_rows)
        # Build dedup set using short names (handles FQ vs short identifier mismatches)
        existing_pairs: set[tuple] = set()
        for js in join_specs:
            l = js["left"]["identifier"].split(".")[-1].lower()
            r = js["right"]["identifier"].split(".")[-1].lower()
            existing_pairs.add(tuple(sorted([l, r])))
        # Merge ontology joins
        ontology_joins = self._get_ontology_join_specs(table_identifiers)
        for oj in ontology_joins:
            l = oj["left"]["identifier"].split(".")[-1].lower()
            r = oj["right"]["identifier"].split(".")[-1].lower()
            pair = tuple(sorted([l, r]))
            if pair not in existing_pairs:
                join_specs.append(oj)
                existing_pairs.add(pair)
        # Merge metric view joins (all MVs, not just applied — joins are structural)
        selected_shorts = {t.split(".")[-1].lower() for t in table_identifiers}
        mv_join_specs = self._extract_mv_join_specs(metric_views, existing_pairs, selected_shorts)
        join_specs.extend(mv_join_specs)
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
            "assembler_warnings": mv_warnings,
            "requested_mv_count": len(metric_view_names) if metric_view_names is not None else None,
        }

    def _load_genie_reference(self) -> str:
        """Load genie_reference.json and format into prompt text."""
        candidates = [
            os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "configurations", "agent_references", "genie_reference.json"),
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
        # Only include the most essential reference sections to keep prompt lean.
        # date_function_rules are already in the system prompt rules section.
        parts = []
        for section_key in ["sql_snippet_patterns", "date_function_rules"]:
            val = ref.get(section_key)
            if not val:
                continue
            if isinstance(val, dict):
                desc = val.get("description", "")
                header = f"\n### {section_key}"
                if desc:
                    header += f"\n{desc}"
                # Compact JSON (no indent) to save tokens
                parts.append(header + "\n" + json.dumps(
                    {k: v for k, v in val.items() if k != "description"},
                ))
            elif isinstance(val, str):
                parts.append(f"\n### {section_key}\n{val}")
            else:
                parts.append(f"\n### {section_key}\n{json.dumps(val)}")
        result = "\n".join(parts) if parts else ""
        # Hard cap to prevent bloated prompts
        if len(result) > 4000:
            result = result[:4000] + "\n[truncated]"
        return result

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
            f"source_columns, confidence, COALESCE(entity_role, 'primary') AS entity_role, "
            f"entity_uri, source_ontology "
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
        entity_rows = getattr(self, "_cached_entity_rows", None) or self._get_ontology_entities(tables)
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

        entity_rows = getattr(self, "_cached_entity_rows", None) or self._get_ontology_entities(tables)
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

    @staticmethod
    def _resolve_mv_location(mv, fallback_catalog, fallback_schema):
        """Resolve catalog/schema for an MV: deployed cols > assembler fallback.

        The fallback catalog/schema (from the assembler) is the metagen config
        schema, which is where ``apply_metric_views`` deploys by default.
        source_table points to the DATA table, not the metric view object, so
        it must not be used as a location fallback.
        """
        cat = mv.get("deployed_catalog")
        sch = mv.get("deployed_schema")
        return cat or fallback_catalog, sch or fallback_schema

    def _get_metric_views(self, tables: List[str]) -> tuple[list[dict], list[str]]:
        # Build FQ and short match sets separately to prefer exact FQ matches
        fq_values = set()
        short_values = set()
        for t in tables:
            fq_values.add(t)
            parts = t.split(".")
            if len(parts) == 3:
                fq_values.add(f"{parts[1]}.{parts[2]}")
                short_values.add(parts[2])
            elif len(parts) == 2:
                short_values.add(parts[1])
        all_values = fq_values | short_values
        match_list = ", ".join(f"'{v}'" for v in all_values)
        rows = _safe_sql(
            self.ws,
            self.wh,
            f"""
            SELECT metric_view_name, source_table, json_definition, status,
                   deployed_catalog, deployed_schema
            FROM {self._fq('metric_view_definitions')}
            WHERE status IN ('applied', 'validated')
              AND source_table IN ({match_list})
        """,
        )
        # Deduplicate: when a short name (e.g. "orders") maps to tables in
        # multiple schemas, MVs matched only via that short name are ambiguous.
        # Count how many distinct FQ tables share each short name; warn on ambiguous.
        short_name_counts: dict[str, int] = {}
        for t in tables:
            bare = t.split(".")[-1]
            short_name_counts[bare] = short_name_counts.get(bare, 0) + 1
        ambiguous_shorts = {s for s, c in short_name_counts.items() if c > 1}
        if ambiguous_shorts:
            deduped = []
            for mv in rows:
                src = mv.get("source_table", "")
                if src in fq_values:
                    deduped.append(mv)
                elif src not in ambiguous_shorts:
                    deduped.append(mv)
                else:
                    logger.warning(
                        "Skipping MV '%s' with ambiguous short source_table '%s' "
                        "(matches tables in multiple schemas)",
                        mv.get("metric_view_name"), src,
                    )
            rows = deduped
        warnings: list[str] = []
        applied = [mv for mv in rows if mv.get("status") == "applied"]
        non_applied = [mv for mv in rows if mv.get("status") != "applied"]
        if not applied:
            return non_applied, warnings

        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _check(mv):
            cat, sch = self._resolve_mv_location(mv, self.catalog, self.schema)
            fq_name = f"`{cat}`.`{sch}`.`{mv['metric_view_name']}`"
            try:
                r = self.ws.statement_execution.execute_statement(
                    warehouse_id=self.wh,
                    statement=f"SELECT 1 FROM {fq_name} LIMIT 1",
                    wait_timeout="15s",
                    format=Format.JSON_ARRAY,
                    disposition=Disposition.INLINE,
                )
                state = r.status.state.value if r.status and r.status.state else "UNKNOWN"
                if state in ("SUCCEEDED", "CLOSED"):
                    return mv, None
                return None, f"Metric view '{mv['metric_view_name']}' not queryable at {fq_name} (state: {state})"
            except Exception as e:
                logger.warning("Metric view %s existence check failed: %s", mv["metric_view_name"], e)
                return None, f"Metric view '{mv['metric_view_name']}' existence check failed at {fq_name}"

        verified = list(non_applied)
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(_check, mv): mv for mv in applied}
            for f in as_completed(futures):
                try:
                    result, warn = f.result(timeout=20)
                    if result:
                        verified.append(result)
                    elif warn:
                        warnings.append(warn)
                except Exception as e:
                    mv = futures[f]
                    warnings.append(f"Metric view '{mv['metric_view_name']}' check timed out")
        return verified, warnings

    def _sample_categorical_values(
        self, columns: list[dict]
    ) -> dict[str, dict[str, list]]:
        """Sample distinct values for STRING columns (useful for filter suggestions)."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        samples: dict[str, dict[str, list]] = {}
        string_cols = [
            c
            for c in columns
            if c.get("data_type", "").upper() in ("STRING", "VARCHAR")
        ]
        capped = string_cols[:20]
        if not capped:
            return samples

        def _fetch(col):
            tbl = col["table_name"]
            cn = col["column_name"]
            fq_tbl = self._qualify(tbl)
            rows = _safe_sql(
                self.ws, self.wh,
                f"SELECT DISTINCT `{cn}` AS val FROM {fq_tbl} WHERE `{cn}` IS NOT NULL LIMIT 15",
            )
            return tbl, cn, [r["val"] for r in rows] if rows else []

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(_fetch, c): c for c in capped}
            for f in as_completed(futures):
                try:
                    tbl, cn, vals = f.result(timeout=30)
                    if vals:
                        samples.setdefault(tbl, {})[cn] = vals
                except Exception:
                    pass  # skip failed columns silently
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

        entity_map: dict[str, list[dict]] = {}
        for e in entity_rows:
            src_tables = e.get("source_tables") or []
            if isinstance(src_tables, str):
                src_tables = [src_tables]
            for t in src_tables:
                entity_map.setdefault(t, []).append(e)
                entity_map.setdefault(t.split(".")[-1], []).append(e)

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
            ent_list = entity_map.get(tname) or entity_map.get(tname.split(".")[-1]) or []
            ent_info = ent_list[0] if ent_list else None
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
            for ei in ent_list:
                et = ei.get("entity_type", "")
                if not et:
                    continue
                desc = ei.get("description", "")
                if desc:
                    header += f"\n  Entity: {et} -- {desc}"
                else:
                    header += f"\n  Entity: {et}"
                if ei.get("source_ontology"):
                    header += f"\n  Standard: {ei['source_ontology']}"
                rels_str = ", ".join(rel_summary.get(et, []))
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

    def _extract_mv_join_specs(
        self, metric_views: list[dict], existing_pairs: set[tuple],
        selected_short_names: set[str] | None = None,
    ) -> list[dict]:
        """Extract join_specs from metric view json_definition.joins[].

        Only emits a join when BOTH tables are in the selected data sources.
        FK/ontology joins take priority (existing_pairs checked first).
        """
        specs = []
        for mv in metric_views:
            jd = mv.get("json_definition") or {}
            if isinstance(jd, str):
                try:
                    jd = json.loads(jd)
                except (json.JSONDecodeError, TypeError):
                    continue
            joins = jd.get("joins", [])
            source_table = mv.get("source_table", "")
            left_short = source_table.split(".")[-1].lower()
            for j in joins:
                target_fq = j.get("source", "") or j.get("table", "")
                if not target_fq:
                    continue
                right_short = target_fq.split(".")[-1].lower()
                # Only emit if target table is in the selected data sources
                if selected_short_names and right_short not in selected_short_names:
                    continue
                pair = tuple(sorted([left_short, right_short]))
                if pair in existing_pairs:
                    continue
                on_clause = j.get("on", "")
                alias = j.get("name", right_short)
                sql = on_clause.replace("source.", f"{left_short}.")
                if alias.lower() != right_short:
                    sql = sql.replace(f"{alias}.", f"{right_short}.")
                if sql:
                    existing_pairs.add(pair)
                    specs.append({
                        "id": uuid.uuid4().hex[:32],
                        "left": {"identifier": source_table},
                        "right": {"identifier": target_fq},
                        "sql": [sql],
                    })
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

        entity_map_ds: dict[str, list[dict]] = {}
        for e in (entity_rows or []):
            for t in (e.get("source_tables") or []):
                if isinstance(t, str):
                    entity_map_ds.setdefault(t, []).append(e)
                    entity_map_ds.setdefault(t.split(".")[-1], []).append(e)

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
            ent_list_ds = entity_map_ds.get(tid) or entity_map_ds.get(tid.split(".")[-1]) or []
            for ent in ent_list_ds:
                role = ent.get("entity_role", "primary")
                ent_desc = f"Entity: {ent.get('entity_type', '')} ({role})"
                if ent.get("source_ontology"):
                    ent_desc += f" [{ent['source_ontology']}]"
                desc_parts.append(ent_desc)
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
            mv_cat, mv_sch = self._resolve_mv_location(mv, self.catalog, self.schema)
            mvs.append(
                {
                    "identifier": f"{mv_cat}.{mv_sch}.{mv['metric_view_name']}",
                    "description": desc_lines,
                }
            )

        result: dict[str, Any] = {"tables": tables, "metric_views": mvs}
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
            mv_joins = defn.get("joins", [])

            for m in defn.get("measures", []):
                expr = m.get("expr", "")
                if expr.strip().upper() in ("COUNT(*)", "COUNT(1)"):
                    continue
                expr = _translate_mv_aliases(expr, tbl_alias, mv_joins)
                if known_cols and tbl_alias:
                    expr = self._qualify_columns_in_expr(expr, tbl_alias, known_cols)
                alias = m.get("name", "")
                syns = m.get("synonyms") or _generate_synonyms(alias, comment=m.get("comment", ""))
                comment = m.get("comment", "")
                measures.append(
                    {
                        "alias": alias,
                        "display_name": m.get("display_name") or alias,
                        "sql": [expr],
                        "description": comment or f"From {tbl_alias}",
                        "synonyms": syns or None,
                    }
                )

            for d in defn.get("dimensions", []):
                expr = d.get("expr", "")
                if expr and expr != d.get("name", ""):
                    expr = _translate_mv_aliases(expr, tbl_alias, mv_joins)
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
                filt_expr = _translate_mv_aliases(filt_expr, tbl_alias, mv_joins)
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


# ---------------------------------------------------------------------------
# Section-scoped AI assist for the Genie Space Updater
# ---------------------------------------------------------------------------

_SECTION_PROMPTS = {
    "joins": (
        "Generate join_specs for the given tables. Each join_spec needs:\n"
        '  {{"left": {{"identifier": "catalog.schema.table1"}}, '
        '"right": {{"identifier": "catalog.schema.table2"}}, '
        '"sql": ["table1.col = table2.col"]}}\n'
        "Return a JSON array of join_spec objects."
    ),
    "instructions": (
        "Generate text_instructions as a markdown string for the given tables. "
        "Include data relationships, business rules, disambiguation of common columns, "
        "and usage tips. Return as: {{\"text_instructions\": \"...\"}}"
    ),
    "questions": (
        "Generate sample_questions for business users of these tables. "
        "Return as: {{\"sample_questions\": [\"question1\", \"question2\", ...]}}"
    ),
    "measures": (
        "Generate SQL snippet measures for the given tables. Each measure needs:\n"
        '  {{"alias": "...", "display_name": "...", "sql": ["..."], "synonyms": ["..."], "description": "..."}}\n'
        "Return as: {{\"measures\": [...]}}"
    ),
    "filters": (
        "Generate SQL snippet filters for the given tables. Each filter needs:\n"
        '  {{"display_name": "...", "sql": ["..."]}}\n'
        "Return as: {{\"filters\": [...]}}"
    ),
    "expressions": (
        "Generate SQL snippet expressions (computed columns) for the given tables. Each needs:\n"
        '  {{"alias": "...", "display_name": "...", "sql": ["..."], "synonyms": ["..."]}}\n'
        "Return as: {{\"expressions\": [...]}}"
    ),
    "example_sql": (
        "Generate example_question_sqls for these tables. Each needs:\n"
        '  {{"question": ["..."], "sql": ["SELECT ..."]}}\n'
        "Return as: {{\"example_question_sqls\": [...]}}"
    ),
    "synonyms": (
        "Generate synonyms for columns and measures in the given tables. "
        "Return as: {{\"synonyms\": {{\"column_or_measure_name\": [\"syn1\", \"syn2\"]}}}}"
    ),
}


def generate_section_assist(
    ws: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    section: str,
    table_identifiers: list[str],
    existing_items: list | dict | None = None,
    user_prompt: str = "",
    model_endpoint: str | None = None,
) -> dict:
    """Generate a single section of a Genie space definition via a targeted LLM call."""
    from langchain_community.chat_models import ChatDatabricks

    model = model_endpoint or os.environ.get("LLM_MODEL", "databricks-claude-sonnet-4-6")
    llm = ChatDatabricks(endpoint=model, temperature=0.1, max_tokens=8192, max_retries=1, request_timeout=120)

    assembler = GenieContextAssembler(ws, warehouse_id, catalog, schema)
    ctx = assembler.assemble(table_identifiers)

    section_prompt = _SECTION_PROMPTS.get(section, _SECTION_PROMPTS["instructions"])
    existing_text = ""
    if existing_items:
        existing_text = (
            f"\n\nExisting items (do NOT duplicate, only generate NEW ones):\n"
            f"{json.dumps(existing_items, indent=2)[:4000]}"
        )
    user_extra = f"\n\nUser request: {user_prompt}" if user_prompt else ""

    system = (
        f"You are a Genie Space configuration expert. Generate ONLY the requested section.\n\n"
        f"=== METADATA CONTEXT ===\n{ctx['context_text'][:8000]}\n\n"
        f"=== TASK ===\n{section_prompt}{existing_text}{user_extra}\n\n"
        f"Output ONLY valid JSON wrapped in ```json ``` fences. No explanation."
    )
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": f"Generate the {section} section now."},
    ]
    try:
        result = llm.invoke(messages)
        content = getattr(result, "content", "") or ""
        import re
        m = re.search(r"```(?:json)?\s*\n?(.*?)```", content, re.DOTALL)
        raw = m.group(1).strip() if m else content.strip()
        return json.loads(raw)
    except Exception as e:
        logger.warning("Section assist failed for %s: %s", section, e)
        return {"error": str(e)}
