"""Parse companion DCAT/TTL metadata (rdl:foreignKey, rdl:hasPrimaryKey) for FK ingestion."""

from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

RDL = "http://example.org/rdl/"
SCHEMA = "http://schema.org/"
DCAT = "http://www.w3.org/ns/dcat#"


def resolve_table_fqn(schema_table: str, kb_tables: List[str]) -> Optional[str]:
    """Map ``schema.table`` from companion TTL to a KB ``catalog.schema.table`` FQN."""
    st = schema_table.strip().lower()
    if not st:
        return None
    for t in kb_tables:
        parts = t.split(".")
        if len(parts) >= 3 and ".".join(parts[-2:]).lower() == st:
            return t
    return None


def resolve_fk_target_column(src_col: str, target_pks: List[str]) -> str:
    """Pick the referenced PK column (e.g. disease_uri_parent -> disease_uri)."""
    if not target_pks:
        return src_col
    if src_col in target_pks:
        return src_col
    if src_col.endswith("_parent"):
        base = src_col[: -len("_parent")]
        if base in target_pks:
            return base
    return target_pks[0]


def _parse_pk_literal(raw: str) -> List[str]:
    return [c.strip() for c in raw.split(",") if c.strip()]


def parse_companion_metadata(ttl_path: str) -> Dict[str, dict]:
    """Parse companion TTL into per-dataset constraint dicts keyed by schema.tableName."""
    try:
        import rdflib
        from rdflib import Graph, URIRef
    except ImportError:
        logger.warning("rdflib not installed; cannot parse companion metadata at %s", ttl_path)
        return {}

    g = Graph()
    g.parse(ttl_path, format="turtle")
    RDL_HAS_PK = URIRef(RDL + "hasPrimaryKey")
    RDL_FOREIGN_KEY = URIRef(RDL + "foreignKey")
    RDL_COLUMN = URIRef(RDL + "column")
    RDL_REFERENCES = URIRef(RDL + "references")
    SCHEMA_TABLE = URIRef(SCHEMA + "tableName")

    datasets: Dict[str, dict] = {}
    uri_to_schema_table: Dict[str, str] = {}

    for ds in g.subjects(rdflib.RDF.type, URIRef(DCAT + "Dataset")):
        table_lit = g.value(ds, SCHEMA_TABLE)
        if not table_lit:
            continue
        schema_table = str(table_lit).strip().lower()
        entry: dict = {"primary_key_columns": [], "foreign_keys": {}}
        pk_lit = g.value(ds, RDL_HAS_PK)
        if pk_lit:
            entry["primary_key_columns"] = _parse_pk_literal(str(pk_lit))
        for fk_node in g.objects(ds, RDL_FOREIGN_KEY):
            col_lit = g.value(fk_node, RDL_COLUMN)
            ref_uri = g.value(fk_node, RDL_REFERENCES)
            if col_lit and ref_uri:
                entry["foreign_keys"][str(col_lit).strip()] = str(ref_uri)
        datasets[schema_table] = entry
        uri_to_schema_table[str(ds)] = schema_table

    for schema_table, entry in datasets.items():
        resolved_fks: Dict[str, str] = {}
        for col, ref_uri in entry.get("foreign_keys", {}).items():
            target_st = uri_to_schema_table.get(str(ref_uri))
            if target_st:
                resolved_fks[col] = target_st
        entry["foreign_keys"] = resolved_fks

    return datasets


def build_companion_constraints(
    ttl_path: str,
    kb_tables: List[str],
) -> Dict[str, Tuple[List[str], Dict[str, str]]]:
    """Return ``table_fqn -> (pk_columns, foreign_keys map to 4-part ref_target)``."""
    raw = parse_companion_metadata(ttl_path)
    if not raw:
        return {}

    st_to_fqn: Dict[str, str] = {}
    for schema_table in raw:
        fqn = resolve_table_fqn(schema_table, kb_tables)
        if fqn:
            st_to_fqn[schema_table] = fqn

    pk_by_fqn: Dict[str, List[str]] = {}
    for schema_table, entry in raw.items():
        fqn = st_to_fqn.get(schema_table)
        if fqn and entry.get("primary_key_columns"):
            pk_by_fqn[fqn] = entry["primary_key_columns"]

    out: Dict[str, Tuple[List[str], Dict[str, str]]] = {}
    for schema_table, entry in raw.items():
        src_fqn = st_to_fqn.get(schema_table)
        if not src_fqn:
            continue
        fk_map: Dict[str, str] = {}
        for src_col, target_st in entry.get("foreign_keys", {}).items():
            tgt_fqn = st_to_fqn.get(target_st.lower())
            if not tgt_fqn:
                continue
            target_pks = pk_by_fqn.get(tgt_fqn, raw.get(target_st, {}).get("primary_key_columns", []))
            tgt_col = resolve_fk_target_column(src_col, target_pks)
            fk_map[src_col] = f"{tgt_fqn}.{tgt_col}"
        pks = entry.get("primary_key_columns") or []
        out[src_fqn] = (pks, fk_map)
    return out


def merge_constraint_maps(
    uc: Dict[str, Tuple[List[str], Dict[str, str]]],
    companion: Dict[str, Tuple[List[str], Dict[str, str]]],
) -> Dict[str, Tuple[List[str], Dict[str, str]]]:
    """Merge companion constraints into UC; UC wins on conflicts."""
    merged = dict(companion)
    for table, (uc_pks, uc_fks) in uc.items():
        comp_pks, comp_fks = merged.get(table, ([], {}))
        pks = uc_pks if uc_pks else comp_pks
        fks = dict(comp_fks)
        for col, ref in uc_fks.items():
            if ref:
                fks[col] = ref
        merged[table] = (pks, fks)
    for table, (uc_pks, uc_fks) in uc.items():
        if table not in merged:
            merged[table] = (uc_pks, uc_fks)
    return merged
