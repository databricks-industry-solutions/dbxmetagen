"""Extract and resolve OWL-imported FK/PK constraints for FK prediction.

Emitted at OWL import time as ``declared_constraints`` on the bundle YAML,
resolved to table FQNs after entity discovery in ``merge_owl_declared_fks``.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from dbxmetagen.metadata_import import resolve_fk_target_column
from dbxmetagen.ontology_properties import attribute_name_variants

logger = logging.getLogger(__name__)

# OWL import may set target_entity to XSD local names (e.g. xsd:string -> "string").
_NON_ENTITY_FK_TARGETS = frozenset({
    "string", "integer", "int", "long", "boolean", "float", "double", "decimal",
    "date", "datetime", "time", "anyuri", "uri", "id",
})


def dedupe_string_map(
    m: Optional[Dict[Any, Any]],
    *,
    context: str = "",
) -> Dict[str, str]:
    """Normalize a string map for Spark MAP columns (unique, non-blank keys)."""
    if not m:
        return {}
    bucket: Dict[str, Tuple[str, str]] = {}
    for k, v in m.items():
        if k is None:
            continue
        key = str(k).strip()
        if not key:
            continue
        val = str(v).strip() if v is not None else ""
        if not val:
            continue
        low = key.lower()
        if low in bucket and bucket[low][1] != val:
            logger.debug(
                "dedupe_string_map: key collision %r in %s (%r -> %r, was %r)",
                low, context or "map", key, val, bucket[low][1],
            )
        bucket[low] = (key, val)
    return {orig: val for orig, val in bucket.values()}


def is_uri_key_property(prop_name: str) -> bool:
    """True for natural identifier URI columns (diseaseUri), not parents."""
    if not prop_name:
        return False
    low = prop_name.lower()
    if "parent" in low:
        return False
    return low.endswith("uri") or low.endswith("_uri")


def is_uri_parent_property(prop_name: str) -> bool:
    if not prop_name:
        return False
    low = prop_name.lower()
    return "parent" in low and ("uri" in low or low.endswith("parent"))


def _parent_base_variants(prop_name: str) -> List[str]:
    """diseaseUriParent -> variants for disease_uri."""
    snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", prop_name)
    snake = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", snake).lower()
    for suffix in ("_uri_parent", "uri_parent", "_parent", "parent"):
        if snake.endswith(suffix):
            base = snake[: -len(suffix)]
            if base:
                camel_base = base.replace("_", "")
                return attribute_name_variants(camel_base + "Uri")
    return []


def _entity_uri_pk_columns(defn: Dict[str, Any]) -> List[str]:
    """Return snake_case URI PK column names declared on an entity."""
    out: List[str] = []
    for prop in (defn.get("properties") or {}).values():
        if prop.get("role") == "primary_key":
            for attr in prop.get("typical_attributes") or []:
                low = str(attr).lower()
                if low.endswith("_uri") or low.endswith("uri"):
                    out.append(low)
    return out


def apply_uri_property_overrides(entities: Dict[str, Dict[str, Any]]) -> None:
    """Set explicit PK / object_property roles for URI DatatypeProperties at import."""
    uri_pk_by_entity: Dict[str, List[str]] = {}

    for ent_name, defn in entities.items():
        for attr in defn.get("typical_attributes") or []:
            if not is_uri_key_property(str(attr)):
                continue
            variants = attribute_name_variants(str(attr))
            props = defn.setdefault("properties", {})
            props[str(attr)] = {
                "kind": "data_property",
                "role": "primary_key",
                "typical_attributes": variants,
            }
            uri_pk_by_entity[ent_name] = [v.lower() for v in variants]

    hierarchy_entities = [
        e for e, cols in uri_pk_by_entity.items()
        if cols and "hierarchy" in e.lower()
    ]
    default_hierarchy = hierarchy_entities[0] if hierarchy_entities else None
    if not default_hierarchy:
        default_hierarchy = next(iter(uri_pk_by_entity), None)

    for ent_name, defn in entities.items():
        props = defn.setdefault("properties", {})
        for attr in defn.get("typical_attributes") or []:
            if not is_uri_parent_property(str(attr)):
                continue
            base_vars = _parent_base_variants(str(attr))
            target_entity = None
            if base_vars:
                base_stem = base_vars[0].lower().split("_uri")[0].split("uri")[0]
                best: Optional[Tuple[int, str]] = None
                for cand_ent, pk_cols in uri_pk_by_entity.items():
                    if not pk_cols:
                        continue
                    if not any(
                        bv.lower() in pk_cols or pk_cols[0].startswith(base_stem)
                        for bv in base_vars
                    ):
                        continue
                    rank = 0 if "hierarchy" in cand_ent.lower() else 1
                    if best is None or rank < best[0]:
                        best = (rank, cand_ent)
                if best:
                    target_entity = best[1]
            if not target_entity and default_hierarchy:
                target_entity = default_hierarchy
            variants = attribute_name_variants(str(attr))
            entry: Dict[str, Any] = {
                "kind": "object_property",
                "role": "object_property",
                "typical_attributes": variants,
                "edge": str(attr).lower().replace("_uri_parent", "").replace("uri_parent", "parent"),
            }
            if target_entity:
                entry["target_entity"] = target_entity
            props[str(attr)] = entry


def extract_declared_constraints(entities: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Build bundle-level declared_constraints from parsed OWL entity definitions."""
    primary_keys: List[Dict[str, Any]] = []
    foreign_keys: List[Dict[str, Any]] = []
    seen_pk: Set[Tuple[str, str]] = set()
    seen_fk: Set[Tuple[str, str, str]] = set()

    uri_pk_by_entity = {e: _entity_uri_pk_columns(d) for e, d in entities.items()}

    for ent_name, defn in entities.items():
        for col in uri_pk_by_entity.get(ent_name) or []:
            key = (ent_name, col)
            if key not in seen_pk:
                seen_pk.add(key)
                primary_keys.append({"entity": ent_name, "columns": [col]})

        for prop_name, prop in (defn.get("properties") or {}).items():
            role = prop.get("role")
            variants = [
                v.lower() for v in prop.get("typical_attributes") or attribute_name_variants(prop_name)
            ]

            if role == "object_property":
                target = prop.get("target_entity")
                if not target or target not in entities:
                    continue
                if str(target).lower() in _NON_ENTITY_FK_TARGETS:
                    continue
                target_pk = uri_pk_by_entity.get(target) or []
                if not target_pk:
                    target_pk = [
                        v.lower()
                        for v in attribute_name_variants(f"{target}_id")
                        if v.lower().endswith("_id")
                    ]
                    if not target_pk:
                        target_pk = ["id"]
                fk_key = (ent_name, prop_name, target)
                if fk_key in seen_fk:
                    continue
                seen_fk.add(fk_key)
                foreign_keys.append({
                    "source_entity": ent_name,
                    "source_columns": variants,
                    "target_entity": target,
                    "target_columns": target_pk,
                })

    if not primary_keys and not foreign_keys:
        return {}
    return {"primary_keys": primary_keys, "foreign_keys": foreign_keys}


def _pick_column(candidates: List[str], available: Set[str]) -> Optional[str]:
    avail_lower = {c.lower(): c for c in available}
    for c in candidates:
        hit = avail_lower.get(c.lower())
        if hit:
            return hit
    return None


def resolve_declared_constraints_to_tables(
    constraints: Dict[str, Any],
    entity_tables: Dict[str, List[str]],
    table_columns: Dict[str, Set[str]],
) -> Dict[str, Tuple[List[str], Dict[str, str]]]:
    """Resolve entity-level constraints to ``table_fqn -> (pk_cols, fk_map)``."""
    if not constraints:
        return {}

    entity_primary_table: Dict[str, str] = {}
    for ent, tables in entity_tables.items():
        if tables:
            entity_primary_table[ent] = tables[0]

    out: Dict[str, Tuple[List[str], Dict[str, str]]] = {}

    for pk_entry in constraints.get("primary_keys") or []:
        ent = pk_entry.get("entity")
        if not ent or ent not in entity_primary_table:
            continue
        tbl = entity_primary_table[ent]
        cols_avail = table_columns.get(tbl, set())
        resolved: List[str] = []
        for cand in pk_entry.get("columns") or []:
            picked = _pick_column(attribute_name_variants(cand), cols_avail)
            if picked and picked not in resolved:
                resolved.append(picked)
        if resolved:
            pks, fks = out.get(tbl, ([], {}))
            out[tbl] = (list(dict.fromkeys(pks + resolved)), fks)

    for fk_entry in constraints.get("foreign_keys") or []:
        src_ent = fk_entry.get("source_entity")
        tgt_ent = fk_entry.get("target_entity")
        if not src_ent or not tgt_ent:
            continue
        src_tbl = entity_primary_table.get(src_ent)
        tgt_tbl = entity_primary_table.get(tgt_ent)
        if not src_tbl or not tgt_tbl:
            continue
        src_cols_avail = table_columns.get(src_tbl, set())
        tgt_cols_avail = table_columns.get(tgt_tbl, set())
        src_col = _pick_column(
            [c.lower() for c in fk_entry.get("source_columns") or []],
            src_cols_avail,
        )
        if not src_col:
            continue
        tgt_pk_candidates = [c.lower() for c in fk_entry.get("target_columns") or []]
        tgt_pks, _ = out.get(tgt_tbl, ([], {}))
        tgt_pk_candidates = list(dict.fromkeys(tgt_pk_candidates + [p.lower() for p in tgt_pks]))
        avail_lower = {c.lower(): c for c in tgt_cols_avail}
        resolved_tgt = [
            avail_lower[c] for c in tgt_pk_candidates if c in avail_lower
        ]
        tgt_col = resolve_fk_target_column(src_col.lower(), resolved_tgt or list(tgt_cols_avail))
        if tgt_col.lower() not in avail_lower:
            picked = _pick_column(tgt_pk_candidates, tgt_cols_avail)
            tgt_col = picked or tgt_col
        if not tgt_col or tgt_col.lower() not in avail_lower:
            continue
        tgt_col = avail_lower[tgt_col.lower()]
        pks, fks = out.get(src_tbl, ([], {}))
        fks = dict(fks)
        fks[src_col] = f"{tgt_tbl}.{tgt_col}"
        out[src_tbl] = (pks, fks)

    return out


def owl_fk_managed_columns_for_table(
    constraints: Dict[str, Any],
    entity_tables: Dict[str, List[str]],
    table_columns: Dict[str, Set[str]],
    table_fqn: str,
    *,
    fresh_fk_cols: Optional[Set[str]] = None,
    table_pk_cols: Optional[List[str]] = None,
) -> Set[str]:
    """Physical columns on *table_fqn* whose FK map entries OWL declared_constraints may own.

    Includes declared FK source columns for entities on this table, fresh OWL FK keys,
    and PK columns not re-declared as FKs (clears stale misuse of URI PKs as FK sources).
    """
    entity_primary_table: Dict[str, str] = {}
    for ent, tables in entity_tables.items():
        if tables:
            entity_primary_table[ent] = tables[0]

    managed: Set[str] = set(fresh_fk_cols or ())
    cols_avail = table_columns.get(table_fqn, set())
    avail_lower = {c.lower(): c for c in cols_avail}

    for fk_entry in constraints.get("foreign_keys") or []:
        src_ent = fk_entry.get("source_entity")
        if entity_primary_table.get(src_ent) != table_fqn:
            continue
        for cand in fk_entry.get("source_columns") or []:
            hit = avail_lower.get(str(cand).lower())
            if hit:
                managed.add(hit)

    fresh_lower = {c.lower() for c in (fresh_fk_cols or ())}
    for pk in table_pk_cols or []:
        if pk.lower() not in fresh_lower:
            managed.add(pk)

    return managed


def merge_owl_foreign_keys(
    exist_fks: Dict[str, str],
    fresh_fks: Dict[str, str],
    *,
    constraints: Dict[str, Any],
    entity_tables: Dict[str, List[str]],
    table_columns: Dict[str, Set[str]],
    table_fqn: str,
    table_pk_cols: List[str],
    sweep_owl_fks: bool = False,
) -> Tuple[Dict[str, str], int]:
    """Merge OWL-resolved FKs into an existing map; return (merged, newly_added_count)."""
    merged = dict(exist_fks)
    if sweep_owl_fks:
        managed = owl_fk_managed_columns_for_table(
            constraints,
            entity_tables,
            table_columns,
            table_fqn,
            fresh_fk_cols=set(fresh_fks.keys()),
            table_pk_cols=table_pk_cols,
        )
        merged = {k: v for k, v in merged.items() if k not in managed}

    added = 0
    for col, ref in fresh_fks.items():
        if col not in merged or not merged[col]:
            added += 1
        merged[col] = ref
    return merged, added
