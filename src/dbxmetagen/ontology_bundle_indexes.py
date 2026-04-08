"""Build tier YAML indexes (entities/edges tier 1–3) from extracted or bundle entities.

Used by ``scripts/build_ontology_indexes.py`` and the app import-ontology path.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

logger = logging.getLogger(__name__)


def load_edge_catalog(bundle_yaml: Path) -> Dict[str, Any]:
    """Load ``ontology.edge_catalog`` from a bundle YAML."""
    try:
        raw = yaml.safe_load(bundle_yaml.read_text(encoding="utf-8"))
        return raw.get("ontology", {}).get("edge_catalog", {}) or {}
    except Exception as e:
        logger.warning("Could not load edge_catalog from %s: %s", bundle_yaml, e)
        return {}


def resolve_edge_catalog(repo_root: Path, args: argparse.Namespace) -> Dict[str, Any]:
    """Pick the bundle YAML whose edge_catalog applies to this CLI build."""
    if getattr(args, "from_bundle", None):
        p = repo_root / "configurations" / "ontology_bundles" / f"{args.from_bundle}.yaml"
        if p.is_file():
            return load_edge_catalog(p)
    if getattr(args, "merge_bundle", None):
        p = repo_root / "configurations" / "ontology_bundles" / f"{args.merge_bundle}.yaml"
        if p.is_file():
            return load_edge_catalog(p)
    p = repo_root / "configurations" / "ontology_bundles" / f"{args.bundle_name}.yaml"
    if p.is_file():
        return load_edge_catalog(p)
    return {}


def entities_from_bundle(bundle_path: Path) -> Dict[str, Dict[str, Any]]:
    """Extract entities from a curated bundle YAML, preserving rich fields for tier-3."""
    raw = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
    definitions = raw.get("ontology", {}).get("entities", {}).get("definitions", {})
    edge_catalog = raw.get("ontology", {}).get("edge_catalog", {})

    all_entities: Dict[str, Dict[str, Any]] = {}
    for name, defn in definitions.items():
        uri = defn.get("uri", "")
        source = defn.get("source_ontology", "custom")
        rels = defn.get("relationships", {})

        outgoing_edges = []
        relationships: Dict[str, Dict[str, str]] = {}
        for edge_name, edge_info in rels.items():
            target = edge_info.get("target", "")
            if isinstance(target, list):
                target = target[0]
            cardinality = edge_info.get("cardinality", "unknown")
            cat_entry = edge_catalog.get(edge_name, {})
            inv = cat_entry.get("inverse", "")
            outgoing_edges.append({
                "name": edge_name,
                "uri": "",
                "range": target,
                "inverse": inv,
            })
            relationships[edge_name] = {"target": target, "cardinality": cardinality}

        parents = defn.get("parents", [])
        if not parents and defn.get("parent"):
            parents = [defn["parent"]]

        all_entities[name] = {
            "description": defn.get("description", f"{name} entity"),
            "label": defn.get("label", name),
            "source": source,
            "uri": uri,
            "parents": parents,
            "outgoing_edges": outgoing_edges,
            "keywords": defn.get("keywords", []),
            "synonyms": defn.get("synonyms", []),
            "typical_attributes": defn.get("typical_attributes", []),
            "business_questions": defn.get("business_questions", []),
            "relationships": relationships,
            "properties": defn.get("properties", {}),
        }

    return all_entities


def _endpoint_name(val: Any) -> Optional[str]:
    """Normalize domain/range to a single entity name for profile lookup."""
    if val is None:
        return None
    if isinstance(val, str):
        return val if val.strip() else None
    if isinstance(val, list) and val:
        x = val[0]
        return str(x) if x is not None else None
    return str(val)


def _compact_endpoint_profile(
    entity_name: str,
    all_entities: Dict[str, Dict[str, Any]],
    *,
    max_desc: int = 320,
    max_kw: int = 10,
    max_rels: int = 8,
) -> Optional[Dict[str, Any]]:
    """Bounded summary of an entity for edge tier-3 endpoint context."""
    data = all_entities.get(entity_name)
    if not data:
        return None
    desc = (data.get("description") or "")[:max_desc]
    kws = (data.get("keywords") or [])[:max_kw]
    rels = data.get("relationships") or {}
    rel_preview: Dict[str, str] = {}
    for i, (k, v) in enumerate(rels.items()):
        if i >= max_rels:
            break
        if isinstance(v, dict):
            rel_preview[k] = str(v.get("target", v.get("cardinality", "")))[:100]
        else:
            rel_preview[k] = str(v)[:100]
    return {
        "description": desc,
        "uri": data.get("uri") or "",
        "source_ontology": data.get("source", ""),
        "keywords": kws,
        "relationships_preview": rel_preview,
    }


def _build_edges_t3(
    edges_t2: Dict[str, Dict[str, Any]],
    all_entities: Dict[str, Dict[str, Any]],
    edge_catalog: Optional[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Tier-3 edges: tier-2 fields plus edge_catalog metadata and endpoint class profiles."""
    cat_map = edge_catalog or {}
    out: Dict[str, Dict[str, Any]] = {}
    for ename, base in edges_t2.items():
        row: Dict[str, Any] = dict(base)
        cat = cat_map.get(ename)
        if isinstance(cat, dict):
            for k in ("category", "symmetric"):
                if k in cat and cat[k] is not None:
                    row[k] = cat[k]
            if cat.get("inverse") is not None:
                row["inverse"] = cat["inverse"]
            if cat.get("domain") is not None:
                row["domain"] = cat["domain"]
            if cat.get("range") is not None:
                row["range"] = cat["range"]

        dom_n = _endpoint_name(row.get("domain"))
        rng_n = _endpoint_name(row.get("range"))
        if dom_n:
            dp = _compact_endpoint_profile(dom_n, all_entities)
            if dp:
                row["domain_profile"] = dp
        if rng_n:
            rp = _compact_endpoint_profile(rng_n, all_entities)
            if rp:
                row["range_profile"] = rp

        out[ename] = row
    return out


def build_tiers(
    all_entities: Dict[str, Dict[str, Any]],
    output_dir: Path,
    dry_run: bool = False,
    edge_catalog: Optional[Dict[str, Any]] = None,
) -> Dict[str, int]:
    """Generate tier 1/2/3 YAML files for entities and edges.

    Edge tier 3 adds bundle edge_catalog fields and domain/range endpoint profiles;
    edge tier 2 stays the lighter confirmation slice.
    """
    tier1 = [{"name": k, "description": v["description"][:200], "label": v.get("label", k)}
             for k, v in sorted(all_entities.items())]

    tier2 = {}
    for name, data in all_entities.items():
        edges = data.get("outgoing_edges", [])[:8]
        tier2[name] = {
            "description": data["description"],
            "label": data.get("label", name),
            "source_ontology": data["source"],
            "uri": data["uri"],
            "parents": data.get("parents", []),
            "edges": [e["name"] for e in edges],
        }

    tier3 = {}
    for name, data in all_entities.items():
        tier3[name] = {
            "description": data["description"],
            "label": data.get("label", name),
            "source_ontology": data["source"],
            "uri": data["uri"],
            "parents": data.get("parents", []),
            "keywords": data.get("keywords", []),
            "synonyms": data.get("synonyms", []),
            "typical_attributes": data.get("typical_attributes", []),
            "business_questions": data.get("business_questions", []),
            "relationships": data.get("relationships", {}),
            "properties": data.get("properties", {}),
        }

    all_edges: Dict[str, Dict[str, Any]] = {}
    for ent_name, data in all_entities.items():
        rels = data.get("relationships", {})
        for edge in data.get("outgoing_edges", []):
            ename = edge.get("name")
            if ename and ename not in all_edges:
                card = rels.get(ename, {}).get("cardinality", "unknown") if isinstance(rels.get(ename), dict) else "unknown"
                all_edges[ename] = {
                    "name": ename,
                    "domain": ent_name,
                    "range": edge.get("range"),
                    "ranges": edge.get("ranges", []),
                    "uri": edge.get("uri"),
                    "inverse": edge.get("inverse"),
                    "cardinality": card,
                }

    edges_t1 = [{"name": e["name"], "domain": e["domain"], "range": e.get("range"),
                  "cardinality": e.get("cardinality", "unknown")}
                for e in sorted(all_edges.values(), key=lambda x: x["name"])]
    edges_t2 = {k: v for k, v in all_edges.items()}
    edges_t3 = _build_edges_t3(edges_t2, all_entities, edge_catalog)
    uris = {name: data["uri"] for name, data in all_entities.items() if data.get("uri")}

    counts = {
        "entities_tier1": len(tier1),
        "entities_tier2": len(tier2),
        "entities_tier3": len(tier3),
        "edges_tier1": len(edges_t1),
        "edges_tier2": len(edges_t2),
        "edges_tier3": len(edges_t3),
        "uris": len(uris),
    }

    if dry_run:
        for label, count in counts.items():
            print(f"  {label}: {count} entries")
        return counts

    output_dir.mkdir(parents=True, exist_ok=True)
    tier_data = {
        "entities_tier1": tier1,
        "entities_tier2": tier2,
        "entities_tier3": tier3,
        "edges_tier1": edges_t1,
        "edges_tier2": edges_t2,
        "edges_tier3": edges_t3,
        "equivalent_class_uris": uris,
    }
    for stem, data in tier_data.items():
        yaml_path = output_dir / f"{stem}.yaml"
        with open(yaml_path, "w", encoding="utf-8") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        json_path = output_dir / f"{stem}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=None)
        logger.info("Wrote %s (%d bytes) + .json (%d bytes)", yaml_path, yaml_path.stat().st_size, json_path.stat().st_size)

    return counts
