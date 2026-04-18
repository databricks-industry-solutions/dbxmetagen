"""Utility to convert OWL/TTL ontology files to dbxmetagen bundle YAML format.

Supports both v1 (existing) and v2 (OWL-aligned) output formats.
"""

import logging
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def owl_to_bundle_yaml(
    owl_path: str,
    output_path: Optional[str] = None,
    bundle_name: str = "imported",
    format_version: str = "2.0",
) -> Dict[str, Any]:
    """Convert an OWL/TTL file to dbxmetagen bundle YAML.

    Requires rdflib. Install with: pip install 'dbxmetagen[ontology]'

    Args:
        owl_path: Path to .owl, .ttl, or .rdf file
        output_path: If provided, writes YAML to this path
        bundle_name: Name for the bundle metadata
        format_version: "1.0" for legacy v1 output, "2.0" for OWL-aligned v2

    Returns:
        The bundle dict
    """
    try:
        import rdflib
        from rdflib import OWL, RDF, RDFS
    except ImportError as exc:
        raise ImportError(
            "rdflib is required for ontology import. Install with: pip install 'dbxmetagen[ontology]'"
        ) from exc

    g = rdflib.Graph()
    fmt = "turtle" if owl_path.endswith(".ttl") else "xml"
    g.parse(owl_path, format=fmt)

    source_file = Path(owl_path).name
    source_label = _detect_source_ontology(g) or "OWL"

    entities: Dict[str, Dict[str, Any]] = {}
    for cls in g.subjects(RDF.type, OWL.Class):
        name = _local_name(cls)
        if not name or name.startswith("_"):
            continue
        comment = _get_comment(g, cls, RDFS) or f"{name} entity"
        parents = [_local_name(p) for p in g.objects(cls, RDFS.subClassOf) if _local_name(p)]

        entry: Dict[str, Any] = {
            "description": comment,
            "keywords": [name.lower()],
            "typical_attributes": [],
            "relationships": {},
            "properties": {},
        }
        if parents:
            entry["parent"] = parents[0]

        if format_version.startswith("2"):
            entry["uri"] = str(cls)
            entry["source_ontology"] = source_label
            if parents:
                entry["subclass_of"] = parents[0]
            equiv = list(g.objects(cls, OWL.equivalentClass))
            if equiv:
                entry["owl_equivalent_class"] = str(equiv[0])
            entry["owl_properties"] = []

        entities[name] = entry

    # Data properties
    for prop in g.subjects(RDF.type, OWL.DatatypeProperty):
        prop_name = _local_name(prop)
        if not prop_name:
            continue
        domains = [_local_name(d) for d in g.objects(prop, RDFS.domain) if _local_name(d)]
        ranges = [str(r) for r in g.objects(prop, RDFS.range)]
        for domain_cls in domains:
            if domain_cls in entities:
                entities[domain_cls]["properties"][prop_name] = {
                    "kind": "data_property",
                    "role": "dimension",
                    "typical_attributes": [prop_name],
                }
                entities[domain_cls]["typical_attributes"].append(prop_name)
                if format_version.startswith("2"):
                    entities[domain_cls]["owl_properties"].append({
                        "name": prop_name,
                        "type": "data_property",
                        "uri": str(prop),
                        "datatype": ranges[0] if ranges else None,
                    })

    # Object properties -> edges + entity properties
    edge_catalog: Dict[str, Dict[str, Any]] = {}
    for prop in g.subjects(RDF.type, OWL.ObjectProperty):
        prop_name = _local_name(prop)
        if not prop_name:
            continue
        domains = [_local_name(d) for d in g.objects(prop, RDFS.domain) if _local_name(d)]
        ranges = [_local_name(r) for r in g.objects(prop, RDFS.range) if _local_name(r)]
        inverse_props = [_local_name(i) for i in g.objects(prop, OWL.inverseOf) if _local_name(i)]

        edge_entry: Dict[str, Any] = {"symmetric": False, "category": "business"}
        if domains:
            edge_entry["domain"] = domains[0] if len(domains) == 1 else domains
        if ranges:
            edge_entry["range"] = ranges[0] if len(ranges) == 1 else ranges
        if inverse_props:
            edge_entry["inverse"] = inverse_props[0]
        if format_version.startswith("2"):
            edge_entry["uri"] = str(prop)
            edge_entry["owl_type"] = "ObjectProperty"
        edge_catalog[prop_name] = edge_entry

        for domain_cls in domains:
            if domain_cls in entities:
                target = ranges[0] if ranges else None
                entities[domain_cls]["properties"][prop_name] = {
                    "kind": "object_property",
                    "role": "object_property",
                    "edge": prop_name,
                    "target_entity": target,
                    "typical_attributes": [f"{prop_name}_id"],
                }
                if target:
                    entities[domain_cls]["relationships"][prop_name] = {
                        "target": target, "cardinality": "many-to-one",
                    }
                if format_version.startswith("2"):
                    entities[domain_cls]["owl_properties"].append({
                        "name": prop_name,
                        "type": "object_property",
                        "range": target,
                        "uri": str(prop),
                        "cardinality": "many-to-one",
                        "inverse": inverse_props[0] if inverse_props else None,
                    })

    metadata: Dict[str, Any] = {
        "name": bundle_name,
        "version": "3.0",
        "industry": "imported",
        "description": f"Imported from {source_file}",
        "standards_alignment": source_label,
    }
    if format_version.startswith("2"):
        metadata["format_version"] = "2.0"

    bundle = {
        "metadata": metadata,
        "ontology": {
            "version": "3.0",
            "name": f"{bundle_name} Ontology",
            "description": f"Auto-imported from {source_file}",
            "property_roles": _default_property_roles(),
            "edge_catalog": edge_catalog,
            "entities": {
                "auto_discover": True,
                "discovery_confidence_threshold": 0.4,
                "definitions": entities,
            },
            "relationships": {
                "types": [{"name": n, "inverse": e.get("inverse")} for n, e in edge_catalog.items()]
            },
            "metrics": {"auto_discover": False, "definitions": []},
            "validation": {
                "min_entity_confidence": 0.5,
                "max_entities_per_table": 3,
                "ai_validation_enabled": True,
            },
        },
        "domains": {},
    }

    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            yaml.dump(bundle, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        logger.info(
            "Wrote %s bundle YAML to %s (%d entities, %d edges)",
            f"v{format_version}" if format_version.startswith("2") else "v1",
            output_path, len(entities), len(edge_catalog),
        )

    return bundle


def migrate_v1_to_v2(bundle: Dict[str, Any]) -> Dict[str, Any]:
    """Upgrade a v1 bundle dict to v2 format in-place.
    
    Adds format_version, sets default uri/source_ontology on entities,
    and initializes empty owl_properties lists where missing.
    
    Args:
        bundle: A v1 bundle dict (will be modified in-place).
    
    Returns:
        The same dict, now v2-shaped.
    """
    meta = bundle.setdefault("metadata", {})
    if str(meta.get("format_version", "1.0")).startswith("2"):
        return bundle

    meta["format_version"] = "2.0"
    bundle_name = meta.get("name", "custom")
    ns = f"https://ontology.databricks.com/{bundle_name.lower().replace(' ', '_')}#"

    definitions = bundle.get("ontology", {}).get("entities", {}).get("definitions", {})
    for name, defn in definitions.items():
        defn.setdefault("uri", f"{ns}{name}")
        defn.setdefault("source_ontology", bundle_name)
        if defn.get("parent"):
            defn.setdefault("subclass_of", defn["parent"])
        defn.setdefault("owl_properties", [])

    edge_catalog = bundle.get("ontology", {}).get("edge_catalog", {})
    for edge_name, info in edge_catalog.items():
        info.setdefault("uri", f"{ns}{edge_name}")
        info.setdefault("owl_type", "ObjectProperty")

    logger.info("Migrated bundle '%s' from v1 to v2 (%d entities)", bundle_name, len(definitions))
    return bundle


def _detect_source_ontology(g) -> Optional[str]:
    """Heuristic detection of source ontology from graph namespace prefixes."""
    ns_str = " ".join(str(ns) for _, ns in g.namespaces())
    if "hl7.org/fhir" in ns_str:
        return "FHIR R4"
    if "omop-cdm" in ns_str or "w3id.org/omop" in ns_str:
        return "OMOP CDM"
    if "schema.org" in ns_str:
        return "Schema.org"
    return None


def _local_name(uri) -> Optional[str]:
    s = str(uri)
    if "#" in s:
        return s.split("#")[-1]
    if "/" in s:
        return s.split("/")[-1]
    return None


def _get_comment(g, subject, rdfs) -> Optional[str]:
    for comment in g.objects(subject, rdfs.comment):
        return str(comment)
    return None


def _default_property_roles() -> Dict[str, Dict]:
    return {
        "primary_key": {
            "description": "Surrogate or technical row identifier",
            "maps_to_kind": "data_property",
            "semantic_role": "identifier",
        },
        "business_key": {
            "description": "Natural key meaningful to users",
            "maps_to_kind": "data_property",
            "semantic_role": "identifier",
        },
        "measure": {
            "description": "Numeric value to aggregate",
            "maps_to_kind": "data_property",
            "semantic_role": "metric",
        },
        "dimension": {
            "description": "Categorical filter or grouping",
            "maps_to_kind": "data_property",
            "semantic_role": "dimension",
        },
        "temporal": {
            "description": "Date or timestamp",
            "maps_to_kind": "data_property",
            "semantic_role": "temporal",
        },
        "object_property": {
            "description": "FK to another entity",
            "maps_to_kind": "object_property",
            "semantic_role": "relationship",
        },
        "composite_component": {
            "description": "Part of a multi-column value",
            "maps_to_kind": "data_property",
            "semantic_role": "component",
        },
        "label": {
            "description": "Human-readable name",
            "maps_to_kind": "data_property",
            "semantic_role": "label",
        },
        "audit": {
            "description": "System-generated provenance",
            "maps_to_kind": "data_property",
            "semantic_role": "provenance",
        },
        "derived": {
            "description": "Computed from other columns",
            "maps_to_kind": "data_property",
            "semantic_role": "derived",
        },
    }
