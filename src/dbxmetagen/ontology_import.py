"""Utility to convert OWL/TTL/SKOS/SHACL ontology files to dbxmetagen bundle YAML.

Supports OWL (owl:Class), RDFS (rdfs:Class), SKOS (skos:Concept),
SHACL (sh:NodeShape), and bare rdfs:subClassOf patterns.
Output is v1 or v2 (OWL-aligned) format.
"""

import logging
import re
import yaml
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

_OPAQUE_NAME_RE = re.compile(r"^[0-9]+$")

from dbxmetagen.owl_constraints import (
    apply_uri_property_overrides,
    extract_declared_constraints,
)
from dbxmetagen.ontology_properties import process_bundle, attribute_name_variants

logger = logging.getLogger(__name__)

_EXT_FORMAT = {
    ".ttl": "turtle", ".owl": "xml", ".rdf": "xml",
    ".jsonld": "json-ld", ".nt": "nt", ".n3": "n3",
    ".nq": "nquads", ".trig": "trig",
}


def owl_to_bundle_yaml(
    owl_path: str,
    output_path: Optional[str] = None,
    bundle_name: str = "imported",
    format_version: str = "2.0",
) -> Dict[str, Any]:
    """Convert an OWL/TTL/SKOS/SHACL file to dbxmetagen bundle YAML.

    Requires rdflib. Install with: pip install 'dbxmetagen[ontology]'

    Args:
        owl_path: Path to .owl, .ttl, .rdf, .jsonld, .nt, or .n3 file
        output_path: If provided, writes YAML to this path
        bundle_name: Name for the bundle metadata
        format_version: "1.0" for legacy v1 output, "2.0" for OWL-aligned v2

    Returns:
        The bundle dict (includes ``_diagnostics`` key when 0 entities found)
    """
    try:
        import rdflib
        from rdflib import OWL, RDF, RDFS
    except ImportError as exc:
        raise ImportError(
            "rdflib is required for ontology import. Install with: pip install 'dbxmetagen[ontology]'"
        ) from exc

    SKOS = rdflib.Namespace("http://www.w3.org/2004/02/skos/core#")
    SH = rdflib.Namespace("http://www.w3.org/ns/shacl#")

    g = rdflib.Graph()
    ext = Path(owl_path).suffix.lower()
    fmt = _EXT_FORMAT.get(ext, "turtle")
    g.parse(owl_path, format=fmt)

    source_file = Path(owl_path).name
    source_label = _detect_source_ontology(g) or "OWL"

    entities: Dict[str, Dict[str, Any]] = {}
    seen_uris: set = set()

    # --- Pass 1: explicit class types (OWL, RDFS, SKOS, SHACL) ---
    for cls_type in (OWL.Class, RDFS.Class, SKOS.Concept, SH.NodeShape):
        for cls in g.subjects(RDF.type, cls_type):
            if cls in seen_uris:
                continue
            seen_uris.add(cls)
            name = _entity_name(g, cls, SKOS)
            if not name or name.startswith("_"):
                continue

            # SHACL dedup: skip shapes that target an already-found OWL class
            if cls_type == SH.NodeShape:
                target_cls = next(g.objects(cls, SH.targetClass), None)
                if target_cls and target_cls in seen_uris:
                    continue

            desc = _get_description(g, cls, RDFS, SKOS, SH) or f"{name} entity"
            keywords = _get_keywords(g, cls, name, RDFS, SKOS, SH)
            synonyms = _get_synonyms(g, cls, SKOS)
            parents = _get_parents(g, cls, RDFS, SKOS)
            typical_attrs = _get_shacl_attributes(g, cls, SH) if cls_type == SH.NodeShape else []

            entry: Dict[str, Any] = {
                "description": desc,
                "keywords": keywords,
                "typical_attributes": typical_attrs,
                "relationships": {},
                "properties": {},
            }
            if synonyms:
                entry["synonyms"] = synonyms
            if parents:
                entry["parent"] = parents[0]
                entry["parents"] = parents

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

    # --- Pass 2: bare rdfs:subClassOf subjects ---
    for subj in g.subjects(RDFS.subClassOf, None):
        if subj in seen_uris:
            continue
        name = _entity_name(g, subj, SKOS)
        if not name or name.startswith("_"):
            continue
        seen_uris.add(subj)
        desc = _get_description(g, subj, RDFS, SKOS, SH) or f"{name} entity"
        keywords = _get_keywords(g, subj, name, RDFS, SKOS, SH)
        synonyms = _get_synonyms(g, subj, SKOS)
        parents = [_entity_name(g, p, SKOS) for p in g.objects(subj, RDFS.subClassOf) if _entity_name(g, p, SKOS)]
        entry: Dict[str, Any] = {
            "description": desc,
            "keywords": keywords,
            "typical_attributes": [],
            "relationships": {},
            "properties": {},
        }
        if synonyms:
            entry["synonyms"] = synonyms
        if parents:
            entry["parent"] = parents[0]
            entry["parents"] = parents
        if format_version.startswith("2"):
            entry["uri"] = str(subj)
            entry["source_ontology"] = source_label
            if parents:
                entry["subclass_of"] = parents[0]
            entry["owl_properties"] = []
        entities[name] = entry

    # --- Pass 3: bare skos:broader subjects ---
    for subj in g.subjects(SKOS.broader, None):
        if subj in seen_uris:
            continue
        name = _entity_name(g, subj, SKOS)
        if not name or name.startswith("_"):
            continue
        seen_uris.add(subj)
        desc = _get_description(g, subj, RDFS, SKOS, SH) or f"{name} entity"
        keywords = _get_keywords(g, subj, name, RDFS, SKOS, SH)
        synonyms = _get_synonyms(g, subj, SKOS)
        parents = [_entity_name(g, p, SKOS) for p in g.objects(subj, SKOS.broader) if _entity_name(g, p, SKOS)]
        entry: Dict[str, Any] = {
            "description": desc,
            "keywords": keywords,
            "typical_attributes": [],
            "relationships": {},
            "properties": {},
        }
        if synonyms:
            entry["synonyms"] = synonyms
        if parents:
            entry["parent"] = parents[0]
            entry["parents"] = parents
        if format_version.startswith("2"):
            entry["uri"] = str(subj)
            entry["source_ontology"] = source_label
            if parents:
                entry["subclass_of"] = parents[0]
            entry["owl_properties"] = []
        entities[name] = entry

    # Data properties
    for prop in g.subjects(RDF.type, OWL.DatatypeProperty):
        prop_name = _local_name(prop)
        if not prop_name:
            continue
        domains = [_entity_name(g, d, SKOS) for d in g.objects(prop, RDFS.domain) if _entity_name(g, d, SKOS)]
        ranges = [str(r) for r in g.objects(prop, RDFS.range)]
        for domain_cls in domains:
            if domain_cls in entities:
                # Roles are generated by process_bundle() from typical_attributes;
                # avoid pre-filling naive "dimension" entries that would win merge precedence.
                entities[domain_cls]["typical_attributes"].extend(
                    v for v in attribute_name_variants(prop_name)
                    if v not in entities[domain_cls]["typical_attributes"]
                )
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
        domains = [_entity_name(g, d, SKOS) for d in g.objects(prop, RDFS.domain) if _entity_name(g, d, SKOS)]
        ranges = [_entity_name(g, r, SKOS) for r in g.objects(prop, RDFS.range) if _entity_name(g, r, SKOS)]
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
                prop_variants = attribute_name_variants(prop_name)
                entities[domain_cls]["properties"][prop_name] = {
                    "kind": "object_property",
                    "role": "object_property",
                    "edge": prop_name,
                    "target_entity": target,
                    "typical_attributes": prop_variants,
                }
                for v in prop_variants:
                    if v not in entities[domain_cls]["typical_attributes"]:
                        entities[domain_cls]["typical_attributes"].append(v)
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

    _extract_schema_style_properties(g, rdflib, entities, edge_catalog, format_version)

    metadata: Dict[str, Any] = {
        "name": bundle_name,
        "version": "3.0",
        "industry": "imported",
        "description": f"Imported from {source_file}",
        "standards_alignment": source_label,
        "entity_count": len(entities),
        "edge_count": len(edge_catalog),
        "domain_count": 0,
    }
    if format_version.startswith("2"):
        metadata["format_version"] = "2.0"

    bundle: Dict[str, Any] = {
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

    if entities:
        # Generate property roles and sync ontology.property_roles to the canonical
        # registry, matching the quality of curated bundles (FHIR/OMOP/schema.org).
        process_bundle(bundle, bundle_name)
        apply_uri_property_overrides(entities)
        declared = extract_declared_constraints(entities)
        if declared:
            bundle["ontology"]["declared_constraints"] = declared
    else:
        bundle["_diagnostics"] = _build_diagnostics(g, RDF, owl_path)

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


_SCHEMA_DATATYPE_NAMES = frozenset({
    "Text", "Number", "Integer", "Float", "Boolean", "Date", "DateTime",
    "Time", "URL", "Duration", "Distance", "Energy", "Mass",
})


def _extract_schema_style_properties(
    g, rdflib_mod, entities: Dict[str, Dict], edge_catalog: Dict[str, Dict],
    format_version: str,
) -> None:
    """Extract edges via schema:domainIncludes/rangeIncludes (Schema.org convention)."""
    SCHEMA_DOMAIN = rdflib_mod.URIRef("https://schema.org/domainIncludes")
    SCHEMA_RANGE = rdflib_mod.URIRef("https://schema.org/rangeIncludes")

    if not any(g.triples((None, SCHEMA_DOMAIN, None))):
        return

    entity_names = set(entities.keys()) - _SCHEMA_DATATYPE_NAMES

    for prop, _, domain_cls in g.triples((None, SCHEMA_DOMAIN, None)):
        domain_name = _local_name(domain_cls)
        if not domain_name or domain_name not in entities:
            continue
        prop_name = _local_name(prop)
        if not prop_name:
            continue

        ranges = [_local_name(r) for r in g.objects(prop, SCHEMA_RANGE) if _local_name(r)]
        obj_ranges = [r for r in ranges if r in entity_names]
        data_ranges = [r for r in ranges if r in _SCHEMA_DATATYPE_NAMES]

        if obj_ranges:
            target = obj_ranges[0]
            if prop_name not in edge_catalog:
                edge_entry: Dict[str, Any] = {
                    "symmetric": False, "category": "business",
                    "domain": domain_name, "range": target,
                }
                if format_version.startswith("2"):
                    edge_entry["uri"] = str(prop)
                    edge_entry["owl_type"] = "Property"
                edge_catalog[prop_name] = edge_entry

            entities[domain_name]["relationships"].setdefault(prop_name, {
                "target": target, "cardinality": "unknown",
            })
            entities[domain_name]["properties"].setdefault(prop_name, {
                "kind": "object_property", "role": "object_property",
                "edge": prop_name, "target_entity": target,
                "typical_attributes": [f"{prop_name}_id"],
            })
        elif data_ranges:
            attrs = entities[domain_name]["typical_attributes"]
            if prop_name not in attrs:
                attrs.append(prop_name)


def _detect_source_ontology(g) -> Optional[str]:
    """Detect source ontology from namespaces actually used in the graph.

    Scans predicates and rdf:type objects rather than ``g.namespaces()``,
    because rdflib auto-binds common prefixes (schema.org, brick, etc.) on
    every graph regardless of use, which caused false-positive matches.
    """
    import rdflib

    used = {str(p) for p in g.predicates()}
    used.update(str(o) for o in g.objects(None, rdflib.RDF.type))
    ns_str = " ".join(used)
    if "hl7.org/fhir" in ns_str:
        return "FHIR R4"
    if "omop-cdm" in ns_str or "w3id.org/omop" in ns_str:
        return "OMOP CDM"
    if "schema.org" in ns_str:
        return "Schema.org"
    if "w3.org/2004/02/skos" in ns_str:
        return "SKOS"
    if "w3.org/ns/shacl" in ns_str:
        return "SHACL"
    return None


def _local_name(uri) -> Optional[str]:
    s = str(uri)
    if "#" in s:
        return s.split("#")[-1]
    if "/" in s:
        return s.split("/")[-1]
    return None


def _entity_name(g, uri, skos) -> Optional[str]:
    """Resolve a usable entity name for a URI.

    Most ontologies encode the name in the URI fragment (e.g. ``#Patient``).
    Code-based ontologies use opaque numeric fragments (e.g. ``#1208``); for
    those, fall back to a sanitized ``skos:prefLabel`` so entities surface as
    readable names instead of bare codes. Deterministic per URI, so the same
    name is produced whether the URI appears as a class, parent, or range.
    """
    ln = _local_name(uri)
    if ln and not _OPAQUE_NAME_RE.match(ln):
        return ln
    for val in g.objects(uri, skos.prefLabel):
        sanitized = re.sub(r"[^0-9A-Za-z]+", "_", _strip_lang(val)).strip("_")
        if sanitized:
            return sanitized
    return ln


def _strip_lang(literal) -> str:
    """Return the string value of an rdflib Literal, stripping language tags."""
    return str(literal).strip()


def _get_comment(g, subject, rdfs) -> Optional[str]:
    """Legacy helper: extract rdfs:comment only. Used by ontology_chunker."""
    for comment in g.objects(subject, rdfs.comment):
        return str(comment)
    return None


def _get_description(g, subject, rdfs, skos, sh) -> Optional[str]:
    """Extract description from rdfs:comment, skos:definition, or sh:description."""
    for pred in (rdfs.comment, skos.definition, sh.description):
        for val in g.objects(subject, pred):
            return _strip_lang(val)
    return None


def _get_keywords(g, subject, name: str, rdfs, skos, sh) -> List[str]:
    """Collect keywords from rdfs:label, SKOS labels, sh:name, and skos:notation."""
    kw_set: set = {name.lower()}
    for pred in (rdfs.label, skos.prefLabel, skos.altLabel, skos.hiddenLabel, sh.name):
        for val in g.objects(subject, pred):
            token = _strip_lang(val).lower()
            if token:
                kw_set.add(token)
    for val in g.objects(subject, skos.notation):
        token = _strip_lang(val).strip()
        if token:
            kw_set.add(token.lower())
    return sorted(kw_set)


def _get_synonyms(g, subject, skos) -> List[str]:
    """Collect alternative labels (skos:altLabel, skos:hiddenLabel) as synonyms."""
    syn_set: set = set()
    for pred in (skos.altLabel, skos.hiddenLabel):
        for val in g.objects(subject, pred):
            token = _strip_lang(val).strip()
            if token:
                syn_set.add(token)
    return sorted(syn_set)


def _get_parents(g, subject, rdfs, skos) -> List[str]:
    """Collect parent names from rdfs:subClassOf and skos:broader."""
    parents = []
    for pred in (rdfs.subClassOf, skos.broader):
        for obj in g.objects(subject, pred):
            pname = _entity_name(g, obj, skos)
            if pname and pname not in parents:
                parents.append(pname)
    return parents


def _get_shacl_attributes(g, shape, sh) -> List[str]:
    """Extract typical attribute names from sh:property paths on a SHACL shape."""
    attrs = []
    for prop_node in g.objects(shape, sh.property):
        path = next(g.objects(prop_node, sh.path), None)
        if path:
            attr_name = _local_name(path)
            if attr_name:
                attrs.append(attr_name)
    return attrs


def _build_diagnostics(g, rdf, owl_path: str) -> Dict[str, Any]:
    """Build a diagnostics dict when 0 entities are extracted."""
    total = sum(1 for _ in g)
    type_counter: Counter = Counter()
    for _, _, obj in g.triples((None, rdf.type, None)):
        type_counter[str(obj)] += 1

    top_types = [{"type": t, "count": c} for t, c in type_counter.most_common(10)]

    hints = []
    type_strs = " ".join(type_counter.keys()).lower()
    if "dcat" in type_strs:
        hints.append("File appears to be a DCAT data catalog, not an ontology. "
                      "Consider importing as metadata enrichment instead.")
    elif "foaf" in type_strs:
        hints.append("File uses FOAF vocabulary. Entity extraction expects "
                      "owl:Class, rdfs:Class, skos:Concept, or sh:NodeShape.")
    elif total == 0:
        hints.append("File parsed to 0 triples. Check that the file format "
                      "matches the extension.")
    else:
        hints.append(
            f"File has {total} triples but no recognized class/concept patterns. "
            "Supported: owl:Class, rdfs:Class, skos:Concept, sh:NodeShape, "
            "rdfs:subClassOf, skos:broader."
        )

    return {
        "total_triples": total,
        "top_rdf_types": top_types,
        "hint": " ".join(hints),
    }


def owl_to_chunks(owl_path: str, bundle_name: str) -> List[Dict[str, Any]]:
    """Extract entity and edge text chunks directly from an OWL/TTL file via rdflib.

    This is a convenience wrapper that delegates to
    :func:`ontology_chunker.chunks_from_owl`. Use it at import time when the
    user provides an OWL/TTL file so chunks are written to the Delta table
    immediately without requiring a full bundle intermediate format.
    """
    from dbxmetagen.ontology_chunker import chunks_from_owl
    return chunks_from_owl(owl_path, bundle_name)


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
