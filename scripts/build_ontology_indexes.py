#!/usr/bin/env python3
"""Build tiered ontology index YAMLs from published ontology sources.

Supports three workflows:

1. Auto-extract from published OWL/Turtle (FHIR R4, OMOP CDM, Schema.org):
     python scripts/build_ontology_indexes.py --bundle-name healthcare

2. Generate from a curated bundle YAML (reads uri/source_ontology from definitions):
     python scripts/build_ontology_indexes.py --from-bundle financial_services

3. Load a custom OWL/Turtle file with optional classes-of-interest filter:
     python scripts/build_ontology_indexes.py --custom-source my_ontology.ttl \\
         --custom-label "My Ontology" --bundle-name my_bundle

Curated alignment approach:
  Bundles like financial_services, retail_cpg, and geo_doj use hand-mapped URIs
  to published standards (FIBO, BIAN, GS1, ISO 3166) rather than auto-extracting
  from OWL downloads. This is appropriate when:
    - The source ontology is very large (FIBO: ~30k classes)
    - No easily-parseable OWL/Turtle is publicly available (BIAN)
    - Only a small subset of the vocabulary is relevant
  The --from-bundle mode generates tier indexes from these curated definitions.
"""

import argparse
import hashlib
import logging
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import yaml

logger = logging.getLogger(__name__)

# Published ontology sources
SOURCES = {
    "fhir_r4": {
        "url": "https://hl7.org/fhir/R4/fhir.ttl",
        "format": "turtle",
        "prefix": "http://hl7.org/fhir/",
        "label": "FHIR R4",
    },
    "omop_cdm": {
        "url": "https://raw.githubusercontent.com/vemonet/omop-cdm-owl/main/omop_cdm_v6.ttl",
        "format": "turtle",
        "prefix": "https://w3id.org/omop/ontology/",
        "label": "OMOP CDM",
    },
    "schema_org": {
        "url": "https://schema.org/version/latest/schemaorg-current-https.ttl",
        "format": "turtle",
        "prefix": "https://schema.org/",
        "label": "Schema.org",
    },
}

# ---------------------------------------------------------------------------
# Deny-lists: classes to SKIP when auto-discovering from OWL sources.
# Everything else matching the structural criteria gets extracted automatically.
# ---------------------------------------------------------------------------

FHIR_CONFORMANCE_SKIP: Set[str] = {
    "CapabilityStatement", "StructureDefinition", "StructureMap",
    "OperationDefinition", "SearchParameter", "CodeSystem", "ValueSet",
    "ConceptMap", "NamingSystem", "ImplementationGuide", "GraphDefinition",
    "CompartmentDefinition", "TerminologyCapabilities", "OperationOutcome",
    "MessageDefinition", "Bundle", "Binary", "Parameters",
    "EventDefinition", "Library", "PlanDefinition", "ActivityDefinition",
    "SpecimenDefinition", "ObservationDefinition", "ChargeItemDefinition",
    "TestScript", "TestReport", "ExampleScenario", "Basic",
    "DomainResource", "Resource",
}

OMOP_ABSTRACT_SKIP: Set[str] = {
    "OmopCDMThing", "BasePerson", "BaseVisit", "ClinicalElement",
    "DateDuration", "DatetimeDuration", "Duration", "Era",
    "Event", "Exposure", "Occurrence",
}


def _discover_fhir_resources(g, prefix: str) -> Set[str]:
    """Auto-discover all FHIR Resources via rdfs:subClassOf chain to
    DomainResource/Resource, skipping backbone components, data types,
    and conformance resources."""
    import rdflib as _rdflib
    from rdflib import OWL, RDF, RDFS

    resource_uri = _rdflib.URIRef(prefix + "Resource")
    domain_resource_uri = _rdflib.URIRef(prefix + "DomainResource")

    resource_children: Set[str] = set()
    for cls_type in (OWL.Class, RDFS.Class):
        for cls in g.subjects(RDF.type, cls_type):
            s = str(cls)
            if not s.startswith(prefix):
                continue
            name = _local_name(cls)
            if not name or "." in name or name[0].islower():
                continue
            visited: Set = set()
            frontier = {cls}
            is_resource = False
            for _ in range(4):
                next_frontier: Set = set()
                for node in frontier:
                    if node in visited:
                        continue
                    visited.add(node)
                    if node == resource_uri or node == domain_resource_uri:
                        is_resource = True
                        break
                    for parent in g.objects(node, RDFS.subClassOf):
                        if isinstance(parent, _rdflib.URIRef):
                            next_frontier.add(parent)
                if is_resource:
                    break
                frontier = next_frontier
            if is_resource:
                resource_children.add(name)

    return resource_children - FHIR_CONFORMANCE_SKIP


def _discover_omop_classes(g, prefix: str) -> Set[str]:
    """Auto-discover all concrete OMOP CDM classes (skip abstract hierarchy parents)."""
    from rdflib import OWL, RDF, RDFS

    all_classes: Set[str] = set()
    for cls_type in (OWL.Class, RDFS.Class):
        for cls in g.subjects(RDF.type, cls_type):
            if str(cls).startswith(prefix):
                name = _local_name(cls)
                if name:
                    all_classes.add(name)
    return all_classes - OMOP_ABSTRACT_SKIP


def _discover_schema_org_classes(g, prefix: str) -> Set[str]:
    """Auto-discover ALL Schema.org classes (full ontology)."""
    from rdflib import OWL, RDF, RDFS

    all_classes: Set[str] = set()
    for cls_type in (OWL.Class, RDFS.Class):
        for cls in g.subjects(RDF.type, cls_type):
            if str(cls).startswith(prefix):
                name = _local_name(cls)
                if name:
                    all_classes.add(name)
    return all_classes


_PRIMITIVE_RANGE_NAMES = frozenset({
    "boolean", "integer", "string", "decimal", "uri", "url", "canonical",
    "base64Binary", "instant", "date", "dateTime", "time", "code", "oid",
    "id", "markdown", "unsignedInt", "positiveInt", "uuid", "xhtml",
    "Duration", "Age", "Count", "Distance", "SimpleQuantity",
    "Quantity", "Range", "Period", "Ratio", "Timing",
    "Identifier", "HumanName", "Address", "ContactPoint",
    "CodeableConcept", "Coding", "Annotation", "Attachment",
    "Narrative", "Extension", "Meta", "Reference",
})


def _local_name(uri) -> Optional[str]:
    s = str(uri)
    if "#" in s:
        return s.split("#")[-1]
    if "/" in s:
        return s.split("/")[-1]
    return None


def _get_comment(g, subject, rdfs, limit: int = 300) -> str:
    for comment in g.objects(subject, rdfs.comment):
        return str(comment)[:limit]
    return ""


_STOPWORDS = frozenset({
    "a", "an", "the", "of", "for", "in", "to", "and", "or", "is", "are",
    "that", "this", "it", "with", "on", "at", "by", "from", "as", "be",
    "was", "were", "has", "have", "had", "not", "but", "if", "which",
})


def _derive_keywords(name: str, description: str = "") -> List[str]:
    """Auto-generate keywords from an entity name and optional description.

    Splits camelCase/PascalCase and UPPER_SNAKE_CASE into lowercase words,
    then extracts a few key nouns from the description.
    """
    words: List[str] = []
    # Split camelCase/PascalCase
    parts = re.sub(r"([a-z])([A-Z])", r"\1 \2", name).split()
    # Also handle UPPER_SNAKE_CASE
    if "_" in name:
        parts = name.split("_")
    words.extend(w.lower() for w in parts if len(w) > 1)

    if description:
        desc_tokens = re.findall(r"[A-Za-z]{3,}", description)
        for tok in desc_tokens[:8]:
            low = tok.lower()
            if low not in _STOPWORDS and low not in words:
                words.append(low)
                if len(words) >= 10:
                    break

    seen: set = set()
    deduped = []
    for w in words:
        if w not in seen:
            seen.add(w)
            deduped.append(w)
    return deduped


def _emit_bundle_yaml(
    all_entities: Dict[str, Dict[str, Any]],
    output_path: Path,
    name: str,
    description: str,
    industry: str,
    standards_alignment: str,
    source_urls: Optional[List[str]] = None,
    provenance: Optional[Dict[str, Any]] = None,
) -> None:
    """Generate a bundle YAML suitable for OntologyLoader from extracted entities."""
    metadata = {
        "name": name,
        "version": "1.0",
        "format_version": "2.0",
        "industry": industry,
        "description": description,
        "standards_alignment": standards_alignment,
        "bundle_type": "formal_ontology",
    }
    if source_urls:
        metadata["source_url"] = source_urls[0] if len(source_urls) == 1 else ", ".join(source_urls)
    if provenance:
        metadata["provenance"] = provenance

    definitions = {}
    edge_catalog: Dict[str, Dict[str, Any]] = {}

    for ent_name, data in sorted(all_entities.items()):
        kw = data.get("keywords", [])
        if not kw:
            kw = _derive_keywords(ent_name, data.get("description", ""))
        rels = data.get("relationships", {})
        defn: Dict[str, Any] = {
            "description": data.get("description", f"{ent_name} entity"),
            "uri": data.get("uri", ""),
            "source_ontology": data.get("source", ""),
            "keywords": kw,
        }
        if data.get("parents"):
            defn["parents"] = data["parents"]
        if data.get("synonyms"):
            defn["synonyms"] = data["synonyms"]
        if data.get("typical_attributes"):
            defn["typical_attributes"] = data["typical_attributes"]
        if data.get("business_questions"):
            defn["business_questions"] = data["business_questions"]
        if rels:
            defn["relationships"] = {
                edge: {"target": info.get("target", ""), "cardinality": info.get("cardinality", "unknown")}
                for edge, info in rels.items()
            }
        definitions[ent_name] = defn

        for edge in data.get("outgoing_edges", []):
            ename = edge.get("name")
            if ename and ename not in edge_catalog:
                edge_catalog[ename] = {
                    "source": ent_name,
                    "target": edge.get("range", ""),
                    "uri": edge.get("uri", ""),
                }
                if edge.get("inverse"):
                    edge_catalog[ename]["inverse"] = edge["inverse"]

    metadata["entity_count"] = len(definitions)
    metadata["edge_count"] = len(edge_catalog)

    bundle = {
        "metadata": metadata,
        "ontology": {
            "version": "1.0",
            "name": f"{name} Ontology",
            "description": f"Auto-extracted from {standards_alignment}",
            "entities": {"definitions": definitions},
        },
    }
    if edge_catalog:
        bundle["ontology"]["edge_catalog"] = edge_catalog

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.dump(bundle, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    logger.info("Wrote bundle YAML: %s (%d entities, %d edges)", output_path, len(definitions), len(edge_catalog))


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _download_or_cache(url: str, fmt: str, cache_dir: Optional[Path]) -> "rdflib.Graph":
    import rdflib

    safe_name = url.replace("://", "_").replace("/", "_").replace(".", "_")[:80]
    ext = "ttl" if fmt == "turtle" else "owl"
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = cache_dir / f"{safe_name}.{ext}"
        if cache_path.exists():
            logger.info("Loading from cache: %s", cache_path)
            g = rdflib.Graph()
            g.parse(str(cache_path), format=fmt)
            return g

    logger.info("Downloading: %s", url)
    g = rdflib.Graph()
    g.parse(source=url, format=fmt)

    if cache_dir:
        cache_path = cache_dir / f"{safe_name}.{ext}"
        g.serialize(str(cache_path), format=fmt)
        logger.info("Cached to: %s", cache_path)

    return g


def _extract_classes(
    g: "rdflib.Graph",
    classes_of_interest: Set[str],
    source_label: str,
    prefix: str,
) -> Dict[str, Dict[str, Any]]:
    """Extract entity data from an rdflib graph, filtered to classes_of_interest."""
    import rdflib as _rdflib
    from rdflib import OWL, RDF, RDFS

    entities: Dict[str, Dict[str, Any]] = {}

    seen_uris = set()
    for class_type in (OWL.Class, RDFS.Class):
        for cls in g.subjects(RDF.type, class_type):
            if cls in seen_uris:
                continue
            seen_uris.add(cls)
            if prefix and not str(cls).startswith(prefix):
                continue
            _extract_single_class(g, cls, classes_of_interest, source_label, entities, OWL, RDF, RDFS)

    _extract_schema_domain_edges(g, entities, classes_of_interest, _rdflib)
    _extract_union_domain_edges(g, entities, classes_of_interest, OWL, RDF, RDFS)

    return entities


_SCHEMA_DATATYPE_NAMES = frozenset({
    "Text", "Number", "Integer", "Float", "Boolean", "Date", "DateTime",
    "Time", "URL", "Duration", "Distance", "Energy", "Mass",
})


def _extract_schema_domain_edges(g, entities, classes_of_interest, rdflib_mod):
    """Extract edges and data properties via schema:domainIncludes/rangeIncludes (Schema.org)."""
    SCHEMA_DOMAIN = rdflib_mod.URIRef("https://schema.org/domainIncludes")
    SCHEMA_RANGE = rdflib_mod.URIRef("https://schema.org/rangeIncludes")

    has_schema_props = any(g.triples((None, SCHEMA_DOMAIN, None)))
    if not has_schema_props:
        return

    for prop, _, domain_cls in g.triples((None, SCHEMA_DOMAIN, None)):
        domain_name = _local_name(domain_cls)
        if not domain_name or domain_name not in entities:
            continue
        prop_name = _local_name(prop)
        if not prop_name:
            continue
        ranges = [_local_name(r) for r in g.objects(prop, SCHEMA_RANGE) if _local_name(r)]
        obj_ranges = [r for r in ranges if r in classes_of_interest]
        data_ranges = [r for r in ranges if r in _SCHEMA_DATATYPE_NAMES]

        if obj_ranges:
            for rng in obj_ranges:
                entities[domain_name]["outgoing_edges"].append({
                    "name": prop_name, "uri": str(prop), "range": rng,
                    "ranges": obj_ranges, "inverse": None,
                })
                entities[domain_name]["relationships"][prop_name] = {
                    "target": rng, "cardinality": "unknown",
                }
        elif data_ranges:
            attrs = entities[domain_name]["typical_attributes"]
            if prop_name not in attrs:
                attrs.append(prop_name)


def _extract_union_domain_edges(g, entities, classes_of_interest, OWL, RDF, RDFS):
    """Extract edges from owl:ObjectProperty with owl:unionOf domains (used by OMOP CDM)."""
    from rdflib.collection import Collection

    for prop in g.subjects(RDF.type, OWL.ObjectProperty):
        prop_name = _local_name(prop)
        if not prop_name:
            continue
        domains = list(g.objects(prop, RDFS.domain))
        if not domains:
            continue
        domain_node = domains[0]
        member_names: List[str] = []
        union_list = list(g.objects(domain_node, OWL.unionOf))
        if union_list:
            try:
                members = list(Collection(g, union_list[0]))
                member_names = [_local_name(m) for m in members if _local_name(m)]
            except Exception:
                pass
        else:
            dn = _local_name(domain_node)
            if dn:
                member_names = [dn]

        target_names = [n for n in member_names if n in entities]
        if not target_names:
            continue

        ranges = [_local_name(r) for r in g.objects(prop, RDFS.range) if _local_name(r)]
        inverses = [_local_name(i) for i in g.objects(prop, OWL.inverseOf) if _local_name(i)]
        range_val = ranges[0] if ranges else None
        inverse_val = inverses[0] if inverses else None

        for tgt in target_names:
            existing_names = {e["name"] for e in entities[tgt]["outgoing_edges"]}
            if prop_name in existing_names:
                continue
            entities[tgt]["outgoing_edges"].append({
                "name": prop_name, "uri": str(prop), "range": range_val,
                "ranges": ranges, "inverse": inverse_val,
            })
            entities[tgt]["relationships"][prop_name] = {
                "target": range_val or "Thing", "cardinality": "unknown",
            }


def _resolve_cardinality(g, cls, OWL, RDFS) -> Dict[str, str]:
    """Extract cardinality constraints from owl:Restriction blank nodes on rdfs:subClassOf."""
    card_map: Dict[str, str] = {}
    for restriction in g.objects(cls, RDFS.subClassOf):
        on_prop = list(g.objects(restriction, OWL.onProperty))
        if not on_prop:
            continue
        prop_name = _local_name(on_prop[0])
        if not prop_name:
            continue
        min_c = list(g.objects(restriction, OWL.minCardinality))
        max_c = list(g.objects(restriction, OWL.maxCardinality))
        exact_c = list(g.objects(restriction, OWL.cardinality))
        lo = int(min_c[0]) if min_c else (int(exact_c[0]) if exact_c else None)
        hi = int(max_c[0]) if max_c else (int(exact_c[0]) if exact_c else None)
        if lo is not None or hi is not None:
            if lo == hi and lo is not None:
                card_map[prop_name] = "one-to-one" if lo <= 1 else "one-to-many"
            elif hi is not None and hi <= 1:
                card_map[prop_name] = "one-to-one"
            else:
                card_map[prop_name] = "one-to-many"
    return card_map


def _extract_single_class(g, cls, classes_of_interest, source_label, entities, OWL, RDF, RDFS):
    """Extract a single class from the graph into the entities dict."""
    name = _local_name(cls)
    if not name or name not in classes_of_interest:
        return

    comment = _get_comment(g, cls, RDFS)
    parents = [_local_name(p) for p in g.objects(cls, RDFS.subClassOf) if _local_name(p)]
    parents = [p for p in parents if p in classes_of_interest]

    outgoing_edges = []
    data_props: List[str] = []
    for prop in g.subjects(RDFS.domain, cls):
        is_obj_prop = (
            (prop, RDF.type, OWL.ObjectProperty) in g
            or (prop, RDF.type, RDF.Property) in g
        )
        is_data_prop = (prop, RDF.type, OWL.DatatypeProperty) in g
        prop_name = _local_name(prop)
        if not prop_name:
            continue
        ranges = [_local_name(r) for r in g.objects(prop, RDFS.range) if _local_name(r)]

        if is_data_prop:
            data_props.append(prop_name)
        elif is_obj_prop:
            primary_range = ranges[0] if ranges else None
            if primary_range and primary_range in _PRIMITIVE_RANGE_NAMES and primary_range not in classes_of_interest:
                data_props.append(prop_name)
            else:
                inverses = [_local_name(i) for i in g.objects(prop, OWL.inverseOf) if _local_name(i)]
                outgoing_edges.append({
                    "name": prop_name,
                    "uri": str(prop),
                    "range": primary_range,
                    "ranges": ranges,
                    "inverse": inverses[0] if inverses else None,
                })

    # Extract cardinality from OWL restrictions
    card_map = _resolve_cardinality(g, cls, OWL, RDFS)

    relationships = {}
    for edge in outgoing_edges:
        relationships[edge["name"]] = {
            "target": edge.get("range") or "Thing",
            "cardinality": card_map.get(edge["name"], "unknown"),
        }

    desc = comment or f"{name} entity"
    entities[name] = {
        "description": desc,
        "source": source_label,
        "uri": str(cls),
        "parents": parents,
        "outgoing_edges": outgoing_edges,
        "keywords": _derive_keywords(name, desc),
        "synonyms": [],
        "typical_attributes": data_props[:30],
        "business_questions": [],
        "relationships": relationships,
        "properties": {},
    }


def build_tiers(
    all_entities: Dict[str, Dict[str, Any]],
    output_dir: Path,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Generate tier 1/2/3 YAML files for entities and edges."""
    # --- Tier 1: compact list ---
    tier1 = [{"name": k, "description": v["description"][:200]} for k, v in sorted(all_entities.items())]

    # --- Tier 2 (Confirmation skill input): source_ontology, uri, parents, edge names ---
    tier2 = {}
    for name, data in all_entities.items():
        edges = data.get("outgoing_edges", [])[:8]
        tier2[name] = {
            "description": data["description"],
            "source_ontology": data["source"],
            "uri": data["uri"],
            "parents": data.get("parents", []),
            "edges": [e["name"] for e in edges],
        }

    # --- Tier 3 (Deep Classification skill input): full domain profile ---
    tier3 = {}
    for name, data in all_entities.items():
        tier3[name] = {
            "description": data["description"],
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

    # --- Edge tiers ---
    all_edges: Dict[str, Dict[str, Any]] = {}
    for ent_name, data in all_entities.items():
        for edge in data.get("outgoing_edges", []):
            ename = edge.get("name")
            if ename and ename not in all_edges:
                all_edges[ename] = {
                    "name": ename,
                    "domain": ent_name,
                    "range": edge.get("range"),
                    "ranges": edge.get("ranges", []),
                    "uri": edge.get("uri"),
                    "inverse": edge.get("inverse"),
                }

    edges_t1 = [{"name": e["name"], "domain": e["domain"], "range": e.get("range")}
                for e in sorted(all_edges.values(), key=lambda x: x["name"])]
    edges_t2 = {k: v for k, v in all_edges.items()}
    # --- URI lookup ---
    uris = {name: data["uri"] for name, data in all_entities.items() if data.get("uri")}

    counts = {
        "entities_tier1": len(tier1),
        "entities_tier2": len(tier2),
        "entities_tier3": len(tier3),
        "edges_tier1": len(edges_t1),
        "uris": len(uris),
    }

    if dry_run:
        for label, count in counts.items():
            print(f"  {label}: {count} entries")
        return counts

    output_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "entities_tier1.yaml": tier1,
        "entities_tier2.yaml": tier2,
        "entities_tier3.yaml": tier3,
        "edges_tier1.yaml": edges_t1,
        "edges_tier2.yaml": edges_t2,
        "equivalent_class_uris.yaml": uris,
    }
    for fname, data in files.items():
        path = output_dir / fname
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        logger.info("Wrote %s (%d bytes)", path, path.stat().st_size)

    return counts


def _entities_from_bundle(bundle_path: Path) -> Dict[str, Dict[str, Any]]:
    """Extract entities from a curated bundle YAML, preserving all rich fields for tier-3."""
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


def _entities_from_custom_source(
    source_path: str,
    label: str,
    classes_filter: Optional[Path],
    cache_dir: Optional[Path],
) -> Dict[str, Dict[str, Any]]:
    """Extract entities from a custom OWL/Turtle file or URL."""
    fmt = "turtle" if source_path.endswith((".ttl", ".turtle")) else "xml"
    if source_path.startswith(("http://", "https://")):
        g = _download_or_cache(source_path, fmt, cache_dir)
    else:
        import rdflib
        g = rdflib.Graph()
        g.parse(source_path, format=fmt)

    if classes_filter and classes_filter.is_file():
        coi = set(yaml.safe_load(classes_filter.read_text(encoding="utf-8")))
    else:
        from rdflib import OWL, RDF
        coi = {_local_name(c) for c in g.subjects(RDF.type, OWL.Class) if _local_name(c)}
        logger.info("No filter provided, using all %d classes from source", len(coi))

    prefix = source_path.rsplit("/", 1)[0] + "/" if "/" in source_path else ""
    return _extract_classes(g, coi, label, prefix)


def _merge_bundle_fields(
    auto_entities: Dict[str, Dict[str, Any]],
    bundle_path: Path,
) -> Dict[str, Dict[str, Any]]:
    """Merge domain-specific fields from a curated bundle YAML into auto-extracted entities.

    Auto-extracted entities keep their OWL URIs and edges. The bundle supplies
    keywords, synonyms, typical_attributes, business_questions, properties, and
    richer relationship metadata where available.
    """
    bundle_entities = _entities_from_bundle(bundle_path)
    merged_count = 0
    for name, auto_data in auto_entities.items():
        bundle_data = bundle_entities.get(name)
        if not bundle_data:
            continue
        merged_count += 1
        for field in ("keywords", "synonyms", "typical_attributes", "business_questions", "properties"):
            val = bundle_data.get(field)
            if val:
                auto_data[field] = val
        bundle_rels = bundle_data.get("relationships", {})
        if bundle_rels:
            existing = auto_data.get("relationships", {})
            existing.update(bundle_rels)
            auto_data["relationships"] = existing
    logger.info("Merged domain fields for %d/%d entities from bundle", merged_count, len(auto_entities))
    return auto_entities


def main():
    parser = argparse.ArgumentParser(
        description="Build tiered ontology indexes from published or curated sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--bundle-name", default="healthcare", help="Output bundle subdirectory name")
    parser.add_argument("--cache-dir", default=None, help="Directory to cache downloaded ontology files")
    parser.add_argument("--dry-run", action="store_true", help="Print counts without writing files")
    parser.add_argument("--merge-bundle", metavar="BUNDLE_NAME", default=None,
                        help="Merge domain fields (keywords, synonyms, etc.) from a curated bundle YAML into auto-extracted entities")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--sources", nargs="*", choices=list(SOURCES.keys()), default=None,
                       help="Which published ontology sources to include (default: all)")
    group.add_argument("--from-bundle", metavar="BUNDLE_NAME",
                       help="Generate tier indexes from a curated bundle YAML (reads uri/source_ontology from definitions)")
    group.add_argument("--custom-source", metavar="PATH_OR_URL",
                       help="Load a custom OWL/Turtle file or URL")

    parser.add_argument("--custom-label", default="Custom",
                        help="Label for custom source (used in tier files)")
    parser.add_argument("--classes-filter", default=None,
                        help="YAML file with list of class names to include (for --custom-source)")

    parser.add_argument("--emit-bundle-yaml", action="store_true",
                        help="Also generate a bundle YAML (for OntologyLoader) alongside tier files")
    parser.add_argument("--bundle-label", default=None,
                        help="Display name for the bundle (used in metadata.name)")
    parser.add_argument("--bundle-description", default=None,
                        help="Description for the bundle (used in metadata.description)")
    parser.add_argument("--bundle-industry", default="general",
                        help="Industry tag (used in metadata.industry)")
    parser.add_argument("--bundle-standards", default=None,
                        help="Standards alignment string (used in metadata.standards_alignment)")
    parser.add_argument("--upload-sources", metavar="UC_VOLUME_PATH",
                        help="Upload cached source files to UC Volume (e.g. /Volumes/catalog/schema/ontology_sources)")
    parser.add_argument("--verify", action="store_true",
                        help="Re-download source and verify SHA-256 against provenance in existing bundle")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    repo_root = Path(__file__).resolve().parent.parent
    cache_dir = Path(args.cache_dir) if args.cache_dir else None
    provenance_sources: List[Dict[str, Any]] = []

    if args.from_bundle:
        bundle_yaml = repo_root / "configurations" / "ontology_bundles" / f"{args.from_bundle}.yaml"
        if not bundle_yaml.is_file():
            logger.error("Bundle YAML not found: %s", bundle_yaml)
            sys.exit(1)
        logger.info("Generating tier indexes from curated bundle: %s", args.from_bundle)
        all_entities = _entities_from_bundle(bundle_yaml)
        output_dir = repo_root / "configurations" / "ontology_bundles" / args.from_bundle
        provenance_sources.append({
            "source_type": "curated_bundle",
            "source_file": str(bundle_yaml.name),
        })

    elif args.custom_source:
        try:
            import rdflib  # noqa: F401
        except ImportError:
            logger.error("rdflib is required. Install with: pip install 'dbxmetagen[ontology]'")
            sys.exit(1)
        classes_filter = Path(args.classes_filter) if args.classes_filter else None
        all_entities = _entities_from_custom_source(
            args.custom_source, args.custom_label, classes_filter, cache_dir,
        )
        output_dir = repo_root / "configurations" / "ontology_bundles" / args.bundle_name
        prov_entry: Dict[str, Any] = {
            "source_type": "custom_owl",
            "source_url": args.custom_source,
            "source_format": "turtle" if args.custom_source.endswith((".ttl", ".turtle")) else "xml",
        }
        # Hash local custom source if it's a file
        if not args.custom_source.startswith(("http://", "https://")):
            src_path = Path(args.custom_source)
            if src_path.is_file():
                prov_entry["source_sha256"] = _sha256_file(src_path)
                output_dir.mkdir(parents=True, exist_ok=True)
                dest = output_dir / f"source{src_path.suffix}"
                shutil.copy2(str(src_path), str(dest))
                logger.info("Stored source file: %s", dest)
        provenance_sources.append(prov_entry)

    else:
        try:
            import rdflib  # noqa: F401
        except ImportError:
            logger.error("rdflib is required. Install with: pip install 'dbxmetagen[ontology]'")
            sys.exit(1)
        discover_map = {
            "fhir_r4": _discover_fhir_resources,
            "omop_cdm": _discover_omop_classes,
            "schema_org": _discover_schema_org_classes,
        }
        sources = args.sources or list(SOURCES.keys())
        all_entities: Dict[str, Dict[str, Any]] = {}
        for source_key in sources:
            src = SOURCES[source_key]
            logger.info("Processing %s...", src["label"])
            g = _download_or_cache(src["url"], src["format"], cache_dir)
            classes_of_interest = discover_map[source_key](g, src["prefix"])
            logger.info("  Auto-discovered %d classes from %s", len(classes_of_interest), src["label"])
            entities = _extract_classes(g, classes_of_interest, src["label"], src["prefix"])
            logger.info("  Extracted %d entities from %s", len(entities), src["label"])
            all_entities.update(entities)

            # Build per-source provenance
            prov_entry = {
                "source_type": "published_owl",
                "source_url": src["url"],
                "source_format": src["format"],
                "source_label": src["label"],
                "extraction_method": "auto_discover",
                "classes_discovered": len(classes_of_interest),
                "classes_extracted": len(entities),
            }
            # Hash from cache and persist source file alongside bundle
            if cache_dir:
                safe = src["url"].replace("://", "_").replace("/", "_").replace(".", "_")[:80]
                ext = "ttl" if src["format"] == "turtle" else "owl"
                cp = cache_dir / f"{safe}.{ext}"
                if cp.exists():
                    prov_entry["source_sha256"] = _sha256_file(cp)
                    bundle_source_dir = repo_root / "configurations" / "ontology_bundles" / args.bundle_name
                    bundle_source_dir.mkdir(parents=True, exist_ok=True)
                    dest = bundle_source_dir / f"source_{source_key}.{ext}"
                    shutil.copy2(str(cp), str(dest))
                    logger.info("Persisted source: %s", dest)
            provenance_sources.append(prov_entry)

        output_dir = repo_root / "configurations" / "ontology_bundles" / args.bundle_name

    if args.merge_bundle:
        merge_yaml = repo_root / "configurations" / "ontology_bundles" / f"{args.merge_bundle}.yaml"
        if not merge_yaml.is_file():
            logger.error("Merge bundle YAML not found: %s", merge_yaml)
            sys.exit(1)
        all_entities = _merge_bundle_fields(all_entities, merge_yaml)

    logger.info("Total entities: %d", len(all_entities))

    counts = build_tiers(all_entities, output_dir, dry_run=args.dry_run)

    action = "Would write" if args.dry_run else "Wrote"
    print(f"\n{action} tier indexes to {output_dir}/")
    for label, count in counts.items():
        print(f"  {label}: {count}")

    if args.emit_bundle_yaml and not args.dry_run:
        bundle_label = args.bundle_label or args.bundle_name
        bundle_desc = args.bundle_description or f"Auto-extracted ontology bundle: {bundle_label}"
        source_urls = []
        if not args.from_bundle and not args.custom_source:
            used_sources = args.sources or list(SOURCES.keys())
            source_urls = [SOURCES[k]["url"] for k in used_sources]
            if not args.bundle_standards:
                args.bundle_standards = ", ".join(SOURCES[k]["label"] for k in used_sources)

        provenance = {
            "extraction_date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "extraction_script": "build_ontology_indexes.py",
            "sources": provenance_sources,
            "total_entities": len(all_entities),
            "total_edges": counts.get("edges_tier1", 0),
        }

        bundle_yaml_path = output_dir.parent / f"{args.bundle_name}.yaml"
        _emit_bundle_yaml(
            all_entities,
            bundle_yaml_path,
            name=bundle_label,
            description=bundle_desc,
            industry=args.bundle_industry,
            standards_alignment=args.bundle_standards or bundle_label,
            source_urls=source_urls or None,
            provenance=provenance,
        )
        print(f"  bundle_yaml: {bundle_yaml_path}")

    # --verify: check source hashes against provenance
    if args.verify:
        bundle_yaml_path = output_dir.parent / f"{args.bundle_name}.yaml"
        if bundle_yaml_path.is_file():
            raw = yaml.safe_load(bundle_yaml_path.read_text(encoding="utf-8"))
            prov = raw.get("metadata", {}).get("provenance", {})
            ok, fail = 0, 0
            for src_prov in prov.get("sources", []):
                expected = src_prov.get("source_sha256")
                url = src_prov.get("source_url")
                if not expected or not url:
                    continue
                fmt = src_prov.get("source_format", "turtle")
                import tempfile
                tmp_dir = Path(tempfile.mkdtemp())
                logger.info("Verifying %s ...", url)
                g = _download_or_cache(url, fmt, tmp_dir)
                safe = url.replace("://", "_").replace("/", "_").replace(".", "_")[:80]
                ext = "ttl" if fmt == "turtle" else "owl"
                fpath = tmp_dir / f"{safe}.{ext}"
                if fpath.exists():
                    actual = _sha256_file(fpath)
                    if actual == expected:
                        logger.info("  PASS: %s", url)
                        ok += 1
                    else:
                        logger.warning("  FAIL: %s (expected %s..., got %s...)", url, expected[:16], actual[:16])
                        fail += 1
                shutil.rmtree(tmp_dir, ignore_errors=True)
            print(f"\nVerification: {ok} passed, {fail} failed")
        else:
            logger.error("Bundle YAML not found for --verify: %s", bundle_yaml_path)

    # --upload-sources: copy source files to UC Volume
    if args.upload_sources:
        source_files = list(output_dir.glob("source_*.*")) + list(output_dir.glob("source.*"))
        if source_files:
            try:
                from databricks.sdk import WorkspaceClient
                w = WorkspaceClient()
                for sf in source_files:
                    uc_path = f"{args.upload_sources.rstrip('/')}/{sf.name}"
                    with open(sf, "rb") as fh:
                        w.files.upload(uc_path, fh, overwrite=True)
                    logger.info("Uploaded %s -> %s", sf.name, uc_path)
                print(f"\nUploaded {len(source_files)} source file(s) to {args.upload_sources}")
            except ImportError:
                logger.error("databricks-sdk required for --upload-sources")
            except Exception as e:
                logger.error("Upload failed: %s", e)
        else:
            logger.info("No source files found in %s to upload", output_dir)


if __name__ == "__main__":
    main()
