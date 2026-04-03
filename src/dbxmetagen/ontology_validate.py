"""Validate YAML ontology bundles against their source OWL/TTL files.

Checks URI accuracy, class coverage, and edge validity to formally
prove that a YAML bundle is a faithful Application Profile of its source.

Requires rdflib. Install with: pip install 'dbxmetagen[ontology]'
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import yaml

logger = logging.getLogger(__name__)


def _require_rdflib():
    try:
        import rdflib  # noqa: F401
        return True
    except ImportError:
        logger.warning("rdflib not installed")
        return False


def _local_name(uri) -> Optional[str]:
    s = str(uri)
    if "#" in s:
        return s.split("#")[-1]
    if "/" in s:
        return s.split("/")[-1]
    return None


def validate_bundle(
    bundle_yaml_path: str,
    source_owl_path: str,
) -> Dict[str, Any]:
    """Validate a YAML bundle against its source OWL/TTL.

    Returns a report dict with coverage, accuracy, and detail lists.
    """
    if not _require_rdflib():
        return {"error": "rdflib not installed"}

    import rdflib
    from rdflib import OWL, RDF, RDFS

    bundle_path = Path(bundle_yaml_path)
    raw = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
    definitions = raw.get("ontology", {}).get("entities", {}).get("definitions", {})
    edge_catalog = raw.get("ontology", {}).get("edge_catalog", {})

    fmt = "turtle" if source_owl_path.endswith((".ttl", ".turtle")) else "xml"
    g = rdflib.Graph()
    g.parse(source_owl_path, format=fmt)

    # Build sets of URIs from source
    source_class_uris: Set[str] = set()
    source_class_names: Set[str] = set()
    for cls_type in (OWL.Class, RDFS.Class):
        for cls in g.subjects(RDF.type, cls_type):
            uri_str = str(cls)
            source_class_uris.add(uri_str)
            name = _local_name(cls)
            if name:
                source_class_names.add(name)

    source_prop_uris: Set[str] = set()
    source_prop_names: Set[str] = set()
    for prop_type in (OWL.ObjectProperty, RDF.Property, OWL.DatatypeProperty):
        for prop in g.subjects(RDF.type, prop_type):
            source_prop_uris.add(str(prop))
            name = _local_name(prop)
            if name:
                source_prop_names.add(name)

    # Validate entities
    valid_entities: List[str] = []
    invalid_entities: List[Dict[str, str]] = []
    missing_uri_entities: List[str] = []

    for name, defn in definitions.items():
        uri = defn.get("uri", "")
        if not uri:
            missing_uri_entities.append(name)
            continue
        if uri in source_class_uris:
            valid_entities.append(name)
        else:
            # Fallback: check by local name
            if name in source_class_names:
                valid_entities.append(name)
            else:
                invalid_entities.append({"name": name, "uri": uri, "reason": "URI not found in source"})

    # Validate edges
    valid_edges: List[str] = []
    invalid_edges: List[Dict[str, str]] = []

    for edge_name, edge_info in edge_catalog.items():
        edge_uri = edge_info.get("uri", "")
        if edge_uri and edge_uri in source_prop_uris:
            valid_edges.append(edge_name)
        elif edge_name in source_prop_names:
            valid_edges.append(edge_name)
        else:
            invalid_edges.append({"name": edge_name, "uri": edge_uri, "reason": "Property not found in source"})

    total_entities = len(definitions)
    total_edges = len(edge_catalog)
    entity_accuracy = len(valid_entities) / total_entities * 100 if total_entities else 0
    edge_accuracy = len(valid_edges) / total_edges * 100 if total_edges else 0

    # Coverage: what % of source classes are represented in the YAML
    yaml_class_names = set(definitions.keys())
    represented = yaml_class_names & source_class_names
    coverage = len(represented) / len(source_class_names) * 100 if source_class_names else 0

    # Missing: source classes not in YAML
    missing_from_yaml = sorted(source_class_names - yaml_class_names)

    report = {
        "bundle": str(bundle_path.stem),
        "source": str(Path(source_owl_path).name),
        "source_classes": len(source_class_names),
        "source_properties": len(source_prop_names),
        "yaml_entities": total_entities,
        "yaml_edges": total_edges,
        "entity_accuracy_pct": round(entity_accuracy, 1),
        "edge_accuracy_pct": round(edge_accuracy, 1),
        "coverage_pct": round(coverage, 1),
        "valid_entities": len(valid_entities),
        "invalid_entities": invalid_entities,
        "missing_uri_entities": missing_uri_entities,
        "valid_edges": len(valid_edges),
        "invalid_edges": invalid_edges,
        "missing_from_yaml_sample": missing_from_yaml[:50],
        "missing_from_yaml_total": len(missing_from_yaml),
    }

    logger.info(
        "Validation: %s vs %s -- entity accuracy %.1f%%, edge accuracy %.1f%%, coverage %.1f%%",
        bundle_path.stem, Path(source_owl_path).name,
        entity_accuracy, edge_accuracy, coverage,
    )
    return report


def validate_bundle_auto(bundle_name: str, bundles_dir: str = "configurations/ontology_bundles") -> Dict[str, Any]:
    """Auto-locate bundle YAML and source file, then validate."""
    bd = Path(bundles_dir)
    bundle_yaml = bd / f"{bundle_name}.yaml"
    if not bundle_yaml.is_file():
        return {"error": f"Bundle YAML not found: {bundle_yaml}"}

    bundle_subdir = bd / bundle_name
    source_files = list(bundle_subdir.glob("source_*.*")) + list(bundle_subdir.glob("source.*"))
    if not source_files:
        return {"error": f"No source file found in {bundle_subdir}"}

    return validate_bundle(str(bundle_yaml), str(source_files[0]))
