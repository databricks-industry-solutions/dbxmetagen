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
import logging
import sys
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
        "url": "https://raw.githubusercontent.com/vemonet/omop-cdm-owl/main/omop-cdm.owl",
        "format": "xml",
        "prefix": "https://w3id.org/omop-cdm/",
        "label": "OMOP CDM",
    },
    "schema_org": {
        "url": "https://schema.org/version/latest/schemaorg-current-https.ttl",
        "format": "turtle",
        "prefix": "https://schema.org/",
        "label": "Schema.org",
    },
}

# Healthcare-relevant classes to extract (subset of full ontologies)
FHIR_CLASSES_OF_INTEREST: Set[str] = {
    "Patient", "Practitioner", "Organization", "Encounter", "Condition",
    "Procedure", "Observation", "DiagnosticReport", "MedicationRequest",
    "Medication", "AllergyIntolerance", "Immunization", "CarePlan",
    "Claim", "Coverage", "ExplanationOfBenefit", "Location", "Device",
    "Specimen", "ServiceRequest", "DocumentReference", "Consent",
    "CareTeam", "RelatedPerson", "HealthcareService", "Endpoint",
    "PractitionerRole", "Schedule", "Slot", "Appointment",
    "Communication", "Flag", "Goal", "NutritionOrder",
    "MedicationAdministration", "MedicationDispense", "MedicationStatement",
    "RiskAssessment", "FamilyMemberHistory", "DetectedIssue",
    "ClinicalImpression", "QuestionnaireResponse", "Questionnaire",
    "MeasureReport", "Measure", "Group", "ResearchStudy", "ResearchSubject",
    "Substance", "SupplyRequest", "SupplyDelivery", "Task",
}

OMOP_CLASSES_OF_INTEREST: Set[str] = {
    "PERSON", "VISIT_OCCURRENCE", "CONDITION_OCCURRENCE", "DRUG_EXPOSURE",
    "PROCEDURE_OCCURRENCE", "MEASUREMENT", "OBSERVATION", "DEATH",
    "COST", "PAYER_PLAN_PERIOD", "DRUG_ERA", "CONDITION_ERA",
    "PROVIDER", "CARE_SITE", "LOCATION", "CONCEPT", "VOCABULARY",
    "DOMAIN", "CONCEPT_CLASS", "RELATIONSHIP", "CONCEPT_SYNONYM",
    "SPECIMEN", "NOTE", "DEVICE_EXPOSURE", "EPISODE", "EPISODE_EVENT",
}

SCHEMA_ORG_CLASSES_OF_INTEREST: Set[str] = {
    "Person", "Organization", "Place", "Event", "Product", "Offer",
    "MedicalEntity", "MedicalCondition", "Drug", "Hospital",
    "Physician", "Patient", "MedicalProcedure", "MedicalTest",
    "DiagnosticLab", "MedicalClinic", "InsurancePlan",
    "CreativeWork", "Dataset", "PropertyValue", "QuantitativeValue",
}


def _local_name(uri) -> Optional[str]:
    s = str(uri)
    if "#" in s:
        return s.split("#")[-1]
    if "/" in s:
        return s.split("/")[-1]
    return None


def _get_comment(g, subject, rdfs) -> str:
    for comment in g.objects(subject, rdfs.comment):
        return str(comment)[:120]
    return ""


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
    from rdflib import OWL, RDF, RDFS

    entities: Dict[str, Dict[str, Any]] = {}

    for cls in g.subjects(RDF.type, OWL.Class):
        name = _local_name(cls)
        if not name or name not in classes_of_interest:
            continue

        comment = _get_comment(g, cls, RDFS)
        parents = [_local_name(p) for p in g.objects(cls, RDFS.subClassOf) if _local_name(p)]
        parents = [p for p in parents if p in classes_of_interest]

        outgoing_edges = []
        for prop in g.subjects(RDFS.domain, cls):
            if (prop, RDF.type, OWL.ObjectProperty) in g:
                prop_name = _local_name(prop)
                ranges = [_local_name(r) for r in g.objects(prop, RDFS.range) if _local_name(r)]
                inverses = [_local_name(i) for i in g.objects(prop, OWL.inverseOf) if _local_name(i)]
                outgoing_edges.append({
                    "name": prop_name,
                    "uri": str(prop),
                    "range": ranges[0] if ranges else None,
                    "inverse": inverses[0] if inverses else None,
                })

        relationships = {}
        for edge in outgoing_edges:
            relationships[edge["name"]] = {
                "target": edge.get("range") or "Thing",
                "cardinality": "unknown",
            }

        entities[name] = {
            "description": comment or f"{name} entity",
            "source": source_label,
            "uri": str(cls),
            "parent": parents[0] if parents else None,
            "outgoing_edges": outgoing_edges,
            "keywords": [],
            "synonyms": [],
            "typical_attributes": [],
            "business_questions": [],
            "relationships": relationships,
            "properties": {},
        }

    return entities


def build_tiers(
    all_entities: Dict[str, Dict[str, Any]],
    output_dir: Path,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Generate tier 1/2/3 YAML files for entities and edges."""
    # --- Tier 1: compact list ---
    tier1 = [{"name": k, "description": v["description"][:120]} for k, v in sorted(all_entities.items())]

    # --- Tier 2 (Confirmation skill input): source_ontology, uri, edge names ---
    tier2 = {}
    for name, data in all_entities.items():
        edges = data.get("outgoing_edges", [])[:8]
        tier2[name] = {
            "description": data["description"],
            "source_ontology": data["source"],
            "uri": data["uri"],
            "edges": [e["name"] for e in edges],
        }

    # --- Tier 3 (Deep Classification skill input): full domain profile ---
    tier3 = {}
    for name, data in all_entities.items():
        tier3[name] = {
            "description": data["description"],
            "source_ontology": data["source"],
            "uri": data["uri"],
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
                    "uri": edge.get("uri"),
                    "inverse": edge.get("inverse"),
                }

    edges_t1 = [{"name": e["name"], "domain": e["domain"], "range": e.get("range")}
                for e in sorted(all_edges.values(), key=lambda x: x["name"])]
    edges_t2 = {k: v for k, v in all_edges.items()}
    edges_t3 = {}
    for k, v in all_edges.items():
        entry = dict(v)
        if "inverse" not in entry:
            inv_candidates = [e for e in all_edges.values() if e.get("domain") == v.get("range") and e.get("range") == v.get("domain")]
            if inv_candidates:
                entry["inverse"] = inv_candidates[0]["name"]
        if "domain_constraints" not in entry and v.get("domain"):
            entry["domain_constraints"] = [v["domain"]]
        if "range_constraints" not in entry and v.get("range"):
            entry["range_constraints"] = [v["range"]]
        edges_t3[k] = entry

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
        "edges_tier3.yaml": edges_t3,
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

        all_entities[name] = {
            "description": defn.get("description", f"{name} entity"),
            "source": source,
            "uri": uri,
            "parent": defn.get("parent"),
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
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    repo_root = Path(__file__).resolve().parent.parent
    cache_dir = Path(args.cache_dir) if args.cache_dir else None

    if args.from_bundle:
        bundle_yaml = repo_root / "configurations" / "ontology_bundles" / f"{args.from_bundle}.yaml"
        if not bundle_yaml.is_file():
            logger.error("Bundle YAML not found: %s", bundle_yaml)
            sys.exit(1)
        logger.info("Generating tier indexes from curated bundle: %s", args.from_bundle)
        all_entities = _entities_from_bundle(bundle_yaml)
        output_dir = repo_root / "configurations" / "ontology_bundles" / args.from_bundle

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

    else:
        try:
            import rdflib  # noqa: F401
        except ImportError:
            logger.error("rdflib is required. Install with: pip install 'dbxmetagen[ontology]'")
            sys.exit(1)
        interest_map = {
            "fhir_r4": FHIR_CLASSES_OF_INTEREST,
            "omop_cdm": OMOP_CLASSES_OF_INTEREST,
            "schema_org": SCHEMA_ORG_CLASSES_OF_INTEREST,
        }
        sources = args.sources or list(SOURCES.keys())
        all_entities: Dict[str, Dict[str, Any]] = {}
        for source_key in sources:
            src = SOURCES[source_key]
            logger.info("Processing %s...", src["label"])
            g = _download_or_cache(src["url"], src["format"], cache_dir)
            entities = _extract_classes(g, interest_map[source_key], src["label"], src["prefix"])
            logger.info("  Extracted %d entities from %s", len(entities), src["label"])
            all_entities.update(entities)
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


if __name__ == "__main__":
    main()
