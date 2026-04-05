"""Serialize ontology predictions to OWL/Turtle format via rdflib.

Produces OWL output with:
- owl:equivalentClass linking UC tables to published ontology URIs
- rdfs:subClassOf for entity hierarchy
- owl:ObjectProperty for predicted edges
- dcterms:conformsTo / prov:wasDerivedFrom provenance triples
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


def _require_rdflib():
    try:
        import rdflib  # noqa: F401
        return True
    except ImportError:
        logger.warning("rdflib not installed. Install with: pip install 'dbxmetagen[ontology]'")
        return False


def _load_source_axioms(
    source_turtle_path: str,
    entity_uris: Set[str],
) -> Optional["rdflib.Graph"]:
    """Load relevant axioms from a source OWL/TTL for the given entity URIs.

    Extracts subClassOf chains, restrictions, and property definitions
    for only the classes referenced by our predictions.
    """
    if not _require_rdflib():
        return None
    import rdflib
    from rdflib import OWL, RDF, RDFS

    src = rdflib.Graph()
    fmt = "turtle" if source_turtle_path.endswith((".ttl", ".turtle")) else "xml"
    src.parse(source_turtle_path, format=fmt)

    subset = rdflib.Graph()
    uri_refs = {rdflib.URIRef(u) for u in entity_uris if u}

    for uri in uri_refs:
        # subClassOf chain (up to 5 hops)
        frontier = {uri}
        visited: Set = set()
        for _ in range(5):
            next_f: Set = set()
            for node in frontier:
                if node in visited:
                    continue
                visited.add(node)
                for parent in src.objects(node, RDFS.subClassOf):
                    if isinstance(parent, rdflib.URIRef):
                        subset.add((node, RDFS.subClassOf, parent))
                        next_f.add(parent)
                    elif isinstance(parent, rdflib.BNode):
                        # owl:Restriction blank nodes
                        for p, o in src.predicate_objects(parent):
                            subset.add((parent, p, o))
                        subset.add((node, RDFS.subClassOf, parent))
            frontier = next_f

        # Labels and comments from source
        for pred in (RDFS.label, RDFS.comment):
            for val in src.objects(uri, pred):
                subset.add((uri, pred, val))

        # Properties where this class is the domain
        for prop in src.subjects(RDFS.domain, uri):
            for p, o in src.predicate_objects(prop):
                subset.add((prop, p, o))

    logger.info("Extracted %d source axiom triples for %d entities", len(subset), len(uri_refs))
    return subset


def build_turtle(
    catalog: str,
    schema: str,
    table_predictions: Dict[str, Dict[str, Any]],
    edge_predictions: List[Dict[str, Any]],
    column_metadata: Optional[Dict[str, List[Dict[str, str]]]] = None,
    uri_lookup: Optional[Dict[str, str]] = None,
    source_turtle_paths: Optional[List[str]] = None,
    source_ontology_uris: Optional[List[str]] = None,
    bundle_provenance: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Build an OWL Turtle string from ontology prediction results.

    Args:
        catalog: Unity Catalog catalog name.
        schema: Schema name.
        table_predictions: {table_name: {predicted_entity, equivalent_class_uri, source_ontology, ...}}
        edge_predictions: [{from_table, to_table, predicted_edge, edge_uri, inverse, ...}]
        column_metadata: {table_name: [{name, data_type, ...}]}
        uri_lookup: {entity_name: URI} fallback for missing URIs.
        source_turtle_paths: paths to source OWL/TTL files for axiom enrichment.
        source_ontology_uris: URIs of source ontologies for dcterms:conformsTo.
        bundle_provenance: provenance dict from bundle metadata.

    Returns:
        Turtle string, or None if rdflib is not available.
    """
    if not _require_rdflib():
        return None

    from rdflib import Graph, Literal, URIRef, Namespace, RDF, RDFS, OWL, XSD  # noqa: E402

    HBO = Namespace("https://ontology.databricks.com/dbxmetagen#")
    UC = Namespace("https://databricks.com/unitycatalog/")
    FHIR = Namespace("http://hl7.org/fhir/")
    OMOP = Namespace("https://w3id.org/omop-cdm/")
    SCHEMA = Namespace("https://schema.org/")
    DCTERMS = Namespace("http://purl.org/dc/terms/")
    PROV = Namespace("http://www.w3.org/ns/prov#")

    g = Graph()
    g.bind("hbo", HBO)
    g.bind("uc", UC)
    g.bind("fhir", FHIR)
    g.bind("omop", OMOP)
    g.bind("schema", SCHEMA)
    g.bind("owl", OWL)
    g.bind("dcterms", DCTERMS)
    g.bind("prov", PROV)

    column_metadata = column_metadata or {}
    uri_lookup = uri_lookup or {}

    # Collect all equivalent URIs for source axiom loading
    entity_uris: Set[str] = set()

    for table_name, pred in table_predictions.items():
        table_uri = UC[f"{catalog}/{schema}/{table_name}"]
        entity_name = pred.get("predicted_entity", table_name)
        equiv_uri = pred.get("equivalent_class_uri") or uri_lookup.get(entity_name)

        g.add((table_uri, RDF.type, OWL.Class))
        g.add((table_uri, RDFS.label, Literal(table_name)))

        if pred.get("source_ontology"):
            g.add((table_uri, RDFS.comment, Literal(f"Source: {pred['source_ontology']}")))

        if equiv_uri:
            g.add((table_uri, OWL.equivalentClass, URIRef(equiv_uri)))
            entity_uris.add(equiv_uri)

        hbo_class = HBO[entity_name]
        g.add((hbo_class, RDF.type, OWL.Class))
        if equiv_uri:
            g.add((hbo_class, OWL.equivalentClass, URIRef(equiv_uri)))
        g.add((table_uri, RDFS.subClassOf, hbo_class))

        for col in column_metadata.get(table_name, []):
            col_name = col.get("name") or col.get("column_name", "")
            col_uri = UC[f"{catalog}/{schema}/{table_name}/{col_name}"]
            g.add((col_uri, RDF.type, OWL.DatatypeProperty))
            g.add((col_uri, RDFS.domain, table_uri))
            g.add((col_uri, RDFS.label, Literal(col_name)))

    for edge in edge_predictions:
        from_uri = UC[f"{catalog}/{schema}/{edge['from_table']}"]
        to_uri = UC[f"{catalog}/{schema}/{edge['to_table']}"]
        edge_name = edge.get("predicted_edge", "references")
        edge_uri_str = edge.get("edge_uri")
        edge_uri = URIRef(edge_uri_str) if edge_uri_str else HBO[edge_name]

        g.add((edge_uri, RDF.type, OWL.ObjectProperty))
        g.add((edge_uri, RDFS.domain, from_uri))
        g.add((edge_uri, RDFS.range, to_uri))
        g.add((edge_uri, RDFS.label, Literal(edge_name)))

        inverse = edge.get("inverse")
        if inverse:
            inv_uri = HBO[inverse]
            g.add((edge_uri, OWL.inverseOf, inv_uri))

    # Enrich with source axioms (subClassOf chains, restrictions)
    for src_path in (source_turtle_paths or []):
        try:
            axiom_graph = _load_source_axioms(src_path, entity_uris)
            if axiom_graph:
                g += axiom_graph
        except Exception as e:
            logger.warning("Could not load source axioms from %s: %s", src_path, e)

    # Provenance: dcterms:conformsTo for source ontologies
    ontology_node = HBO[f"{catalog}_{schema}_ontology"]
    g.add((ontology_node, RDF.type, OWL.Ontology))
    g.add((ontology_node, RDFS.label, Literal(f"dbxmetagen ontology for {catalog}.{schema}")))

    for onto_uri in (source_ontology_uris or []):
        g.add((ontology_node, DCTERMS.conformsTo, URIRef(onto_uri)))

    # prov:wasDerivedFrom
    if bundle_provenance:
        for src_prov in bundle_provenance.get("sources", []):
            src_url = src_prov.get("source_url")
            if src_url:
                g.add((ontology_node, PROV.wasDerivedFrom, URIRef(src_url)))
        ext_date = bundle_provenance.get("extraction_date")
        if ext_date:
            g.add((ontology_node, DCTERMS.created, Literal(ext_date, datatype=XSD.dateTime)))

        gen_id = bundle_provenance.get("generation_id") or bundle_provenance.get("job_run_id")
        if gen_id:
            safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in str(gen_id))[:96]
            act = URIRef(HBO[f"activity_{safe}"])
            g.add((act, RDF.type, PROV.Activity))
            g.add((ontology_node, PROV.wasGeneratedBy, act))
            bv = bundle_provenance.get("bundle_version") or bundle_provenance.get("bundle_id")
            if bv:
                g.add((act, DCTERMS.identifier, Literal(str(bv))))
            th = bundle_provenance.get("tier_index_hash")
            if th:
                g.add((act, RDFS.comment, Literal(f"tier_index_hash={th}")))
            mid = bundle_provenance.get("model_endpoint") or bundle_provenance.get("model_id")
            if mid:
                tool = HBO[f"model_{hash(str(mid)) % 10_000_000}"]
                g.add((tool, RDF.type, PROV.Entity))
                g.add((tool, RDFS.label, Literal(str(mid))))
                g.add((act, PROV.used, tool))

    return g.serialize(format="turtle")


def write_turtle(
    catalog: str,
    schema: str,
    table_predictions: Dict[str, Dict[str, Any]],
    edge_predictions: List[Dict[str, Any]],
    output_dir: str = "output/turtle",
    column_metadata: Optional[Dict[str, List[Dict[str, str]]]] = None,
    uri_lookup: Optional[Dict[str, str]] = None,
    source_turtle_paths: Optional[List[str]] = None,
    source_ontology_uris: Optional[List[str]] = None,
    bundle_provenance: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Build Turtle and write to file.

    Returns:
        Path to the written file, or None if rdflib unavailable.
    """
    ttl = build_turtle(
        catalog, schema, table_predictions, edge_predictions,
        column_metadata, uri_lookup,
        source_turtle_paths=source_turtle_paths,
        source_ontology_uris=source_ontology_uris,
        bundle_provenance=bundle_provenance,
    )
    if ttl is None:
        return None

    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    filename = out_path / f"{catalog}_{schema}.ttl"
    filename.write_text(ttl, encoding="utf-8")
    logger.info("Wrote Turtle to %s (%d bytes)", filename, len(ttl))
    return str(filename)
