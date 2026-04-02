"""Serialize ontology predictions to OWL/Turtle format via rdflib.

Produces valid OWL output with:
- owl:equivalentClass linking UC tables to published ontology URIs
- rdfs:subClassOf for entity hierarchy
- owl:ObjectProperty / owl:DatatypeProperty for columns
- owl:inverseOf for bidirectional edges
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _require_rdflib():
    try:
        import rdflib  # noqa: F401
        return True
    except ImportError:
        logger.warning("rdflib not installed. Install with: pip install 'dbxmetagen[ontology]'")
        return False


def build_turtle(
    catalog: str,
    schema: str,
    table_predictions: Dict[str, Dict[str, Any]],
    edge_predictions: List[Dict[str, Any]],
    column_metadata: Optional[Dict[str, List[Dict[str, str]]]] = None,
    uri_lookup: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Build an OWL Turtle string from ontology prediction results.
    
    Args:
        catalog: Unity Catalog catalog name.
        schema: Schema name.
        table_predictions: {table_name: {predicted_entity, equivalent_class_uri, source_ontology, ...}}
        edge_predictions: [{from_table, to_table, predicted_edge, edge_uri, inverse, ...}]
        column_metadata: {table_name: [{name, data_type, ...}]}
        uri_lookup: {entity_name: URI} fallback for missing URIs.
    
    Returns:
        Turtle string, or None if rdflib is not available.
    """
    if not _require_rdflib():
        return None

    from rdflib import Graph, Literal, URIRef, Namespace, RDF, RDFS, OWL  # noqa: E402

    HBO = Namespace("https://ontology.databricks.com/dbxmetagen#")
    UC = Namespace("https://databricks.com/unitycatalog/")
    FHIR = Namespace("http://hl7.org/fhir/")
    OMOP = Namespace("https://w3id.org/omop-cdm/")
    SCHEMA = Namespace("https://schema.org/")

    g = Graph()
    g.bind("hbo", HBO)
    g.bind("uc", UC)
    g.bind("fhir", FHIR)
    g.bind("omop", OMOP)
    g.bind("schema", SCHEMA)
    g.bind("owl", OWL)

    column_metadata = column_metadata or {}
    uri_lookup = uri_lookup or {}

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

    return g.serialize(format="turtle")


def write_turtle(
    catalog: str,
    schema: str,
    table_predictions: Dict[str, Dict[str, Any]],
    edge_predictions: List[Dict[str, Any]],
    output_dir: str = "output/turtle",
    column_metadata: Optional[Dict[str, List[Dict[str, str]]]] = None,
    uri_lookup: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Build Turtle and write to file.
    
    Returns:
        Path to the written file, or None if rdflib unavailable.
    """
    ttl = build_turtle(catalog, schema, table_predictions, edge_predictions,
                       column_metadata, uri_lookup)
    if ttl is None:
        return None

    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    filename = out_path / f"{catalog}_{schema}.ttl"
    filename.write_text(ttl, encoding="utf-8")
    logger.info("Wrote Turtle to %s (%d bytes)", filename, len(ttl))
    return str(filename)
