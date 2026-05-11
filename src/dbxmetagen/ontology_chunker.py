"""Generate text chunks from ontology bundles/OWL files for vector search retrieval.

Produces a flat list of chunk dicts suitable for writing to the ``ontology_chunks``
Delta table and indexing via Databricks Vector Search. Each chunk represents one
entity class or one edge/property definition, with a ``content`` field optimised
for embedding-based retrieval of ontology candidates during entity/edge classification.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import yaml

logger = logging.getLogger(__name__)

_MAX_DESC = 600
_MAX_KEYWORDS = 20
_MAX_COL_PATTERNS = 40
_MAX_RELS = 15
_MAX_QUESTIONS = 5
_MAX_QUESTION_LEN = 120
_ENDPOINT_DESC_LEN = 80


def _join(items: List[str], sep: str = ", ", limit: int = 0) -> str:
    if limit:
        items = items[:limit]
    return sep.join(str(i) for i in items if i)


def _collect_property_aliases(data: Dict[str, Any]) -> List[str]:
    """Extract column-name aliases from the properties dict + typical_attributes."""
    seen: set = set()
    aliases: List[str] = []
    for attr in (data.get("typical_attributes") or []):
        low = str(attr).lower()
        if low not in seen:
            seen.add(low)
            aliases.append(str(attr))
    for _pname, pinfo in (data.get("properties") or {}).items():
        if not isinstance(pinfo, dict):
            continue
        for attr in (pinfo.get("typical_attributes") or []):
            low = str(attr).lower()
            if low not in seen:
                seen.add(low)
                aliases.append(str(attr))
    return aliases


def _entity_content(name: str, data: Dict[str, Any]) -> str:
    """Assemble a rich text chunk for an entity class.

    Field order is optimised for embedding retrieval: column patterns
    (highest query-overlap) first, then description, then business
    questions, then taxonomy/relationship context.
    """
    parts = [name]
    col_patterns = _collect_property_aliases(data)
    if col_patterns:
        parts.append(f"Column patterns: {_join(col_patterns, limit=_MAX_COL_PATTERNS)}")
    parts.append((data.get("description") or name)[:_MAX_DESC])
    bqs = data.get("business_questions") or []
    if bqs:
        q_strs = [str(q)[:_MAX_QUESTION_LEN] for q in bqs[:_MAX_QUESTIONS]]
        parts.append(f"Questions: {_join(q_strs, '; ')}")
    kw = data.get("keywords") or []
    if kw:
        parts.append(f"Keywords: {_join(kw, limit=_MAX_KEYWORDS)}")
    syns = data.get("synonyms") or []
    if syns:
        parts.append(f"Also known as: {_join(syns)}")
    parents = data.get("parents") or []
    if not parents and data.get("parent"):
        parents = [data["parent"]]
    if parents:
        parts.append(f"Parents: {_join(parents)}")
    rels = data.get("relationships") or {}
    if rels:
        rel_strs = []
        for rname, rinfo in list(rels.items())[:_MAX_RELS]:
            target = rinfo.get("target", "") if isinstance(rinfo, dict) else str(rinfo)
            rel_strs.append(f"{rname}->{target}")
        parts.append(f"Related: {_join(rel_strs, '; ')}")
    return ". ".join(parts) + "."


def _edge_content(
    name: str,
    data: Dict[str, Any],
    domain_desc: str = "",
    range_desc: str = "",
) -> str:
    """Assemble a rich text chunk for an edge/property.

    When ``domain_desc`` / ``range_desc`` are provided, they are appended
    as parentheticals to aid disambiguation in large ontologies.
    """
    domain = data.get("domain") or "Any"
    range_ = data.get("range") or "Any"
    if isinstance(domain, list):
        domain = _join(domain)
    if isinstance(range_, list):
        range_ = _join(range_)
    dom_label = f"{domain} ({domain_desc[:_ENDPOINT_DESC_LEN]})" if domain_desc else domain
    rng_label = f"{range_} ({range_desc[:_ENDPOINT_DESC_LEN]})" if range_desc else range_
    parts = [f"{name}: relationship from {dom_label} to {rng_label}"]
    cat = data.get("category") or data.get("facet")
    if cat:
        parts.append(f"Category: {cat}")
    inv = data.get("inverse")
    if inv:
        parts.append(f"Inverse: {inv}")
    card = data.get("cardinality")
    if card and card != "unknown":
        parts.append(f"Cardinality: {card}")
    desc = data.get("description")
    if desc:
        parts.append(desc[:400])
    return ". ".join(parts) + "."


def chunks_from_bundle(bundle_path: str, bundle_name: str) -> List[Dict[str, Any]]:
    """Generate chunks from a curated dbxmetagen bundle YAML.

    Works for v1 and v2 bundles alike. Reads ``ontology.entities.definitions``
    and ``ontology.edge_catalog`` to produce one chunk per entity and one per edge.
    """
    raw = yaml.safe_load(Path(bundle_path).read_text(encoding="utf-8"))
    ontology = raw.get("ontology", {})
    definitions = ontology.get("entities", {}).get("definitions", {})
    edge_catalog = ontology.get("edge_catalog", {})
    meta = raw.get("metadata", {})
    source_label = meta.get("standards_alignment") or meta.get("name") or bundle_name
    now = datetime.now(timezone.utc).isoformat()
    chunks: List[Dict[str, Any]] = []

    # Build a lookup of entity descriptions for enriching edge chunks
    entity_descs: Dict[str, str] = {}
    for ename, edefn in definitions.items():
        entity_descs[ename] = (edefn.get("description") or "")[:_ENDPOINT_DESC_LEN]

    for name, defn in definitions.items():
        parents = defn.get("parents") or []
        if not parents and defn.get("parent"):
            parents = [defn["parent"]]
        chunks.append({
            "chunk_id": f"entity::{bundle_name}::{name}",
            "chunk_type": "entity",
            "ontology_bundle": bundle_name,
            "source_ontology": defn.get("source_ontology") or source_label,
            "name": name,
            "content": _entity_content(name, defn),
            "uri": defn.get("uri") or "",
            "domain": None,
            "range_entity": None,
            "parent_entities": _join(parents),
            "keywords": _join(defn.get("keywords") or [], "; "),
            "tier": "tier1_plus",
            "updated_at": now,
        })

    # Build edge chunks from edge_catalog + entity relationships for richer context
    seen_edges: set = set()
    for ename, einfo in edge_catalog.items():
        seen_edges.add(ename)
        dom_str = str(einfo.get("domain") or "")
        rng_str = str(einfo.get("range") or "")
        chunks.append({
            "chunk_id": f"edge::{bundle_name}::{ename}",
            "chunk_type": "edge",
            "ontology_bundle": bundle_name,
            "source_ontology": source_label,
            "name": ename,
            "content": _edge_content(
                ename, einfo,
                domain_desc=entity_descs.get(dom_str, ""),
                range_desc=entity_descs.get(rng_str, ""),
            ),
            "uri": einfo.get("uri") or "",
            "domain": dom_str,
            "range_entity": rng_str,
            "parent_entities": None,
            "keywords": None,
            "tier": "tier1_plus",
            "updated_at": now,
        })

    # Pick up edges declared in entity relationships but missing from edge_catalog
    for ent_name, defn in definitions.items():
        for rname, rinfo in (defn.get("relationships") or {}).items():
            if rname in seen_edges:
                continue
            seen_edges.add(rname)
            target = rinfo.get("target", "") if isinstance(rinfo, dict) else str(rinfo)
            edge_data = {
                "domain": ent_name,
                "range": target,
                "cardinality": rinfo.get("cardinality", "unknown") if isinstance(rinfo, dict) else "unknown",
            }
            chunks.append({
                "chunk_id": f"edge::{bundle_name}::{rname}",
                "chunk_type": "edge",
                "ontology_bundle": bundle_name,
                "source_ontology": source_label,
                "name": rname,
                "content": _edge_content(
                    rname, edge_data,
                    domain_desc=entity_descs.get(ent_name, ""),
                    range_desc=entity_descs.get(target, ""),
                ),
                "uri": "",
                "domain": ent_name,
                "range_entity": target,
                "parent_entities": None,
                "keywords": None,
                "tier": "tier1_plus",
                "updated_at": now,
            })

    logger.info(
        "chunks_from_bundle(%s): %d entity + %d edge chunks",
        bundle_name,
        sum(1 for c in chunks if c["chunk_type"] == "entity"),
        sum(1 for c in chunks if c["chunk_type"] == "edge"),
    )
    return chunks


def chunks_from_owl(owl_path: str, bundle_name: str) -> List[Dict[str, Any]]:
    """Generate chunks from an OWL/TTL file using rdflib SPARQL extraction.

    Extracts ``owl:Class`` entities with labels, comments, and parent classes,
    and ``owl:ObjectProperty`` edges with domain/range/inverse metadata.
    Requires rdflib (install with ``pip install 'dbxmetagen[ontology]'``).
    """
    try:
        import rdflib
        from rdflib import OWL, RDF, RDFS
    except ImportError as exc:
        raise ImportError(
            "rdflib is required for OWL chunk extraction. "
            "Install with: pip install 'dbxmetagen[ontology]'"
        ) from exc

    g = rdflib.Graph()
    fmt = "turtle" if owl_path.endswith(".ttl") else "xml"
    g.parse(owl_path, format=fmt)

    from dbxmetagen.ontology_import import _detect_source_ontology, _local_name, _get_comment
    source_label = _detect_source_ontology(g) or "OWL"
    now = datetime.now(timezone.utc).isoformat()
    chunks: List[Dict[str, Any]] = []

    # Collect class comments/labels for endpoint description enrichment
    class_comments: Dict[str, str] = {}
    class_labels: Dict[str, List[str]] = {}
    for cls in g.subjects(RDF.type, OWL.Class):
        cname = _local_name(cls)
        if not cname:
            continue
        comment = _get_comment(g, cls, RDFS) or ""
        class_comments[cname] = comment[:_ENDPOINT_DESC_LEN]
        labels = [str(lbl) for lbl in g.objects(cls, RDFS.label)]
        if labels:
            class_labels[cname] = labels

    # Collect data properties per domain class for enriching entity chunks
    class_attrs: Dict[str, List[str]] = {}
    for prop in g.subjects(RDF.type, OWL.DatatypeProperty):
        pname = _local_name(prop)
        if not pname:
            continue
        for d in g.objects(prop, RDFS.domain):
            dn = _local_name(d)
            if dn:
                class_attrs.setdefault(dn, []).append(pname)

    # Collect object property edges per domain class
    class_rels: Dict[str, List[str]] = {}
    for prop in g.subjects(RDF.type, OWL.ObjectProperty):
        pname = _local_name(prop)
        if not pname:
            continue
        for d in g.objects(prop, RDFS.domain):
            dn = _local_name(d)
            if dn:
                ranges = [_local_name(r) for r in g.objects(prop, RDFS.range) if _local_name(r)]
                target = ranges[0] if ranges else "?"
                class_rels.setdefault(dn, []).append(f"{pname}->{target}")

    # Entity chunks from owl:Class
    for cls in g.subjects(RDF.type, OWL.Class):
        name = _local_name(cls)
        if not name or name.startswith("_"):
            continue
        comment = _get_comment(g, cls, RDFS) or f"{name} entity"
        parents = [_local_name(p) for p in g.objects(cls, RDFS.subClassOf) if _local_name(p)]
        attrs = class_attrs.get(name, [])
        rels = class_rels.get(name, [])

        # Build structured data dict so _entity_content handles ordering
        entity_data: Dict[str, Any] = {
            "description": comment,
            "typical_attributes": attrs,
            "parents": parents,
        }
        if rels:
            entity_data["relationships"] = {
                r.split("->")[0]: {"target": r.split("->")[1]} if "->" in r else {"target": "?"}
                for r in rels
            }

        # Build keywords from rdfs:label + lowercased name
        kw_set: set = set()
        kw_set.add(name.lower())
        for lbl in class_labels.get(name, []):
            kw_set.add(lbl.lower())
        keywords_list = sorted(kw_set)

        chunks.append({
            "chunk_id": f"entity::{bundle_name}::{name}",
            "chunk_type": "entity",
            "ontology_bundle": bundle_name,
            "source_ontology": source_label,
            "name": name,
            "content": _entity_content(name, entity_data),
            "uri": str(cls),
            "domain": None,
            "range_entity": None,
            "parent_entities": _join(parents),
            "keywords": _join(keywords_list, "; "),
            "tier": "tier1_plus",
            "updated_at": now,
        })

    # Edge chunks from owl:ObjectProperty
    for prop in g.subjects(RDF.type, OWL.ObjectProperty):
        pname = _local_name(prop)
        if not pname:
            continue
        domains = [_local_name(d) for d in g.objects(prop, RDFS.domain) if _local_name(d)]
        ranges = [_local_name(r) for r in g.objects(prop, RDFS.range) if _local_name(r)]
        inverses = [_local_name(i) for i in g.objects(prop, OWL.inverseOf) if _local_name(i)]
        comment = _get_comment(g, prop, RDFS) or ""

        domain_str = domains[0] if len(domains) == 1 else (_join(domains) or "Any")
        range_str = ranges[0] if len(ranges) == 1 else (_join(ranges) or "Any")

        edge_data = {
            "domain": domain_str,
            "range": range_str,
            "inverse": inverses[0] if inverses else None,
            "description": comment,
        }
        chunks.append({
            "chunk_id": f"edge::{bundle_name}::{pname}",
            "chunk_type": "edge",
            "ontology_bundle": bundle_name,
            "source_ontology": source_label,
            "name": pname,
            "content": _edge_content(
                pname, edge_data,
                domain_desc=class_comments.get(domain_str, ""),
                range_desc=class_comments.get(range_str, ""),
            ),
            "uri": str(prop),
            "domain": domain_str,
            "range_entity": range_str,
            "parent_entities": None,
            "keywords": None,
            "tier": "tier1_plus",
            "updated_at": now,
        })

    logger.info(
        "chunks_from_owl(%s): %d entity + %d edge chunks",
        bundle_name,
        sum(1 for c in chunks if c["chunk_type"] == "entity"),
        sum(1 for c in chunks if c["chunk_type"] == "edge"),
    )
    return chunks


def build_ontology_chunks_table(
    spark: "SparkSession",
    catalog_name: str,
    schema_name: str,
    bundle_name: str,
    source: str,
) -> int:
    """Write ontology chunks to a Delta table.

    Args:
        spark: Active SparkSession.
        catalog_name: UC catalog.
        schema_name: UC schema.
        bundle_name: Ontology bundle identifier (used for filtering).
        source: Path to a bundle YAML or OWL/TTL file.

    Returns:
        Number of chunks written.
    """
    fq_table = f"{catalog_name}.{schema_name}.ontology_chunks"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fq_table} (
            chunk_id        STRING NOT NULL,
            chunk_type      STRING,
            ontology_bundle STRING,
            source_ontology STRING,
            name            STRING,
            content         STRING,
            uri             STRING,
            domain          STRING,
            range_entity    STRING,
            parent_entities STRING,
            keywords        STRING,
            tier            STRING,
            updated_at      TIMESTAMP
        ) USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)

    ext = Path(source).suffix.lower()
    if ext in (".owl", ".ttl", ".rdf"):
        chunks = chunks_from_owl(source, bundle_name)
    else:
        chunks = chunks_from_bundle(source, bundle_name)

    if not chunks:
        logger.warning("No chunks generated from %s", source)
        return 0

    from pyspark.sql import Row
    rows = [Row(**c) for c in chunks]
    df = spark.createDataFrame(rows)

    df.createOrReplaceTempView("_ontology_chunks_src")
    # MERGE with content-change guard: only update when content/keywords/name
    # actually changed. Prevents unnecessary updated_at bumps and CDF noise
    # when the bundle YAML hasn't changed between runs.
    spark.sql(f"""
        MERGE INTO {fq_table} AS tgt
        USING _ontology_chunks_src AS src
        ON tgt.chunk_id = src.chunk_id
        WHEN MATCHED AND (
            COALESCE(tgt.content, '') != COALESCE(src.content, '')
            OR COALESCE(tgt.keywords, '') != COALESCE(src.keywords, '')
            OR COALESCE(tgt.name, '') != COALESCE(src.name, '')
        ) THEN UPDATE SET
            tgt.chunk_type = src.chunk_type,
            tgt.ontology_bundle = src.ontology_bundle,
            tgt.source_ontology = src.source_ontology,
            tgt.name = src.name,
            tgt.content = src.content,
            tgt.uri = src.uri,
            tgt.domain = src.domain,
            tgt.range_entity = src.range_entity,
            tgt.parent_entities = src.parent_entities,
            tgt.keywords = src.keywords,
            tgt.tier = src.tier,
            tgt.updated_at = src.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    spark.sql("DROP VIEW IF EXISTS _ontology_chunks_src")

    safe_bundle = bundle_name.replace("'", "''")
    count = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {fq_table} WHERE ontology_bundle = '{safe_bundle}'"
    ).collect()[0]["cnt"]
    logger.info("ontology_chunks now has %d rows for bundle '%s'", count, bundle_name)
    return count
