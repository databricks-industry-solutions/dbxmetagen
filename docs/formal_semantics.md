# Formal semantics: what dbxmetagen ontology outputs are (and are not)

This document is for architects, governance, and customer security reviews. It aligns product language with W3C practice without over-claiming.

## What we ship

1. **Reference vocabulary slice (TBox subset)**  
   Curated YAML bundles under `configurations/ontology_bundles/` and tier indexes (`entities_tier*.yaml`, `edges_tier*.yaml`) derived from published OWL/Turtle where applicable (e.g. FHIR R4, OMOP CDM, Schema.org) or hand-mapped industry profiles.

2. **Application profile**  
   Bundle metadata (`format_version: "2.0"`), `property_roles`, `edge_catalog`, and entity definitions with `uri` / `source_ontology` document how *your* catalog uses external terms. This matches common **FAIR** and **DCAT** patterns: a documented profile over external IRIs, not a replacement for the full source ontology.

3. **Semantic annotations (A-box style)**  
   Table/column classifications and graph edges (e.g. `instance_of`, FK-derived relationships) link **Unity Catalog assets** to **class IRIs** from the profile. These are **candidate annotations** with confidence and provenance, not axioms proved by a reasoner.

## Mapping to standard vocabularies

| Artifact | Typical RDF predicate / pattern | Notes |
|----------|----------------------------------|--------|
| Entity type for a table | `rdf:type` / `a` pointing at an `owl:Class` IRI (or `skos:Concept` if you export SKOS) | Export via `ontology_turtle.build_turtle` |
| Equivalence / alignment | `owl:equivalentClass`, `skos:exactMatch`, `rdfs:seeAlso` | When bundle encodes URI |
| Provenance | `prov:wasGeneratedBy`, `prov:used`, `dcterms:conformsTo` | Job run, bundle id/version, model endpoint |

## What we do **not** claim

- **OWL 2 DL soundness-complete reasoning** (consistency, classification of arbitrary TBoxes) at pipeline runtime. That requires a dedicated OWL reasoner and a closed modeling workflow; catalog metadata generation is a different job.
- **Complete import** of large source ontologies (e.g. full FIBO). We intentionally subset and deny-list irrelevant classes.
- **Logical truth** of every LLM-suggested label until validated (human review, rules, or offline checks).

## Why those gaps usually do not block value

- **Search, Genie context, and governance** need **traceable IRIs**, **human-readable definitions**, and **review workflows**—not necessarily full DL reasoning over every table.
- **Application profiles** are the standard way communities reuse big vocabularies without importing them wholesale.

## Operational requirements for “formal enough”

- Pin **bundle id, version, and tier index hash** with outputs (see pipeline and Delta tables).
- Run **bundle validation** (`ontology_validate`) against source TTL when `source_*` files exist.
- Treat low-confidence predictions as **review queue** items, not production tags.

## Tier index semantics (predictor)

- **Entity tiers:** tier 1 is a compact screen; tier 2 adds structure (source, URI, parents, edge names); tier 3 is the full class profile (keywords, relationships, attributes) for the deep pass.
- **Edge tiers:** tier 2 is the confirmation slice (name, domain, range, URI, inverse). Tier 3 is **never** a blind copy of tier 2: it adds bundle `edge_catalog` metadata where defined (e.g. category, symmetric, refined domain/range/inverse) plus compact `domain_profile` / `range_profile` summaries for endpoint entities when those classes exist in the entity index. Edge Pass 3 runs only when tier 3 is strictly richer than tier 2 for the scoped candidates.

## Tier indexes vs bundle YAML (operator contract)

Predictors read **generated** files under `configurations/ontology_bundles/<bundle>/` (`entities_tier*.yaml`, `edges_tier*.yaml`), not the parent `<bundle>.yaml` at runtime. After you edit **`ontology.edge_catalog`**, entity definitions, or other bundle YAML that should change tier content, **re-run** `scripts/build_ontology_indexes.py` for that bundle so tier files stay aligned. The app API `GET /api/ontology/bundles` includes **`tier_indexes_stale`** when the bundle YAML file is newer than the generated tier artifacts (or indexes are missing).

## Versioning

When `metadata.version` or `format_version` changes, re-run tier index generation (`scripts/build_ontology_indexes.py`) and regression evaluation if you maintain a gold dataset.
