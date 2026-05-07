# Domain Prediction and Ontology Architecture

## Overview

Domain classification and ontology discovery are two distinct pipeline stages with a one-way dependency: domain feeds into ontology, never the reverse.

```
domain_classifier -> table_knowledge_base.domain -> ontology discovery (entity affinity boost)
                                                  -> entity roles
                                                  -> column properties
                                                  -> named relationships
```

## Pipeline Execution Order

Defined in `recipes/analytics_pipeline.job.yml`:

1. **Metadata generation** (comment, domain, PII) -- populates `metadata_generation_log`
2. **build_knowledge_base** -- aggregates results into `table_knowledge_base` and `column_knowledge_base`, including `domain` and `subdomain` per table
3. **build_ontology** -- reads `table_knowledge_base.domain` to boost entity discovery

Domain classification must complete before ontology discovery runs. The `build_knowledge_base` step bridges them by materializing domain predictions.

## Domain Classification (`src/dbxmetagen/domain_classifier.py`)

Two-stage LLM pipeline:

1. **Keyword prefilter** -- tokenizes table/column names, scores against domain keyword lists
2. **Stage 1 (LLM)** -- classifies the table's domain from the top candidates
3. **Stage 2 (LLM)** -- classifies the subdomain within the predicted domain (only subdomains for THAT domain are provided)

Configuration sources (in priority order):
- Ontology bundle YAML (`configurations/ontology_bundles/*.yaml`) under `domain_config:`
- Standalone domain config (`configurations/domain_config_healthcare.yaml`)
- Built-in defaults (12 domains)

## Domain-Entity Affinity

The key integration point between domain and ontology.

### What it does

When discovering which ontology entity types apply to a table, the keyword prefilter (`EntityDiscoverer._keyword_prefilter()` in `ontology.py`) boosts entity candidates by **+1.5** if their entity type appears in the `domain_entity_affinity` set for the table's predicted domain.

### Where it's defined

1. **Hardcoded defaults** -- `_DEFAULT_DOMAIN_ENTITY_AFFINITY` in `src/dbxmetagen/ontology.py` (lines 103-206). Maps domain names to lists of entity types:

```python
_DEFAULT_DOMAIN_ENTITY_AFFINITY = {
    "healthcare": ["Patient", "Provider", "Encounter", "Condition", ...],
    "financial_services": ["Account", "Transaction", "Portfolio", ...],
    "retail_cpg": ["Product", "Order", "Customer", ...],
    # ... 12 domains total
}
```

2. **Per-bundle overrides** -- `configurations/ontology_bundles/*.yaml` under `ontology.domain_entity_affinity:`:

```yaml
ontology:
  domain_entity_affinity:
    clinical:
      [Patient, Provider, Encounter, Condition, ...]
```

3. **Loading** -- `load_domain_entity_affinity()` checks the bundle config first, falls back to hardcoded defaults.

### How the boost works

In `EntityDiscoverer._keyword_prefilter()`:

```python
affinity_set = self._domain_entity_affinity.get(domain.lower(), set())
for edef in self.entity_definitions:
    kw_score = <base keyword score>
    if edef.name in affinity_set:
        kw_score += 1.5  # domain affinity boost
```

Tables with a predicted domain of "healthcare" will rank Patient/Provider/Encounter higher than generic entity types, even before the LLM classification step.

## New Ontology Model (Level 4)

After entity discovery (which incorporates domain affinity), three additional classification steps run. These do NOT directly use domain information -- they operate on entities that already have domain-influenced confidence scores.

### Entity Role Classification (`classify_entity_roles`)

Marks each entity as `primary` or `referenced` per table based on granularity and confidence. One primary entity per table.

### Column Property Classification (`classify_column_properties`)

Classifies each column in tables with a primary entity into property roles: `primary_key`, `business_key`, `object_property`, `measure`, `dimension`, `temporal`, `geographic`, `label`, `audit`. Uses bundle-match (confidence 0.95) then heuristic fallback (0.40-0.85).

### Named Relationship Discovery (`discover_named_relationships`)

Builds entity-to-entity relationships from:
- FK predictions (e.g., Order `references` Customer)
- Column properties (columns classified as `object_property` with a `linked_entity_type`)
- Configured hierarchy in ontology bundles (subclass relationships)
- Auto-inverse edges from the edge catalog

## Data Flow Diagram

```
+---------------------+
| domain_classifier   |
| (comment/PI/domain) |
+--------+------------+
         |
         v
+--------+------------+
| table_knowledge_base|  <-- domain, subdomain per table
+--------+------------+
         |
         v
+--------+-------------------+
| EntityDiscoverer           |
|  _keyword_prefilter()      |  <-- domain_entity_affinity boost (+1.5)
|  _ai_classify_tables()     |
+--------+-------------------+
         |
         v
+--------+-------------------+
| ontology_entities          |  <-- entity_type, confidence, discovery_confidence
+--------+-------------------+
         |
    +----+----+----+
    |         |    |
    v         v    v
classify_  classify_  discover_named_
entity_    column_    relationships
roles      properties
    |         |    |
    v         v    v
entity_role  ontology_   ontology_
is_canonical column_     relationships
             properties
```

## Notes

- **`subdomain`** is stored in `table_knowledge_base` but is NOT currently used by the ontology system. It could be used in the future to further refine entity affinity.
- The dependency is strictly one-way: domain -> ontology. There is no circular dependency.
- If domain classification is skipped or unavailable, ontology discovery still works -- the affinity boost simply doesn't apply and entity scoring relies solely on keyword matching and LLM classification.
