# Configuration Files

Two systems classify your tables: **domain prediction** assigns a business domain, **ontology prediction** tags entity types on tables and columns. Both read from YAML configs in this folder.

## Quick start

Most users only need one setting. In `variables.yml`, set `ontology_bundle` to match your industry:

```yaml
ontology_bundle:
  default: healthcare    # or: general, financial_services, retail_cpg, fhir_r4, omop_cdm, schema_org
```

The bundle handles both domain and ontology prediction. Done.

## What's in a bundle

Each file in `ontology_bundles/` is self-contained:

```
ontology_bundles/
  healthcare.yaml          # Clinical, payer, pharma, diagnostics
  general.yaml             # Cross-industry defaults
  financial_services.yaml  # Banking, insurance, capital markets
  retail_cpg.yaml          # E-commerce, merchandising, supply chain
  fhir_r4.yaml             # FHIR R4 formal ontology (healthcare interop)
  omop_cdm.yaml            # OMOP CDM formal ontology (observational research)
  schema_org.yaml          # Schema.org formal ontology (general/web)
```

A bundle has three sections:

| Section | Purpose |
|---------|---------|
| `metadata` | Name, version, industry, and optional `tag_key` override |
| `ontology.entities` | Entity definitions with keywords and typical_attributes used for classification |
| `domains` | Domain/subdomain definitions used for domain prediction |

The `ontology.domain_entity_affinity` map inside each bundle connects domains to entity types, boosting classification accuracy when both systems run together.

## Overriding domain prediction separately

If you want ontology entities from one bundle but domain definitions from a standalone file, set both:

```yaml
ontology_bundle:
  default: healthcare
domain_config_path:
  default: ../configurations/domain_config_healthcare.yaml
```

When `domain_config_path` is set, it takes precedence over the bundle's `domains` section for domain prediction only. Ontology prediction still reads from the bundle.

When `domain_config_path` is empty, domain prediction falls back to the bundle's `domains` section. This is the recommended approach for new deployments.

## Standalone configs (legacy)

- `domain_config_healthcare.yaml` -- Standalone domain config. Predates the bundle system. Still works, still supported.
- `ontology_config.yaml` -- Standalone ontology entity definitions without a `domains` section or `domain_entity_affinity`. Used by the `ontology_config_path` variable. Works independently of bundles.

These exist for backward compatibility. Bundles are preferred because they keep domains and entities aligned in one file.

## Writing a custom config

### Custom domain config

```yaml
version: "1.1"
mutually_exclusive: false
domains:
  my_domain:
    name: "My Domain"
    description: "What this domain covers"
    keywords: ["keyword1", "keyword2"]
    subdomains:
      my_subdomain:
        name: "My Subdomain"
        description: "What this subdomain covers"
        keywords: ["sub_keyword1", "sub_keyword2"]
```

The domain classifier only reads `name`, `description`, and `keywords` from each domain and subdomain. These three fields are what the LLM sees when classifying a table.

Other fields you may encounter in existing configs:

- `version` -- Metadata for humans. The code stores it but never branches on it. `"1.0"` was the original format, `"1.1"` added subdomain descriptions.
- `mutually_exclusive` -- Not consumed by any code path. The classifier always picks one domain.
- `typical_table_patterns` -- Regex patterns like `"^patient"` or `"^claim"`. Not used at runtime. They document which table names typically belong to a subdomain but have no effect on classification.

### Custom ontology bundle

Copy an existing bundle and modify it. The key parts:

**Entity definitions** -- Each entity needs `description`, `keywords`, and `typical_attributes`. Keywords match against table/column names. Typical attributes score columns for entity assignment.

```yaml
ontology:
  entities:
    definitions:
      MyEntity:
        description: "What this entity represents"
        keywords: ["table_name_patterns", "column_name_patterns"]
        typical_attributes: [id, name, status, created_date]
        relationships:
          belongs_to: { target: OtherEntity, cardinality: "many-to-one" }
```

**Domain-entity affinity** -- Maps domains to the entity types likely found in that domain. Boosts classification scores.

```yaml
ontology:
  domain_entity_affinity:
    my_domain: [MyEntity, OtherEntity]
```

**Validation** -- Controls classification thresholds.

```yaml
ontology:
  validation:
    min_entity_confidence: 0.5    # Below this, entities are discarded
    max_entities_per_table: 3     # Cap on distinct entity types per table
    classification_model: "databricks-claude-sonnet-4-6"
```

### Tag key override

Bundles can specify a custom UC tag key via `metadata.tag_key`. When set, `apply_tags` writes entity classifications under that tag instead of the default `entity_type`.

## Variable reference

| Variable | Controls | Default |
|----------|----------|---------|
| `ontology_bundle` | Which bundle to load | `healthcare` |
| `domain_config_path` | Standalone domain config override | `../configurations/domain_config_healthcare.yaml` |
| `ontology_config_path` | Standalone ontology config (no domains) | `configurations/ontology_config.yaml` |
