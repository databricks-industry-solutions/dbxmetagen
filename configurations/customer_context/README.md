# Customer Context

Customer context files provide domain-specific knowledge that dbxmetagen injects into LLM prompts during comment generation, PI detection, and domain classification. Context is scoped hierarchically so one entry can enrich hundreds of tables.

## Format

Each YAML file contains a `contexts` list:

```yaml
contexts:
  - scope: "my_catalog.claims"
    scope_type: "schema"           # catalog | schema | table | pattern
    context_label: "Claims schema"  # optional, for display in the app
    context_text: |
      This schema contains healthcare claims data sourced from our Cerner EHR
      integration. The MRN (medical record number) is the primary patient
      identifier across all tables. Date fields use UTC.
    priority: 0                     # optional, default 0 (higher = applied later)
```

## Scope Types

| scope_type | scope format | example | matches |
|---|---|---|---|
| `catalog` | `catalog_name` | `prod_healthcare` | All tables in the catalog |
| `schema` | `catalog.schema` | `prod_healthcare.claims` | All tables in the schema |
| `table` | `catalog.schema.table` | `prod_healthcare.claims.patients` | One specific table |
| `pattern` | `catalog.schema.glob` | `prod_healthcare.claims.dim_*` | Tables matching the glob |

When multiple scopes match a table, they are concatenated in order: catalog -> schema -> pattern -> table. The total injected context is capped at **500 words**.

## Limits

- **500 words** maximum per context entry
- **500 words** maximum total injected per table (after concatenation)
- Keep entries focused and specific -- the LLM sees this alongside table metadata, sample data, and other enrichment context

## Usage

1. Add YAML files to this directory
2. Run the `build_customer_context` notebook or pipeline task to seed the Delta table
3. Set `use_customer_context: true` in your job configuration
4. Context can also be managed via the dbxmetagen app UI
