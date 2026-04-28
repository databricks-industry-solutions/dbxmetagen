# Configuration Reference

## Key Settings

Most important settings in `variables.yml`:

**Privacy & Security:**
- `allow_data`: Send data to LLM (false = maximum privacy)
- `allow_data_in_comments`: Allow data in generated comments
- `sample_size`: Rows sampled per table (0 = no data sampling)
- `disable_medical_information_value`: Treat all medical data as PHI

**Model & Performance:**
- `model`: LLM endpoint (recommend `databricks-claude-sonnet-4-6`)
- `columns_per_call`: Columns per LLM call (5-10 recommended)
- `temperature`: Model creativity (0.1 for consistency)
- `max_tokens`: Maximum output length

**Workflow:**
- `apply_ddl`: Apply changes directly to Unity Catalog (false = generate only)
- `ddl_output_format`: Output format (excel, tsv, or sql)
- `allow_manual_override`: Enable CSV-based overrides

**PI Detection:**
- `include_deterministic_pi`: Use Presidio for rule-based PII detection
- `tag_none_fields`: Tag columns classified as non-sensitive

## Full Variable Reference

| Variable | Description | Default |
|----------|-------------|---------|
| catalog_name | Target catalog | None |
| schema_name | Output schema | metadata_results |
| volume_name | Output volume | generated_metadata |
| allow_data | Send data to LLM | true |
| allow_data_in_comments | Include data in comments | true |
| sample_size | Rows to sample | 5 |
| add_metadata | Include extended metadata | true |
| include_datatype_from_metadata | Include data types | false |
| include_possible_data_fields_in_metadata | Include min/max (may leak PII) | true |
| disable_medical_information_value | Treat medical data as PHI | false |
| solo_medical_identifier | MRN classification (pii or phi) | pii |
| model | LLM endpoint | databricks-claude-sonnet-4-6 |
| temperature | Model temperature | 0.1 |
| max_tokens | Maximum output tokens | 8192 |
| max_prompt_length | Maximum prompt length | 16384 |
| columns_per_call | Columns per LLM call | 20 |
| word_limit_per_cell | Max words per cell | 100 |
| limit_prompt_based_on_cell_len | Truncate long cells | true |
| apply_ddl | Apply DDL to tables | false |
| ddl_output_format | DDL format (sql/tsv/excel) | sql |
| reviewable_output_format | Review file format | tsv |
| review_input_file_type | Review input format | tsv |
| review_output_file_type | Review output format | excel |
| review_apply_ddl | Apply reviewed DDL | false |
| include_deterministic_pi | Use Presidio detection | true |
| spacy_model_names | SpaCy model for Presidio | en_core_web_lg |
| tag_none_fields | Tag non-sensitive columns | true |
| allow_manual_override | Enable CSV overrides | true |
| override_csv_path | Override CSV path | metadata_overrides.csv |
| use_customer_context | Enrich prompts with customer context | false |
| acro_content | Acronym dictionary | {"DBX":"Databricks"} |
| table_names_source | Table list source | csv_file_path |
| source_file_path | Table list file | table_names.csv |
| control_table | Checkpoint table | metadata_control_{} |
| catalog_tokenizable | Tokenizable catalog name | __CATALOG_NAME__ |
| format_catalog | Format catalog variable | false |
| domain_config_path | Path to custom domain config YAML | (bundled default) |
| ontology_bundle | Ontology bundle name from `configurations/ontology_bundles/` | general |
| ontology_config_path | Path to custom ontology config YAML | (bundled default) |
| federation_mode | Enable for federated catalog sources | false |
| ontology_vs_index | FQ name of VS index for ontology vector retrieval (e.g. `catalog.schema.ontology_vs_index`). When set, entity/edge classification uses vector search instead of full tier-1 dump, reducing token cost for large ontologies. Leave empty to disable. | (empty) |

See `variables.yml` for complete descriptions and additional advanced options.

## Prompt Enrichment

These features inject additional context into the LLM prompt alongside table metadata and sample data. Each is independently toggleable and requires a prerequisite step to have run first.

| Variable | What it does | Prerequisite |
|----------|-------------|--------------|
| `use_kb_comments` | Fill empty UC comments with knowledge base descriptions | Build Knowledge Base step |
| `use_ontology_context` | Add entity type classification as a hint | Ontology Discovery step |
| `include_profiling_context` | Inject column profiling stats (distinct count, null rate, min/max) | Profiling step |
| `include_constraint_context` | Inject PK/FK constraint roles | Extended Metadata step |
| `include_lineage` | Append upstream/downstream table lineage | Extended Metadata step (or live system tables) |
| `use_customer_context` | Inject customer-provided domain knowledge scoped by catalog/schema/table | Customer Context table (seed from YAML or app) |

All flags default to `false` except `include_lineage` (defaults `true`). If the prerequisite table doesn't exist, the flag is automatically disabled for that run with a warning.

### Customer Context

Customer context lets you inject domain-specific knowledge into LLM prompts so that generated descriptions, PI classifications, and domain predictions reflect your organization's terminology and conventions. Context is scoped hierarchically -- one entry at the schema level enriches every table in that schema.

**Scope types:**

| scope_type | scope format | matches |
|---|---|---|
| `catalog` | `my_catalog` | All tables in the catalog |
| `schema` | `my_catalog.my_schema` | All tables in the schema |
| `table` | `my_catalog.my_schema.my_table` | One specific table |
| `pattern` | `my_catalog.my_schema.dim_*` | Tables matching the glob pattern |

When multiple scopes match, they are concatenated from broadest to most specific. Total injected context is capped at **500 words** per table.

**Setup:**

1. Create YAML files in `configurations/customer_context/`:

   ```yaml
   contexts:
     - scope: "prod_healthcare.claims"
       scope_type: "schema"
       context_label: "Claims schema"
       context_text: |
         This schema contains healthcare claims data sourced from our Cerner EHR
         integration. The MRN (medical record number) is the primary patient
         identifier across all tables. Date fields use UTC.
   ```

2. Seed the Delta table by running the `build_customer_context` notebook, or manage entries directly in the app (Customer Context button on the Generate Metadata page).

3. Enable in your job configuration:

   ```yaml
   use_customer_context: true
   ```

**Performance:** The entire `customer_context` table is loaded once per run (1 SQL query). Per-table scope resolution is pure Python string matching against the cached rows -- zero SQL overhead per table, making it the lowest-cost enricher in the pipeline.

**App UI:** The Customer Context page (accessible via the Customer Context button on the Generate Metadata page) provides:
- Create, edit, and soft-delete context entries with a live word counter
- Upload YAML files to bulk-import entries
- Filter by scope type (catalog, schema, table, pattern)
- Resolve preview -- enter any table name and see exactly what context would be injected

**Example:** A ClinicalTrials.gov example is included at `configurations/customer_context/example_clinical_trials.yaml` with entries for catalog, schema, table, and pattern scopes covering NCT identifiers, NLM controlled vocabularies, and dimension table conventions.

## Privacy Controls

Maximize privacy by combining these settings:
```yaml
allow_data: false                           # No data sent to LLM
allow_data_in_comments: false              # No data in output
sample_size: 0                             # No sampling
include_possible_data_fields_in_metadata: false  # No min/max
```

For healthcare data:
```yaml
disable_medical_information_value: true    # All medical = PHI
solo_medical_identifier: phi               # MRN always PHI
include_deterministic_pi: true             # Use Presidio
```

Note: Default PPT endpoints are NOT HIPAA-compliant. Configure secure endpoints as needed.

## Usage Patterns

### Output Review Workflow

Each run exports logs to `/Volumes/{catalog}/{schema}/generated_metadata/{user}/{date}/exportable_run_logs/`

To review and edit:
1. Download Excel/TSV file
2. Edit metadata:
   - **Comments**: Edit `column_content` column
   - **PI**: Edit `classification` and `type` columns
   - **Domain**: Edit `domain` and `subdomain` columns
3. Save to `/Volumes/{catalog}/{schema}/generated_metadata/{user}/reviewed_outputs/`
4. Run `sync_reviewed_ddl` notebook with filename
5. Set `review_apply_ddl: true` to apply changes

### Manual Overrides

Create `metadata_overrides.csv` for consistent corrections:
```csv
catalog,schema,table,column,override_type,override_value
prod,claims,*,member_id,classification,pii
prod,*,*,mrn,classification,phi
*,*,*,ssn,classification,pii
```

Use `*` wildcards for broad application. Enable with `allow_manual_override: true`.

### Performance Tuning

**Faster, less detail:**
```yaml
columns_per_call: 20
sample_size: 5
```

**Slower, more detail:**
```yaml
columns_per_call: 3
sample_size: 20
```

Recommended balanced settings: `columns_per_call: 5-10`, `sample_size: 10`

## PI Classification Rules

**PII (Personally Identifiable Information):** Name, address, SSN, email, phone number

**PHI (Protected Health Information):** Medical records, diagnoses, treatment dates, plus any PII linked to health data

**PCI (Payment Card Information):** Card numbers, CVV, expiration dates, cardholder names

**Classification logic:**
- Individual columns classified by content (name = PII, diagnosis = PHI)
- Tables inherit highest classification from columns
- Exception: Name/address columns in medical tables remain PII (not PHI) unless they contain health information
- Medical tables with any PHI column = PHI table

Configure with `solo_medical_identifier` and `disable_medical_information_value` for stricter/looser rules.

## Implementation Notes

### Data Sampling
- Samples `sample_size` rows per table, filtered for non-null values
- Cells truncated to `word_limit_per_cell` words
- Chunked by `columns_per_call` for scalable LLM processing

### Metadata Extraction
- `DESCRIBE EXTENDED` metadata optionally included
- Filtered based on privacy settings
- Acronyms expanded via `acro_content`

### PI Detection
- Presidio (rule-based) runs first if `include_deterministic_pi: true`
- LLM reviews Presidio results and provides final classification
- Classification enforced at column and table levels

### DDL Generation
- Generated as SQL, TSV, or Excel based on `ddl_output_format`
- Paths constructed: `/{user}/{date}/` for isolation
- Applied directly if `apply_ddl: true`, otherwise written to volume

### Checkpointing
- Control table tracks processed tables
- Supports resuming incomplete runs
- Prevents duplicate processing

## Domain Classification

Categorizes tables into business domains using a two-stage LLM pipeline: keyword pre-filter, then domain classification, then subdomain classification. Domain configuration is defined in the `domain_config` section of an ontology bundle YAML (e.g. `configurations/ontology_bundles/example_iot.yaml`) or as a standalone YAML file passed via `domain_config_path`.

**12 default domains** (aligned with DAMA DMBOK, FHIR, OMOP): clinical, diagnostics, payer, pharmaceutical, quality_safety, research, finance, operations, workforce, customer, technology, governance. Each domain includes subdomains with keywords and descriptions.

Customize domains and subdomains by editing the bundle's `domain_config` section or providing a standalone YAML via `domain_config_path`.

## Federation Mode

When `federation_mode=true`, dbxmetagen adapts for federated catalogs in Unity Catalog:

| Feature | Status | Notes |
|---------|--------|-------|
| SELECT / spark.read.table | Works | Standard reads via federation |
| DESCRIBE TABLE | Works | Basic column info available |
| SHOW TABLES IN | Works | Schema listing via federation |
| DESCRIBE DETAIL | Skipped | Delta-specific |
| DESCRIBE EXTENDED | Skipped | May return limited metadata |
| ALTER TABLE / COMMENT ON | Skipped | Cannot modify federated tables |
| SET TAGS / UNSET TAGS | Skipped | Cannot tag federated tables |
| Output tables | Works | All output tables are Delta |

## Lakebase (Optional)

Lakebase accelerates graph queries in the dashboard's deep analysis agent by serving `graph_nodes` and `graph_edges` from a managed PostgreSQL instance instead of the SQL warehouse. This is **optional** -- the app automatically falls back to UC Delta queries when Lakebase is not configured.

**Setup:**

1. Provision a Lakebase instance in your workspace (Settings > Compute > Lakebase)
2. Run `sync_graph_lakebase_job` after the analytics pipeline to replicate graph tables:
   ```bash
   databricks bundle run sync_graph_lakebase_job -t dev -p <profile>
   ```
3. Attach the Lakebase database as a resource to the dbxmetagen app in the Databricks Apps UI. This automatically sets `PGHOST` and related connection variables for the app.

**Job parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `lakebase_catalog` | `dbxmetagen_graphrag` | Unity Catalog name for the synced Lakebase database |
| `lakebase_instance_name` | `dbxmetagen` | Lakebase instance name in your workspace |

The sync job uses the Databricks SDK's synced database tables API to replicate Delta tables into Lakebase with automatic change data capture. The app detects Lakebase via the `PGHOST` environment variable and uses OAuth token authentication.

## Community Summaries

The analytics pipeline generates AI summaries for groups of tables that share the same `(domain, subdomain)` in `table_knowledge_base`. Summaries are stored in the `community_summaries` Delta table and automatically included in the metadata vector index as `community_summary` documents, enabling the deep analysis agent to answer broad questions about data domains.

The `build_community_summaries` task runs as part of `full_analytics_pipeline_job` (after `final_analysis`, before `build_vector_index`). It uses `AI_QUERY` to generate 3-5 sentence summaries per community.

**Configuration:** The notebook accepts a `min_tables` widget (default `2`) controlling the minimum number of tables a domain/subdomain group must have to qualify as a community. When run via the pipeline job, the default is always used.

## Vector Search

dbxmetagen uses Databricks Vector Search for hybrid (ANN + keyword) semantic search. All indexes share a single endpoint and use the `databricks-gte-large-en` embedding model.

### Endpoint

All indexes are hosted on a shared `dbxmetagen-vs` endpoint (STANDARD type). The endpoint is created automatically by whichever index builder runs first -- no manual provisioning is needed. Both builders call `ensure_endpoint()` which is idempotent: create if missing, skip if already online.

To create the endpoint manually (e.g. via `docs/MANUAL_DEPLOYMENT.md`):

```bash
databricks api post /api/2.0/vector-search/endpoints --json '{"name": "dbxmetagen-vs", "endpoint_type": "STANDARD"}'
```

### Indexes

| Index | Source Table | Purpose | Built By | Pipeline Stage |
|-------|-------------|---------|----------|----------------|
| `metadata_vs_index` | `metadata_documents` | Semantic search over all metadata: tables, columns, entities, FKs, metric views, community summaries | `build_vector_index` task | End (after all metadata is ready) |
| `ontology_vs_index` | `ontology_chunks` | Entity/edge classification via vector retrieval instead of full tier-1 dump. Reduces token cost for large ontologies. | `build_ontology_vector_index` task | Mid-pipeline (after `build_ontology`) |

**`metadata_vs_index`** is the primary index. It powers the deep analysis agent's `search_metadata` tool, the MCP Vector Search server, and the app's semantic search. It unions documents from `table_knowledge_base`, `column_knowledge_base`, `ontology_entities`, `metric_view_definitions`, `fk_predictions`, and `community_summaries` into a single `metadata_documents` Delta table, then syncs to Vector Search. Built by:

```bash
databricks bundle run build_vector_index_job -t dev -p <profile>
```

**`ontology_vs_index`** is optional. When set via `ontology_vs_index` in your configuration, entity/edge classification uses HYBRID vector search to narrow candidates instead of loading the full tier-1 entity list into the LLM prompt. This is only beneficial for large ontologies (100+ entities). Built by:

```bash
databricks bundle run full_analytics_pipeline_job -t dev -p <profile>
# Or standalone:
# Run the build_ontology_vector_index notebook directly
```

In the full analytics pipeline, `build_ontology_vector_index` runs after `build_ontology` and before `build_vector_index`. On a first run, `build_ontology_vector_index` creates the endpoint; `build_vector_index` reuses it.

## Compatibility

**Databricks Runtime:**
- Tested: DBR 14.3 LTS, 15.4 LTS, 16.4 LTS (+ ML versions)
- Views: Only DBR 16.4+
- Excel: ML runtimes only (use TSV on standard runtimes)

**Cross-version DDL:**
- DDL generated on 16.4 may not apply on 14.3
- Test in same environment where applying

