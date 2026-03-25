# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is dbxmetagen?

dbxmetagen is a Python library + Databricks App for automated metadata generation on Unity Catalog tables. It uses LLMs (via Databricks Foundation Model endpoints) to generate table/column descriptions, detect PII/PHI/PCI, and classify business domains. It also builds knowledge bases, knowledge graphs, ontologies, embeddings, foreign key predictions, and data quality scores from the generated metadata.

## Build & Test Commands

```bash
# Install dependencies (Poetry 2.0+)
poetry install
poetry install --extras pi          # Include spaCy model for PI detection

# Build wheel
poetry build                        # Output: dist/dbxmetagen-*.whl

# Run all tests (3 suites: core, DDL regenerator, binary/variant)
./run_tests.sh

# Quick mode (core tests only)
./run_tests.sh -q

# Run a single test file
poetry run pytest tests/test_domain_classifier.py -v

# Run a specific test
poetry run pytest tests/test_domain_classifier.py::TestDomainClassifier::test_name -v
```

**Important:** DDL regenerator tests (`test_ddl_regenerator.py`) and binary/variant tests (`test_binary_variant_types.py`) must run in separate processes due to import conflicts — they import mlflow/databricks-sdk which collide with mocked modules in the core suite. The `run_tests.sh` script handles this automatically. Never run `pytest tests/` directly; always use `run_tests.sh` or explicitly `--ignore` the conflicting files.

### Frontend (React App)

```bash
cd apps/dbxmetagen-app/app/src
npm install
npm run build                       # Output: src/dist/ (committed to repo)
npm run dev                         # Local dev server
```

The built `src/dist/` directory is committed and synced to Databricks via DAB. Rebuild after any frontend changes.

## Deployment

```bash
# Deploy to Databricks (builds wheel, syncs bundle, starts app)
./deploy.sh --profile DEFAULT --target dev
./deploy.sh --profile DEFAULT --target dev --no-app    # Skip app
./deploy.sh --profile DEFAULT --target dev --permissions  # Grant UC perms for app SPN

# Run jobs directly
databricks bundle run metadata_generator_job -t dev -p DEFAULT \
  --params table_names='catalog.schema.*',mode=comment
```

Deployment requires a `{target}.env` file (copy from `example.env`). Required vars: `DATABRICKS_HOST`, `catalog_name`, `schema_name`, `warehouse_id`.

`deploy.sh` generates `databricks.yml` from `databricks.yml.template` and `app.yaml` from `app.yaml.template` by stamping env vars — do not edit the generated files directly. It also copies `configurations/` into the app source dir temporarily for DAB sync (cleaned up after deploy). On first deploy, it runs a second deploy pass to wire up the app service principal's permissions.

## Architecture

### Core Library (`src/dbxmetagen/`)

Entry point: `main.py` → validates config, sets up mode-specific dependencies, initializes infrastructure (DDL dirs, control table, queue), then calls `generate_and_persist_metadata()`.

**Key modules:**
- `processing.py` (~3500 lines) — Core pipeline: `chunk_df()`, `sample_df()`, type conversion (BINARY→base64, VARIANT→JSON), DDL generation, table claiming, result persistence
- `metadata_generator.py` — ABC `MetadataGenerator` with `CommentGenerator` and `PIIdentifier` implementations; `MetadataGeneratorFactory` creates the right one per mode
- `domain_classifier.py` — Two-stage LLM pipeline: stage-1 selects domain from candidates, stage-2 selects subdomain; uses `_enforce_value()` (exact → normalized → substring → trigram similarity) to snap LLM output to allowed values
- `chat_client.py` — ABC `ChatClient` with `DatabricksClient` wrapping OpenAI client; supports raw completion and structured completion (Pydantic response models via `ChatDatabricks`)
- `prompts.py` (~72KB) — All LLM prompts. ABC `Prompt` with `CommentPrompt`/`PIPrompt`/`DomainPrompt`/`CommentNoDataPrompt` implementations; `PromptFactory` creates per mode. Prompts can be enriched with knowledge base data and ontology context
- `config.py` — `MetadataConfig` class: loads 60+ params from runtime kwargs > `variables.yml` > `variables.advanced.yml` > env vars > defaults. Pass `skip_yaml_loading=True` for unit tests
- `deterministic_pi.py` — spaCy/Presidio PII scan that runs before the LLM call in PI mode
- `error_handling.py` — Exponential backoff retry logic for LLM calls

**Response models (Pydantic):**
- `CommentResponse` — flexible JSON/array parsing with field validators for malformed LLM output
- `PIResponse` / `PIColumnContent` — normalizes misspellings ("pii"→"pi"), classification values: `pi`, `phi`, `pci`, `medical_information`, `None`
- `DomainResult` / `SubdomainResult` / `TableClassification` — domain pipeline outputs

**Processing flow per table:**
1. `claim_table()` via control_table (concurrent safety)
2. `read_table_with_type_conversion()` — handles BINARY/VARIANT/TIMESTAMP edge cases
3. `chunk_df()` → split columns into groups of `columns_per_call` (default 20)
4. `sample_df()` → filter null-heavy rows, limit to `sample_size` rows
5. `PromptFactory.create_prompt()` → `MetadataGeneratorFactory.create_generator()` → LLM call with retries
6. Post-processing: `split_and_hardcode_df()`, optional CSV overrides, DDL generation
7. Results written to `metadata_generation_log` Delta table with `mergeSchema`

### Analytics Pipeline (post-generation)

Runs as Databricks jobs (notebooks in `notebooks/`). The `full_analytics_pipeline_job` is the main orchestrator with 14 tasks across 7 stages:

1. **KB Building** (parallel): `build_knowledge_base` + `build_column_kb` → `build_schema_kb` + `extract_extended_metadata`
2. **Knowledge Graph**: `build_knowledge_graph` (depends on column KB + schema KB + extended metadata)
3. **Analytics** (parallel): `generate_embeddings`, `run_profiling`, `build_ontology`
4. **Secondary**: `build_similarity_edges`, `cluster_analysis`, `compute_data_quality`, `validate_ontology`
5. **FK Prediction**: `predict_foreign_keys` (depends on similarity edges)
6. **Final**: `final_analysis` (depends on FK + clustering + quality + ontology)
7. **Indexing**: `build_vector_index`

### Web Dashboard (`apps/dbxmetagen-app/app/`)

**Backend:** FastAPI (`api_server.py`, ~7300 lines, ~100 endpoints) — organized into route groups:
- Job management (list, run, poll status)
- Metadata CRUD (log, knowledge bases, DDL generation/review/apply)
- Graph & analytics (graph traversal, FK predictions)
- Ontology (entity discovery, tagging, export)
- Semantic layer (metric view generation, validation, deployment)
- Genie space building (question generation, context assembly, agent-driven space creation, Databricks Genie API deployment)
- Agent chat (deep analysis, analyst comparison)
- Catalog browsing and coverage stats

**Long-running operations use background task + polling** (not WebSocket/SSE): POST returns a `task_id`, frontend polls a GET endpoint until `status=done`.

**Caching:** TTL caches at multiple layers — YAML configs (5min), job list (30s), coverage (60s), semantic layer context (2min), singleton LLM/agent instances.

**Frontend:** React 19 + Vite + Tailwind CSS (`src/`). Key components map to dashboard tabs: `AgentChat.jsx`, `BatchJobs.jsx`, `MetadataReview.jsx`, `Ontology.jsx`, `GraphExplorer.jsx`, `SemanticLayer.jsx`, `GenieBuilder.jsx`.

**Agents** (`agent/` directory, LangGraph-based):
- `deep_analysis.py` — Supervisor → Planner → Retrieval → Analyst → Respond pipeline; two modes: "graphrag" (vector search + graph traversal + SQL) vs "baseline" (SQL only on KB tables)
- `genie_agent.py` — ReAct agent that validates SQL, samples values, and builds `serialized_space` JSON for Databricks Genie API
- `genie_builder.py` — `GenieContextAssembler`: gathers KB, FK, ontology, metric views, column synonyms as deterministic context (no hallucinated tables/columns)
- `analyst_agent.py` — Dual-mode SQL agent: "blind" (schema introspection only) vs "enriched" (full semantic layer); compare mode runs both in parallel
- `guardrails.py` — Input validation (length, injection detection), output sanitization (strips leaked secrets/tokens), iteration limits (max 8 agent iterations, 120s timeout)
- `common.py` — Shared LLM singletons, ReAct graph builder, SQL guard utilities

### Databricks Asset Bundles (DAB)

- `databricks.yml.template` — Bundle config with 3 targets (dev, demo, prod). Syncs app configs, frontend dist, and app.yaml. Artifact is a pre-built whl
- `variables.yml` — 60+ runtime parameters (catalog, schema, mode, model, sampling, DDL, domain config, ontology bundle, permissions, etc.)
- `variables.advanced.yml` — Benchmarking, tag names, custom endpoints
- `resources/jobs/` — 18 job definitions covering: single-mode generation, parallel-mode generation, serverless variants, KB building, full analytics pipeline, profiling, ontology, FK prediction, semantic layer, DDL sync, graph-to-Lakebase sync
- `resources/apps/dbxmetagen_app.yml` — App manifest with 16 job resources, SQL warehouse, and API scopes (files, serving endpoints, SQL execution)

### Configuration

- `configurations/ontology_bundles/` — Industry-specific ontology + domain configs: `general.yaml`, `healthcare.yaml` (FHIR/OMOP-aligned), `financial_services.yaml`, `retail_cpg.yaml`, `geo_doj.yaml`
- `configurations/ontology_config.yaml` — Entity type definitions with keywords and discovery confidence threshold
- `configurations/domain_config_healthcare.yaml` — Legacy standalone domain config (alternative to bundle approach)

## Key Design Patterns

- **Factory pattern:** `MetadataGeneratorFactory`, `PromptFactory`, and mode dispatch throughout `processing.py` keep mode-specific logic isolated
- **Concurrent table processing:** Tables are claimed via a `control_table` (status: pending→in_progress→completed/failed) with run_id/task_id tracking and timeout-based abandonment recovery. `apply_ddl=true` is not allowed with concurrent tasks
- **Federation mode:** When `federation_mode=true`, supports federated catalogs (Redshift, Snowflake) by disabling DESCRIBE EXTENDED, ALTER TABLE, SET TAGS — all output is Delta-native
- **Prompt engineering:** All LLM prompts live in `prompts.py`. Prompts can be enriched with KB comments (`use_kb_comments`), profiling stats (`include_profiling_context`), constraints (`include_constraint_context`), and lineage (`include_lineage`)
- **Incremental processing:** When `incremental=true`, skips tables already in the log
- **LLM response resilience:** Pydantic validators normalize malformed LLM output — handles misspellings, nested lists, stringified arrays, unexpected classification values. Generator-level retries (5x) + chat-level exponential backoff (3x)
- **Security:** PII/PHI data is never logged — only metadata about detections. DDL logged as count/size only. Agent output sanitized for leaked tokens/credentials

## Test Conventions

- Tests use PySpark locally (no Databricks connection needed)
- `conftest.py` globally stubs heavy deps (pyspark, mlflow, openai, grpc, databricks_langchain) as MagicMock, plus internal module stubs via `install_processing_stubs()`/`uninstall_processing_stubs()`
- Key fixtures: `mock_spark` (SparkSession mock), `test_config` (MetadataConfig with `skip_yaml_loading=True`), `base_config_kwargs`, `sample_table_rows`
- pytest markers: `integration` (requires Databricks, in `notebooks/integration_tests/`), `app` (legacy, excluded by default)
- `test_app_logic.py` is excluded from default runs via `pyproject.toml` addopts
- Integration tests (`notebooks/integration_tests/`) are Databricks notebooks numbered `test_01` through `test_19` plus e2e variants for serverless/standard/ML clusters

## Version

Version is tracked in both `pyproject.toml` and `apps/dbxmetagen-app/app/src/package.json` — keep them in sync.
