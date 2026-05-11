---
name: dbxmetagen-development
description: >-
  Development guidance for the dbxmetagen project -- AI-powered metadata generation
  for Databricks Unity Catalog. Use when working on dbxmetagen source code, tests,
  DDL generation, metadata review workflow, semantic layer, FK prediction, ontology,
  the FastAPI+React dashboard app, or deployment via Databricks Asset Bundles.
  Triggers on dbxmetagen, metadata generation, DDL, review workflow, knowledge base,
  knowledge graph, Genie builder, metric views.
---

# dbxmetagen Development Guide

For full architecture details, read [CLAUDE.md](../../../CLAUDE.md). This skill covers
the essentials and hard-won pitfalls.

## Current Environment

**Always use these values for deployment and CLI operations:**

| Setting | Value |
|---------|-------|
| Databricks CLI profile | `DMVM` |
| Deploy target | `demo` |
| Catalog | `eswanson_demo` |
| Schema | `metadata_results` |

```bash
# Deploy
./deploy.sh --profile DMVM --target demo --no-app

# Run jobs
databricks bundle run <job_name> -t demo -p DMVM

# Or via jobs API (needed when param values contain commas)
databricks jobs run-now -p DMVM --json '{"job_id": ..., "job_parameters": [...]}'
```

## Project Overview

**What:** Python library + Databricks App for automated metadata generation on Unity Catalog tables.
Uses LLMs to generate comments, detect PII, classify domains, build knowledge graphs, predict FKs,
generate metric views, and create Genie spaces.

**Key entry points:**

| File | Role |
|------|------|
| `src/dbxmetagen/main.py` | CLI entry point, config validation, mode dispatch |
| `src/dbxmetagen/processing.py` | Core pipeline (~3500 lines): chunking, sampling, DDL generation, persistence |
| `src/dbxmetagen/ddl_regenerator.py` | DDL review workflow: load TSV/Excel, replace comments/tags, export SQL, apply |
| `src/dbxmetagen/fk_prediction.py` | FK candidate generation, scoring, AI judgment, direction enforcement |
| `src/dbxmetagen/semantic_layer.py` | Metric view generation, SQL autofix, Genie space context |
| `apps/dbxmetagen-app/app/api_server.py` | FastAPI backend (~7300 lines, ~100 endpoints) |
| `apps/dbxmetagen-app/app/src/App.jsx` | React frontend entry point |

## Build and Test

```bash
# Install
uv sync
uv sync --extra pi           # Include spaCy model for PI detection

# Build wheel
uv build

# Run ALL tests (handles import conflicts automatically)
./run_tests.sh

# Quick mode (core tests only)
./run_tests.sh -q

# Single file
uv run pytest tests/test_domain_classifier.py -v

# Single test
uv run pytest tests/test_ddl_regenerator.py::TestReplaceCommentInDDL::test_comment_containing_single_quote -v
```

### Critical Testing Rules

1. **Never run `pytest tests/` directly.** DDL regenerator tests and binary/variant tests must
   run in separate processes due to mlflow/databricks-sdk import conflicts. Use `run_tests.sh`
   or explicitly `--ignore` the conflicting files.

2. **Use `skip_yaml_loading=True`** when constructing `MetadataConfig` in tests:
   ```python
   config = MetadataConfig(
       skip_yaml_loading=True,
       catalog_name="test", schema_name="test", table_names="test.table",
   )
   ```

3. **`conftest.py` stubs heavy deps** (pyspark, mlflow, openai, grpc, databricks_langchain)
   as MagicMock. Key fixtures: `mock_spark`, `test_config`, `sample_table_rows`.

4. **Integration tests** live in `notebooks/integration_tests/` as Databricks notebooks
   (not pytest). They run on a Databricks cluster via DAB jobs.

### Frontend

```bash
cd apps/dbxmetagen-app/app/src
npm install
npm run build    # Output: src/dist/ (committed to repo)
npm run dev      # Local dev server
```

Rebuild after any frontend changes -- `src/dist/` is committed and synced via DAB.

## Key Patterns and Pitfalls

### DDL Comment Handling (the #1 source of bugs)

The review workflow data flow:

```
AI generates comment
  -> _create_*_ddl_func() sanitizes " -> ', wraps in double quotes
  -> Export to TSV/Excel
  -> User edits column_content (or ddl) in spreadsheet
  -> load_metadata_file() reads back with pd.read_csv
  -> update_ddl_row() calls replace_comment_in_ddl()
  -> export_metadata() writes .sql file (one DDL per line)
  -> extract_ddls_from_file() splits by semicolon
  -> apply_ddl_to_databricks() executes via spark.sql()
```

**Rules that MUST be followed:**

- **Quote regex uses backreference `\2`**, not a separate capture group:
  ```python
  # CORRECT
  r'(COMMENT ON TABLE [^"\']+ IS\s+)(["\'])(.*?)\2'
  # WRONG (matches mismatched quotes, breaks on apostrophes)
  r'(COMMENT ON TABLE [^"\']+ IS\s+)(["\'])(.*?)(["\'])'
  ```

- **`is_column_comment` must check for `"COMMENT ON COLUMN"` or `"ALTER COLUMN"`**,
  never bare `"COLUMN"` -- the word "Column" commonly appears in AI-generated comment text.

- **Sanitize `new_comment` in `replace_comment_in_ddl`** the same way DDL generators do:
  `new_comment.replace('""', "'").replace('"', "'")`. This keeps the review path consistent
  with the generation path.

- **All `pd.read_csv` calls for TSV files** MUST include `keep_default_na=False, na_values=[]`.
  Without this, literal `"None"` strings (used for `column_name` on table-level rows) become NaN.

- **`extract_ddls_from_file` splits on `;` naively** (`content.split(";")`). If a comment
  contains a semicolon, the DDL gets corrupted. This is a known limitation -- avoid semicolons
  in comment text when using the SQL export path.

### LLM Response Parsing

- `AIMessage.content` can be a `list` (not just `str`) when the model returns structured blocks.
  `chat_client.py` handles this in `invoke_structured`.
- Pydantic validators in response models normalize malformed LLM output (misspellings,
  nested lists, stringified arrays). Don't add redundant validation -- check existing validators first.

### SQL Autofix (Metric Views)

Functions in `semantic_layer.py` (and duplicated in `api_server.py`):
- `_fix_unquoted_literals` -- quotes multi-word string literals in `=`, `!=`, `IN` clauses
- `_fix_then_else_literals` -- quotes unquoted values after `THEN`/`ELSE`
- `_fix_case_quoting` -- fixes CASE expression quoting

All autofix functions need guards to avoid corrupting:
- Already-quoted strings
- Numeric values
- Column references (contain `.`)
- Function calls (contain `(`)
- SQL reserved words

### FK Prediction

- FK prediction accepts `table_names`. When set, candidate pairs are filtered post-union to
  include only pairs where at least one of `table_a`/`table_b` matches. It still reads from
  `graph_nodes`/`graph_edges` (the full knowledge graph) for cross-table discovery, but limits
  expensive AI_QUERY calls to relevant pairs only.
- Self-referential FKs (`src_table == dst_table AND src_column == dst_column`) are filtered
- Direction is enforced via AI judge + cardinality fallback (`_enforce_direction`)
- Reverse-pair cleanup runs before MERGE to prevent duplicates
- Query history candidates extracted from `system.query.history` JOIN patterns
- `write_graph_edges()` uses `merge_edges(source_system='fk_predictions')` -- every edge
  must include `edge_id` and `source_system`

### Graph Edges Write Contract

**All writes to `graph_edges` must use `merge_edges()` from `knowledge_graph.py`.**

```python
from dbxmetagen.knowledge_graph import merge_edges
merge_edges(spark, target_table, edges_df, source_system="my_source")
```

Rules:
- Never use DELETE+INSERT, `saveAsTable("append")`, or raw SQL INSERT for `graph_edges`
- Every edge DataFrame must include `edge_id` (`CONCAT_WS('::', src, dst, relationship)`)
  and `source_system`
- Four `source_system` values: `knowledge_graph`, `ontology`, `fk_predictions`, `embedding_similarity`
- MERGE is scoped by `source_system` -- each module only sweeps its own stale edges
- If a module produces zero edges, `merge_edges` deletes all rows for that `source_system`
- `align_edge_schema()` pads missing columns as typed NULLs to match the 16-column canonical schema

## Deployment

```bash
# Standard deploy (builds wheel, syncs bundle, starts app)
./deploy.sh --profile DMVM --target demo

# Jobs only, skip app entirely
./deploy.sh --profile DMVM --target demo --no-app

# Grant UC permissions for app service principal
./deploy.sh --profile DMVM --target demo --permissions
```

**Key rules:**
- `deploy.sh` generates `databricks.yml` from `databricks.yml.template` by stamping env vars.
  **Never edit `databricks.yml` directly.**
- Similarly, `app.yaml` is generated from `app.yaml.template`.
- Requires `{target}.env` file (copy from `example.env`). Required vars:
  `DATABRICKS_HOST`, `catalog_name`, `schema_name`, `warehouse_id`.
- `configurations/` is copied into the app source dir temporarily for DAB sync (cleaned up after).

## Running the Eval Pipeline from CLI

The eval harness lives in a git worktree at `../dbxmetagen-eval` on branch `feat/evaluation-job`.
It uses synthetic FHIR healthcare tables (patients, providers, encounters, diagnoses, medications,
lab_results) with known ground truth for comments, PII, domain, ontology, and FK predictions.

### Three-step workflow

```bash
cd /path/to/dbxmetagen-eval

# 1. Deploy (builds wheel, syncs bundle -- skip the app)
./deploy.sh --profile DMVM --target demo --no-app

# 2. Run the full analytics pipeline with FHIR bundle + eval tables
#    IMPORTANT: You MUST pass ontology_bundle and table_names explicitly.
#    The job YAML default for ontology_bundle is "healthcare" (not fhir_r4)
#    and table_names defaults to "" (all tables). Neither is correct for eval.
#
#    NOTE: `databricks bundle run --param` chokes when values contain commas.
#    Use `databricks jobs run-now --json` instead (get job_id from `databricks bundle summary`):
JOB_ID=$(databricks bundle summary -t demo -p DMVM -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['resources']['jobs']['full_analytics_pipeline_job']['id'])")
databricks jobs run-now -p DMVM --json "{
  \"job_id\": $JOB_ID,
  \"job_parameters\": {
    \"ontology_bundle\": \"fhir_r4\",
    \"incremental\": \"false\",
    \"table_names\": \"eswanson_demo.metadata_results.patients,eswanson_demo.metadata_results.providers,eswanson_demo.metadata_results.encounters,eswanson_demo.metadata_results.diagnoses,eswanson_demo.metadata_results.medications,eswanson_demo.metadata_results.lab_results\"
  }
}"

# 3. Run the eval comparison (scores pipeline output against ground truth)
databricks bundle run eval_compare_job -t demo -p DMVM \
  --param run_label="describe-your-change"
```

Alternatively, run step 2 through the **app UI** where you select the FHIR ontology
and the specific eval tables -- the app passes `ontology_bundle` and `table_names`
as job parameters, overriding the defaults correctly.

### What the eval jobs do

| Job | What it does |
|-----|-------------|
| `eval_setup_job` | Creates synthetic tables + expected-value tables in `eval_data` schema (run once) |
| `full_analytics_pipeline_job` | Runs the full pipeline (KB, ontology, embeddings, FK, etc.) against the eval tables |
| `eval_compare_job` | Compares pipeline output tables against expected values, writes scores to `eval_results` |

### Key gotcha

The `full_analytics_pipeline_job` YAML has `ontology_bundle` defaulting to `"healthcare"`
(hardcoded, not `${var.ontology_bundle}`). When triggered from the app, the app overrides this
with whatever you select. When running from CLI, you **must** pass `--param ontology_bundle=fhir_r4`
or it will use the wrong bundle.

## Investment Ops E2E Eval (Built-In)

The repo includes a self-contained e2e eval that builds a star schema from public market
data and scores pipeline outputs. Unlike the FHIR eval (which lives in a separate worktree),
this runs entirely from the main repo.

```bash
# Full run (downloads data + generates + evaluates)
databricks bundle run eval_e2e_job -t demo -p DMVM

# Skip data download, reuse existing tables
databricks bundle run eval_e2e_job -t demo -p DMVM --params="skip_generation=true"

# Test incrementality by excluding tables (use -- syntax for comma values)
databricks bundle run eval_e2e_job -t demo -p DMVM \
  -- --skip_generation true --exclude_tables "dim_index,fct_index_membership"
```

**Key parameters:** `skip_generation` (skip yfinance download), `skip_pipeline` (skip everything),
`skip_idempotency` (skip re-run), `exclude_tables` (comma-separated short table names).

**Caution:** `skip_generation` only skips data table creation. Metadata generation (comment/PI/domain)
still runs unless `skip_pipeline=true`. See `docs/E2E_EVAL_AND_INCREMENTALITY.md` for full details.

## Common Tasks

### Adding a new unit test

1. Create in `tests/test_*.py` (or add to existing test class)
2. Use `_make_config()` helper or `MetadataConfig(skip_yaml_loading=True, ...)`
3. If testing DDL functions, add to `tests/test_ddl_regenerator.py` (runs in separate process)
4. Mark known pre-existing bugs as `@pytest.mark.xfail(reason="...")` -- do not skip them

### Modifying DDL generation or review

Read the full data flow above. The two code paths that generate DDLs:
- **Generation:** `processing.py` -> `_create_table_comment_ddl_func()` / `_create_column_comment_ddl_func()`
- **Review:** `ddl_regenerator.py` -> `replace_comment_in_ddl()` / `replace_pii_tags_in_ddl()`

Both paths must apply the same sanitization. If you change one, check the other.

### Adding a new mode or generator

Follow the Factory pattern in `processing.py`:
1. Add mode to `MetadataConfig` validation
2. Create `Prompt` subclass in `prompts.py`
3. Create `MetadataGenerator` subclass in `metadata_generator.py`
4. Register in `PromptFactory` and `MetadataGeneratorFactory`
5. Add processing logic in `processing.py`'s mode dispatch

## Version

Version is tracked in 4 places -- keep all in sync:
- `pyproject.toml` (`version = "X.Y.Z"`)
- `src/dbxmetagen/__init__.py` (`__version__ = "X.Y.Z"`)
- `apps/dbxmetagen-app/app/src/package.json`
- `apps/dbxmetagen-app/app/src/package-lock.json`
