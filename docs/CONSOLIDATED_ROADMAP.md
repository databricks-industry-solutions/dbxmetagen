# dbxmetagen Consolidated Roadmap

All open work items from every roadmap and plan document, organized by theme. Each item is tagged with implementation status and priority.

**Status key:** `DONE` | `PARTIAL` | `OPEN` | `DEFERRED` | `KILLED`
**Priority key:** `P0` (ship-blocker) | `P1` (next sprint) | `P2` (planned) | `P3` (backlog)
**Effort key:** `S` (< half day) | `M` (1-2 days) | `L` (3+ days)

**Source documents** (all archived in `docs/archive/`):
- `ROADMAP_CRITICAL_ISSUES.md` (abbreviated RC)
- `ONTOLOGY_FINISHING_ROADMAP.md` (abbreviated OF)
- `ONTOLOGY_EXPERT_REVIEW.md` (abbreviated OE)
- `ONTOLOGY_UI_ROADMAP.md` (abbreviated OU)
- `GENIE_JOIN_RELIABILITY_ROADMAP.md` (abbreviated GJ)
- `SCALING.md` (abbreviated SC)
- `ai_query_scaling.md` (abbreviated AQ)
- `PIP_INSTALLABLE_PLAN.md` (abbreviated PI)

**Last reconciled against codebase:** 2026-05-03

---

## 1. Core Metadata Generation Robustness

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| MG-1 | Chat client garbage fallback on JSON parse failure | DONE | -- | -- | RC 1.1 |
| MG-2 | PII column-to-table rollup (`compute_table_sensitivity`) | OPEN | P1 | M | RC 1.2 |
| MG-3 | Hardcoded entity suggestions in semantic layer | OPEN | P2 | S | RC 1.3 |
| MG-4 | Quality metrics exposed to users | OPEN | P2 | L | RC 1.4 |
| MG-5 | Day-2 re-run lifecycle / `_review_status` tracking | OPEN | P1 | M | RC 1.5 |
| MG-6 | Structured output vs regex JSON recovery | OPEN | P2 | M | RC 2.1 |
| MG-7 | Schema-level PII reconciliation via FK graph | OPEN | P2 | M | RC 2.3 |
| MG-8 | Ontology as generation context (inject entity type into prompts) | OPEN | P2 | M | RC 2.4 |
| MG-9 | Audit trail for metadata state transitions | OPEN | P2 | M | RC 2.5 |
| MG-10 | `enrich_from_knowledge_base()` should override stale UC comments | OPEN | P2 | S | apply_ddl analysis |
| MG-11 | `MetadataConfig` schema validation (Pydantic model with field validators) | OPEN | P2 | M | code audit |

### MG-1: Chat client garbage fallback on JSON parse failure

**Status: DONE** -- `chat_client.py` now raises `ValueError` with context on `json.JSONDecodeError`, providing the raw text snippet for debugging. The hard-fail path is implemented.

### MG-2: PII column-to-table rollup

**Status: OPEN** -- Table-level `has_pii`/`has_phi` are manual checkboxes in the review UI with no computed suggestion. The rollup rules exist only as prompt prose in `variables.yml` lines 80-83.

**Work:**
- Add `compute_table_sensitivity(column_classifications) -> {"has_pii": bool, "has_phi": bool, "reasoning": str}` in `processing.py` or new `sensitivity.py`
- Call after PI generation completes per table
- Show computed value as a badge/recommendation in `MetadataReview.jsx` next to the checkbox

**Files:** `src/dbxmetagen/processing.py`, `apps/.../MetadataReview.jsx`

### MG-3: Hardcoded entity suggestions in semantic layer

**Status: OPEN** -- Only 3 entity types (Encounter, Patient, Order) have rich measure suggestions in `semantic_layer.py` lines 304-308. All other entities (Person, Organization, Product, Transaction, Location, Event, Reference, Metric, Document) get a generic one-liner.

**Work:** Extend `entity_suggestions` to cover all general bundle entities. Ideally move suggestions into ontology YAML config.

**Files:** `src/dbxmetagen/semantic_layer.py`

### MG-4: Quality metrics exposed to users

**Status: OPEN** -- The eval system exists in `eval/` but is not user-facing. No acceptance rate tracking, no PII agreement metrics, no quality trends.

**Work:**
- Track review actions: compare saved values against original AI values, record `(timestamp, table, column, field, original, final, action)`
- Add `/api/quality-metrics` endpoint with acceptance rate, correction patterns, trends
- Surface in a Quality tab or Coverage section

**Files:** `apps/.../api_server.py`, new `review_audit` table, `apps/.../Coverage.jsx`

### MG-5: Day-2 re-run lifecycle

**Status: OPEN** -- Control table `_status` only tracks `completed`/`failed`. No concept of `reviewed`, `applied`, or `stale`. Re-runs can silently overwrite human edits.

**Work:**
- Add `_review_status` column: `generated -> reviewed -> applied -> stale`
- Set status on review save, DDL apply, and re-run
- In review UI, show both versions for stale tables with accept/keep choice

**Files:** `src/dbxmetagen/processing.py` (control table schema, `mark_table_completed`, `mark_table_failed`), `apps/.../MetadataReview.jsx`

### MG-6: Structured output vs regex JSON recovery

**Status: OPEN** -- Uses `re.search(r"\{.*\}", ...)` for JSON extraction. Works pragmatically but fragile.

**Work:**
- For endpoints that support it, add `response_format={"type": "json_object"}`
- Keep regex as fallback
- Validate recovered structure matches expected schema before accepting
- Log recovered vs failed entries in `_parse_individual_objects()`

**Files:** `src/dbxmetagen/chat_client.py`, `src/dbxmetagen/semantic_layer.py`

### MG-7: Schema-level PII reconciliation via FK graph

**Status: OPEN** -- Tables classified in isolation. Child tables linked by FK to a patient table don't inherit PII context.

**Work:**
- After FK prediction + PII generation, build table relationship graph
- Flag inconsistencies (child table with patient IDs not marked PII) as warnings in review UI

**Files:** `src/dbxmetagen/fk_prediction.py`, `apps/.../MetadataReview.jsx`

### MG-8: Ontology as generation context

**Status: OPEN** -- Ontology discovery runs after metadata generation. Entity context could improve comments, PII, and domain quality if injected into prompts.

**Work:**
- After ontology runs once, serialize entity-to-table mapping
- On subsequent generation runs, inject entity type + relationships into prompts
- Documentation + workflow change for recommended pipeline order

**Files:** `src/dbxmetagen/prompts.py`, `src/dbxmetagen/processing.py`

### MG-9: Audit trail for metadata state transitions

**Status: OPEN** -- No record of who generated, reviewed, or applied metadata.

**Work:**
- Add `metadata_audit_log` table: `(timestamp, user, table_name, column_name, action, field, previous_value, new_value)`
- Capture user from Databricks OAuth on each write
- Log: generation events, review saves, DDL applications, tag writes

**Files:** `apps/.../api_server.py`, `src/dbxmetagen/processing.py`

### MG-10: `enrich_from_knowledge_base()` should override stale UC comments

**Status: OPEN** -- When `use_kb_comments=true`, `enrich_from_knowledge_base()` in `prompts.py` (L171-217) only fills `table_comments` and column `comment` slots when empty. If UC has a stale comment from a prior `apply_ddl=true` run and the KB has a newer description, KB does not win.

**Context:** The full analytics pipeline reads from the log/KB and is unaffected by `apply_ddl=false`. PI and domain modes keep column `comment`/`description` from DESCRIBE EXTENDED as LLM context, but `use_kb_comments` already backfills when those slots are empty. The only real gap is the stale-overrides-newer case.

**Work:**
- When `use_kb_comments=true`, always overwrite `table_comments` from KB (user has explicitly opted into "trust the KB")
- For column-level: always overwrite `column_metadata[col]["comment"]` from KB when KB has a value

**Files:** `src/dbxmetagen/prompts.py` (lines 192-217)

---

## 2. Ontology System

> **Note:** Items below have been verified against the codebase (2026-03-24). See `docs/ONTOLOGY_IMPROVEMENT_PLAN.md` for detailed code-line evidence.

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| ON-1 | Wire classifier to bundle YAML (`_build_bundle_property_index`) | DONE | -- | -- | OE 1, OF |
| ON-2 | EdgeCatalog with domain/range validation | DONE | -- | -- | OE 3, OF |
| ON-3 | Per-column confidence + `discovery_method` | DONE | -- | -- | OE 5, OF |
| ON-4 | Remove legacy `link` SQL filter (heuristic already fixed) | DONE | -- | -- | OF 1A |
| ON-5 | Auto-generate inverse edges | DONE | -- | -- | OF 1B |
| ON-6 | Incremental mode bundle version check | DONE | P1 | M | OF 1C |
| ON-7 | Property classification test coverage | PARTIAL | P1 | S | OF 1D |
| ON-8 | Conformance validation (entity schema vs discovered) | DONE | -- | -- | OF 2A |
| ON-9 | Composite component grouping | DEFERRED | P3 | M | OF 2B |
| ON-10 | Subdomain -> entity affinity | DEFERRED | P2 | S+M | OF 2C |
| ON-11 | JSON-LD export -- add column properties + Schema.org mappings | PARTIAL | P2 | M | OF 3A, OE 4 |
| ON-12 | Bundle version in UC tags | OPEN | P2 | S | OF 3B |
| ON-13 | 5GNF trait nodes | KILLED | -- | -- | OE, OF |
| ON-14 | Row-level instance nodes | KILLED | -- | -- | OE, OF |
| ON-15 | OWL/TTL import | DEFERRED | P3 | L | OF, OE |
| ON-16 | Cardinality validation on relationships | DEFERRED | P3 | M | OF, OE |
| ON-17 | Feedback loop from steward overrides | DEFERRED | P3 | L | OF, OU 6 |
| ON-18 | Multi-ontology / crosswalk mode (secondary URIs vs canonical bundle) | OPEN | P2 | L | Provenance plan |

### ON-4: Remove legacy `link` SQL filter

**Status: DONE** -- The `'link'` literal has been removed from the SQL filter in `discover_named_relationships`. The query now filters only on `property_role = 'object_property'`.

### ON-5: Auto-generate inverse edges

**Status: DONE** -- `discover_named_relationships` now has an inverse-edge generation pass after the three relationship-building loops. Uses `catalog.get_inverse()` to look up inverse edge names, flips cardinality, and marks `source="auto_inverse"`. Symmetric edges and already-present reverse pairs are handled via the `seen` set.

### ON-6: Incremental mode bundle version check

**Status: DONE** -- The incremental SQL in both `discover_entities_from_tables` and `discover_entities_from_columns` already contained `COALESCE(oe.last_bundle_version, '') != '{current_bv}'`, but `OntologyBuilder._get_bundle_version()` returned only the version number (e.g. `"1.0"`) while `EntityDiscoverer._get_bundle_version()` returned `"{bundle}:{ver}"` (e.g. `"fhir_r4:1.0"`). This format mismatch meant the stored and compared values never matched, making the check a no-op.

**Fix applied:** Aligned `OntologyBuilder._get_bundle_version()` to return `"{bundle}:{ver}"` like `EntityDiscoverer`. Also added a `logger.warning` in `run()` when `ontology_entities` contains entities from a different bundle.

**Files:** `src/dbxmetagen/ontology.py`

### ON-7: Property classification test coverage

**Status: PARTIAL** -- Significant unit test coverage added in 0.9.0: `TestHeuristicClassifyImprovements`, `TestBuildBundlePropertyIndex`, `TestHealthcareBundlePropertyIndex`, `TestFhirBundlePropertyIndex`, `TestClassificationModelConsolidation`. These cover bundle-match tier logic, heuristic classification, and multi-bundle property indexing.

**Remaining:** No integrated test for the full `classify_column_properties` bundle-match -> heuristic-fallback flow end-to-end. Downgraded from P0 to P1 given the existing unit coverage.

**Files:** `tests/test_ontology.py`

### ON-8: Conformance validation

**Status: DONE** -- `validate_entity_conformance()` method exists on `OntologyBuilder` and is called in `run()`. Compares bundle `EntityDefinition.properties` against discovered columns, computes `conformance_score`, stores in entity `attributes` map. Logs warnings for tables with < 50% coverage.

### ON-9: Composite component grouping

**Status: DEFERRED (was OPEN P2)** -- **Verified:** `PropertyDefinition` has the `composite_columns` field (parsed at line 807), but `_build_bundle_property_index` (3441-3450) only indexes `typical_attributes`. No bundle YAML defines `composite_columns` on any property. This is building from scratch, not completing a half-done feature. Defer until a customer use case drives YAML definitions.

### ON-10: Subdomain -> entity affinity

**Status: DEFERRED (was OPEN P1)** -- **Verified:** No bundle YAMLs define `subdomain_entity_affinity`. `_keyword_prefilter` (981-987) only checks `domain`. Pre-requisite: verify `subdomain` is consistently populated in `table_knowledge_base` by domain classification. Without data, the feature can't be validated.

### ON-11: JSON-LD export -- complete

**Status: PARTIAL (effort revised from L to M)** -- **Verified:** `/api/ontology/export` exists at `api_server.py` line 2449. Exports entities + relationships as JSON-LD. Column properties are probed (line 2486) but result is discarded. Endpoint *exists* contrary to roadmap's "NOT ADDRESSED" claim.

**Remaining work:** Include column properties in `@graph`. Improve Schema.org type mappings (currently all `schema:Thing`). Validate with JSON-LD validator.

**Files:** `apps/.../api_server.py`

### ON-12: Bundle version in UC tags

**Status: OPEN** -- `apply_entity_tags` writes `ontology.entity_type` but not `ontology.bundle_version`.

**Fix:** Add `ontology.bundle_version` tag alongside `ontology.entity_type`.

**Files:** `src/dbxmetagen/ontology.py`, `apps/.../api_server.py`

### ON-13 & ON-14: KILLED items

- **5GNF trait nodes:** Wrong for dbxmetagen's scale. Flat properties on nodes are correct.
- **Row-level instance nodes:** Fundamentally different product. Orders of magnitude different compute.

### ON-15 through ON-17: DEFERRED items

- **OWL/TTL import:** `ontology_import.py` exists with `owl_to_bundle_yaml()` but is incomplete. Wait for customer request.
- **Cardinality validation:** Requires data distribution analysis. Not essential for discovery.
- **Feedback loop ML:** Requires sufficient override data. Continue storing overrides (already done). Defer pipeline.

### ON-18: Multi-ontology / crosswalk mode

**Status: OPEN** -- Canonical `entity_uri`, `source_ontology`, and relationship provenance are tied to the **active** ontology bundle (see three-pass classification, column entity rows, and `predict_edge`). Optional future mode: LLM or mapping tables may propose **secondary** equivalent classes or labels in **another** standard (for example FHIR vs OMOP) while primary UC tags and review badges remain aligned to the selected bundle. Would need configuration plus separate columns or JSON attributes for crosswalk vs canonical values.

**Depends on:** Clear canonical-vs-LLM behavior (implemented in ontology provenance work).

---

## 3. Ontology UI

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| UI-1 | MVP A: Recommended entity override | DONE | -- | -- | OU MVP |
| UI-2 | MVP B: Entity type combobox | DONE | -- | -- | OU MVP |
| UI-3 | MVP C: Editable column property roles + tagging | DONE | -- | -- | OU MVP |
| UI-4 | MVP D: Review status workflow | DONE | -- | -- | OU MVP |
| UI-5 | MVP E: Entity + domain header redesign | DONE | -- | -- | OU MVP |
| UI-6 | Phase 2: Core vs extension bundle separation | OPEN | P2 | L | OU Ph2 |
| UI-7 | Phase 2: Normalized relationship types | OPEN | P2 | M | OU Ph2 |
| UI-8 | Phase 2: Stable IDs and URIs | OPEN | P2 | M | OU Ph2 |
| UI-9 | Phase 2: Bundle versioning | OPEN | P2 | S | OU Ph2 |
| UI-10 | Phase 3: Relationship tags as UC metadata | OPEN | P2 | L | OU Ph3 |
| UI-11 | Phase 3: Auto-join generation from relationship tags | OPEN | P2 | M | OU Ph3 |
| UI-12 | Phase 4: Entity graph preview per table | OPEN | P3 | L | OU Ph4 |
| UI-13 | Phase 4: Cross-table entity browser | OPEN | P3 | L | OU Ph4 |
| UI-14 | Phase 5: UC tags as system of record | OPEN | P2 | M | OU Ph5 |
| UI-15 | Phase 5: Semantic SQL helpers (entity-level views) | OPEN | P3 | M | OU Ph5 |
| UI-16 | Phase 5: "Generate sample query" from ontology | OPEN | P3 | S | OU Ph5 |
| UI-17 | Phase 5: Nightly lineage/validation jobs | OPEN | P3 | L | OU Ph5 |
| UI-18 | Phase 6: Focused prediction feature set | OPEN | P3 | M | OU Ph6 |
| UI-19 | Phase 6: Hard constraints (max entities, min confidence) | OPEN | P3 | S | OU Ph6 |
| UI-20 | Phase 6: Human-in-the-loop feedback signal | OPEN | P3 | L | OU Ph6 |
| UI-21 | MetadataReview component decomposition (64 useState) | OPEN | P3 | L | RC 2.2 |

---

## 4. Genie Space Generation

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| GN-1 | Extract MV joins into `join_specs` during assembly | DONE | -- | -- | GJ 1 |
| GN-2 | Force-merge prebuilt `join_specs` post-agent | DONE | -- | -- | GJ 2 |
| GN-3 | Align agent prompt with auto-merge pattern | DONE | -- | -- | GJ 3 |
| GN-4 | Warn when `join_specs` empty with multiple tables | DONE | -- | -- | GJ 4 |
| GN-5 | `_qualify_columns_in_expr` cross-table column awareness | DEFERRED | P1 | M | GJ |
| GN-6 | Join_spec SQL dry-run validation | DEFERRED | P2 | M | GJ |
| GN-7 | Deploy retry join stripping workaround | DONE | -- | -- | GJ |
| GN-8 | Value sampling batching (50 sequential queries) | DEFERRED | P2 | M | GJ |
| GN-9 | Context window measurement and truncation | DEFERRED | P2 | M | GJ |
| GN-10 | `_strip_out_of_scope_sql` regex ineffectiveness | DEFERRED | P3 | S | GJ |

### GN-1: Extract MV joins into `join_specs`

**Status: DONE** -- `_extract_mv_join_specs()` in `genie/context.py` extracts joins from metric view `json_definition`, resolves aliases via `source` field, deduplicates against existing FK/ontology pairs.

### GN-2: Force-merge prebuilt `join_specs` post-agent

**Status: DONE** -- `_merge_prebuilt_join_specs()` in `genie/agent.py` force-merges pre-built joins after agent output. Pre-built wins for same `(left, right)` pair; agent can only add new pairs.

### GN-3: Align agent prompt with auto-merge pattern

**Status: DONE** -- Agent prompt updated to use "will be merged automatically" language for join_specs (matching sql_snippets pattern). Rule 8 updated to instruct agent to generate only additional joins.

### GN-4: Warn on empty `join_specs`

**Status: DONE** -- `_validate_output()` in `genie/agent.py` warns when `source_count > 1` and joins are empty or insufficient (`< source_count - 1`).

---

## 5. Data Engineering Performance

### 5a. Core Pipeline Performance

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| DE-1 | Batch log writes (per-table single-row Delta writes) | OPEN | P2 | S | RC DE-1 |
| DE-2 | Parallelize DDL execution (sequential collect-then-loop) | OPEN | P1 | M | RC DE-2 |
| DE-3 | Remove redundant DataFrame materializations | OPEN | P1 | S | RC DE-3 |
| DE-6 | Error messages fed as metadata into LLM prompts | DONE | -- | -- | RC DE-6 |
| DE-7a | Triple materialization in `write_ddl_df_to_volume` | OPEN | P2 | S | RC DE-7 |
| DE-7b | Dead temp view + unused var in `sample_values()` | OPEN | P2 | S | RC DE-7 |
| DE-7c | Unused constant `JOIN_SAMPLE_SIZE` | OPEN | P2 | S | RC DE-7 |
| DE-8 | Codebase-wide `.collect()` audit (~232 calls, 21 files) | OPEN | P3 | L | RC DE-8 |

### 5b. Full Pipeline Performance (Ontology, FK, Extended Metadata)

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| DE-4 | FK prediction collect-then-loop (700+ sequential SQL) | OPEN | P1 | L | RC DE-4 |
| DE-5 | DESCRIBE DETAIL in for-loop with hard cap at 100 | OPEN | P2 | M | RC DE-5 |

### 5c. Scaling Recommendations

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| R1 | Replace similarity CROSS JOIN with Vector Search ANN | DONE | -- | -- | SC R1 |
| R2 | Cap knowledge graph edges per attribute value | DONE | -- | -- | SC R2 |
| R3 | Batch DESCRIBE EXTENDED into `information_schema` query | OPEN | P0 | S | SC R3 |

> **R3 caveat:** Not all fields returned by DESCRIBE EXTENDED are available in `information_schema`. Fields like column-level statistics (min, max, distinct_count, avg_col_len, max_col_len, num_nulls) from ANALYZE TABLE COMPUTE STATISTICS are only exposed via DESCRIBE EXTENDED. A full replacement is not possible -- the migration should batch what `information_schema` does cover (column names, types, comments, nullability) and fall back to DESCRIBE EXTENDED only for statistics-dependent paths.
| R4 | Paginate semantic layer context build | OPEN | P1 | M | SC R4 |
| R5 | Stream ontology column classification | OPEN | P1 | M | SC R5 |
| R6 | Document and tune multi-task sharding | OPEN | P1 | S | SC R6 |
| R7 | Auto-tune `columns_per_call` for wide tables | OPEN | P2 | S | SC R7 |
| R8 | Column-count gate for FK prediction | OPEN | P2 | S | SC R8 |
| R9 | Configurable profiling column cap | OPEN | P2 | S | SC R9 |
| R10 | Remove/gate Python embedding fallback | OPEN | P3 | S | SC R10 |

### 5d. AI_QUERY / LLM Call Optimization

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| AQ-1 | Batch column classification (O(NC) -> O(N)) | OPEN | P1 | M | AQ 1 |
| AQ-2 | Batch table classification (O(N) -> O(N/20)) | OPEN | P1 | M | AQ 2 |
| AQ-3 | Raise FK AI threshold (0.3 -> 0.5+) | OPEN | P2 | S | AQ 3 |
| AQ-4 | Vectorized SQL AI_QUERY for ontology | OPEN | P2 | L | AQ 4 |
| AQ-5 | Async/concurrent Python LLM calls | OPEN | P2 | M | AQ 5 |

### 5e. Graph Edge Quality

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| GE-1 | `co_accessed` edges from query audit history | OPEN | P2 | M | RC GE-1 |
| GE-2 | Similarity threshold calibration | OPEN | P3 | M | RC GE-2 |
| GE-3 | Domain label reconciliation (synonym merging) | OPEN | P2 | M | RC GE-3 |

### 5f. Code Architecture

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| DE-9 | God-module decomposition (`api_server.py` 9.8K lines, `ontology.py` 5.3K, `processing.py` 4K) | OPEN | P2 | L | code audit |
| DE-10 | Dependency injection for testability (7+ internal module stubs required to test `processing.py`) | OPEN | P2 | L | code audit |

---

## 6. Packaging & Distribution

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| PK-1 | Fix import structure (`from dbxmetagen...`) | DONE | -- | -- | PI Ph1 |
| PK-2 | Dual-mode notebooks (pip + DAB) | DONE | -- | -- | PI Ph2 |
| PK-3 | SpaCy model optional extras | DONE | -- | -- | PI Ph3 |
| PK-4 | Package metadata in `pyproject.toml` | OPEN | P3 | S | PI Ph4 |
| PK-5 | Distribution strategy (PyPI / private / git) | OPEN | P3 | S | PI Ph5 |
| PK-6 | CI/CD wheel build + test | OPEN | P3 | M | PI Ph6 |
| PK-7 | Remove dead `app_service_principal_application_id` var and double-deploy from `deploy.sh` | OPEN | P2 | S | deploy analysis |

### PK-2: Dual-mode notebooks

**Status: DONE** -- All imports use `from dbxmetagen...` (PK-1 completed). Notebooks work in both DAB deployment and pip-installed modes.

---

## 7. Dismissed or Low Priority

| ID | Item | Status | Priority | Source |
|----|------|--------|----------|--------|
| T3-1 | SQL injection via f-strings | LOW RISK | P3 | RC 3.1 |
| T3-2 | Sequential processing within a task | ADDRESSED | -- | RC 3.2 |
| T3-3 | MetadataReview component decomposition | OPEN | P3 | RC 2.2 |
| T3-4 | TypeScript migration | NICE-TO-HAVE | P3 | RC 3.6 |
| T3-5 | React Router | NICE-TO-HAVE | P3 | RC 3.6 |
| T3-6 | Multi-person approval workflow | ENTERPRISE | P3 | RC 3.7 |

### T3-1: SQL injection via f-strings

Most f-string SQL interpolates catalog/schema/table names validated by UC naming rules. One real exception: `mark_table_failed()` in `processing.py` (~line 1131) interpolates `error_message` with only `replace("'", "''")` escaping, which doesn't handle backslashes.

### T3-2: Sequential processing within a task

The `for table in config.table_names` loop is sequential within a single task, but the architecture already supports multi-task parallelism via `claim_table()` and the control table. Concurrency is at the Databricks Jobs level.

---

## Summary by Status

| Status | Count |
|--------|-------|
| DONE | 23 |
| PARTIAL | 2 |
| OPEN | 41 |
| DEFERRED | 10 |
| KILLED | 2 |

## Recommended Implementation Order

**Wave 1 -- P0 (ship-blockers):**
1. ~~ON-7: Property classification test coverage~~ -- PARTIAL (unit coverage added, integrated flow test remains P1)
2. ~~GN-1 through GN-4: Genie join reliability fixes~~ -- DONE
3. ~~R1: Vector Search ANN for similarity~~ -- DONE (use_ann=True default, cross-join fallback, method telemetry)
4. R3: Batch DESCRIBE EXTENDED (S) -- only remaining P0

**Wave 2 -- P1 (next sprint):**
5. ~~ON-4: Remove legacy `link` SQL filter~~ -- DONE
6. ~~ON-5: Auto-generate inverse edges~~ -- DONE
7. ~~ON-6: Bundle version check for incremental~~ -- DONE
8. ~~ON-8: Conformance validation~~ -- DONE
9. ~~MG-1: Chat client hard-fail path~~ -- DONE
10. MG-2: PII column-to-table rollup (M)
11. MG-5: Day-2 re-run lifecycle (M)
12. DE-2: Parallelize DDL execution (M)
13. DE-3: Remove redundant materializations (S)
14. DE-4: FK prediction parallelization (L)
15. R4-R6: Semantic layer pagination, ontology streaming, multi-task docs (M+M+S)
16. AQ-1, AQ-2: Batch column + table classification (M+M)

**Wave 3 -- P2 (planned):**
17. MG-3, MG-4, MG-6-MG-10: Remaining metadata robustness items
18. ON-11 (complete JSON-LD), ON-12 (bundle version UC tags): Ontology polish
19. ON-10: Subdomain entity affinity (gated on verifying subdomain data)
20. UI-6 through UI-11: Ontology UI Phase 2-3
21. DE-1, DE-5, DE-7: Performance cleanup
22. R7-R9, AQ-3-AQ-5: Scaling optimizations
23. GE-1, GE-3: Graph edge quality
24. PK-3: SpaCy model optional extras

**Wave 4 -- P3 (backlog):**
25. UI-12 through UI-20: Ontology UI Phase 4-6
26. DE-8, R10: Deep performance work
27. PK-4-PK-6: Distribution
28. T3 items, GE-2: Low priority / nice-to-have

---

## 0.8.8 Test Coverage Gaps (P1)

The following areas lack test coverage as of 0.8.8 and should be addressed in the next sprint:

| ID | Area | Notes |
|----|------|-------|
| TG-1 | FK delete endpoint SQL correctness | WHERE clause now uses src_table/dst_table; needs a test asserting correct SQL generation to prevent regression |
| TG-2 | OBO auth flow | Tests exist in `test_app_logic.py` but are excluded from default run by `pyproject.toml` addopts. Either include in CI or add a core-suite equivalent |
| TG-3 | `evaluation/ground_truth.py` | 684 lines with no test coverage |
| TG-4 | DDL async apply (bundle apply background task) | No test for the background worker + polling pattern |
| TG-5 | New API deployment routes | `/api/deploy/*` endpoints have no tests |
| TG-6 | Property roles end-to-end | Unit tests exist; no integration test for bundle-match -> heuristic-fallback -> FK candidate flow (ON-7) |
