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

**Last reconciled against codebase:** 2026-05-10

---

## 1. Core Metadata Generation Robustness

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| MG-1 | Chat client garbage fallback on JSON parse failure | DONE | -- | -- | RC 1.1 |
| MG-2 | PII column-to-table rollup (`compute_table_sensitivity`) | OPEN | P1 | M | RC 1.2 |
| MG-3 | Entity suggestions in semantic layer — CLOSED (non-issue): already data-driven, not hardcoded (semantic_layer.py:731). | DONE | P2 | S | RC 1.3 |
| MG-4 | Quality metrics exposed to users — PARTIAL: quality_score exists in schema; not yet exposed to users. | PARTIAL | P2 | L | RC 1.4 |
| MG-5 | Day-2 re-run lifecycle / `_review_status` tracking | OPEN | P1 | M | RC 1.5 |
| MG-6 | Structured output vs regex JSON recovery | PARTIAL (v0.10.61) | P2 | M | RC 2.1 |
| MG-7 | Schema-level PII reconciliation via FK graph | OPEN | P2 | M | RC 2.3 |
| MG-8 | Ontology as generation context (inject entity type into prompts) — DONE: ontology context injected via enrich_from_ontology() (prompts.py:180-216, called processing.py:2450). | DONE | P2 | M | RC 2.4 |
| MG-9 | Audit trail for metadata state transitions — DONE: entity_tag_audit_log (ontology.py:3910/4032/4045). | DONE | P2 | M | RC 2.5 |
| MG-10 | `enrich_from_knowledge_base()` should override stale UC comments — PARTIAL: injects KB but fills EMPTY slots only (prompts.py:244/264); does NOT override stale UC comments (the open part; see KB-2). | PARTIAL | P2 | S | apply_ddl analysis |
| MG-11 | `MetadataConfig` schema validation (Pydantic model with field validators) | OPEN | P2 | M | code audit |
| MG-12 | PI prompt: add negative examples for clinical vocabulary — PARTIAL: positive clinical examples exist; no explicit negative examples. | PARTIAL | P1 | S | PI precision analysis |
| MG-13 | Tighten Presidio deference rule (#10) — the rule exists (prompts.py:957) but is intentionally loose; make it stricter. | OPEN | P1 | S | PI precision analysis |
| MG-14 | Presidio `diagnosis_code` regex too broad -- raise threshold or gate — PARTIAL: score_threshold gating exists; diagnosis_code regex still broad (deterministic_pi.py:127). | PARTIAL | P2 | S | PI precision analysis |
| MG-15 | Preserve `type` field downstream instead of collapsing to `protected` — PARTIAL: type preserved at column level; collapsed to protected only at table level (processing.py:3217). | PARTIAL | P2 | S | PI precision analysis |
| MG-16 | PI confidence threshold gating (discard low-confidence classifications) — DONE: Presidio score_threshold gating (deterministic_pi.py:158/172). | DONE | P2 | S | PI precision analysis |
| MG-17 | SWIFT/BIC dropped: `swift`/`iban` patterns were under supported_entity=CREDIT_CARD -> hit the card Luhn gate -> SWIFT (non-numeric) always dropped. Fixed: own recognizers emitting SWIFT_CODE/IBAN_CODE (bypass Luhn). | DONE | P2 | S | UAT PI scenario |
| MG-18 | UAT PI gold not normalized for equivalent classes (pi/pii, phi/medical_information) | OPEN | P3 | S | UAT PI scenario |
| FK-11 | Provably-disjoint FK pair (join probe ran, join_matched=0 AND ri_score=0) still scored final_confidence ~0.6. Fixed: collapse final_confidence 0.25x on the same never_joins signal, so it drops below threshold (not just is_fk=false). | DONE | P2 | S | UAT FK scenario (uat_fk_hard region_id trap) |
| MG-19 | `luhn_checksum(res.score)` in classify_column passes the SCORE (float) not the matched TEXT -> always False -> every deterministic CREDIT_CARD match dropped. Needs matched-text plumbing + presidio to verify (risk: order_ref trap). Not fixed blind. — CONFIRMED bug: luhn_checksum(res.score) passes score not text (deterministic_pi.py:279) → every deterministic CREDIT_CARD match dropped. PRIORITIZE. | OPEN | P1 | S | UAT PI scenario (deep-dive during MG-17) |
| MG-20 | Special-char table identifiers (e.g. a `$` in a federated Redshift table name like `…1$raw`) break SQL that interpolates the FQN bare -> `PARSE_SYNTAX_ERROR at '$'`. **DONE (v0.10.61-64):** shared `quote_fqn()` (`databricks_utils.py`) backtick-quotes each dotted segment; applied to every customer-source-table SQL site -- profiling, FK source sampling, `extended_metadata` DESCRIBE DETAIL + DESCRIBE CATALOG, `processing.get_column_types_from_describe` (DESCRIBE TABLE), the type-conversion read, DESCRIBE EXTENDED (processing + `prompts.py`). The final gap (the actual data read) was MG-22. **Verified live on `uat_ddl_edges.dollar$raw`:** generation "Table processed" + 4 metadata rows; profiling covers it. `very_wide_table` (120 cols) also clean (242 rows -> ON-19 confirmed). | DONE | P1 | M | Customer log + live UAT |
| MG-22 | **Silent `$`-table skip (the read path, sub-bug of MG-20).** `read_table_with_type_conversion` read the table via `spark.read.table(fqn)` in the no-special-types branch. `spark.read.table` parses the identifier through the SAME `parseTableIdentifier` as `spark.table()`, so `$` raised `PARSE_SYNTAX_ERROR`; the exception was caught in `get_generated_metadata_data_aware` -> returned `[]` -> `review_and_generate_metadata` -> `(None,None)` -> "Skipped - No metadata generated", yet `mark_table_completed` still ran (control=`completed`, zero metadata rows). An earlier assumption that `spark.read.table` tolerates `$` was WRONG. **DONE (v0.10.64):** route ALL source reads through `spark.sql(f"SELECT * FROM {quote_fqn(name)}")` (parse-safe) -- `read_table_with_type_conversion` (both branches), profiling `_profile_table_delta`/`_federated`, and the override source-col check; corrected the `quote_fqn` docstring. Verified live: `dollar$raw` now processes with 4 metadata rows. Regression scenario `uat_ddl_edges.dollar$raw` + `TestGenerationPathIdentifierQuoting` guard against reintroduction. | DONE | P1 | M | Live UAT (uat_ddl_edges) |
| MG-21 | Log spam: `[NOTICE] Using a notebook authentication token` repeats ~90x in a single run, burying real errors. **DONE (v0.10.61):** shared `new_workspace_client()` passes `product='dbxmetagen', disable_notice=True` (graceful fallback for older SDKs), routed through the hot-path `chat_client` auth-fallback + secret-fetch sites. Deliberately NOT a shared singleton -- each call gets its own client so concurrent LLM calls don't contend on shared SDK auth/HTTP state (per Eli: singleton would add latency at 50-100+ concurrent calls). | DONE | P3 | S | Customer log |

### Prediction Quality — name-independence + federation-safe scale (PQ)

Make FK/join prediction, fact/dim role inference, metric-view patterns, and Genie rooms
work GENERALLY (customers don't use `_id/_key/_code` or `fct_/dim_` universally; a `_code`
is sometimes NOT a FK). Improve recall (catch npi/ndc/mrn/email with no suffix) AND
precision, federation-safely and at scale. Root cause: FK SCORING is data-rich but
CANDIDATE GENERATION is name-gated, so suffix-less keys never reach the data probe (proven:
npi 20/20, ndc 10/10 → zero FK predictions in a live UAT). Guiding principle: only change
what's broken/missing — the current approach works fairly well. Full design:
`.claude/plans/let-s-move-the-advanced-gleaming-moler.md`.

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| PQ-1 | Data-driven FK candidate generator (value-overlap/containment, name-independent; tiered from CACHED profiling; SR_DATA_OVERLAP lowest trust; federation = LIMIT+collect local compute) | DONE (recall verified live; precision item PQ-1a resolved) | P1 | L | UAT two-ontology + fk_hard |
| PQ-1a | **DONE (v0.10.60)** — 1:1 table-mirror false positive: `dim_customer.customer_name -> dim_customer_staging.customer_name` (a dimension + its exact staging copy) was predicted `is_fk=true` (conf 0.879). **Root cause was NOT the mirror veto failing to fire** — the debug build showed the current code already STOPS generating this pair as a candidate (embedding same-block duplicate-table suppression + candidate-time veto), so the scoring-time veto never sees it. The lingering `is_fk=true` row was **stale**: the cumulative `fk_predictions` MERGE (keyed on `src_column,dst_column`) only upserts and never retracts a prediction that ceases to be produced (the FP row's `updated_at` was 2 days older than the re-run's fresh rows). **Fix:** added `_sweep_stale_predictions` — mirrors the ontology `sweep_stale_entities` contract exactly (gated `sweep_stale AND NOT incremental`, table-scoped, `review_updated_at IS NULL` steward-preserving); on a scoped sweep run it retracts auto-generated predictions not re-emitted this run. Reuses the existing `sweep_stale` flag already threaded through `run()`. Removed the temporary debug logging. | DONE | P1 | S | UAT fk_hard |
| PQ-2 | Extend existing `_detect_pattern` (profiling.py, already persists `pattern_detected`) with npi/ndc/cusip; consume in FK Tier-1 bucketing (NOT a new library) | DONE | P2 | S | UAT |
| PQ-3 | `_sample_categorical_values` (genie/context.py:709) does `SELECT DISTINCT` on source (federated full-scan storm); read cached `column_profiling_stats.sample_values` instead | DONE | P1 | S | federation audit |
| PQ-4 | `_validate_kpi_formula` runs LIMIT-1 per KPI×table with NO federation guard (~40 source queries in revalidate); add guard + N×M cap/dedup | DONE | P1 | S | federation audit |
| PQ-5 | Role inference + Genie `id_cols` naming reduction (`_infer_role` naming 0.35->0.20 as constants; genie prefer FK/ontology PK signals over `_id` suffix) | DONE | P2 | M | UAT |
| PQ-6 | Suffix-less + `_code`-not-FK benchmark scenario (`uat_fk_suffixless`) + eval_compare harness extension; the measurement gate for PQ-1 | DONE | P1 | M | UAT |
| PQ-7 | Bound MV `_validate_expr`/`_yaml_dry_run` federation round-trip counts (LIMIT 0/schema-only, lower sev) | DONE | P3 | S | federation audit |
| PQ-8 | **Federation validation mode for KPI dry-runs (LIMIT 1 → LIMIT 0).** `_validate_kpi_formula` validates with `SELECT {formula} FROM {table} LIMIT 1`. Since KPI formulas are aggregates, `LIMIT 1` does NOT bound the scan — it computes the aggregate over the whole table, and on a federated source may not push down (full remote scan + transfer). **Bounded to ~1 query/KPI today** by PQ-4's cap(5)+dedup+stop-at-first, so acceptable for now — this is a follow-up, not a blocker. Add a federation-aware mode (foreign/`FOREIGN`/`EXTERNAL` catalog via `_is_federated_catalog`, or `FEDERATION_MODE`) that swaps the probe to `LIMIT 0`. **Reasoning (the whole point):** the only difference is that `LIMIT 0` *skips the actual query to avoid repeated table scans* and validates the *logical plan* instead — the analyzer still fully resolves the formula (column existence, types, aggregate/GROUP BY legality, syntax; all analysis-phase, independent of LIMIT), while Spark's `OptimizeLimitZero` prunes the subtree to an empty `LocalTableScan` so **no rows are scanned or pulled**. Verified on-warehouse: `EXPLAIN … LIMIT 0` → `LocalTableScan <empty>` (billion-row source fully pruned) vs `LIMIT 1` → full `Range` scan + aggregate; and `SUM(bad_col) … LIMIT 0` still errors `UNRESOLVED_COLUMN`. The ONLY thing skipped is data-dependent RUNTIME errors (div/0, cast/overflow on real values) — fragile/time-varying, can false-fail a structurally-valid KPI, and already not checked for metric views (`_validate_expr` is already `LIMIT 0`, per PQ-7). Requires redefining KPI-validation success as "query did not throw" (not row-count based), else `LIMIT 0`'s 0-rows result mislabels every KPI "empty" in `reduce_kpi_validation` (kpi_logic.py). Refs: `_validate_kpi_formula` (api_server.py), `_is_federated_catalog`. | OPEN | P2 | S | federation audit (Eli) |

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

**Status: PARTIAL (v0.10.61)** -- The recovery path now classifies failures by `finish_reason`
(see ON-20): `invoke_structured` distinguishes truncation (`length`) and empty (`{}`/blank)
from genuine parse failures, validates against the Pydantic schema before accepting, and logs
finish_reason + response length. Still uses `re.search`/`raw_decode` for extraction (works
pragmatically).

**Remaining work:**
- For endpoints that support it, add `response_format={"type": "json_object"}` on the fallback invoke
- Apply the same finish_reason-aware recovery to `semantic_layer.py`'s ad-hoc JSON parsing

**Files:** `src/dbxmetagen/chat_client.py` (done), `src/dbxmetagen/semantic_layer.py` (remaining)

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

### MG-12: PI prompt -- add negative examples for clinical vocabulary

**Status: OPEN** -- The PI prompt's few-shot examples cover clear PII (names, SSNs, emails) and healthcare with embedded identifiers (physician notes mentioning patient names). No example shows the LLM correctly classifying standardized medical vocabulary (diagnosis codes, medication names, lab test names, units) as `medical_information` rather than `phi` when Presidio falsely flags them.

**Work:** Add one few-shot user/assistant pair showing a clinical table where Presidio flags columns like `diagnosis_code`, `medication_name`, `lab_test`, `unit` as PHI, and the correct answer classifies them as `medical_information` with moderate confidence.

**Files:** `src/dbxmetagen/prompts.py` (PIPrompt.create_prompt_template)

### MG-13: PI prompt -- tighten Presidio deference rule (#10)

**Status: OPEN** -- Rule #10 says: "If Presidio finds PII, but you recognize that there is also medical information present, classify as phi with high confidence." This causes any Presidio hit in a medical context to escalate to PHI. Standardized vocabulary (ICD codes, drug names, lab test names) should remain `medical_information` at the column level even when Presidio fires, because these are not identifiers.

**Work:** Reword rule #10 bullet 3 to: "If Presidio finds PII and the column contains actual identifying data embedded in medical text (e.g., patient names in physician notes), classify as phi. Standardized medical vocabulary (diagnosis codes, medication names, lab test names, units, reference ranges) should remain medical_information even if Presidio flags them."

**Files:** `src/dbxmetagen/prompts.py` (PIPrompt.create_prompt_template, rule #10)

### MG-14: Presidio `diagnosis_code` regex too broad

**Status: OPEN** -- The `diagnosis_code` pattern in `deterministic_pi.py` is `\b[A-Z]\d{2}\.?\d{1,2}\b` at score 0.6. This matches any single uppercase letter followed by 2-3 digits (version strings, grade values, dosage shorthand). All PHI patterns feed into a single `MEDICAL_RECORD_NUMBER` entity type, so any hit classifies the column as PHI.

**Work:** Either raise the pattern score to 0.8 (requiring context words to reach the 0.6 threshold), tighten the regex to require the ICD-10 format more specifically (e.g., `\b[A-Z][0-9]{2}\.[0-9]{1,2}\b` -- require the dot), or add `diagnosis_code` to `entities_to_ignore` in `classify_column`.

**Files:** `src/dbxmetagen/deterministic_pi.py`

### MG-15: Preserve `type` field downstream instead of collapsing to `protected`

**Status: OPEN** -- `hardcode_classification()` in `processing.py` replaces the LLM's classification with `"protected"` for any non-None type. This destroys the distinction between actual PII/PHI and medical vocabulary. Downstream consumers (KB, dashboard, tags) can't tell "this column has SSNs" from "this column has medication names."

**Work:** Preserve the original `type` value (`pii`, `phi`, `pci`, `medical_information`) through to the knowledge base. The `protected` label can remain as a convenience boolean/tag but should not replace the granular classification.

**Files:** `src/dbxmetagen/processing.py` (hardcode_classification), `src/dbxmetagen/knowledge_base.py`

### MG-16: PI confidence threshold gating

**Status: OPEN** -- The LLM can output confidence as low as 0.3 and the result still becomes `protected`. No threshold filters low-confidence classifications.

**Work:** Add a `pi_confidence_threshold` config parameter (default 0.5). Classifications below this threshold are downgraded to `None`. Applied post-LLM, pre-hardcode.

**Files:** `src/dbxmetagen/processing.py`, `src/dbxmetagen/config.py`, `variables.yml`

### MG-17: SWIFT/BIC code lost to LOCATION NER collision

**Status: OPEN** -- A realistic SWIFT/BIC value (e.g. an 11-char bank identifier embedding a country code) is picked up by the spaCy NER as a `LOCATION` entity, which outranks the custom SWIFT PCI pattern recognizer. `LOCATION` is then filtered as an aggressive/false-positive entity, so the column collapses to `None` -- a missed PCI detection. Confirmed via a UAT payments table: `iban` (same bank-identifier family) correctly classified PCI while `swift` came back `None`, with `presidio_results` showing `{classification: PII, entities: [LOCATION]}`. The SWIFT regex itself matches the value; the loss is in entity priority + false-positive filtering, not the pattern.

**Work:**
- Give the custom SWIFT (and other high-specificity PCI/PHI pattern) recognizers priority over generic spaCy NER entities when their spans overlap, OR
- Exclude columns whose custom-recognizer match is a strong PCI/PHI signal from the `LOCATION`/aggressive-entity filter, so a co-occurring LOCATION guess cannot suppress them.
- Add a regression case (SWIFT/BIC value -> PCI) alongside the existing PI precision/recall tests.

**Files:** `src/dbxmetagen/deterministic_pi.py` (recognizer registration + `classify_column` entity filtering)

### MG-18: UAT PI gold not normalized for equivalent classes

**Status: OPEN** -- The `uat_expected_pi` gold table (UAT scenario builder) uses `pi`/`phi`, but the pipeline emits the equivalent classes `pii` and `medical_information`. Naive string comparison flags these equivalent matches as diffs, inflating the apparent error rate (observed: `icd10_code` gold `phi` vs actual `medical_information` is a correct match, not a miss).

**Work:** Normalize equivalence classes in the scoring step / gold table: treat `pi`≡`pii` and `phi`≡`medical_information` (and any other documented synonyms) as matches. Update `tests/data/build_uat_scenarios.py` gold and the eval_e2e scoring join.

**Files:** `tests/data/build_uat_scenarios.py`, `tests/data/build_test_warehouse.py` (scoring), `docs/UAT_SANITY_CHECK.md`

---



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
| ON-12 | Bundle version in UC tags — DONE: bundle version written to UC tags (ontology.py:3943-4009). | DONE | P2 | S | OF 3B |
| ON-13 | 5GNF trait nodes | KILLED | -- | -- | OE, OF |
| ON-14 | Row-level instance nodes | KILLED | -- | -- | OE, OF |
| ON-15 | OWL/TTL import | DEFERRED | P3 | L | OF, OE |
| ON-16 | Cardinality validation on relationships | DEFERRED | P3 | M | OF, OE |
| ON-17 | Feedback loop from steward overrides | DEFERRED | P3 | L | OF, OU 6 |
| ON-18 | Multi-ontology / crosswalk mode (secondary URIs vs canonical bundle) | OPEN | P2 | L | Provenance plan |
| ON-19 | Batch column classification truncates on WIDE tables: a 144-col table produced an 11424-char response cut off at `max_tokens=4096` -> invalid/partial JSON. The `len(columns) <= n*1.25` remainder-merge (n=120 -> up to 150 cols in ONE call) sent oversized batches, and truncation wasn't detected as truncation. **DONE (v0.10.61):** replaced count-merge with output-token-budget chunking (`_COLS_PER_CLASSIFY_CHUNK=60`), raised `max_tokens` 4096->8192, and `_classify_column_chunk_resilient` recursively BISECTS on `StructuredTruncationError` (ai_query only as last resort on a single column) so no column is lost. | DONE | P1 | M | Customer log (wide biotech tables) |
| ON-20 | Empty/`{}` responses (`Expecting value: line 1 column 1`; `classifications Field required`) parse-failed with no finish-reason context, so a retryable empty/truncated response looked identical to genuine garbage. **DONE (v0.10.61)** with MG-6: `invoke_structured` now reads `finish_reason` and raises `StructuredTruncationError` (length) or `StructuredEmptyResponseError` (empty/`{}`) -- both `ValueError` subclasses (backward compatible) -- and logs finish_reason + length. This is the signal ON-19/ON-21 bisect on. | DONE | P2 | S | Customer log |
| ON-21 | **geo_classifier has the SAME truncation bug as ON-19, worse**: `max_tokens=2048`, called `with_structured_output` directly (no fallback), and SILENTLY defaulted every column to non_geographic on any failure -> wide tables mis-classified with no error. **DONE (v0.10.61):** routed through `invoke_structured` (gains truncation signal), output-token chunking (`_GEO_COLS_PER_CHUNK=60`), `max_tokens` 2048->8192, and `_classify_geo_chunk_resilient` bisects on truncation -- defaulting only as a true last resort on a single column. | DONE | P1 | S | ON-19 sibling audit |
| ON-22 | **Table-scope ontology relationship reads (two-bundles-per-schema, different table sets).** Confirmed real customer config: multiple ontology bundles coexisting in ONE output schema on DIFFERENT table sets (finance/commercial/life-sciences data + their own ontologies in one `metadata_results`). Bundle is a PROVENANCE tag, not a scoping key. Storage is already correct; entities/FKs/joins are already table-scoped. **LOW severity, cosmetic:** ontology relationships are consumed as DESCRIPTIVE TEXT ONLY (`context.py:993,1053`; `semantic_layer.py:766-768`) — NOT joins (those come from `fk_predictions`, table-scoped, intentionally cross-bundle) — so a shared entity-type name (both bundles have `Organization`) only bleeds a wrong descriptive relationship line across table sets. **Fix (table-keyed, never bundle-keyed):** (a) reuse existing `evidence_table` to require the originating table be in scope; (b) add nullable `source_tables` to `ontology_relationships` for bundle-defined rows. Structural cross-ontology integration (FK/joins across table sets) MUST be preserved — ON-22 does not touch it. Extends ON-18; multi-schema UI-swap/enterprise-connect are EN-1/EN-2. — PARTIAL: multi-bundle infra DONE (table-scoped storage/sweep/edges); remaining = consumer-side evidence_table filtering (genie/context.py:507, semantic_layer.py:714) + a source_tables column on ontology_relationships. | PARTIAL | P2 | M | Customer ask (Mohit/Eli) + multi-bundle consumer audit |

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

### ON-22: Table-scope ontology relationship reads (two-bundles-per-schema, different table sets)

**Status: OPEN (P2).** **REFRAMED 2026-08 after Eli review** (was "Bundle-scoped ontology consumers,"
P3). The confirmed real customer requirement: multiple ontology bundles coexisting in ONE output schema
on **different table sets** — one `metadata_results` schema coherently holding finance data (finance
bundle), commercial data (commercial bundle), and life-sciences data (LS bundle) at once. This is a
first-class supported configuration, which removes the old P3 rationale ("off the recommended path / no
customer needs two bundles in one schema"). The same-tables / multi-standard crosswalk case (FHIR *and*
OMOP semantics on the SAME tables) remains **ON-18**, not this. Multiple output *schemas* — UI swap and
enterprise cross-schema connect — are **EN-1 / EN-2**, out of scope here.

**Bundle is a PROVENANCE tag, NOT a scoping key.** Genie spaces and metric views must be scoped by the
*tables in play*, never gated by bundle — same philosophy as FK prediction. A user never picks a bundle
to filter an artifact.

**HARD REQUIREMENT — structural integration crosses ontology boundaries.** Even though the finance /
commercial / LS ontologies do not integrate with each other, the underlying tables must still integrate
structurally (a finance table joining a commercial table on `customer_id`). This already works and ON-22
MUST preserve it: FK prediction is table-level and intentionally bundle-agnostic (see pitfall #8; the
OB-6 guard is per-`catalog.schema`, not per-bundle), and `_build_join_specs` reads `fk_predictions`
filtered only by `src_table/dst_table IN (selected)` (`context.py:461`). ON-22 touches ONLY ontology
*relationship* reads (descriptive text) — never FK/join generation.

**SEVERITY: LOW — cosmetic, not a correctness bug.** Storage is already correct (entities carry
`ontology_bundle` + `source_tables`; nodes namespaced `entity::{bundle}::{name}`; same-bundle edge
guards; table-scoped sweeps). The correctness-bearing reads are already table-scoped: **entities** filter
by `source_tables` (`context.py:481-491`, `semantic_layer.py:690-697`), and **joins/FKs** come only from
`fk_predictions` (table-scoped) — NOT from ontology relationships. Verified: ontology relationships feed
**descriptive text only** — the `"ENTITY RELATIONSHIPS:"` section (`context.py:993,1053`) and the
`"Relationships: …"` line (`semantic_layer.py:766-768`). They do NOT drive joins, metric-view structure,
or entity scoping. So when two table-sets share an entity-type name (both bundles have `Organization`),
the only defect is that a **descriptive relationship line bleeds across table sets** (a clinical
`Organization` header showing a finance `holds Account` line). Noise in context prose, not a broken
artifact. The two read sites:
- `genie/context._get_entity_relationships` (`context.py:503-516`) already scopes by entity-type
  membership (`src OR dst` present on the selected tables); the residual leak is shared type names.
- `semantic_layer.build_context` (`semantic_layer.py:644`) is **schema-wide by design** (no table param;
  reads all of `table_knowledge_base`) and keys `rel_by_entity` by entity-type NAME.

**Work — two tiers, both TABLE-keyed (never bundle-keyed); descriptive-only, so no correctness-regression risk:**
- **(a) Now (cheap, no schema change):** reuse the `evidence_table` column that already records the
  originating table for FK/link-derived rows (`ontology.py:6085,6128`). In `_get_entity_relationships`,
  additionally require `evidence_table` (when present) to be in the selected tables; in `build_context`,
  attach a relationship line to an entity only when its `evidence_table` is one of that entity's
  `source_tables`. Bundle-defined rows (`source='bundle'`, no evidence_table) fall back to today's
  type-name behavior.
- **(b) Later (precise, schema change):** add nullable `source_tables ARRAY<STRING>` to
  `ontology_relationships` (via `_REL_MIGRATION_COLUMNS`, `ontology.py:3391`), populate at write time in
  `discover_named_relationships` — `[src_table, dst_table]` for FK-derived, and the reverse of the
  existing `table_to_primary` map (`ontology.py:6042`) for bundle-defined/link rows — then filter reads
  by scoped tables directly. Covers the residual bundle-defined-row case `evidence_table` can't.

**Not** the root of Mohit's error — that was ON-19 (wide-table truncation), fixed. Extends ON-18.
Update `.cursor/rules/ontology-patterns.mdc:158-163` (which frames the limitation as needing a bundle
column) to this table-scoping framing.

**Files:** (a) `src/dbxmetagen/genie/context.py`, `src/dbxmetagen/semantic_layer.py`; (b) + `src/dbxmetagen/ontology.py`; tests.

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
| GN-11 | Metric-view model builder reverted saved ERD (recommendation endpoint never overlaid saved `erd_json`); now overlays saved roles/grain/schema_type + clears `_erd_cache` on save | DONE | -- | S | UAT model-builder |
| GN-12 | Improver diminishing-returns detection + guided hand-off. Replaced the blunt `improveRound>=3` hint with a health-delta plateau detector (`computeImproveGuidance` in GenieUpdater.jsx: no health gain in the last round, or round cap) that surfaces a reasoned recommendation keyed to the residual dimensions — add benchmark questions (when `semantic_gap` is the residual the improver structurally can't close), refine manually, or continue in the Genie workbench. Frontend-only; builds on existing `pre_health`/`health.dimensions`/`improveRound`. | DONE | P2 | M | Customer ask (Eli) |

### GN-11: Metric-view model builder reverted saved ERD

**Status: DONE** -- The visual model builder loads via `GET /api/semantic-layer/erd-recommendation`, which always recomputed a fresh heuristic ERD and never read the project's saved `erd_json`. Saving node roles/grain/schema_type, switching to the Generate tab (which unmounts the designer), and returning to Model reverted the edits. Fixed with `_load_saved_erd()` + `_overlay_saved_erd()` overlaying saved node roles/grain + schema_type onto the recommendation (confirmed joins already round-trip via `fk_predictions`, so edges are untouched), plus clearing `_erd_cache` on the ERD PATCH so a reload right after save is not served a pre-save cached recommendation. Verified live: saved a distinctive edit, re-fetched, overlay honored it (`user_confirmed=true`). Files: `apps/dbxmetagen-app/app/api_server.py`.

### GN-1: Extract MV joins into `join_specs`

**Status: DONE** -- `_extract_mv_join_specs()` in `genie/context.py` extracts joins from metric view `json_definition`, resolves aliases via `source` field, deduplicates against existing FK/ontology pairs.

### GN-2: Force-merge prebuilt `join_specs` post-agent

**Status: DONE** -- `_merge_prebuilt_join_specs()` in `genie/agent.py` force-merges pre-built joins after agent output. Pre-built wins for same `(left, right)` pair; agent can only add new pairs.

### GN-3: Align agent prompt with auto-merge pattern

**Status: DONE** -- Agent prompt updated to use "will be merged automatically" language for join_specs (matching sql_snippets pattern). Rule 8 updated to instruct agent to generate only additional joins.

### GN-4: Warn on empty `join_specs`

**Status: DONE** -- `_validate_output()` in `genie/agent.py` warns when `source_count > 1` and joins are empty or insufficient (`< source_count - 1`).

---

## 4c. Metric-View Reverse-Sync (externally-authored / externally-edited MVs)

**Problem.** dbxmetagen treats `metric_view_definitions` as the source of truth and pushes
OUTWARD (definition -> UC `WITH METRICS` view -> `semantic_graph` nodes/edges -> `vector_index`
`metadata_documents`). Metric views can also be **created or edited outside dbxmetagen** (a
different owner, the same name, or a same-source/overlapping-content view). Those never flow
BACK, so the knowledge graph / semantic graph / VS index silently drift from what is actually
deployed in UC. This cluster closes the round-trip.

**What already exists (build on, don't rebuild):** `semantic_graph.SemanticGraphBuilder`
decomposes MVs into `metric_view`/`measure`/`dimension` nodes+edges from `json_definition`;
`vector_index` builds MV summary/measures/dimensions docs from the same; `api_server` (~L6737)
ALREADY discovers UC MVs from `information_schema.tables` (`table_type='METRIC_VIEW'`) not present
in `metric_view_definitions` -- but only surfaces name+catalog+schema in a list (no definition
captured, no downstream sync). `transfer_metric_view_ownership` makes owner a first-class concept.
The missing middle is: read a deployed MV's definition back from UC, normalize to `json_definition`,
upsert with provenance, then let the existing graph/VS builders consume it.

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| SL-1 | **MV definition read-back parser.** Read a deployed MV's YAML body from UC (`information_schema.views.view_definition` / DESCRIBE) and parse it into the internal `json_definition` shape -- the inverse of `metric_view_core._serialize_to_yaml`. Must tolerate constructs dbxmetagen never emits (hand-authored YAML). This is the core new primitive everything else depends on. | OPEN | P2 | L | Reverse-sync feature |
| SL-2 | **Provenance + drift columns on `metric_view_definitions`.** Add `source_origin` (`dbxmetagen`/`external_import`/`external_edit`), `deployed_owner`, `last_synced_at`, and a drift flag. Analogous to `graph_edges.source_system` + entity `auto_discovered`. Lets graph/VS attribute sources and lets re-generation avoid clobbering imported defs. | OPEN | P2 | S | Reverse-sync feature |
| SL-3 | **Reconcile pass (exact match).** For each UC MV, match on `metric_view_name` + `deployed_catalog.deployed_schema` (name is unique within a UC schema; a *different owner* is the SAME object -> record owner, don't fork). Import unknown MVs as `source_origin=external_import`. **Authority: UC is truth for imports; for a dbxmetagen-authored MV edited outside, do NOT silently overwrite the stored def -- flag drift for review** (mirrors the `review_updated_at` steward-lock). Never destructive. | OPEN | P2 | M | Reverse-sync feature |
| SL-4 | **Sync imported/updated MVs to the three consumers.** Once SL-3 upserts a definition, drive `semantic_graph` (metric_view/measure/dimension nodes+edges) and `vector_index` (`metadata_documents` + VS) off it -- reuse existing builders; add MV `source_origin` attribution to nodes/docs. Confirm `merge_edges`/doc sweeps treat imported MVs like any other source (no orphan/clobber). — PARTIAL: graph-sync infra exists (api_server.py:15253+); upstream reconcile (SL-3) missing; VS-index sync not implemented. | PARTIAL | P2 | M | Reverse-sync feature |
| SL-5 | **Fuzzy 'possibly-related' suggestions (NOT auto-merge).** Same source table + measure/dimension overlap but a DIFFERENT name is surfaced as a review-UI suggestion only -- never auto-merged into one node (auto-merge risks collapsing two genuinely-distinct views / corrupting the graph). Preserves the human-in-the-loop contract. | OPEN | P3 | M | Reverse-sync feature |
| SL-6 | **Drift dashboard / review surface.** Show UC MVs missing from the graph, dbxmetagen MVs whose deployed YAML has drifted from the stored def, and orphaned graph/VS entries for MVs deleted in UC. The human decides import/overwrite/ignore per row. | OPEN | P3 | M | Reverse-sync feature |

**Sequencing:** SL-1 (parser) + SL-2 (columns) are prerequisites; SL-3 (reconcile) then SL-4 (sync)
deliver the core value; SL-5/SL-6 are follow-on precision/UX. **Design decisions locked with Eli:**
exact name+schema auto-reconciles, fuzzy is suggest-only (SL-5), UC-is-truth-for-imports with
drift-flagging for steward-authored defs (SL-3). A deleted-in-UC MV should sweep its graph/VS
entries the same way `sweep_stale_docs`/`merge_edges` sweep other sources (steward-locked defs
preserved).

## 4b. App UX / Onboarding

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| UX-1 | Interactive product tour (`react-joyride`) hidden behind `TOUR_ENABLED=false` in `App.jsx` until polished — steps are stale/thin and need a rewrite before re-enabling | OPEN | P3 | S | UI polish pass |

### UX-1: Finish/re-enable the interactive tour

**Status: OPEN** -- The `react-joyride` tour (`TOUR_STEPS` in `App.jsx`, triggered by the "Take the Interactive Tour" button in `GettingStarted.jsx`) is gated off via `const TOUR_ENABLED = false`: the `<Joyride>` render and the `onStartTour` prop are both suppressed. The three existing steps (auth badge, More menu, header guide) are thin and partly stale after nav changes. To re-enable: flip `TOUR_ENABLED`, refresh the step targets/copy to match the current nav (no "Guide" in More; header "?" button), and consider a first-run auto-start using the existing `dbxmetagen_tourSeen` localStorage key. **Files:** `apps/dbxmetagen-app/app/src/App.jsx`, `components/GettingStarted.jsx`.

---

## 5. Data Engineering Performance

### 5a. Core Pipeline Performance

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| DE-1 | Batch log writes (per-table single-row Delta writes) — DONE: batched Delta append (processing.py:188). | DONE | P2 | S | RC DE-1 |
| DE-2 | Parallelize DDL execution (sequential collect-then-loop) — PARTIAL: column comments batch per-table on DBR 16.3+; table-level DDL still sequential. | PARTIAL | P1 | M | RC DE-2 |
| DE-3 | Remove redundant DataFrame materializations | OPEN | P1 | S | RC DE-3 |
| DE-6 | Error messages fed as metadata into LLM prompts | DONE | -- | -- | RC DE-6 |
| DE-7a | Triple materialization in `write_ddl_df_to_volume` — DONE. | DONE | P2 | S | RC DE-7 |
| DE-7b | Dead temp view + unused var in `sample_values()` — DONE. | DONE | P2 | S | RC DE-7 |
| DE-7c | Unused constant `JOIN_SAMPLE_SIZE` — DONE (constant removed). | DONE | P2 | S | RC DE-7 |
| DE-8 | Codebase-wide `.collect()` audit (~232 calls, 21 files) | OPEN | P3 | L | RC DE-8 |

### 5b. Full Pipeline Performance (Ontology, FK, Extended Metadata)

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| DE-4 | FK prediction collect-then-loop (700+ sequential SQL) — PARTIAL: batch sampling + concurrent RI checks added; not fully vectorized. | PARTIAL | P1 | L | RC DE-4 |
| DE-5 | DESCRIBE DETAIL is per-table now (extended_metadata.py:413), not a 100-capped loop — original framing stale; verify no batching/rate-limit needed. | OPEN | P2 | M | RC DE-5 |

### 5c. Scaling Recommendations

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| R1 | Replace similarity CROSS JOIN with Vector Search ANN | DONE | -- | -- | SC R1 |
| R2 | Cap knowledge graph edges per attribute value | DONE | -- | -- | SC R2 |
| R3 | Batch DESCRIBE EXTENDED into `information_schema` query | DONE | -- | S | SC R3 |
| R3a | **Minor/future:** inline `if federation_mode: return {}` guard at the top of `_fetch_column_stats_concurrent` (`prompts.py:490`) as defense-in-depth. NOT needed for the current release — federation is already safe via the upstream `add_metadata=false` force (`config.py:290-298`) + entry gate (`prompts.py:86-87`), so R3 is DONE. This only protects against a FUTURE second caller of the stats path that forgets the `add_metadata` gate (would fire failing DESCRIBE EXTENDED at federated tables — non-fatal but wasteful/log-spammy). One line + a test asserting `{}`/no-SQL when `federation_mode=true`; mirrors `processing.py:224`. | OPEN | P3 | S | R3 review (Eli, defer) |

> **R3 done (verified 2026-08-16):** Implemented as a two-tier fetch in `prompts.py`. **Tier 1**
> (`_fetch_batch_column_metadata`, `prompts.py:451-488`) batches the info_schema-available fields
> (column names, types, comments) into a SINGLE `system.information_schema.columns` query for all
> columns. **Tier 2** (`_fetch_column_stats_concurrent`, `prompts.py:490-522`) fetches ONLY the
> stats fields that info_schema cannot expose (min/max/num_nulls/avg_col_len/etc. from ANALYZE
> STATISTICS) via concurrent per-column DESCRIBE EXTENDED (`ThreadPoolExecutor`, max 8). A full
> replacement was never possible (the stats caveat), so this is the terminal design: batch what's
> batchable, parallelize the irreducible remainder.
>
> **Federation-safe:** `config.py:290-298` forces `add_metadata=false` when `federation_mode=true`
> (DESCRIBE EXTENDED unsupported on federated tables), and the whole stats path is gated behind
> `if self.config.add_metadata` (`prompts.py:86-87`) — so federated tables issue ZERO DESCRIBE
> EXTENDED calls. **Graceful on unsupported tables:** Tier-1 is try/except → `{}` (`prompts.py:471`),
> and each Tier-2 per-column failure is caught/logged/skipped (`prompts.py:517-521`) so Tier-1 data
> still returns — no crash on views/unsupported types. Optional future hardening (not required): add
> an inline `if federation_mode: return {}` to `_fetch_column_stats_concurrent` for defense-in-depth,
> matching the `processing.py:224` pattern.
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
| AQ-1 | Batch column classification (O(NC) -> O(N)) — PARTIAL: chunked by columns_per_call (O(N/chunk)); not a single AI_QUERY call. | PARTIAL | P1 | M | AQ 1 |
| AQ-2 | Batch table classification (O(N) -> O(N/20)) | OPEN | P1 | M | AQ 2 |
| AQ-3 | Raise FK AI threshold (0.3 -> 0.5+) — DONE: FK AI threshold already 0.7 (fk_prediction.py:227), above the 0.5 target. | DONE | P2 | S | AQ 3 |
| AQ-4 | Vectorized SQL AI_QUERY for ontology | OPEN | P2 | L | AQ 4 |
| AQ-5 | Async/concurrent Python LLM calls — PARTIAL: async classify_table_domain_async() exists (domain_classifier.py:888) but not wired into the pipeline. | PARTIAL | P2 | M | AQ 5 |

### 5e. Graph Edge Quality

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| GE-1 | `co_accessed` edges from query audit history | OPEN | P2 | M | RC GE-1 |
| GE-2 | Similarity threshold calibration | OPEN | P3 | M | RC GE-2 |
| GE-3 | Domain label reconciliation (synonym merging) | OPEN | P2 | M | RC GE-3 |

### 5f. Code Architecture

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| DE-9 | God-module decomposition (`api_server.py` **16.1K** lines, `ontology.py` 5.3K, `processing.py` 4K) — FULL decomposition; see DE-11 for the minimal enabling subset to do first. | OPEN | P2 | L | code audit |
| DE-10 | Dependency injection for testability (7+ internal module stubs required to test `processing.py`) | OPEN | P2 | L | code audit |
| DE-11 | **Minimal `api_server.py` split (prep/enabler — do FIRST).** api_server.py is 16.1K lines / ~190 endpoints and forces serialization of any workstream that touches it (DP-13, EN-1, SLG-1). Behavior-preserving minimal split (NOT the full DE-9): (1) extract the shared foundation into a `_common.py` — `execute_sql`/`execute_sql_meta`, `fq`, `CATALOG`/`SCHEMA`, `get_workspace_client`, the TTL caches+locks, model config, error regexes, `_MAX_RESULT_ROWS`; (2) move the two largest cohesive route groups (candidates by size: ontology 40, semantic-layer 39, genie 21 — pick the two with the LOWEST shared-state coupling at spec time) into `routers/*.py` as FastAPI `APIRouter`s wired via `app.include_router()`. No endpoint paths or behavior change. Acceptance: the registered-route set (`app.routes` paths) is IDENTICAL before/after (add a test asserting this), app boots, core suite green. Removes ~79 endpoints from api_server.py and unblocks parallel workstreams. Scoped subset of DE-9. | OPEN | P1 | M | workstream enabler (Eli) |

### 5g. Pipeline Incrementality (May 2026)

All items below were completed in a single sprint. Every analytics pipeline task now follows a
watermark-based incremental pattern with content-change guards and stable timestamp propagation.

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| INC-1 | Watermark-based early exit for all analytics tasks | DONE | -- | L | incrementality audit |
| INC-2 | Content-change guards on all MERGE statements | DONE | -- | M | incrementality audit |
| INC-3 | Stable `updated_at` propagation (upstream timestamps, not `current_timestamp()`) | DONE | -- | M | incrementality audit |
| INC-4 | Knowledge graph embedding nullification on comment change | DONE | -- | S | incrementality audit |
| INC-5 | FK prediction incremental scoping (`_changed_tables` OR filtering) | DONE | -- | M | incrementality audit |
| INC-6 | Similarity edges incremental watermark + `table_names` scoping | DONE | -- | M | incrementality audit |
| INC-7 | Ontology column properties: overwrite -> MERGE with deterministic `property_id` | DONE | -- | S | incrementality audit |
| INC-8 | Community summaries: per-community freshness check + MERGE | DONE | -- | S | incrementality audit |
| INC-9 | Vector index: 6-source upstream watermark check | DONE | -- | S | incrementality audit |
| INC-10 | Data quality: watermark on `profiling_snapshots.snapshot_time` | DONE | -- | S | incrementality audit |
| INC-11 | Ontology validator: early exit on 0 unvalidated entities | DONE | -- | S | incrementality audit |
| INC-12 | Schema KB: gate AI calls on `incremental` flag | DONE | -- | S | incrementality audit |
| INC-13 | E2E eval `exclude_tables` parameter for incrementality testing | DONE | -- | S | incrementality audit |

### 5h. Ontology Cross-Bundle Isolation & Sweep Closure (Jun 2026)

Hardening of multi-bundle coexistence and sweep cleanup. Canonical invariants in
`.cursor/rules/ontology-patterns.mdc`.

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| OB-1 | Same-bundle resolver in `_build_structural_edges` AND `discover_inter_entity_relationships` (entity-concept edges never cross bundles) | DONE | -- | M | cross-bundle audit |
| OB-2 | `_purge_orphaned_bundle_seeds()` post-discovery sweep for orphaned foreign-bundle table-less seeds | DONE | -- | M | bundle-switch audit |
| OB-3 | `_purge_stale_relationships()` non-incremental sweep of all auto-generated `ontology_relationships` (incl. `configured`); fixes `categorized_as`/`category_of` regression | DONE | -- | M | relationships audit |
| OB-4 | `_build_structural_edges` excludes `categorized_as`/`category_of` while preserving bundle-defined `contains`/`part_of`/`member_of`/`has_part` | DONE | -- | S | relationships audit |
| OB-5 | FK pair canonicalization swaps `col`/`table`/`dtype` in lockstep (3 generator sites) | DONE | -- | S | fk label audit |
| OB-6 | Column-property FK generator: same-block (catalog.schema) guard gated on `ontology_cross_block` (was linking by entity type across schemas) | DONE | -- | S | UAT FK scenario |
| OB-7 | Data-probe veto: force `is_fk=false` when join probe found zero overlap (`join_matched=0 AND ri_score=0`), exempting declared FKs -- stops skip-AI tiers asserting non-joining pairs | DONE | -- | S | UAT FK scenario |
| OB-8 | `fk_predictions` MERGE has no delete-by-source path -> a spurious row already persisted is not corrected when a fix stops the pair being regenerated (goes stale). "Sweep stale edges" only cleans `graph_edges`, not `fk_predictions`. **DONE (v0.10.60)** via `_sweep_stale_predictions` -- same fix as PQ-1a. | DONE | P2 | M | UAT FK scenario |
| OB-9 | One-FK-per-child-column resolution: a single child column claiming `is_fk=true` to >1 parent table (polymorphic reference, not a valid SQL FK) keeps only the highest-confidence target; demotes the rest to `is_fk=false`. Fixes multi-primary-table entity-type collisions (e.g. one `Organization`/`Person` type primary for two tables in a schema) that the same-block guard (OB-6) and never-joins veto (OB-7) both miss. | DONE | -- | S | UAT snowflake scenario |
| OB-10 | Standalone `fk_prediction_job` never exposed `sweep_stale_edges` as a job parameter (the notebook widget existed but was unwired), so the job could never clean orphaned `graph_edges` -- a demoted/removed pair lingered as a stale predicted_fk edge. Wired the param through job YAML -> notebook widget -> `merge_edges(sweep_stale=True)`. | DONE | -- | S | UAT snowflake scenario |
| OB-11 | `join_validate` raised `RuntimeError: no federation sample views for 0 candidate pair(s)` in `federation_mode` when there were ZERO FK candidate pairs -- the `if not fragments` guard fired unconditionally even when `rows` was empty (nothing to validate). **DONE (v0.10.61):** gated the raise on `rows and self.config.federation_mode`, so an empty candidate set falls through to the graceful zero-join result (like the non-federation path); only a real failure (had pairs, built no sample views) still raises. | DONE | P1 | S | Customer log (federated, no cross-table key overlap) |
| OB-12 | **Value-overlap FK scope over-match.** `get_value_overlap_candidates` `_in_scope` did `scope_pats = [t.lower().rstrip('*')]` then `t == p or t.startswith(p)`. For an EXACT (non-wildcard) `table_names` entry like `cat.sch.orders`, `rstrip('*')` is a no-op so `startswith` matched sibling tables sharing the prefix (`orders_archive`, `orders_2023`), leaking them into a run scoped to one table. **DONE (v0.10.65):** split into exact-set (equality) + wildcard-prefixes (keep trailing `.` guard), matching `table_names_col_filter` semantics. Regression tests `test_exact_table_scope_excludes_prefix_siblings` + `test_wildcard_scope_still_matches_schema`. | DONE | P2 | S | Code review (Isaac) |

### Code-review findings (Isaac Review, feature branch) -- verified-real, deferred

These survived verification against the code (F3 SQL-skeleton-dedup was checked and dismissed as working-as-designed). OB-12 (scope) + CC-1 (customer-context MERGE, below) were fixed immediately; these are the real-but-lower-priority remainder.

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| CR-1 | **Loose distinctive-format regexes** (`profiling.py`): `NPI_PATTERN=^\d{10}$` matches any 10-digit number (phone/account/order id); `CUSIP_PATTERN=^[0-9A-Z]{9}$` matches any 9-char code (SKU). Both feed the relaxed 0.30 value-overlap containment bar, so unrelated same-shape columns can bucket together and emit spurious FK candidates. MITIGATED today by the parent-uniqueness gate + join probe + never_joins veto, so precision impact is bounded, not zero. Fix: tighten (NPI checksum / NDC-style segmenting; treat CUSIP as advisory) or require ontology/name corroboration before applying the relaxed bar to `npi`/`cusip`. — CONFIRMED loose (profiling.py:70/72). PRIORITIZE. | OPEN | P1 | S | Code review (Isaac) |
| CR-2 | **Truncation detection only in the fallback path** (`chat_client.invoke_structured`): the `finish_reason=='length'` -> `StructuredTruncationError` classification lives only in the `except` branch; the primary `with_structured_output(...).invoke()` path returns directly with no finish_reason check. A tool-calling endpoint that returns a truncated-but-parseable structure without raising would bypass ON-19's bisect. In practice the batch-classify endpoints fall through to the JSON branch (so ON-19 works there), but this isn't universal. Fix: inspect finish_reason on the primary path too (where the SDK surfaces it). — PARTIAL: truncation detection exists but only in the fallback path, not the primary tool-calling path. | PARTIAL | P2 | S | Code review (Isaac) |
| CR-3 | **`StructuredEmptyResponseError` never specially handled** downstream: it's raised on an empty/`{}` completion but only `StructuredTruncationError` is caught in geo/ontology; empty falls to the generic fallback, so its documented "retryable transient empty" intent is unrealized (not broken -- the fallback still returns a result). Fix: catch it for one bare retry before defaulting. | OPEN | P3 | S | Code review (Isaac) |
| CR-4 | **O(n^2) value-overlap bucket loop** (`fk_prediction.get_value_overlap_candidates`): the nested `i x j` over each `(dtype-family, pattern)` bucket does set-intersections for every ordered pair; the `emitted >= ceiling` break bounds ACCEPTED candidates, not the rejected-pair work (the common case). A wide schema with hundreds of `numeric_id` columns in one bucket runs the full n^2 on the driver. Fix: pre-filter bucket members (e.g. require compatible distinct-count ranges / a MinHash prefilter) or cap bucket size before the pairwise loop. | OPEN | P2 | M | Code review (Isaac) |
| CC-1 | **customer-context re-seed MERGE clobbered `created_by`.** The MATCHED branch set `tgt.created_by = src.created_by` (always `'yaml_seed'`) + `tgt.scope`/`tgt.scope_type`, contradicting the adjacent comment ("updates only text/label/priority + updated_at") and resetting UI-set operator provenance on every re-seed. **DONE (v0.10.65):** MATCHED branch now updates only `context_text`/`context_label`/`priority`/`updated_at` (scope/scope_type are derived from the `context_id` key so invariant); test extended to assert `tgt.created_by` is absent from the MERGE. | DONE | P3 | S | Code review (Isaac) |

### OB-8: `fk_predictions` stale-row cleanup

**Status: DONE (v0.10.60)** -- FK predictions were written via an upsert-only MERGE keyed on `(src_column, dst_column)` with no delete path. Once a pair stopped being generated (e.g. after OB-6 blocked a cross-schema pair, a fix stopped generating a mirror pair, or the tables left scope), an already-persisted `is_fk=true` row lingered indefinitely. The OB-7 veto only rewrites a row if a later run regenerates that exact pair. `sweep_stale_edges` did NOT help: it sweeps `graph_edges` scoped to `source_system='fk_predictions'`, a different table -- so the phantom row in `fk_predictions` (which the app FK views and ERD designer read directly) survived, and the two tables drifted out of sync. This is the same defect class as **PQ-1a** (a 1:1 table-mirror FP that stayed `is_fk=true` after the code stopped generating it).

**Work (shipped):** Added `_sweep_stale_predictions` in `write_predictions` (`fk_prediction.py`). Gated `sweep_stale AND NOT incremental`, table-scoped (`src_table` OR `dst_table` in the run's `table_names`; empty = whole-schema), steward-preserving (`review_updated_at IS NULL` only). Deletes auto-generated predictions absent from the freshly-produced staging view (`NOT EXISTS`, functionally equivalent to `WHEN NOT MATCHED BY SOURCE` scoped to the run). Reuses the existing `sweep_stale` flag already threaded through `run()`. Mirrors the ontology `sweep_stale_entities` contract exactly. Verified live on `uat_fk_hard`: the stale mirror FP was retracted (0 rows) while real FKs/traps stayed correct.

**Files:** `src/dbxmetagen/fk_prediction.py` (`_sweep_stale_predictions`, called from `write_predictions`)

### OB-9: One-FK-per-child-column resolution

**Status: DONE** -- A single fully-qualified child column (`src_column`, always the FK side after `_enforce_direction`) cannot be a referential FK to more than one parent table -- that is a polymorphic reference, not expressible as a SQL FK constraint and a frequent source of bad downstream joins. Root cause: when one ontology entity type is `primary` for two tables in the same schema (e.g. both `dim_facility` and `dim_health_system` classify as `Organization`; both `dim_patient` and a bridge classify as `Person`), the column-property candidate generator crosses an `object_property` column against EVERY primary table of its linked type, and the `column_property_skip_ai` path stamps them all `is_fk=true`. The extra targets slip past the same-block guard (OB-6 -- they are same-schema) and the never-joins veto (OB-7 -- small integer id domains coincidentally overlap so `join_matched>0`, or the fan-out target's key is merely non-unique).

**Work (shipped):** In `write_predictions`' final projection, after per-pair dedup, rank surviving `is_fk=true` targets per `src_column` (is_fk-true first, declared-FK next, then `final_confidence` desc, then a stable `dst_column` tiebreak) and demote all but the winner to `is_fk=false`. Declared FKs (`SR_DECLARED`) are exempt from demotion but authoritatively win the ranking, demoting any competing prediction on the same child column. Rows are retained (not deleted), so the ERD recommender and review UI -- which read on confidence, not `is_fk` -- still surface them for optional steward confirmation, while `graph_edges` / metric views / Genie / DDL (all filter `is_fk=true`) no longer see the spurious join.

**Verified:** live on `uat_snowflake_health` -- `fct_encounter.patient_id` resolves to `dim_patient` (0.90) not the bridge (0.80); `patient_facility_bridge.facility_id` resolves to `dim_facility` (0.92) not `dim_health_system` (0.89). 7 tests in `TestOneFkPerChildColumn`.

**Files:** `src/dbxmetagen/fk_prediction.py` (`write_predictions`), `tests/test_fk_prediction.py`

### OB-10: standalone FK job could not sweep stale graph edges

**Status: DONE** -- The `predict_foreign_keys.py` notebook has always had a `sweep_stale_edges` widget (default false) that flows to `write_graph_edges(sweep_stale=...)` -> `merge_edges(sweep_stale=True)`, which deletes orphaned `predicted_fk` edges from `graph_edges` for `source_system='fk_predictions'`. But the standalone `fk_prediction.job.yml` never declared `sweep_stale_edges` as a job parameter nor passed it into `base_parameters`, so the job always ran with the widget default (false) and could never clean orphans. Observed on `uat_snowflake_health`: after OB-9 demoted two pairs to `is_fk=false`, `fk_predictions` self-healed (in-place MERGE overwrite), but two stale `predicted_fk` edges lingered in `graph_edges` because this job's `merge_edges` ran with `sweep_stale=False`. (Distinct from OB-8, which is the still-open gap that `fk_predictions` itself has no delete-by-source path.)

**Work (shipped):** Added `sweep_stale_edges` (default `"false"`, backward compatible) to `fk_prediction.job.yml` job parameters and wired it into the notebook task `base_parameters`. Running the job with `sweep_stale_edges=true` now clears orphaned FK graph edges. Do NOT hand-DELETE `graph_edges` (pitfall #6) -- the sweep goes through `merge_edges`.

**Files:** `resources/jobs/fk_prediction.job.yml`

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
| PK-7 | Remove dead `app_service_principal_application_id` var and double-deploy from `deploy.sh` | DONE | P2 | S | deploy analysis — deploy.sh removed; collapsed to `bundle deploy` + `scripts/grant_app_permissions.sh` (static bundle YAML, single-pass, build hook) |

### PK-2: Dual-mode notebooks

**Status: DONE** -- All imports use `from dbxmetagen...` (PK-1 completed). Notebooks work in both DAB deployment and pip-installed modes.

---

## 7. Dismissed or Low Priority

| ID | Item | Status | Priority | Source |
|----|------|--------|----------|--------|
| T3-1 | SQL injection via f-strings | LOW RISK | P3 | RC 3.1 |
| T3-2 | Sequential processing within a task | ADDRESSED | -- | RC 3.2 |
| T3-4 | TypeScript migration | NICE-TO-HAVE | P3 | RC 3.6 |
| T3-5 | React Router | NICE-TO-HAVE | P3 | RC 3.6 |
| T3-6 | Multi-person approval workflow | ENTERPRISE | P3 | RC 3.7 |

### T3-1: SQL injection via f-strings

Most f-string SQL interpolates catalog/schema/table names validated by UC naming rules. One real exception: `mark_table_failed()` in `processing.py` (~line 1131) interpolates `error_message` with only `replace("'", "''")` escaping, which doesn't handle backslashes.

### T3-2: Sequential processing within a task

The `for table in config.table_names` loop is sequential within a single task, but the architecture already supports multi-task parallelism via `claim_table()` and the control table. Concurrency is at the Databricks Jobs level.

---

## 8. Enterprise & Multi-Tenancy / Access Control

Customer-driven (Mohit) August 2026. The previous dbxmetagen implementation lacked all of these; the
current branch already added a few (see the verification note below). Items scoped from a code-level
investigation this session — coupling points are named in the detail blocks.

### Customer verification (Mohit, 2026-08)

The three reported bugs are **already FIXED on the current feature branch (v0.10.67)** and are tracked
as DONE above: wide-table entity-type-as-string (**ON-19/20/21**, commit `c027da2`), `$`-in-identifier
parse errors (**MG-20/22**, `fe16afa`/`c027da2`), and the FK 0-candidate federation `RuntimeError`
(**OB-11**, `8fe624b`). The **"multiple ontologies for one BU" testing error was the ON-19 wide-table
truncation** (confirmed against Mohit's logs by Eli) — not the ON-22 consumer gap. **Backward
compatibility** deploying the feature branch over `main` is **verified safe**: all new columns are
nullable via `ADD COLUMN IF NOT EXISTS` + `mergeSchema`, with fallback SELECTs (e.g.
`semantic_layer.py:1497`) and schema-alignment padding; no NOT-NULL/renamed/PK/MERGE-key changes.
Recommended gate before promoting: one live backward-compat smoke test (feature branch over a
`main`-created schema on DMVM → app + one pipeline pass → no missing-column errors).

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| EN-1 | Runtime metadata-result-schema selection in the app (schema picker; make `fq()`/`CATALOG`/`SCHEMA` request-scoped instead of startup globals — ~28 call sites; caches keyed by schema). Top customer ask. Builds on `schema_name` filter params + per-MV `deployed_catalog`/`deployed_schema` precedent. — PRIORITIZED this cycle. | OPEN | P1 | L | Customer ask (Mohit) |
| EN-2 | Cross-schema semantic-layer stitching at enterprise level (schema registry + federated/union reads across `metadata_results` schemas + cross-schema entity/FK disambiguation). Design-spike first. Depends on EN-1. — FOLDED into EV-* (Enterprise Unified View); see new EV section. | OPEN | P2 | L | Customer ask (Mohit) |
| EN-3 | External context sources for the agent — a structured UC table or an EXTERNAL vector index (already-ingested data; NOT reaching out to SharePoint). Index/endpoint are hardwired to `{CATALOG}.{SCHEMA}.{VS_INDEX_SUFFIX}` today; add `EXTERNAL_VECTOR_INDEXES`/`EXTERNAL_KB_TABLES` config + union into retrieval with source attribution. — FOLDED into CTX-3 (agent-side external context). | OPEN | P2 | M | Customer ask (Mohit) |
| EN-4 | Review-only user role (review + save/apply in Review-and-Apply only). No role system today — OBO identity only. Add role context + `require_role` gate on write/generation endpoints + frontend nav hiding. Relates to T3-6. — DEFERRED: OBO+UC already enforce per-user data security; in-app roles are lower marginal value. | OPEN | P3 | M | Customer ask (Mohit) |
| EN-5 | Restricted / no-job-execution mode (DISABLE_JOB_EXECUTION) — gate the run-a-job endpoints (metadata gen, analytics pipeline, sync). Rationale: jobs run under their configured run_as (SP/owner) regardless of OBO, so OBO does NOT stop a viewer from launching SP-privileged compute; 'read-only' really means 'no job execution' (optionally also gate in-app writes). Absorbs EN-6's read-only intent. | OPEN | P2 | M | Customer ask (Mohit) |

### EN-1: Runtime metadata-result-schema selection

**Status: OPEN** -- `CATALOG`/`SCHEMA` are read once at app startup (`api_server.py:403`) and the
module-level `fq()` helper (~28 call sites) hardcodes them; one app instance serves one schema. The
library side (`SemanticLayerConfig.fq`, `semantic_layer.py:289`) is already per-instance, and per-MV
`deployed_catalog`/`deployed_schema` (`api_server.py:2152`) is the closest existing flexibility. **Work:**
make schema request-scoped (a picker → per-request fq context; key `_cache`/agent/index caches by
schema; scope ERD/FK/KB reads by the selected schema). **Files:** `apps/.../api_server.py` (fq + ~28
sites, caches), `apps/.../src/components/*` (schema picker).

### EN-2: Cross-schema semantic-layer stitching

**Status: OPEN (design-spike first)** -- Single-schema is baked into KB/ontology/FK/genie reads
(`genie/context.py::_fq`, `_fetch_erd_inputs` `api_server.py:8144`). Needs a **schema registry**,
federated/union reads across multiple `metadata_results` schemas, and cross-schema entity/FK
disambiguation (a "Patient" in schema A vs B). Depends on EN-1. Produce a short design doc before code.

### EN-3: External context sources

**Status: OPEN** -- Vector index/endpoint are composed as `{CATALOG}.{SCHEMA}.{VS_INDEX_SUFFIX}`
(`api_server.py:~14846`, `vector_index.py:26`) and KB reads go through `fq()` — all pinned to
dbxmetagen's own schema. **Work:** add `EXTERNAL_VECTOR_INDEXES` / `EXTERNAL_KB_TABLES` config, let
`_get_api_vs_index()` accept an endpoint/index, union external sources into the deep-analysis/Genie
retrieval with source attribution + dedup. Reframes the "SharePoint/unstructured" ask as consuming
already-ingested data, not outbound fetching. **Files:** `apps/.../api_server.py`, `src/dbxmetagen/vector_index.py`.

### EN-4: Review-only user role

**Status: OPEN** -- No role system today; only OBO identity (`_OBO_ENABLED`, `_get_effective_client`
`api_server.py:132`). **Work:** add a role context (header- or config-mapped) + a `require_role`
dependency gating write/generation endpoints (`/api/jobs/run`, `/api/agent/*`, `/api/genie/*`,
`/api/ontology/*`, `apply-ddl`, `apply-tags`) while allowing review save + apply-in-review; hide the
corresponding nav/actions in `App.jsx`. Relates to T3-6. **Files:** `apps/.../api_server.py`, `apps/.../src/App.jsx`.

### EN-5: Read-only deployment mode

**Status: OPEN** -- `federation_mode` (no ALTER/tags) and per-request `apply_ddl` exist, but there is no
central write gate. **Work:** `READ_ONLY_MODE` env flag gating all write endpoints (KB PATCH,
apply-ddl(+bundle), ontology/tag apply, job submit) → 403 with a clear message; document the
restricted-service-principal fallback. Shares the enforcement point with EN-4. **Files:** `apps/.../api_server.py`.

### EN-6 (removed)

REMOVED (decomposed): production-readiness epic split into — API test suite → a QA item; audit log → MG-9 (done); OBO trust-boundary → a small doc item; read-only → EN-5. Not tracked as one epic.

---

## Enterprise Unified View (EV) — cross-schema stitching across multiple dbxmetagen OUTPUT schemas

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| EV-1 | Output-schema registry — system-of-record table + app config listing registered metadata_results schemas (catalog.schema, domain, owner, last_run). Foundation the rest reads. | OPEN | P2 | M | Enterprise (Eli) |
| EV-2 | Federated vector retrieval — agent queries MULTIPLE per-schema VS indexes at once; fan-out + reciprocal-rank-fusion merge; results schema-attributed. | OPEN | P2 | M | Enterprise (Eli) |
| EV-3 | A2A orchestrator over per-schema metadata agents — orchestrator routes a question to domain/schema-specialized agents and aggregates with provenance. | OPEN | P2 | L | Enterprise (Eli) |
| EV-4 | Enterprise KB union — union/federated view over per-schema table_knowledge_base + metadata_documents, keyed by the EV-1 registry. | OPEN | P2 | M | Enterprise (Eli) |
| EV-5 | Enterprise knowledge-graph overlay — merge per-schema graph_nodes/edges with cross-schema entity resolution (link entity::X across schemas) + inter-schema edges. | OPEN | P2 | L | Enterprise (Eli) |
| EV-6 | Cross-schema FK/relationship discovery — candidate pairs spanning schemas (cross-domain joins), registry-gated. | OPEN | P2 | M | Enterprise (Eli) |
| EV-7 | Verify + harden cross-schema metric-view / Genie assembly (reportedly partly works already) — make explicit, guard, and test. | OPEN | P3 | S | Enterprise (Eli) |

*Sequencing: EV-1 → (EV-2, EV-4) → (EV-3, EV-5, EV-6); EV-7 standalone.*

---

## New Feature & Quality Backlog (this cycle)

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| CTX-1 | External-context layer: design/architect ingesting/consolidating/indexing an external source (VS index, volume, table, or schema) into a queryable context store with a clean access API. Foundation. | OPEN | P2 | L | Eli |
| CTX-2 | Generation-side consumption of CTX-1 — wire external context into comment/PI/domain/FK prompts (extends existing enrich_from_ontology / enrich_from_knowledge_base hooks). | OPEN | P2 | M | Eli |
| CTX-3 | Agent-side consumption of CTX-1 — retrieval agents (metadata research, analyst) query the context layer. Absorbs former EN-3. | OPEN | P2 | M | Eli |
| KB-2 | Sync EXISTING UC comments into the knowledge base (ingest, not generate). Complements MG-10 (which only fills empty slots). | OPEN | P2 | M | Eli |
| FK-2 | Explicit FK hints from a user-supplied JSON/dtype file — seed/boost/lock FK candidate generation. | OPEN | P2 | M | Eli |
| PROF-2 | Federation-safe profiling correctness — current profiling is often wrong and unguarded on federated sources (no federation guard in _fetch_column_stats_concurrent prompts.py:490 = R3a; caps hardcoded profiling.py:82 = R9). Fix accuracy while bounding federated scans (pushdown-friendly stats / bounded sample / clean skip). | OPEN | P1 | M | Eli |
| AGT-1 | Metadata research agent: add MLflow tracing + reduce token consumption, preserving effectiveness. | OPEN | P2 | M | Eli |
| SLG-1 | Semantic-layer knowledge-graph de-dup correctness — graph node dedup currently keys on node_id = definition_id+name (api_server.py:15438), NOT source/filter; fix so same-name/different-expression measures are distinguished and identical measures across defs can merge. | OPEN | P2 | M | Eli |
| UX-2 | Button tooltips/explanations (e.g. the Sync button) across the app UI. | OPEN | P3 | S | Eli |

---

## 9. Lakebase / GraphRAG Graph Sync (opt-in)

**What happens today (documented 2026-08-16).** Lakebase (managed Postgres) is an **opt-in
accelerator** for the exploration agents' graph queries — NOT part of the default deploy path, and
the app/graph work fully on UC Delta tables without it.

- **Deploy does NOT provision a Lakebase instance.** `bundle deploy` ships only the
  `sync_graph_lakebase` job (+ `lakebase_job_cluster`). There is no `database_instance` resource in
  the bundle and no post-deploy script creates one. `notebooks/sync_graph_to_lakebase.py` calls
  `create_database_catalog()` / `create_synced_database_table()` against a **pre-existing** instance
  (default name `dbxmetagen`, `resources/app_variables.yml`) — never `create_database_instance()`.
- **"Sync to Lakebase" (SyncOps) flow:** `SyncOps.jsx` → `runJob('sync_graph_lakebase')` →
  `/api/jobs/run` → the job → the notebook, which (1) enables CDF on `graph_nodes`/`graph_edges`,
  (2) creates the Lakebase catalog in the existing instance, (3) creates SNAPSHOT synced tables.
- **Preconditions (silent failures if unmet):** the Lakebase instance must already exist; the
  analytics pipeline must have produced `graph_nodes`/`graph_edges`; the app must be attached to a
  Lakebase database resource (`PGHOST` set → `/api/config` `lakebase_configured=true`).

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| LB-1 | **`lakebase_database_name` unwired (confirmed bug).** The bundle var exists (`resources/app_variables.yml`, default `knowledge_base`) but `sync_graph_lakebase.job.yml` never declares/passes it, so `sync_graph_to_lakebase.py` falls back to its hardcoded widget default `databricks_postgres` (line ~18) — the configured value is silently ignored, and the two defaults contradict. Works on a stock instance (has `databricks_postgres`) but breaks if the target PG database differs. **Fix:** declare `lakebase_database_name` as a job parameter + pass it in `base_parameters`; reconcile to ONE default (recommend `databricks_postgres`, the built-in DB, overridable). | OPEN | P2 | S | Lakebase trace (Eli) |
| LB-2 | **Opt-in gating + UX.** UI now greys the "Lakebase Sync" button + input until `lakebase_configured` (PGHOST) is true, with a tooltip/note that deploy does not create the instance (`SyncOps.jsx`). `example.env` documents the opt-in provisioning steps. **Remaining:** add a fast-fail in the notebook that emits a clear "Lakebase instance `<name>` not found — provision it first" message instead of a raw SDK error; consider documenting the provisioning steps in a user-facing doc. | PARTIAL (UI greying + docs done 2026-08-16) | P3 | S | Lakebase trace (Eli) |
| LB-3 | **Optional: deploy-time provisioning path.** Decide whether to offer a guided/DAB provisioning of the Lakebase instance (vs. keep it fully manual/opt-in). Product decision, not currently planned. | OPEN | P3 | M | Lakebase trace (Eli) |

**Files:** `resources/jobs/sync_graph_lakebase.job.yml`, `notebooks/sync_graph_to_lakebase.py`,
`resources/app_variables.yml`, `apps/dbxmetagen-app/app/src/components/SyncOps.jsx`, `example.env`.

---

## 10. Deployment Config Hygiene — THIS RELEASE (deployment must work)

Top-priority, from the 2026-08-16 config/env audit (`variables.yml`, `variables.advanced.yml`,
`resources/app_variables.yml`, `resources/apps/dbxmetagen_app.yml` config.env, `app.yaml`,
`variable-overrides*.example.json`, `example.env`, bundled `src/dbxmetagen/variables*.yml`).
**Good news first:** `app.yaml` carries only the launch command (no `env:` — config.env is the sole
source, zero duplication there); the only `.template` (`requirements.txt.template`) is LIVE
(`build_artifacts.sh:77`); the bundled `src/dbxmetagen/variables*.yml` copies are currently identical
to root; all `variable-overrides*.example.json` keys map to real declared variables; `example.env`
has no stale/removed references. The issues below are the real ones.

| ID | Item | Status | Priority | Effort | Source |
|----|------|--------|----------|--------|--------|
| DP-1 | **config.env hardcodes model/node_type — bundle-var overrides silently ignored (CONFIRMED).** `resources/apps/dbxmetagen_app.yml` sets `GRAPHRAG_MODEL` (L20) and `LLM_MODEL` (L62) to the literal `"databricks-claude-sonnet-4-6"` and `NODE_TYPE` (L22) to `"i3.2xlarge"`, while sibling env vars use `${var.*}`. Overriding `var.model` / `var.node_type` (node_type is offered in `variable-overrides.example.json`) does NOT reach the app — it keeps the hardcoded value. **Fix:** `value: "${var.model}"` / `"${var.node_type}"` (confirm `var.model` exists in variables.yml). **DONE 2026-08-16:** `LLM_MODEL` now uses `${var.model}` (functional); vestigial `GRAPHRAG_MODEL`/`NODE_TYPE` also point at vars. | DONE | -- | S | Config audit (Eli) |
| DP-2 | **No fail-fast for required vars; `catalog_name` default is the string `"None"` (CONFIRMED).** `variables.yml:4` `default: None` (unquoted → literal `"None"`), which violates the field's own "cannot be none/null/empty" rule; `warehouse_id` default `""` (L285). A bare/mis-overridden deploy SUCCEEDS then fails at runtime with a confusing "catalog not found: None" / empty-warehouse error instead of a clear message. **Fix:** validate required vars at app startup (and/or a preflight) — error clearly if `catalog_name ∈ {None, none, null, ""}` or warehouse unset; consider normalizing the sentinel to `""`. Makes deployment reliably work-or-clearly-fail. **DONE 2026-08-16:** `catalog_name` default `None`→`""`; `_compute_config_errors` surfaces `config_valid`/`config_errors` via `/api/config` → blocking UI banner (`App.jsx`) + startup ERROR log; unit-tested. | DONE | -- | S | Config audit (Eli) |
| DP-3 | **Orphaned legacy app `apps/uc-metadata-assistant/` — DECISION: KEEP (won't fix).** Confirmed dead (last touched 2026-03-14; NOT wired into the bundle — databricks.yml syncs only `apps/dbxmetagen-app/app/**` + `resources/apps/*.yml`; referenced only by `apps/readme.md` + `tests/test_merged_functionality.py`). It does NOT deploy and does NOT affect dbxmetagen-app deployment, so per Eli (2026-08-16) we intentionally leave it in place rather than remove it. Noted here + in `apps/readme.md` so it isn't mistaken for live. | WON'T FIX | -- | -- | Config audit (Eli) |
| DP-4 | **Duplicate `deploying_user` declaration.** Declared in BOTH `variables.yml:241` and `resources/app_variables.yml:2`, both defaulting to `${workspace.current_user.userName}` ("must match" by comment only). Can diverge if one is hardcoded. **Fix:** de-dupe to one declaration, or document the intentional split. | OPEN | P3 | S | Config audit (Eli) |
| DP-5 | **Manual sync of bundled `src/dbxmetagen/variables*.yml`.** Copies are identical to root TODAY, but sync is a manual `cp` (CLAUDE.md) — silent drift risk for wheel-backfill. **Fix:** add a test/build-hook assert that root == bundled copies. | OPEN | P3 | S | Config audit (Eli) |
| DP-6 | **Doc gap:** the cluster-policy "override the whole cluster block, not bare `policy_id`" gotcha is documented in `databricks.yml`/`variables.yml`/`variable-overrides.example.json` but NOT in `example.env`. Add a one-line pointer. | OPEN | P3 | S | Config audit (Eli) |
| DP-7 | **UV / pip proxy + private-index support (CONFIRMED GAP — required).** `scripts/build_artifacts.sh` runs bare `uv build` with NO passthrough of `HTTP_PROXY`/`HTTPS_PROXY`/`UV_INDEX_URL`/`PIP_INDEX_URL`; README documents a private index ONLY for Databricks-internal laptops and tells external customers to use public PyPI. Air-gapped / proxy-restricted customers have no supported path for (a) the wheel build hook AND (b) the app's runtime `pip install -r requirements.txt` on Databricks Apps. **Fix:** pass through proxy/index env in `build_artifacts.sh`; document a supported `UV_INDEX_URL`/proxy setup for external customers; decide + document the app-runtime index mechanism (requirements.txt `--index-url`/`--extra-index-url` or app config). **DONE 2026-08-16 (docs-only — NO code change needed):** verified the build hook already passes `UV_INDEX_URL`/`UV_NATIVE_TLS`/`HTTP(S)_PROXY` through to `uv build` (env not scrubbed); README + example.env now document that for external customers, plus the platform-managed app-runtime `pip install` caveat (workspace mirror or `requirements.txt.template` edit). | DONE | -- | S | Deploy audit (Eli) |
| DP-8 | **`app_display_name` regression vs main.** Settable on main (`app.yaml.template` sed), NOT settable via the one override file on the branch — DAB SDK strips empty strings (`omitempty`) so `config.env` intentionally omits `APP_DISPLAY_NAME` (dbxmetagen_app.yml L75-80); the only way to set it is hand-editing that committed YAML. Cosmetic (app reads `os.environ.get("APP_DISPLAY_NAME","")`), but it IS a parity loss the "all main vars settable" bar flags. **Fix:** make it settable from the single override file (e.g. a bundle var with a non-empty default). **DONE 2026-08-16:** added `app_display_name` var (default `"dbxmetagen"`) + `APP_DISPLAY_NAME`→`${var.app_display_name}`; now overridable via variable-overrides.json (must be non-empty). | DONE | -- | S | Config audit (Eli) |
| DP-9 | **Deploy-doc accuracy (simple instructions that work).** (a) README claims "example.env documents every available variable" — no longer true (example.env is now instructional). (b) `model` is overridable (bundle var, `${var.model}`) but absent from both `variable-overrides*.example.json` and example.env — undiscoverable (pairs with DP-1 making the APP honor it). (c) The BASIC `variable-overrides.example.json` doesn't state the required `.databricks/bundle/<target>/` load path (only README/example.env/advanced do). **Fix:** correct the README claim; add `model` (+ note app-hardcoding until DP-1) to the example + example.env; add the load-path line to the basic example. **DONE 2026-08-16:** README claim corrected + a bold first-deploy callout added; `model` added to both example JSONs + example.env; load-path `_comment` added to the basic example; `example.env` marked documentation-only. Also fixed a PRE-EXISTING red test — `test_deployment_wiring` now ignores `_comment*` doc keys. | DONE | -- | S | Deploy audit (Eli) |
| DP-10 | **Legacy `{target}.env` flow kept fully working (backward compat).** `deploy.sh` reframed from "DEPRECATED" to "legacy — fully supported": if a `{target}.env` is present it is sourced and its scalar values forwarded as `--var` (catalog/schema/warehouse/vs_endpoint/node_type/budget_policy/enable_obo/app_name/app_name_suffix/app_display_name/model), so existing customers deploy with NO migration. Warns on the 3 knobs that changed shape (`policy_id`→cluster block; `spn_id`→`run_as`; `permission_groups/users`→`app_permissions`). OBO scopes are declared by default now (see DP-12), so deploy.sh does no scope handling — `enable_obo` just flips the runtime principal. Also restored the pip→uv proxy bridge the old script had (existing proxy customers keep working). `grant_app_permissions.sh` already honors the exported env vars, so grants resolve too. Validated under macOS bash 3.2. | DONE | -- | S | Eli (legacy support) |
| DP-11 | **Domain-only customers can pick a standalone domain taxonomy in the Industry Ontology dropdown (UI).** Backend+library already supported domain prediction from a standalone `domain_config` (no ontology; `domain_classifier.load_domain_config(config_path=…)`), but the app only surfaced domain configs when a CUSTOM ontology was selected. Merged the standalone domain taxonomies into the main ontology `<select>` as a "Domain taxonomies (domain classification only)" optgroup — a `domain:`-prefixed option sets `domain_config` and clears `ontology_bundle`, so a customer who needs domain classification but not an ontology selects their custom domain YAML directly. `BatchJobs.jsx` + dist rebuild. | DONE | -- | S | Eli (domain UX) |
| DP-12 | **`user_api_scopes` declared on EVERY deploy (decoupled from `enable_obo`).** Per Eli: declaring scopes is about capability; `enable_obo` is a RUNTIME switch for which principal (user token vs app SP) makes calls, and must not gate the declaration. Regression from main: the consolidation moved OBO scope injection out of `deploy.sh` (imperative) into a var defaulting to `[]`, so OBO users had to re-enumerate scopes. Fix: default `user_api_scopes` (`app_variables.yml`) to the full set across main's paths — `files.files`, `serving.serving-endpoints`, `sql.statement-execution`, `dashboards.genie` (app actively uses the first 3; serving is parity/future, override to drop). Confirmed via a full main scan that these are the ONLY scopes declared anywhere (no iam/workspace/clusters/vectorsearch). Declaring scopes requires the (GA) Apps user-token-passthrough feature; override `user_api_scopes=[]` to opt out where a workspace lacks it. Removed the now-redundant deploy.sh injection. Files: `app_variables.yml`, `dbxmetagen_app.yml`, `deploy.sh`, `CONFIGURATION.md`, `PERMISSIONS.md`, `README.md`, `example.env`, `variable-overrides.advanced.example.json`, `MANUAL_DEPLOYMENT.md`. | DONE | -- | S | Eli (OBO scopes) |
| DP-13 | **Workspace-UI deploy: app env not applied (compute starts, but CATALOG_NAME/WAREHOUSE_ID missing) — TOP REMAINING RELEASE RISK.** The consolidation (`550f019`) moved app env out of `app.yaml` (now command-only) into bundle `config.env`; the CLI flow (`bundle deploy` + `bundle run`) applies it, but the UI deploy/restart path appears to read `app.yaml` (no env) — observed by Eli across agentworks + this project ("lifecycle starts compute but doesn't pull env"). Mechanism UNCONFIRMED (repo comments + a generic docs line say config.env applies on all paths; empirical evidence says not). **Diagnostic-first** (plan Part 1): compare env after bundle-editor Deploy vs Apps-page Deploy vs restart, via `GET /api/config` (`catalog_name`/`config_valid`), `GET /api/jobs/health` (`known_job_ids_configured`), and the DP-2 banner. **Contingent fix:** (i/ii) generate `app.yaml`'s `env:` at deploy in `scripts/build_artifacts.sh` from `variable-overrides.json` scalars so the app source carries env on every path/restart — job IDs omitted (the `ws.jobs.list()` fallback at `api_server.py:1174` covers them); OR (flow) enforce bundle-editor Deploy → Start only (never Apps-page raw Deploy) + docs/guard. DP-2 banner is the built-in early-warning. | OPEN | **P1 (this release — UI deploy blocker)** | M | Eli (UI deploy env) |

### Manual UAT deploys required before shipping (DP-UAT — the confidence gate)

Do these on a real workspace (DMVM) before release; each is a distinct path a customer will hit. This is the top-priority verification for a safe deploy:

1. **Fresh-clone CLI deploy (happy path):** clone → create `.databricks/bundle/<target>/variable-overrides.json` from the example (catalog/schema/warehouse only) → `databricks bundle deploy` → `bundle run dbxmetagen_app` → `grant_app_permissions.sh` → app starts healthy, one metadata-gen job runs. Confirms the "touch one file, deploy" story end-to-end with NO npm locally.
2. **Workspace-UI deploy (NOT web terminal):** UI Deploy button → verify the `artifacts.build` hook has `uv`+Python 3.11 in the UI build env and produces the wheel (UNVERIFIED today) → Apps > Deploy + Start → run the manual UC-grant SQL (§7) + create/permission the VS endpoint (§8) per `MANUAL_DEPLOYMENT.md`. Confirms the CLI-less path.
3. **Backward-compat over a `main`-created schema:** deploy the branch against a schema first generated on `main`; app + one pipeline pass with no missing-column errors (from the Part-1 smoke test).
4. **Override coverage:** a deploy that sets node_type, the whole `metadata_job_cluster` block WITH a `policy_id`, `budget_policy_id`, `run_as` SP, `app_permissions`, and `enable_obo`+`user_api_scopes` — confirm each takes effect (jobs on the policy/cluster, app access, OBO SQL works). Verifies no config-parity regression and the cluster-policy full-block path on BOTH engines.
5. **Model override (after DP-1):** override `var.model`; confirm BOTH jobs and the app use the new model (guards the DP-1 fix).
6. **Proxy/private-index deploy (after DP-7):** on a proxy-restricted setup, confirm the build hook + app runtime install resolve through the configured index.
7. **Multi-target one-workspace:** deploy two targets with distinct `app_name_suffix`; confirm two apps coexist (no last-deploy-wins clobber).
8. **Legacy `{target}.env` flow (DP-10):** with an existing `dev.env` (catalog/schema/warehouse + a couple optionals) and NO `variable-overrides.json`, run `./deploy.sh -t dev -p <profile>` → confirm the values are forwarded as `--var`, the app resolves them, and `grant_app_permissions.sh` grants against the right catalog/schema. Also confirm the pip→uv proxy bridge picks up a configured pip index.

**This-release cut (top priority — deployment must work):** DP-1, DP-2, DP-7, DP-8, DP-9 are **DONE
(implemented + tested 2026-08-16)** — pending only a frontend `dist/` rebuild (done) and the **DP-UAT
manual deploy matrix**, which is now the sole remaining gate. DP-3 is intentionally KEPT (documented dead
code, per Eli). DP-4/5/6 are low-risk cleanups that can follow. **Confirmed already-good (no action):** one-file
override story (`.databricks/bundle/<target>/variable-overrides.json`), app.yaml↔config.env (no dup),
committed `dist/` (no local npm), `deploy.sh` deprecated-but-working shim, workspace-UI deploy path
documented in `MANUAL_DEPLOYMENT.md`, and full core-variable parity with main.

---

## Summary by Status

| Status | Count |
|--------|-------|
| DONE | 43 |
| PARTIAL | 3 |
| OPEN | 58 |
| DEFERRED | 10 |
| KILLED | 2 |

> R3 (batch DESCRIBE EXTENDED) closed DONE 2026-08-16 — **no open P0 items remain.**
> 2026-08-16 audit additions: R3a (federation guard, P3); LB-1/2/3 (Lakebase, §9); DP-1..DP-9 +
> DP-UAT (deployment config hygiene, §10). **DP-1/2/7/8/9 implemented + tested 2026-08-16**; DP-3 is
> WON'T FIX / kept; DP-4/5/6 remain P3; DP-UAT manual deploy matrix is the remaining release gate.
> Counts are approximate.

> +7 OPEN in the 2026-08 customer-driven pass: ON-22 + EN-1..EN-6 (section 8). The three reported
> bugs and backward-compat were verified already-DONE (see the section 8 verification note).

## Recommended Implementation Order

**Wave 1 -- P0 (ship-blockers):**
1. ~~ON-7: Property classification test coverage~~ -- PARTIAL (unit coverage added, integrated flow test remains P1)
2. ~~GN-1 through GN-4: Genie join reliability fixes~~ -- DONE
3. ~~R1: Vector Search ANN for similarity~~ -- DONE (use_ann=True default, cross-join fallback, method telemetry)
4. ~~R3: Batch DESCRIBE EXTENDED (S)~~ -- DONE (2026-08-16): two-tier info_schema batch + concurrent
   stats; federation-safe. **No open P0 items remain.**

**Wave 2 -- P1 (next sprint):**
5. ~~ON-4: Remove legacy `link` SQL filter~~ -- DONE
6. ~~ON-5: Auto-generate inverse edges~~ -- DONE
7. ~~ON-6: Bundle version check for incremental~~ -- DONE
8. ~~ON-8: Conformance validation~~ -- DONE
9. ~~MG-1: Chat client hard-fail path~~ -- DONE
10. MG-13: Tighten PI prompt Presidio deference rule (S) -- highest-impact, lowest-risk PI precision fix
11. MG-12: Add negative few-shot example for clinical vocabulary (S) -- reinforces MG-13
12. MG-2: PII column-to-table rollup (M)
13. MG-5: Day-2 re-run lifecycle (M)
12. DE-2: Parallelize DDL execution (M)
13. DE-3: Remove redundant materializations (S)
14. DE-4: FK prediction parallelization (L)
15. R4-R6: Semantic layer pagination, ontology streaming, multi-task docs (M+M+S)
16. AQ-1, AQ-2: Batch column + table classification (M+M)

**Wave 3 -- P2 (planned):**
17. MG-14: Tighten Presidio diagnosis_code regex (S) -- reduces false positive inputs to LLM
18. MG-15: Preserve `type` field instead of collapsing to `protected` (S) -- enables downstream filtering
19. MG-16: PI confidence threshold gating (S) -- discards low-confidence noise
20. MG-3, MG-4, MG-6-MG-10: Remaining metadata robustness items
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

**Enterprise & Access (customer-driven, 2026-08) — sequencing:**
0. Ship the current feature branch to the customer (resolves the 3 bugs; backward-compat verified — run the smoke test first).
1. EN-4 + EN-5 together (shared role/write-gate plumbing).
2. EN-1 (schema selection, high customer value, L) → then EN-2 design spike.
3. EN-3 (external context) and ON-22 (table-scoped ontology relationship reads) in parallel — independent.
4. EN-6 production-readiness hardening as an ongoing track alongside the above.

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
