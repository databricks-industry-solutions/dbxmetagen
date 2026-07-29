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
| MG-3 | Hardcoded entity suggestions in semantic layer | OPEN | P2 | S | RC 1.3 |
| MG-4 | Quality metrics exposed to users | OPEN | P2 | L | RC 1.4 |
| MG-5 | Day-2 re-run lifecycle / `_review_status` tracking | OPEN | P1 | M | RC 1.5 |
| MG-6 | Structured output vs regex JSON recovery | OPEN | P2 | M | RC 2.1 |
| MG-7 | Schema-level PII reconciliation via FK graph | OPEN | P2 | M | RC 2.3 |
| MG-8 | Ontology as generation context (inject entity type into prompts) | OPEN | P2 | M | RC 2.4 |
| MG-9 | Audit trail for metadata state transitions | OPEN | P2 | M | RC 2.5 |
| MG-10 | `enrich_from_knowledge_base()` should override stale UC comments | OPEN | P2 | S | apply_ddl analysis |
| MG-11 | `MetadataConfig` schema validation (Pydantic model with field validators) | OPEN | P2 | M | code audit |
| MG-12 | PI prompt: add negative examples for clinical vocabulary | OPEN | P1 | S | PI precision analysis |
| MG-13 | PI prompt: tighten Presidio deference rule (#10) | OPEN | P1 | S | PI precision analysis |
| MG-14 | Presidio `diagnosis_code` regex too broad -- raise threshold or gate | OPEN | P2 | S | PI precision analysis |
| MG-15 | Preserve `type` field downstream instead of collapsing to `protected` | OPEN | P2 | S | PI precision analysis |
| MG-16 | PI confidence threshold gating (discard low-confidence classifications) | OPEN | P2 | S | PI precision analysis |
| MG-17 | SWIFT/BIC code lost to LOCATION NER collision -> classified None instead of PCI | OPEN | P2 | S | UAT PI scenario |
| MG-18 | UAT PI gold not normalized for equivalent classes (pi/pii, phi/medical_information) | OPEN | P3 | S | UAT PI scenario |

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
| GN-11 | Metric-view model builder reverted saved ERD (recommendation endpoint never overlaid saved `erd_json`); now overlays saved roles/grain/schema_type + clears `_erd_cache` on save | DONE | -- | S | UAT model-builder |

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
| OB-8 | `fk_predictions` MERGE has no delete-by-source path -> a spurious row already persisted is not corrected when a fix stops the pair being regenerated (goes stale). "Sweep stale edges" only cleans `graph_edges`, not `fk_predictions`. | OPEN | P2 | M | UAT FK scenario |
| OB-9 | One-FK-per-child-column resolution: a single child column claiming `is_fk=true` to >1 parent table (polymorphic reference, not a valid SQL FK) keeps only the highest-confidence target; demotes the rest to `is_fk=false`. Fixes multi-primary-table entity-type collisions (e.g. one `Organization`/`Person` type primary for two tables in a schema) that the same-block guard (OB-6) and never-joins veto (OB-7) both miss. | DONE | -- | S | UAT snowflake scenario |
| OB-10 | Standalone `fk_prediction_job` never exposed `sweep_stale_edges` as a job parameter (the notebook widget existed but was unwired), so the job could never clean orphaned `graph_edges` -- a demoted/removed pair lingered as a stale predicted_fk edge. Wired the param through job YAML -> notebook widget -> `merge_edges(sweep_stale=True)`. | DONE | -- | S | UAT snowflake scenario |

### OB-8: `fk_predictions` stale-row cleanup

**Status: OPEN** -- FK predictions are written via an upsert-only MERGE keyed on `(src_column, dst_column)` with no `WHEN NOT MATCHED BY SOURCE ... DELETE`. Once a pair stops being generated (e.g. after OB-6 blocks a cross-schema pair, or the tables leave scope), an already-persisted `is_fk=true` row lingers indefinitely. The OB-7 veto only rewrites a row if a later run regenerates that exact pair. `sweep_stale_edges` does NOT help: it sweeps `graph_edges` scoped to `source_system='fk_predictions'`, a different table -- so the phantom row in `fk_predictions` (which the app FK views and ERD designer read directly) survives, and the two tables can drift out of sync.

**Work:**
- Add a scoped delete path to the `fk_predictions` write: a `WHEN NOT MATCHED BY SOURCE` clause restricted to the run's `table_names` (mirroring the `merge_edges` per-source sweep), gated behind a flag / non-incremental run, and preserving steward-reviewed rows (`review_updated_at IS NOT NULL`).
- Consider a one-off maintenance sweep for existing deployments carrying stale rows.

**Files:** `src/dbxmetagen/fk_prediction.py` (the MERGE + DELETE around the staging view write)

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
| DONE | 36 |
| PARTIAL | 2 |
| OPEN | 46 |
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
