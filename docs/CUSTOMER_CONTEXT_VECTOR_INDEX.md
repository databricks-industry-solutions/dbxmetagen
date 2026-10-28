# Customer-context Vector Search index (opt-in)

A **Delta-Sync Vector Search index over the `customer_context` table** with managed embeddings
on the `context_text` column, provisioned on the existing `dbxmetagen-vs` endpoint. It enables
**semantic** retrieval of customer context — "find the context most meaningfully related to this
table/question" — on top of the deterministic scope hierarchy and the lexical `ranked` thinning.

It is **off by default** and **opt-in**. Turning it on, building it, and syncing it has **no effect
on metadata generation**.

---

## The hard guarantees

These were explicit design constraints, and the implementation enforces them:

1. **Opt-in.** Every endpoint returns `503` unless `enable_context_vector_index=true`
   (`ENABLE_CONTEXT_VECTOR_INDEX` env). The UI panel only appears when it is enabled.
2. **Generation is untouched.** The batch metadata pipeline (`comment` / `pi` / `domain` modes)
   resolves customer context **exactly as before** — from the in-memory cache using the scope
   hierarchy (`exact`) or the optional lexical `ranked` thinning. It **never** calls Vector Search.
   There is no new import, no per-table VS query, and no network dependency anywhere on the
   generation hot path. A VS outage, a missing index, or a half-built index cannot slow down or
   break a generation run.
3. **Non-blocking build.** Sync kicks off create + sync and **returns immediately**; the index
   provisions asynchronously. Nothing waits for readiness in-request. The app polls a status
   endpoint. First build can take a few minutes; that time is never charged to a user request or
   a generation job.
4. **Best-effort.** Endpoint/index hiccups return a clear status or a typed error, never a 500
   cascade. If the index is not built yet, search returns a `409` telling the user to sync first.

---

## How to enable

1. Set the bundle variable (default `false`):
   ```yaml
   # variables.yml  (or variable-overrides.json / --var / BUNDLE_VAR_enable_context_vector_index)
   enable_context_vector_index:
     default: true
   ```
   The embedding endpoint follows `embedding_model` (default `databricks-bge-large-en`); the VS
   endpoint follows `vs_endpoint_name` (default `dbxmetagen-vs`, auto-created by the grants script).
2. Redeploy the app (`./deploy.sh -t <target> -p <profile>`). The app SP already has VS access via
   `scripts/grant_app_permissions.sh`.
3. In the app, open **Customer Context** → the **Semantic context index** panel → **Sync to vector
   index**. Watch the status line until `Index: ready`. Click sync again after you add or edit
   entries (the Delta-Sync pipeline is `TRIGGERED`).

Enabling the feature also flips Change Data Feed on `customer_context` (a metadata-only property,
set lazily on first sync) so the Delta-Sync index can pick up incremental changes.

---

## Endpoints

| Method + path | Purpose |
|---|---|
| `GET  /api/customer-context/vector/status` | Endpoint + index state, indexed-row count, active-entry count. Returns `{enabled:false}` when the feature is off. |
| `POST /api/customer-context/vector/sync`   | Provision (if needed) + trigger a sync. Non-blocking; returns a status + `steps`. |
| `POST /api/customer-context/vector/search` | Semantic search over **active** entries. Body `{query, scope_type?, num_results?}`. Always filters `active = TRUE`. |

The index: primary key `context_id`, embedding source `context_text`, synced columns
`context_id, context_text, scope, scope_type, context_label, priority, active`.

---

## How this context can be used in later steps

The index is a **shared semantic asset**. Once it exists, any step that benefits from "the most
relevant human-authored context" can query it — without each step re-implementing matching, and
without coupling to the generation pipeline.

1. **App — semantic resolve-preview.** Today the *Preview Resolution* panel shows what the scope
   hierarchy injects for a table. The index lets the app additionally show the context that is
   *semantically* closest to a table or a free-text question, surfacing relevant notes that live
   under a different (but related) scope.

2. **Agents / RAG (`agent/`).** The deep-analysis and analyst agents already retrieve from the
   `metadata_documents` index. The customer-context index is a second, curated retriever: when a
   user asks the metadata agent about a table or domain, the agent can ground its answer in the
   organization's own authored context (definitions, caveats, SME notes), not just generated
   metadata. Point a retriever tool at `customer_context_vs_index` alongside `metadata_vs_index`.

3. **Genie space building (`src/dbxmetagen/genie/`).** Genie context assembly can pull the
   top semantically-relevant context entries for the tables in a space and fold them into the
   space's instructions / descriptions, so the Genie room inherits the same business language the
   stewards wrote — improving NL-to-SQL grounding.

4. **Downstream analytics pipeline.** Knowledge-base, ontology, and community-summary steps can
   enrich their LLM prompts with the nearest context entries for the tables they are summarizing,
   the same way they already optionally use profiling/lineage context — but now semantically
   retrieved rather than scope-matched only.

5. **Scalable `vector` retrieval mode for generation (opt-in).** This is the point of the index:
   a retrieval path that **scales with the size of the context set**.

   > **Status — not yet wired into generation.** The index and the library retrieval path below
   > exist and are unit-tested, but `customer_context_retrieval` is not yet delivered to the
   > generation jobs as a parameter/widget, so setting it (including via `--var`/`BUNDLE_VAR`) does
   > **not** currently change what a generation run does — it reads the bundled default (`exact`).
   > Wiring this into metadata generation *and* semantic-layer generation, with a per-run toggle and
   > the required `run_as` Vector Search grant, is tracked in
   > [issue #260](https://github.com/databricks-industry-solutions/dbxmetagen/issues/260). The
   > description below is the intended design once wired.

   Set `customer_context_retrieval=vector` (default `exact`). Then, per table being generated:

   ```python
   WorkspaceClient().vector_search_indexes.query_index(
       index_name   = f"{catalog}.{schema}.customer_context_vs_index",
       query_text   = "<table short name> <col1> <col2> ...",   # what is already known
       query_type   = "HYBRID",                                  # semantic ANN + BM25 keyword
       columns      = ["context_id","context_text","scope","scope_type","context_label","priority"],
       num_results  = customer_context_top_k,
       filters_json = '{"scope": ["cat","cat.sch","cat.sch.tbl"], "active": true}',   # PREFILTER
   )
   ```

   - The scope hierarchy is pushed into the index as a **prefilter** (`scope IN [...]`), so the
     search is confined to this table's scope server-side and **never scans the whole index**.
   - **HYBRID** ranking catches exact tokens (table/column names, codes, acronyms) via BM25 and
     meaning via ANN.
   - **Wildcard (`pattern`) scopes** can't be an equality prefilter, so the few pattern rows are
     prefetched once (small, bounded) and merged in — the hierarchy stays complete.
   - **No whole-table load.** In `vector` mode the run prefetches **only** the pattern rows, not
     the entire `customer_context` table — the memory/scan bottleneck of `exact`/`ranked` is gone.
   - **Fail-open, two triggers:** if the flag is `exact`/`ranked` → the in-memory path (unchanged);
     if the flag is `vector` **but the index is unavailable** (missing / not-ready / error) → a
     **bounded** per-table SQL read (`scope IN (...) OR scope_type='pattern'`) resolved the normal
     way. A VS problem can never stall or break a generation run.
   - Results flow through the **same** budget/ordering (`_truncate_preserving_specificity`) as every
     other mode — only *retrieval* changed, not *injection*.

   Default runs (`exact`/`ranked`) are byte-for-byte unchanged and never touch Vector Search. The
   seam is `prompts.py:enrich_from_customer_context` → `customer_context.resolve_customer_context_vector`.
   A future per-run UI checkbox (vs the deploy-level `customer_context_retrieval` var) can toggle it
   per generation run by threading the mode as a job parameter.

In every case the index is **read** downstream; it is **built/synced only** from the app's explicit
action (or a future scheduled job mirroring the governed-tags pattern). Generation remains a pure
consumer of the in-memory cache.

---

## Operational notes

- **Cost.** One Delta-Sync index on an endpoint you already run. Embeddings are recomputed only for
  changed rows (CDF + `TRIGGERED` pipeline), and `customer_context` is small (human-authored), so
  this is cheap and bounded.
- **Freshness.** The index reflects the last sync. Click **Sync** after bulk uploads/decompose; a
  scheduled sync job can be added later if desired.
- **Teardown.** Set `enable_context_vector_index=false` and redeploy to hide/disable the feature.
  Delete the index from the VS UI/SDK if you want to reclaim it; the `customer_context` table and
  all generation behavior are unaffected either way.
