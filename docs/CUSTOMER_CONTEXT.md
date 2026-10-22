# Customer Context

Domain knowledge — glossaries, naming conventions, business rules — injected into generation
prompts, scoped by **catalog / schema / table / pattern**. Entries live in the `customer_context`
Delta table and are managed from the app's **Customer Context** tab or seeded from YAML.

When a table matches several scopes, the entries are combined broadest-first (catalog → schema →
pattern → table) up to a word budget; under budget pressure the most-specific entry is kept.

## Ingestion lanes

Uploads are routed by file type. The structured lane is deterministic; the others use AI.

| Lane | File types | What happens |
|------|-----------|--------------|
| **Structured** | `.yaml` / `.json` with a `contexts:` list | Parsed directly into entries — no LLM. Each carries its own scope. Authoritative: re-uploading a scope replaces it (the previous entry is kept as inactive history). |
| **Document** | `.txt` `.md` `.pdf` `.docx` `.pptx` `.csv` `.xlsx` | Attached to one scope you pick. Long documents are summarized to a concise entry (`ai_extract`); short ones are kept verbatim. Binary files are parsed server-side with `ai_parse_document` (needs a Pro/Serverless SQL warehouse); spreadsheets are serialized to text first. |
| **Decompose** | same as Document | Splits one document into many entries, each assigned to a **real** table under the catalog/schema you pick (names are validated — invented tables are never used). |

**Spreadsheets and PDFs carry free-form context**, so they go through the AI lane. Only already-structured
`.yaml` / `.json` take the deterministic lane.

Every upload (context, acronyms, descriptions, decompose) can come from **your computer** or **from a
UC Volume** — toggle *From my computer* / *From a volume* and browse to the file. Volume files are read
with the app's access: the **logged-in user's identity under OBO**, otherwise the **app service principal**
(which the grants script gives `READ VOLUME` on the deployment schema). Volumes elsewhere require that
identity to have `READ VOLUME`.

### Decompose scope

- Pick **catalog + schema** (leave Table blank) → pieces spread across that schema's real tables;
  anything that can't be placed falls back to your scope.
- Pick a **table** → *every* piece is pinned to that table. An explicit table choice is honored
  exactly; the split never wanders to a sibling table.

## Overwrite protection

A manual entry or a single-scope document upload that lands on a scope which **already has active
context** does not overwrite silently. The app shows the existing vs. incoming text and you choose:

| Choice | Result |
|--------|--------|
| **Append** | Keep existing, add the new text as its own entry. |
| **Combine** | LLM-merge existing + incoming into one entry; the originals are kept as inactive history. |
| **Replace** | Retire the existing entries (kept as history), write the new one. |

A clean scope writes immediately. Editing an existing entry updates it in place. Decompose always
appends (each piece is keyed by its content, so re-runs don't duplicate).

## Acronyms & descriptions

Two scope-aware inputs on the same tab, uploaded as `.xlsx` / `.csv` / `.json` / `.yaml` with
tolerant column names (e.g. *Abbreviation* / *Acronym*; *Column_Name*; *Comment* / *Description*):

- **Acronyms** — abbreviations the model expands, layered over the built-in global list (schema
  scope wins over catalog).
- **Descriptions** — column descriptions applied **verbatim** (the model is skipped for those
  columns), layered under any `metadata_overrides.csv`.

## Retrieval modes

How matching context is selected per table, set by `customer_context_retrieval`:

| Mode | Behavior |
|------|----------|
| `exact` *(default)* | All matching entries, combined up to the word budget. In memory. |
| `ranked` | Within the same scope filter, keep the `customer_context_top_k` (default 8) entries most lexically relevant to the table; catalog-level directives always kept. In memory. |
| `vector` | **Scalable, opt-in.** Per-table scope-prefiltered hybrid Vector Search — does not load the whole table into memory; falls back to a bounded per-table read if the index is unavailable. Requires `enable_context_vector_index` and a built index. See **[Vector index](CUSTOMER_CONTEXT_VECTOR_INDEX.md)**. |

The scope hierarchy is the hard filter in all three; default (`exact`/`ranked`) never touches
Vector Search.

> **Status.** The retrieval mode is currently a **deploy-level default** (the bundled
> `variables.yml`), not a per-run option, and the `vector` mode is **not yet wired into the
> generation jobs** — so today generation effectively uses `exact`. Delivering `vector` (and a
> per-run toggle) to metadata generation *and* semantic-layer generation is tracked in
> [issue #260](https://github.com/databricks-industry-solutions/dbxmetagen/issues/260). The
> library path and the index already exist; enrichment is best-effort and never blocks a run.

## Limits & guardrails

| Limit | Value | Setting |
|-------|-------|---------|
| Upload size (any file) | 25 MB | `max_upload_mb` |
| Entry length | 500 words | — |
| Decompose output | ≤200 entries, ≤150 words each | — |
| Acronyms / descriptions rows | 1,000 per upload | — |
| Document surface sent to the LLM | truncated (~100–200K chars) | — |

Oversized files are rejected before they are read into memory — split them by scope. These are
guardrails against breaking/expensive uploads, not limits normal glossaries approach.

## Uploading context — workflow

1. Open the app's **Customer Context** tab.
2. Choose the input: **Context** (free-form), **Acronyms**, or **Descriptions** (verbatim column comments).
3. Pick the source — **From my computer** or **From a volume** (browse to the file).
4. Set the **scope** so it matches the tables you'll generate on:
   - *Context document* → attach to one scope, or **decompose** (split across the schema's tables, or pin to one table).
   - *Acronyms* → schema or catalog.
   - *Descriptions* → catalog + schema (per-row `Table_Name` wins).
5. On a scope collision you'll be asked to **append / combine / replace** (see above).
6. Use **Preview Resolution** on a table to confirm exactly what it will receive.
7. Generate metadata with **Use Customer Context** enabled.

## Guidelines

- **Scope to match.** Context only applies to a table whose catalog/schema/pattern/table it is scoped to.
  To affect `cat.sch.tbl`, scope at `cat`, `cat.sch`, a matching pattern, or the table itself.
- **Keep entries concise.** Each entry caps at 500 words and is injected into prompts; prefer short,
  factual notes. For large/table-specific knowledge, prefer decompose (per-table) over one huge blob.
- **Pick the right lane.** Clean, already-structured entries → `.yaml`/`.json`. Everything free-form
  (prose, glossaries, spreadsheets, PDFs) → the document/decompose lane.
- **Decompose placement.** Leave *Table* blank to spread across a schema's tables; set *Table* to pin
  every piece to that one table.
- **Descriptions are verbatim.** Use the Descriptions input when you want exact column comments and the
  LLM skipped for those columns.
- **Split very large files** by schema rather than uploading one enormous file (25 MB hard cap).

## Deploy options

Customer context works out of the box. Relevant settings (bundle variables → app env):

| Setting | Default | Purpose |
|---------|---------|---------|
| `max_upload_mb` | `25` | Hard ceiling on any single upload (guardrail against egregious files). |
| `enable_context_vector_index` | `false` | Opt-in: build/sync the semantic index from the app. See [Vector index](CUSTOMER_CONTEXT_VECTOR_INDEX.md). |
| `vs_endpoint_name` | `dbxmetagen-vs` | Vector Search endpoint used if the index is enabled. |
| `embedding_model` | `databricks-bge-large-en` | Managed-embedding endpoint for the index. |
| `enable_obo` | `false` | When true, volume reads and SQL run as the logged-in user; otherwise the app service principal. |
| `default_view_mode` | `advanced` | Initial UI; `simplified` hides advanced tools. Per-user, remembered in the browser. |

`scripts/grant_app_permissions.sh` grants the app service principal `READ VOLUME` (and the Vector Search
endpoint) on the deployment schema. Reading context files from **other** volumes requires the acting
identity (app SP, or the user under OBO) to have `READ VOLUME` there.

## Where to look

- **App:** the **Customer Context** tab (entries, acronyms, descriptions, file upload, decompose,
  vector-index sync) and **Preview Resolution** to see exactly what a table would receive.
- **Seeding from YAML:** point `main()` at a folder of `*.yaml` files (`customer_context_yaml_dir`);
  see `examples/customer_context.yaml`.
- **Vector index deep-dive:** [CUSTOMER_CONTEXT_VECTOR_INDEX.md](CUSTOMER_CONTEXT_VECTOR_INDEX.md).
