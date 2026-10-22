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

## Where to look

- **App:** the **Customer Context** tab (entries, acronyms, descriptions, file upload, decompose,
  vector-index sync) and **Preview Resolution** to see exactly what a table would receive.
- **Seeding from YAML:** point `main()` at a folder of `*.yaml` files (`customer_context_yaml_dir`);
  see `examples/customer_context.yaml`.
- **Vector index deep-dive:** [CUSTOMER_CONTEXT_VECTOR_INDEX.md](CUSTOMER_CONTEXT_VECTOR_INDEX.md).
