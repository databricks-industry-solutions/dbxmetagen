# Metadata Governance Workflow

dbxmetagen supports multiple operational modes for metadata generation, review, and publication. This document describes the data flow, the available modes, and how human edits are protected.

## Data Flow

```
LLM generation
    |
    v
metadata_generation_log  (append-only Delta table, all runs)
    |
    v
build_knowledge_base / build_column_knowledge_base  (MERGE, latest wins)
    |
    v
table_knowledge_base / column_knowledge_base  (internal source of truth)
    |
    +---> Knowledge Graph (graph_nodes)
    +---> Vector Index (metadata_documents)
    +---> Community Summaries
    +---> Semantic Layer (metric view generation)
    +---> Genie Spaces (context assembly)
    +---> Deep Analysis Agent (via vector search)

Optional (parallel, independent):
    LLM generation --[apply_ddl=true]--> Unity Catalog (COMMENT ON / SET TAGS)
```

Key point: **all downstream consumers read from the knowledge base tables, not from Unity Catalog**. Whether or not DDL is applied to UC has no effect on the vector index, Genie spaces, semantic layer, or the knowledge graph. UC application is an optional step for publishing metadata externally.

## Operational Modes

### Mode 1: Generate and Apply

Set `apply_ddl=true`. Comments are generated, written to the log, and immediately applied to Unity Catalog via `COMMENT ON TABLE/COLUMN` and `SET TAGS`. The analytics pipeline then builds the KB from the log and feeds downstream.

Best for: initial bulk generation where review is not needed.

### Mode 2: Generate Only (No Apply)

Set `apply_ddl=false`. Comments are generated and written to the log but not applied to UC. The analytics pipeline builds the KB from the log and all downstream consumers receive the generated metadata.

Best for: populating the internal metadata layer without touching UC. Useful when UC permissions are restricted or when you want downstream features (vector search, Genie, semantic layer) without committing to UC publication.

### Mode 3: Generate, Review, Then Apply

Set `apply_ddl=false`. Comments are generated and written to the log. A human reviews them in the dashboard UI or via the exported TSV/Excel files. After review, the user can:

- **Apply from the UI**: use the dashboard's apply controls to push approved comments to UC
- **Apply via DDL regenerator**: edit the exported TSV, then run `ddl_regenerator.py` with `review_apply_ddl=true`
- **Set review status**: mark tables as `unreviewed`, `in_review`, or `approved` in the review UI for governance tracking

Best for: enterprise workflows where human sign-off is required before UC publication.

### Mode 4: Generate, Edit in UI, Save to KB

Generate metadata (with or without `apply_ddl`). Then edit comments, domains, or classifications directly in the review UI. Edits are saved to `table_knowledge_base` / `column_knowledge_base` with a `review_updated_at` timestamp. These edits flow downstream immediately (on the next vector index / Genie / semantic layer rebuild) without requiring UC publication.

Best for: iterative refinement of metadata quality. A domain expert corrects LLM-generated descriptions, and the corrections propagate through the entire downstream chain.

## How Human Edits Are Protected

When you edit a comment in the review UI, the KB row's `review_updated_at` is set to `current_timestamp()`. On the next `build_knowledge_base` run, the MERGE compares timestamps:

- If `review_updated_at > source.updated_at`: the human edit is **preserved** (the generation is older than the edit)
- If `review_updated_at IS NULL` or `review_updated_at <= source.updated_at`: the generated value wins (standard behavior)

This means:

- **Routine KB rebuilds** (same generation data) will not overwrite your edits
- **Explicit re-generation** for a table produces a new log row with a newer `updated_at`, which will override the stale human edit on the next KB build
- You do not need to take any special action to protect edits -- the timestamp comparison is automatic

The same protection applies to `comment`, `domain`, and `subdomain` on the table KB, and `comment` and `classification` on the column KB.

## Review Status

The review UI provides a `review_status` field on each table in the KB with three values:

| Status | Meaning |
|--------|---------|
| `unreviewed` | Default. No human has reviewed this table's metadata. |
| `in_review` | A reviewer is actively working on this table. |
| `approved` | A reviewer has signed off on the metadata. |

`review_status` is a governance label for tracking and reporting. It does not affect the MERGE behavior or downstream data flow -- it is informational. The coverage dashboard shows a summary of review progress by status.

## Unity Catalog vs Knowledge Base

| Aspect | Knowledge Base (KB) | Unity Catalog (UC) |
|--------|--------------------|--------------------|
| Role | Internal source of truth for dbxmetagen pipeline | External publication for catalog consumers |
| Written by | `build_knowledge_base` (from log) or UI edits | `apply_ddl` during generation, or manual apply from UI |
| Read by | Vector index, Genie, semantic layer, knowledge graph, community summaries, deep analysis agent | Catalog Explorer, external tools, other teams' SQL |
| Required for downstream | Yes | No |

If you never apply DDL to UC, the entire dbxmetagen pipeline still works. UC application is for making the metadata visible outside of dbxmetagen.

## Re-run Behavior

When you re-run metadata generation for tables that were previously processed:

1. New rows are appended to `metadata_generation_log` (the log is append-only)
2. `build_knowledge_base` takes the latest row per table (by `_created_at`) and MERGEs into the KB
3. If a table had human edits with a newer `review_updated_at`, those edits are preserved
4. If the re-generation produced a newer timestamp, the new generation overrides the old human edit
5. `review_status` is not reset by the MERGE -- it stays at whatever value was set in the UI

To force a fresh start for a specific table: re-generate it (which produces a new log entry with a current timestamp), then rebuild the KB. The new generation will override any prior human edits because its timestamp is newer.
