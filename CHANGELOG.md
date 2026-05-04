# Changelog

## 0.9.0

### Features

- **FK prediction improvements**: Hard dtype filtering on all candidate generators (excludes float, decimal, boolean, date, timestamp, binary, variant, struct, array, map), PK-match scoring signal, system column exclusion, and name-based candidate fixes
- **Ontology vector retrieval**: Vector Search-backed entity and edge prediction with chunked embedding pipeline and ontology vector index builder
- **Community summaries**: AI-generated domain/subdomain community summaries stored in Delta and indexed for Vector Search enrichment of the deep analysis agent
- **Similarity edges ANN path**: Full ANN similarity via Vector Search with automatic cross-join fallback and method telemetry reporting
- **Customer context enrichment**: Optional customer/industry context injected into LLM prompts for more relevant metadata generation
- **Deep analysis agent quality**: Improved GraphRAG retrieval, tool selection, and answer synthesis; MLflow trace link below agent chat input
- **Genie space builder**: Full-featured in-app Genie builder with 3-phase LLM agent, FK/ontology/MV join merging, schema normalization, deploy retries, and join validation
- **Semantic layer / metric views**: Generate, validate, and apply metric views to Unity Catalog; optional Genie space creation from applied views
- **MCP server setup**: Job and documentation for configuring Unity Catalog MCP servers with IoT example bundle
- **Notebook deployment pipeline**: No-CLI deployment via Databricks notebooks (NB01 jobs/infra, NB02 app/permissions) with `variables.yml` defaults, policy support, and OBO configuration
- **Entity browser**: Entity-first navigation with conformance view in the dashboard
- **Graph explorer improvements**: Subgraph visualization and improved graph data display
- **KB human edit protection**: MERGE guard using `review_updated_at` to preserve human-edited knowledge base entries

### Fixes

- **SSE output sanitization**: All agent answer paths (streaming, meta/irrelevant early returns, quick_answer) now pass through `sanitize_output` for credential redaction
- **Similarity edges telemetry**: `run()` reports the actual method used (`crossjoin`, `ann`, or `crossjoin_fallback`) instead of the requested config flag
- **FK dtype exclusion for parameterized types**: `decimal(38,0)` and similar types correctly normalized via `SPLIT` before exclusion check
- **Version sync**: `__init__.py` version aligned with `pyproject.toml` at 0.9.0
- **Ontology confidence cleanup**: Handles both `confidence` column variants correctly
- **FK DDL and graph edges**: Read from authoritative `fk_predictions` table instead of stale intermediate results
- **Baseline mode routing**: Correct routing for nodata prompt enrichment and validation
- **Lakebase graph sync**: SDK and job configuration fixes for Delta-to-Lakebase replication
- **Ontology bundle entity/edge counts**: Correct display in dashboard

### Documentation

- **Permissions reference** (`docs/PERMISSIONS.md`): Unified doc covering two-identity model (app SPN vs job owner), UC grants, OBO mode, Vector Search, and end-user access
- **Configuration guide** (`docs/CONFIGURATION.md`): Updated for community summaries, Vector Search, Lakebase, OBO, and ontology bundles
- **Domain/ontology architecture** (`docs/DOMAIN_ONTOLOGY_ARCHITECTURE.md`): Explains formal vs custom ontology bundles, domain YAML, and how they interact
- **Manual deployment guide** (`docs/MANUAL_DEPLOYMENT.md`): Added VS endpoint CAN_USE grant for manual deployers
- **MCP servers guide** (`docs/MCP_SERVERS.md`): Setup and configuration for Unity Catalog MCP servers
- **README**: Reorganized with slimmer overview, links to detailed docs, corrected permissions model description

### Infrastructure

- Version bump to 0.9.0 (`pyproject.toml`, `package.json`, `__init__.py`)
- 17 job resources bound to the app (added `setup_mcp_servers_job`)
- Frontend rebuild with React 19 + Vite
- Test suite: 2245 tests passing (4 skipped, 3 xfailed)
