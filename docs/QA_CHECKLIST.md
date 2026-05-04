# dbxmetagen -- Manual QA & Regression Checklist

Use this checklist when validating a new release before merging to `main`. Copy into an issue or PR description and check items as you go.

---

## 1. Deployment

- [ ] Fresh `./deploy.sh --profile <PROFILE> --target <TARGET>` succeeds
- [ ] Redeploy (second run) succeeds without errors
- [ ] `--permissions` flag grants Unity Catalog permissions for the app service principal
- [ ] `--no-app` flag skips app deployment
- [ ] App starts and is accessible at the deployed URL
- [ ] Version in the app header matches the release version
- [ ] `.whl` artifact uploads correctly and job definitions reference the matching build path

## 2. Core Metadata Generation -- Single Table

- [ ] **Comment** mode generates table and column descriptions
- [ ] **PI** mode identifies PII/PHI/PCI classifications correctly
- [ ] **Domain** mode classifies domain and subdomain
- [ ] **All** mode produces comments, PI, and domain in a single pass
- [ ] Results are written to the `metadata_generation_log` Delta table with the expected schema
- [ ] DDL statements are generated and applied (table + column comments)
- [ ] Tags are applied (PI classifications, domain tags)

## 3. Core Metadata Generation -- Schema Wildcard & Bulk

- [ ] `catalog.schema.*` wildcard expands to all tables in the schema
- [ ] Mixed input (`catalog.schema.table1, catalog.schema2.*`) resolves correctly
- [ ] `columns_per_call` parameter is respected (verify chunk sizes in logs)
- [ ] Concurrent LLM calls execute in parallel for multi-chunk tables
- [ ] Batch DDL application produces fewer statements than individual column DDLs would
- [ ] Incremental mode (`incremental=true`) skips already-processed tables
- [ ] Full-refresh mode (`incremental=false`) reprocesses all tables

## 4. Advanced Metadata Pipeline -- Full Analytics

- [ ] Full analytics pipeline job runs end-to-end successfully
- [ ] Pipeline is blocked in the UI when no ontology bundle is selected
- [ ] Domain-related jobs are blocked when neither ontology nor domain bundle is selected
- [ ] Table filter with literal table names filters correctly
- [ ] Table filter with `catalog.schema.*` wildcards filters correctly
- [ ] Table filter and incremental mode compose as AND conditions (filter does not override incremental)

## 5. Advanced Metadata -- Individual Steps

- [ ] **Build column KB** completes and populates the knowledge base
- [ ] **Build schema KB** completes
- [ ] **Extract extended metadata** runs and produces results
- [ ] **Profiling** runs on filtered or all tables
- [ ] **Knowledge graph** builds with correct node and edge counts
- [ ] **Ontology classification** tags entities correctly
- [ ] **FK prediction** generates candidate foreign keys
- [ ] **Clustering** completes without excessive widget delays
- [ ] **Embeddings / vector index** builds successfully
- [ ] **Similarity edges** are computed
- [ ] **Data quality scores** are computed
- [ ] **Final analysis** notebook completes

## 6. Metric Views & Semantic Layer

- [ ] Metric view generation discovers tables from all source schemas (not just one schema)
- [ ] Generated metric views include `deployed_location` tracking columns
- [ ] `apply_metric_views` deploys to the correct `catalog.schema`
- [ ] `deployed_schema` is derived from the source table's schema, not the top-level config schema
- [ ] Failed or not-queryable metric views are excluded from Genie context
- [ ] Metric view YAML validation passes

## 7. Genie Room Builder

- [ ] Context assembly pulls tables from the knowledge base correctly
- [ ] Only deployed and queryable metric views are included in Genie context
- [ ] SQL examples are generated and syntactically valid
- [ ] Join specs are merged correctly from FK predictions
- [ ] Genie space deploys to the Databricks Genie API without errors
- [ ] No duplicate or stale tables appear in the generated space

## 8. App UI -- Batch Jobs Tab

- [ ] Mode selection works: Comment, PI, Domain, All Three
- [ ] "All Three" mode disables the redundant single-mode run button
- [ ] Ontology bundle dropdown populates with available bundles
- [ ] Domain bundle dropdown works independently of ontology bundle
- [ ] v1 ontology bundles load and display correctly
- [ ] v2 ontology bundles load and display correctly
- [ ] Standalone domain selection (no ontology bundle) works
- [ ] `columns_per_call` input is visible and defaults to the configured value
- [ ] `columns_per_call` value flows through to submitted job parameters
- [ ] Job submission triggers correctly and returns a task ID
- [ ] Job polling updates status until completion
- [ ] Error states display meaningful, user-friendly messages

## 9. App UI -- Coverage Tab

- [ ] Loads without errors
- [ ] Summary statistics are clear and not overwhelming
- [ ] Information hierarchy is obvious (high-level first, details on demand)
- [ ] Empty state is handled gracefully when no tables have been processed

## 10. App UI -- Ontology Tab

- [ ] Entity counts match expected values per bundle
- [ ] Edge counts match expected values per bundle
- [ ] Stale search index warning is clear and informative
- [ ] Entity browser displays entities with correct properties

## 11. App UI -- General UX

- [ ] App loads without JavaScript console errors
- [ ] Navigation between all tabs works, no broken routes
- [ ] Tooltips are present on non-obvious fields
- [ ] Loading indicators display during data fetches
- [ ] Error messages are user-friendly (not raw stack traces)
- [ ] Empty states include helpful guidance text
- [ ] Button hierarchy is clear (primary vs. secondary actions)
- [ ] Disabled buttons show a clear reason via tooltip or adjacent text
- [ ] Language throughout is professional and consistent

## 12. App UI -- Other Tabs (Smoke Tests)

- [ ] **Metadata Review** -- loads, displays generated metadata, edit/approve flow works
- [ ] **Graph Explorer** -- renders graph, node and edge interactions work
- [ ] **Semantic Layer** -- displays metric views and validation status
- [ ] **Genie Builder** -- context assembly and space creation flow completes
- [ ] **Agent Chat** -- sends messages and receives responses
- [ ] **Vector Search** -- queries return results
- [ ] **Entity Browser** -- navigates entities correctly

## 13. Permissions & Security

- [ ] App service principal permissions determine what the app can **see** (catalog browsing)
- [ ] Job-run permissions from the deployer determine what jobs can **modify** (DDL apply, tags)
- [ ] No PII/PHI data appears in application logs (only detection metadata)
- [ ] Table names and bundle paths are sanitized against SQL injection
- [ ] Agent output is sanitized for leaked tokens or credentials

## 14. Federation Mode

- [ ] `federation_mode=true` disables DESCRIBE EXTENDED, ALTER TABLE, and SET TAGS
- [ ] All output remains Delta-native
- [ ] No regressions for federated catalog users

## 15. Automated Tests

- [ ] `./run_tests.sh` passes all test suites (core, DDL regenerator, binary/variant)
- [ ] `./run_tests.sh -q` (quick mode, core only) passes
- [ ] CI pipeline passes on push
- [ ] Integration tests (Databricks notebooks) all pass

## 16. Documentation

- [ ] `README.md` is accurate and up to date
- [ ] Permissions note in README explains app SPN vs. deployer permissions
- [ ] `consumer-guide.md` is present and tracked
- [ ] Quickstart docs match the current deployment flow
- [ ] Common troubleshooting scenarios are documented
- [ ] Version is consistent across `pyproject.toml`, `__version__`, `package.json`, `package-lock.json`, and the FastAPI app

## 17. Key Regressions to Watch

- [ ] Existing users with literal `table_names` (no wildcards) still work correctly
- [ ] DDL application does not double-apply (batch and individual)
- [ ] "All" mode does not produce duplicate rows in the log table
- [ ] PI mode deterministic pre-scan (spaCy/Presidio) still runs before the LLM call
- [ ] Domain classification value snapping still works correctly
- [ ] Bundle root path resolution uses the correct split count
- [ ] `keep_default_na=False` is present on all `pd.read_csv` calls for metadata files
- [ ] Control table claiming works correctly with concurrent tasks
