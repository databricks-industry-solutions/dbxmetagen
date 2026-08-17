# Apps

## dbxmetagen-app (primary)

The main application for this repository. Provides a full-featured UI for metadata generation, domain/ontology classification, PII detection, knowledge graph analytics, semantic layer management, Genie space building, and agent-based metadata exploration. Deployed and managed via Databricks Asset Bundles (`databricks bundle deploy` + `scripts/grant_app_permissions.sh`) -- see the root `databricks.yml` and `resources/` for configuration.

See [dbxmetagen-app/](dbxmetagen-app/) for details.

## uc-metadata-assistant (legacy — unmaintained, kept for reference)

> **NOTE:** This is a legacy standalone prototype. It is **not part of the dbxmetagen product**, is
> **not maintained** (last updated early 2026), does **not** use the `dbxmetagen` library, and is
> **not** wired into the DAB deployment pipeline — `databricks bundle deploy` never touches it. It is
> **intentionally retained for reference only**; do not treat it as a live or supported deployment
> path. For all real use, deploy **dbxmetagen-app** above.

A self-contained Flask app for simpler metadata generation and governance use cases. If used, it must
be configured and deployed entirely independently.

See [uc-metadata-assistant/](uc-metadata-assistant/) for its (unmaintained) setup instructions.
