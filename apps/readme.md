# Apps

## dbxmetagen-app (primary)

The main application for this repository. Provides a full-featured UI for metadata generation, domain/ontology classification, PII detection, knowledge graph analytics, semantic layer management, Genie space building, and agent-based metadata exploration. Deployed and managed via Databricks Asset Bundles -- see the root `deploy.sh` and `resources/` for configuration.

See [dbxmetagen-app/](dbxmetagen-app/) for details.

## uc-metadata-assistant (standalone)

A self-contained Flask app for simpler metadata generation and governance use cases. It does not use the `dbxmetagen` library and is not wired into the DAB deployment pipeline. It must be configured and deployed independently.

See [uc-metadata-assistant/](uc-metadata-assistant/) for setup instructions.
