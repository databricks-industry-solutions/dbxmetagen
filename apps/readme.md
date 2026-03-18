# Apps

## dbxmetagen-app (primary)

The main application for this repository. Provides a full-featured UI for metadata generation, domain/ontology classification, PII detection, knowledge graph analytics, semantic layer management, Genie space building, and agent-based metadata exploration. Deployed and managed via Databricks Asset Bundles -- see the root `deploy.sh` and `resources/` for configuration.

See [dbxmetagen-app/](dbxmetagen-app/) for details.

**No-DAB deployment:** If you cloned this repo into your Databricks workspace and want to deploy the app without the CLI or DABs, use the setup notebook at `examples/00_deploy_app.py`. The pre-built frontend is checked into git, so no Node.js build step is needed.

## uc-metadata-assistant (standalone)

A self-contained Flask app for simpler metadata generation and governance use cases. It does not use the `dbxmetagen` library and is not wired into the DAB deployment pipeline. It must be configured and deployed independently.

See [uc-metadata-assistant/](uc-metadata-assistant/) for setup instructions.
