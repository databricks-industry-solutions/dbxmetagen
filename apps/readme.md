# Apps

## dbxmetagen-app (primary)

The main application for this repository. Provides a full-featured UI for metadata generation, domain/ontology classification, PII detection, knowledge graph analytics, semantic layer management, Genie space building, and agent-based metadata exploration. Deployed and managed via Databricks Asset Bundles (`databricks bundle deploy` + `scripts/grant_app_permissions.sh`) -- see the root `databricks.yml` and `resources/` for configuration.

See [dbxmetagen-app/](dbxmetagen-app/) for details.

## uc-metadata-assistant (removed)

The legacy standalone Flask prototype that used to live here has been removed from this branch. It was never part of the dbxmetagen product or the DAB deployment. It is preserved on the `main_uc_metadata_assistant_archive` branch for reference.
