"""Validate that the static app bundle YAML and api_server.py stay in sync.

Since the deploy consolidation, app env lives in `config.env` of the static
`resources/apps/dbxmetagen_app.yml` (it overrides the command-only app.yaml on
deploy); there are no more .template files. These tests prevent the class of
regressions where a new job is added to one file but not wired through the
others, or where hardcoded per-workspace values leak into the committed bundle.
"""

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
APP_RESOURCES = ROOT / "resources" / "apps" / "dbxmetagen_app.yml"
APP_YAML = ROOT / "apps" / "dbxmetagen-app" / "app" / "app.yaml"
API_SERVER = ROOT / "apps" / "dbxmetagen-app" / "app" / "api_server.py"


def _app_block():
    res = yaml.safe_load(APP_RESOURCES.read_text())
    return res["resources"]["apps"]["dbxmetagen_app"]


def test_config_env_uses_variables_not_hardcoded():
    """Per-workspace values must be ${var.*}, never hardcoded (public repo)."""
    text = APP_RESOURCES.read_text()
    assert "${var.catalog_name}" in text, "config.env missing ${var.catalog_name}"
    assert "${var.schema_name}" in text, "config.env missing ${var.schema_name}"
    assert "eswanson_demo" not in text, "app resource YAML contains a hardcoded catalog name"
    # The workspace host must never be committed.
    assert "cloud.databricks.com" not in text, "app resource YAML contains a hardcoded host"


def test_app_yaml_is_command_only():
    """app.yaml must carry only the command; env is owned by config.env."""
    app = yaml.safe_load(APP_YAML.read_text())
    assert "command" in app, "app.yaml missing command"
    assert "env" not in app, "app.yaml must not define env (config.env is the source of truth)"


def test_all_job_resources_wired_in_config_env():
    """Every job declared as an app resource must be referenced via config.env value_from."""
    app = _app_block()
    job_names = {r["name"] for r in app["resources"] if "job" in r}
    wired = {e["value_from"] for e in app["config"]["env"] if "value_from" in e}
    missing = job_names - wired - {"sql_warehouse"}
    assert not missing, f"Job resources not wired in config.env: {missing}"


def test_job_env_map_matches_config_env():
    """Every *_JOB_ID env var the app reads must be present in config.env."""
    env_vars = set(re.findall(r'"([A-Z_]+_JOB_ID)"', API_SERVER.read_text()))
    env_names = {e["name"] for e in _app_block()["config"]["env"]}
    missing = env_vars - env_names
    assert not missing, f"_JOB_ENV_MAP vars missing from config.env: {missing}"


def test_config_env_job_ids_map_to_declared_resources():
    """Every value_from must resolve to a declared resource name (DAB validates this)."""
    app = _app_block()
    res_names = {r["name"] for r in app["resources"]}
    referenced = {e["value_from"] for e in app["config"]["env"] if "value_from" in e}
    unresolved = referenced - res_names
    assert not unresolved, f"config.env value_from with no matching resource: {unresolved}"
