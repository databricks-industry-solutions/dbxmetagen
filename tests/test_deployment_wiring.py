"""Validate that the static app bundle YAML and api_server.py stay in sync.

Since the deploy consolidation, app env lives in `config.env` of the static
`resources/apps/dbxmetagen_app.yml` (it overrides the command-only app.yaml on
deploy); there are no more .template files. These tests prevent the class of
regressions where a new job is added to one file but not wired through the
others, or where hardcoded per-workspace values leak into the committed bundle.
"""

import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
APP_RESOURCES = ROOT / "resources" / "apps" / "dbxmetagen_app.yml"
APP_YAML = ROOT / "apps" / "dbxmetagen-app" / "app" / "app.yaml"
API_SERVER = ROOT / "apps" / "dbxmetagen-app" / "app" / "api_server.py"
OVERRIDES_EXAMPLE = ROOT / "variable-overrides.example.json"


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


def test_config_env_job_ids_are_read_by_app():
    """Every *_JOB_ID env var wired in config.env must actually be read by the app.

    Note the direction: config.env is a SUBSET of _JOB_ENV_MAP, not equal to it.
    The app wires only its primary-launch jobs via value_from (to stay under the
    Databricks Apps 20-resource cap); the rest are resolved at runtime by the
    ws.jobs.list() name-match fallback in run_job(). So we assert every wired var
    is read, not that every read var is wired.
    """
    env_vars = set(re.findall(r'"([A-Z_]+_JOB_ID)"', API_SERVER.read_text()))
    wired_job_id_names = {
        e["name"] for e in _app_block()["config"]["env"]
        if e["name"].endswith("_JOB_ID")
    }
    unread = wired_job_id_names - env_vars
    assert not unread, f"config.env wires job-ID vars the app never reads: {unread}"


def test_config_env_job_ids_map_to_declared_resources():
    """Every value_from must resolve to a declared resource name (DAB validates this)."""
    app = _app_block()
    res_names = {r["name"] for r in app["resources"]}
    referenced = {e["value_from"] for e in app["config"]["env"] if "value_from" in e}
    unresolved = referenced - res_names
    assert not unresolved, f"config.env value_from with no matching resource: {unresolved}"


def test_config_env_entries_have_a_source():
    """Every config.env entry must have a non-empty `value` or a `value_from`.

    DAB config.env cannot carry an optionally-empty value: the Go SDK strips
    empty strings via omitempty, and the Apps deploy then rejects the entry with
    "Must specify environment variable source using either value or valueFrom",
    aborting `bundle run`. So a `value: "${var.x}"` whose var defaults to "" is a
    latent deploy failure. Guard against committing one.
    """
    env = _app_block()["config"]["env"]
    # An entry is safe if it has a value_from, or a value that is a non-empty
    # literal OR a ${var.*} reference whose resolved value we can't check here but
    # which must itself be non-empty at deploy time. We flag only literal-empty
    # values, which are the ones that reproducibly break the deploy.
    bad = [
        e["name"] for e in env
        if "value_from" not in e and str(e.get("value", "")).strip() == ""
    ]
    assert not bad, (
        f"config.env entries with an empty literal value (break `bundle run`): {bad}. "
        "Omit the entry entirely if it can be empty; the app should read it with a default."
    )


def test_overrides_example_is_valid_and_declared():
    """variable-overrides.example.json must be valid JSON and every key it
    documents must be a declared bundle variable, so copying it to
    .databricks/bundle/<target>/variable-overrides.json actually resolves.

    The example is the committed template users copy into the DAB auto-load path;
    a repo-root variable-overrides.json is NOT auto-loaded, so the example must
    stay in sync with variables.yml (+ app_variables.yml) to be useful.
    """
    assert OVERRIDES_EXAMPLE.exists(), "variable-overrides.example.json is missing"
    example = json.loads(OVERRIDES_EXAMPLE.read_text())
    assert isinstance(example, dict) and example, "example must be a non-empty JSON object"

    declared = set()
    for vf in ("variables.yml", "variables.advanced.yml", "resources/app_variables.yml"):
        doc = yaml.safe_load((ROOT / vf).read_text()) or {}
        declared |= set((doc.get("variables") or {}).keys())

    unknown = set(example) - declared
    assert not unknown, (
        f"variable-overrides.example.json has keys not declared as bundle variables: {unknown}"
    )
    # Must not carry real per-workspace values (public repo).
    assert example.get("catalog_name") != "eswanson_demo", "example leaks a real catalog name"


def test_app_resource_count_under_cap():
    """Databricks Apps allow at most 20 resources per app. Exceeding it fails the
    deploy with 'Number of app resources must be less than or equal to 20'.
    This guard catches an over-wired app resource block before deploy time.
    """
    app = _app_block()
    n = len(app["resources"])
    assert n <= 20, f"App declares {n} resources; the Databricks Apps cap is 20"
