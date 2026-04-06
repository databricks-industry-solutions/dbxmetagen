"""Validate that app.yaml.template, dbxmetagen_app.yml, and api_server.py stay in sync.

These tests prevent the class of regressions where a new job is added to one
file but not wired through the others, or where hardcoded values leak into
the template.
"""

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "apps" / "dbxmetagen-app" / "app" / "app.yaml.template"
APP_RESOURCES = ROOT / "resources" / "apps" / "dbxmetagen_app.yml.template"
API_SERVER = ROOT / "apps" / "dbxmetagen-app" / "app" / "api_server.py"


def test_template_uses_placeholders():
    text = TEMPLATE.read_text()
    assert "__CATALOG_NAME__" in text, "Template missing __CATALOG_NAME__ placeholder"
    assert "__SCHEMA_NAME__" in text, "Template missing __SCHEMA_NAME__ placeholder"
    assert "eswanson_demo" not in text, "Template contains hardcoded catalog name"


def _load_yaml_template(path):
    """Load a YAML template, stripping bare __PLACEHOLDER__ lines."""
    import re
    text = path.read_text()
    text = re.sub(r'^__[A-Z_]+__\s*$', '', text, flags=re.MULTILINE)
    return yaml.safe_load(text)


def test_all_job_resources_wired_in_template():
    res = _load_yaml_template(APP_RESOURCES)
    tmpl = yaml.safe_load(TEMPLATE.read_text())

    job_names = {
        r["name"]
        for r in res["resources"]["apps"]["dbxmetagen_app"]["resources"]
        if "job" in r
    }
    wired = {e["valueFrom"] for e in tmpl["env"] if "valueFrom" in e}
    missing = job_names - wired - {"sql_warehouse"}
    assert not missing, f"Job resources not wired in app.yaml.template: {missing}"


def test_job_env_map_matches_template():
    env_vars = set(re.findall(r'"([A-Z_]+_JOB_ID)"', API_SERVER.read_text()))
    tmpl_names = {e["name"] for e in yaml.safe_load(TEMPLATE.read_text())["env"]}
    missing = env_vars - tmpl_names
    assert not missing, f"_JOB_ENV_MAP vars missing from app.yaml.template: {missing}"
