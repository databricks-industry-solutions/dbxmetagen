"""scripts/bundle_identity_check.py: identity settings that silently misbehave."""

import io
import json
import sys

from scripts.bundle_identity_check import main, status, warnings

SP = "00000000-0000-0000-0000-000000000000"
ME = "deployer@example.com"


def _bundle(run_as=None, jobs=None, deployer=ME, alert_email=None, app_permissions=None):
    b = {"workspace": {"current_user": {"userName": deployer}},
         "variables": {"run_as": {"value": run_as if run_as is not None else {"user_name": deployer}},
                       "alert_email": {"value": alert_email if alert_email is not None else deployer},
                       "app_permissions": {"value": app_permissions if app_permissions is not None else []}}}
    if jobs is not None:
        b["resources"] = {"jobs": jobs}
    return b


# ---- status (run_as) -------------------------------------------------------

def test_default_deployer_identity_prints_nothing():
    assert status(_bundle(jobs={"a": {}})) == ""


def test_sp_override_no_job_uses_it_is_ignored():
    assert status(_bundle({"service_principal_name": SP}, {"a": {}, "b": {"run_as": None}})) == \
        f"{SP}\tservice_principal_name\tignored"


def test_sp_override_used_by_a_job():
    jobs = {"a": {}, "b": {"run_as": {"service_principal_name": SP}}}
    assert status(_bundle({"service_principal_name": SP}, jobs)).endswith("\tused")


def test_other_user_override_ignored():
    assert status(_bundle({"user_name": "other@example.com"}, {"a": {}})) == \
        "other@example.com\tuser_name\tignored"


def test_no_job_resources_is_unknown_not_ignored():
    assert status(_bundle({"service_principal_name": SP})).endswith("\tunknown")


def test_sp_override_equal_to_deploying_sp_prints_nothing():
    assert status(_bundle({"service_principal_name": SP}, {"a": {}}, deployer=SP)) == ""


def test_user_override_differing_only_in_case_prints_nothing():
    assert status(_bundle({"user_name": "Deployer@Example.com"}, {"a": {}})) == ""


# ---- warnings --------------------------------------------------------------

def test_user_deploy_with_defaults_has_no_warnings():
    assert warnings(_bundle(jobs={"a": {}}), "dev") == []


def test_ignored_run_as_warns():
    w = warnings(_bundle({"service_principal_name": SP}, {"a": {}}), "prod")
    assert len(w) == 1 and "no job uses it" in w[0] and ".databricks/bundle/prod/" in w[0]


def test_sp_deploy_with_defaults_warns_about_alerts_and_app_access():
    w = warnings(_bundle(jobs={"a": {}}, deployer=SP), "prod")
    assert len(w) == 2
    assert "alert_email" in w[0] and SP in w[0]
    assert "app_permissions" in w[1]


def test_sp_deploy_fully_configured_has_no_warnings():
    b = _bundle(jobs={"a": {}}, deployer=SP, alert_email="ops@example.com",
                app_permissions=[{"group_name": "analysts", "level": "CAN_USE"}])
    assert warnings(b, "prod") == []


def test_placeholder_alert_email_warns():
    w = warnings(_bundle(jobs={"a": {}}, alert_email="data-team@example.com"), "prod")
    assert len(w) == 1 and "placeholder" in w[0]


def test_empty_alert_email_warns():
    assert any("alert_email is empty" in w for w in warnings(_bundle(jobs={"a": {}}, alert_email=""), "dev"))


# ---- CLI -------------------------------------------------------------------

def test_unreadable_input_prints_nothing(monkeypatch, capsys):
    monkeypatch.setattr(sys, "stdin", io.StringIO("not json"))
    main([])
    main(["--warnings"])
    assert capsys.readouterr().out == ""


def test_main_status_line(monkeypatch, capsys):
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(_bundle({"service_principal_name": SP}, {"a": {}}))))
    main([])
    assert capsys.readouterr().out == f"{SP}\tservice_principal_name\tignored\n"


def test_main_warnings(monkeypatch, capsys):
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(_bundle(jobs={"a": {}}, deployer=SP))))
    main(["--warnings", "--target", "prod"])
    out = capsys.readouterr().out
    assert out.count("WARNING:") == 2 and ".databricks/bundle/prod/variable-overrides.json" in out
