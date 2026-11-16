"""scripts/run_as_status.py: detect a run_as override that no job applies."""

import io
import json
import sys

from scripts.run_as_status import main, status

SP = "00000000-0000-0000-0000-000000000000"
ME = "deployer@example.com"


def _bundle(run_as=None, jobs=None):
    b = {"workspace": {"current_user": {"userName": ME}},
         "variables": {"run_as": {"value": run_as if run_as is not None else {"user_name": ME}}}}
    if jobs is not None:
        b["resources"] = {"jobs": jobs}
    return b


def test_default_deployer_identity_prints_nothing():
    assert status(_bundle(jobs={"a": {}})) == ""


def test_sp_override_no_job_uses_it_is_ignored():
    assert status(_bundle({"service_principal_name": SP}, {"a": {}, "b": {"run_as": None}})) == \
        f"{SP}\tservice_principal_name\tignored"


def test_sp_override_used_by_a_job():
    jobs = {"a": {}, "b": {"run_as": {"service_principal_name": SP}}}
    assert status(_bundle({"service_principal_name": SP}, jobs)).endswith("\tused")


def test_other_user_override_ignored():
    out = status(_bundle({"user_name": "other@example.com"}, {"a": {}}))
    assert out == "other@example.com\tuser_name\tignored"


def test_no_job_resources_is_unknown_not_ignored():
    # Callers only block on "ignored"; missing resources must not block a deploy.
    assert status(_bundle({"service_principal_name": SP})).endswith("\tunknown")


def test_unreadable_input_prints_nothing(monkeypatch, capsys):
    monkeypatch.setattr(sys, "stdin", io.StringIO("not json"))
    main()
    assert capsys.readouterr().out == ""


def test_main_prints_one_line(monkeypatch, capsys):
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(_bundle({"service_principal_name": SP}, {"a": {}}))))
    main()
    assert capsys.readouterr().out == f"{SP}\tservice_principal_name\tignored\n"
