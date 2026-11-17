#!/usr/bin/env python3
"""Identity checks on a resolved bundle, shared by deploy.sh and grant_app_permissions.sh.

Reads the JSON of ``databricks bundle validate|summary --output json`` on stdin.

Default mode prints ONE tab-separated line describing ``var.run_as``, or nothing when
it is the deploying identity::

    <identity>\t<service_principal_name|user_name>\t<used|ignored|unknown>

``used``     at least one job's resolved run_as is this identity
``ignored``  no job runs as it (jobs run as the deployer instead)
``unknown``  the JSON carries no job resources, so it cannot tell

``--warnings [--target T]`` prints human-readable warnings instead (nothing if all
is well), for settings that silently misbehave:

* ``run_as`` set but applied to no job. Jobs omit run_as and run as the deploying
  identity; older releases applied var.run_as to every job, so an existing
  override is now ignored.
* ``alert_email`` that is not an email address. It defaults to the deploying
  identity, which for a service principal is its application id, so job failure
  alerts would reach nobody.
* Deploying as a service principal with empty ``app_permissions``: only the
  service principal and workspace admins can open the app.

Never fails: unreadable input prints nothing (callers keep their old behavior).
"""

import argparse
import json
import sys
from typing import List


def _value(bundle: dict, name: str, default=None):
    var = (bundle.get("variables") or {}).get(name) or {}
    return var.get("value", var.get("default", default))


def _deployer(bundle: dict) -> str:
    return ((bundle.get("workspace") or {}).get("current_user") or {}).get("userName") or ""


def _is_email(value: str) -> bool:
    return "@" in value and "." in value.split("@")[-1]


def status(bundle: dict) -> str:
    ra = _value(bundle, "run_as") or {}
    if not isinstance(ra, dict):
        return ""
    deployer = _deployer(bundle)
    if ra.get("service_principal_name"):
        kind, ident = "service_principal_name", ra["service_principal_name"]
    elif ra.get("user_name"):
        kind, ident = "user_name", ra["user_name"]
    else:
        return ""
    # Running as the deployer is what jobs do anyway (a user, or a service principal
    # deploying from CI, whose userName is its application id). Case-insensitive:
    # the CLI and an override may differ only in case.
    if ident.strip().lower() == deployer.strip().lower():
        return ""
    jobs = (bundle.get("resources") or {}).get("jobs")
    if not isinstance(jobs, dict) or not jobs:
        state = "unknown"
    else:
        used = any(str(((j or {}).get("run_as") or {}).get(kind) or "").lower() == ident.lower()
                   for j in jobs.values())
        state = "used" if used else "ignored"
    return f"{ident}\t{kind}\t{state}"


def warnings(bundle: dict, target: str = "<target>") -> List[str]:
    overrides = f".databricks/bundle/{target}/variable-overrides.json"
    out = []

    line = status(bundle)
    if line and line.endswith("\tignored"):
        ident, kind, _ = line.split("\t")
        out.append(
            f"WARNING: run_as is set to {kind} '{ident}', but no job uses it. Jobs run as the\n"
            f"  deploying identity (older releases applied var.run_as to every job; this one does not).\n"
            f"  If that is what you want, delete \"run_as\" from {overrides} to silence this.\n"
            f"  To run jobs as a service principal, deploy AS it; to run specific jobs as a different\n"
            f"  identity, add  run_as: ${{var.run_as}}  to those jobs (see run_as in variables.yml)."
        )

    alert = str(_value(bundle, "alert_email") or "").strip()
    if not _is_email(alert):
        shown = f"'{alert}'" if alert else "empty"
        out.append(
            f"WARNING: alert_email is {shown}, which is not an email address, so job failure alerts\n"
            f"  will reach nobody. It defaults to the deploying identity, and a service principal's\n"
            f"  is its application id. Set a team address in {overrides}:\n"
            f"    \"alert_email\": \"data-team@example.com\""
        )

    deployer = _deployer(bundle)
    if deployer and not _is_email(deployer) and not (_value(bundle, "app_permissions") or []):
        out.append(
            f"WARNING: deploying as a service principal with empty app_permissions: only the service\n"
            f"  principal and workspace admins can open the app. Grant your users access in {overrides}:\n"
            f"    \"app_permissions\": [{{\"group_name\": \"<your-group>\", \"level\": \"CAN_USE\"}}]"
        )
    return out


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--warnings", action="store_true", help="print human-readable warnings")
    ap.add_argument("--target", default="<target>", help="bundle target, used in the fix hints")
    args = ap.parse_args(argv)
    try:
        bundle = json.load(sys.stdin)
        if args.warnings:
            for w in warnings(bundle, args.target):
                print(w)
        else:
            out = status(bundle)
            if out:
                print(out)
    except Exception:
        pass


if __name__ == "__main__":
    main()
