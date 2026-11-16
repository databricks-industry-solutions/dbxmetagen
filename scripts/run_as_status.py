#!/usr/bin/env python3
"""Report whether the bundle's ``run_as`` variable actually applies to any job.

Jobs omit ``run_as`` by default and run as the deploying identity; ``var.run_as``
only takes effect on jobs that reference it (the advanced "deploy as X, run jobs
as a different SP Y" pattern -- see variables.yml). Older releases applied it to
every job, so an existing override can now be silently ignored. deploy.sh and
grant_app_permissions.sh use this to detect that case.

Reads the JSON of ``databricks bundle validate|summary --output json`` on stdin and
prints ONE tab-separated line, or nothing when run_as is the deploying identity:

    <identity>\t<service_principal_name|user_name>\t<used|ignored|unknown>

``used``     at least one job's resolved run_as is this identity
``ignored``  no job runs as it (jobs run as the deployer instead)
``unknown``  the JSON carries no job resources, so it cannot tell

Never fails: unreadable input prints nothing (callers keep their old behavior).
"""

import json
import sys


def status(bundle: dict) -> str:
    var = (bundle.get("variables") or {}).get("run_as") or {}
    ra = var.get("value", var.get("default")) or {}
    if not isinstance(ra, dict):
        return ""
    deployer = ((bundle.get("workspace") or {}).get("current_user") or {}).get("userName") or ""
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


def main() -> None:
    try:
        out = status(json.load(sys.stdin))
    except Exception:
        out = ""
    if out:
        print(out)


if __name__ == "__main__":
    main()
