# Migration Guide: Upgrading to the Consolidated Deployment (0.10.x+)

Earlier dbxmetagen releases deployed via a `deploy.sh` script that **generated**
`databricks.yml`, `app.yaml`, and `resources/apps/dbxmetagen_app.yml` from
`.template` files. That approach has been **replaced** with a plain Databricks
Asset Bundles (DAB) deploy: those files are now **static and committed**, there is
**no `deploy.sh`**, and deployment is `databricks bundle deploy` + `bundle run`
(or the workspace UI equivalent). See the [Quickstart](../README.md#quickstart)
and [Workspace UI Deployment](MANUAL_DEPLOYMENT.md) for the new flow.

This guide is for **existing users upgrading a workspace that was deployed with
the old `deploy.sh`**. A fresh clone into a new workspace needs none of this.

---

## TL;DR

```bash
# 1. Pull the new version
git pull            # (or re-clone the Git Folder in the workspace)

# 2. Clear the old deploy's stale synced files (SAFE -- see below)
databricks bundle destroy -t <target> -p <profile>

# 3. Redeploy fresh
databricks bundle deploy -t <target> -p <profile>
databricks bundle run   -t <target> -p <profile> dbxmetagen_app
scripts/grant_app_permissions.sh -t <target> -p <profile>   # only if using OBO / app-SP catalog access
```

> **Your generated data is NOT affected by the destroy.** See "Is this safe?" below.

---

## Why a destroy is needed (once)

DAB **syncs** your source files into a per-target deploy directory
(`.../.bundle/<bundle>/<target>/files/...`) and deploys the app/jobs from there.
When the old `.template` files were removed and `app.yaml` was rewritten to the new
static (command-only) format, **DAB's sync did not delete the old copies already
sitting in the deploy directory** -- sync adds and updates, but does not prune
files that disappeared from the source.

The result: a workspace upgraded in place can end up with a **stale `app.yaml`**
(the old templated one, with an inline `env:` block and `__PLACEHOLDER__` values)
still in the deploy target. The Databricks Apps builder reads that stale file and
fails with:

```
No command to run and no Python file found. Please add a 'command' field...
```

A one-time `bundle destroy` clears the stale deploy directory so the next deploy
syncs only the current files.

---

## Is this safe? (What `bundle destroy` does and does NOT remove)

**`bundle destroy` only removes what the bundle manages: the jobs and the app.**
It does **NOT** touch any data you have generated. Specifically:

| Removed by destroy | NOT touched by destroy |
|--------------------|------------------------|
| The dbxmetagen jobs (recreated on redeploy) | Your catalog, schemas, and tables |
| The `dbxmetagen-app` app resource (recreated on redeploy) | Generated metadata, comments, PI/domain results |
| Bundle deploy state / synced files | Knowledge bases, ontologies, embeddings, knowledge graph (`graph_edges`), FK predictions |
| | The `metadata_generation_log`, control tables, review data |
| | Vector Search endpoint + indexes |

All dbxmetagen **output lives in Unity Catalog tables created by job *runs*, not
by the bundle**, so destroying and redeploying the jobs/app leaves every bit of
your generated work in place. The redeploy simply recreates the jobs and app that
point at that same data.

If you would still rather not destroy, see "Alternative: surgical cleanup" below.

---

## Deploy from ONE location per workspace

The dbxmetagen app is a **singleton by name** in a workspace. If you deploy from
**both** the CLI and the workspace UI (Git Folder), or to **multiple targets**
(`dev`, `demo`), they all try to own the single app `dbxmetagen-app` and the last
deploy wins -- which can leave the app pointing at one deploy's source while its
config/overrides come from another. Symptoms include `WAREHOUSE_ID not configured`,
`OBO disabled`, `catalog access failed`, or `No job IDs found` even though your
overrides look correct.

**Pick one deploy location and one target per workspace.** If you genuinely need
multiple instances in one workspace, set `app_name_suffix` (e.g. `-dev`, `-team1`)
in each instance's `variable-overrides.json` so the apps and job names stay
distinct and don't overwrite each other.

---

## Variable override changes

If your old `variable-overrides.json` (or `.env`) set a cluster or budget policy,
the keys changed:

| Old | New |
|-----|-----|
| `policy_id: "<id>"` (standalone) | Override the whole `metadata_job_cluster` (and/or `lakebase_job_cluster`) complex variable and add `policy_id` + `apply_policy_default_values` inside it. See the example in `databricks.yml`. |
| `budget_policy_id: "<id>"` | **Unchanged** -- still a plain string variable. |

Why: the newer DAB "direct" deploy engine (used by the workspace UI Deploy button
and recent CLIs) does not strip an empty `policy_id: ""`, and the Jobs API rejects
`''` as an invalid cluster policy ID. Substituting the whole cluster block is the
only pattern that supports both with-policy and without-policy deploys. If you set
no policy, you don't need to do anything -- a bare deploy applies none.

---

## No more `npm` for deployment

The React frontend is **prebuilt and committed** (`apps/dbxmetagen-app/app/src/dist/`).
Deploying does **not** require Node.js or `npm`. Only contributors who modify the
frontend rebuild it (`cd apps/dbxmetagen-app/app/src && npm install && npm run build`).

---

## Alternative: surgical cleanup (avoid destroy)

If you cannot run `bundle destroy`, you can instead delete the stale files from the
deploy target directory and redeploy. In the workspace, delete these from your
bundle's deploy files path
(`.../.bundle/<bundle>/<target>/files/apps/dbxmetagen-app/app/`):

- `app.yaml.template`  (removed from the project; should not exist)
- any `app.yaml` that contains an `env:` block or `__PLACEHOLDER__` text (the new
  `app.yaml` is command-only)

Then redeploy (`bundle deploy` + `bundle run dbxmetagen_app`, or the UI equivalent)
so the current files sync in. Note this does not resolve multiple-deploy-location
confusion (see above) -- a destroy + single-location redeploy is cleaner.

---

## Verifying a good deploy

After redeploying and starting the app, the app's **Diagnostics** should show your
`catalog`, `schema`, and `WAREHOUSE_ID` configured, OBO state matching your
`enable_obo` setting, and job IDs resolved. If any show as unconfigured, re-check
that you deployed the intended target and that its
`.databricks/bundle/<target>/variable-overrides.json` has the right values.
