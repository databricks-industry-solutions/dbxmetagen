# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Jobs & Infrastructure
# MAGIC
# MAGIC Builds the wheel, creates the 8 essential dbxmetagen jobs, and provisions
# MAGIC a Vector Search endpoint. Run this **before** Notebook 02 (app deploy).
# MAGIC
# MAGIC Supports `setup` and `teardown` modes.

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.68.0 hatchling tomli -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, glob, shutil, subprocess, sys, time
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "metadata_results", "Schema Name")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID")
dbutils.widgets.text("repo_path", "", "Repo root (e.g. /Workspace/Repos/<user>/dbxmetagen)")
dbutils.widgets.text("volume_path", "", "Volume for wheel (e.g. /Volumes/cat/sch/vol)")
dbutils.widgets.text("app_name", "dbxmetagen-app", "App Name")
dbutils.widgets.text("node_type", "i3.2xlarge", "Node Type")
dbutils.widgets.text("spark_version", "17.3.x-cpu-ml-scala2.13", "Spark Version")
dbutils.widgets.text("vs_endpoint_name", "dbxmetagen-vs", "Vector Search Endpoint")
dbutils.widgets.text("policy_id", "", "Cluster Policy ID (optional)")
dbutils.widgets.text("budget_policy_id", "", "Serverless Budget Policy ID (optional)")
dbutils.widgets.dropdown("mode", "setup", ["setup", "teardown"], "Mode")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
warehouse_id = dbutils.widgets.get("warehouse_id")
repo_path = dbutils.widgets.get("repo_path").rstrip("/")
volume_path = dbutils.widgets.get("volume_path").rstrip("/")
app_name = dbutils.widgets.get("app_name")
node_type = dbutils.widgets.get("node_type")
spark_version = dbutils.widgets.get("spark_version")
vs_endpoint_name = dbutils.widgets.get("vs_endpoint_name")
policy_id = dbutils.widgets.get("policy_id").strip()
budget_policy_id = dbutils.widgets.get("budget_policy_id").strip()
mode = dbutils.widgets.get("mode")

notebooks_path = f"{repo_path}/notebooks"

assert catalog_name, "catalog_name is required"
assert warehouse_id, "warehouse_id is required"
assert repo_path, "repo_path is required -- set to the Git folder root"
assert volume_path, "volume_path is required -- wheel is copied here for job clusters"
assert os.path.exists(f"{repo_path}/pyproject.toml"), (
    f"pyproject.toml not found at {repo_path}. Is this the dbxmetagen repo root?"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load variables.yml defaults
# MAGIC
# MAGIC Job parameter defaults come from `variables.yml` in the repo so they stay
# MAGIC in sync with `deploy.sh` / DAB deployments.

# COMMAND ----------

import yaml

_vars_path = f"{repo_path}/variables.yml"
with open(_vars_path) as f:
    _all_vars = yaml.safe_load(f).get("variables", {})

def var(name, fallback=""):
    """Get default value from variables.yml, with optional fallback."""
    v = _all_vars.get(name, {})
    val = v.get("default", fallback) if isinstance(v, dict) else v
    if isinstance(val, bool):
        return "true" if val else "false"
    return str(val)

print(f"Loaded {len(_all_vars)} variables from {_vars_path}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, compute, vectorsearch as vs_svc
from databricks.sdk.errors import NotFound

w = WorkspaceClient()
current_user = w.current_user.me().user_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Wheel
# MAGIC
# MAGIC Builds from the Git folder with a deploy timestamp so the app platform
# MAGIC always reinstalls. The build runs in `/tmp` to avoid modifying the repo.

# COMMAND ----------

if mode == "setup":
    with open(f"{repo_path}/pyproject.toml", "rb") as f:
        base_version = tomllib.load(f)["project"]["version"]
    deploy_version = f"{base_version}+{int(time.time())}"

    build_dir = "/tmp/dbxmetagen_build"
    dist_dir = "/tmp/dbxmetagen_dist"
    for d in (build_dir, dist_dir):
        if os.path.exists(d):
            shutil.rmtree(d)

    shutil.copytree(f"{repo_path}/src", f"{build_dir}/src")
    shutil.copy(f"{repo_path}/pyproject.toml", f"{build_dir}/pyproject.toml")
    shutil.copy(f"{repo_path}/README.md", f"{build_dir}/README.md")
    if os.path.exists(f"{repo_path}/LICENSE.md"):
        shutil.copy(f"{repo_path}/LICENSE.md", f"{build_dir}/LICENSE.md")

    for cfg in ("variables.yml", "variables.advanced.yml"):
        repo_cfg = f"{repo_path}/{cfg}"
        pkg_cfg = f"{build_dir}/src/dbxmetagen/{cfg}"
        if os.path.exists(repo_cfg):
            shutil.copy(repo_cfg, pkg_cfg)

    pyproject = f"{build_dir}/pyproject.toml"
    with open(pyproject) as f:
        content = f.read()
    content = content.replace(f'version = "{base_version}"', f'version = "{deploy_version}"')
    with open(pyproject, "w") as f:
        f.write(content)

    subprocess.check_call(
        [sys.executable, "-m", "pip", "wheel", "--no-deps",
         "--no-build-isolation", "--wheel-dir", dist_dir, build_dir],
        stdout=subprocess.DEVNULL,
    )

    whl_path = glob.glob(f"{dist_dir}/dbxmetagen-*.whl")[0]
    whl_name = os.path.basename(whl_path)
    print(f"Built wheel: {whl_name} (version {deploy_version})")

    os.makedirs(volume_path, exist_ok=True)
    shutil.copy(whl_path, f"{volume_path}/{whl_name}")
    print(f"Copied wheel to {volume_path}/{whl_name}")
else:
    print("Teardown mode -- skipping wheel build")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Definition Helpers

# COMMAND ----------

whl_volume_path = None
if mode == "setup":
    whls = sorted(glob.glob(f"{volume_path}/dbxmetagen-*.whl"))
    assert whls, f"No wheel found in {volume_path}"
    whl_volume_path = whls[-1]
    print(f"Wheel for jobs: {whl_volume_path}")

whl_lib = compute.Library(whl=whl_volume_path) if whl_volume_path else None
graphframes_lib = compute.Library(pypi=compute.PythonPyPiLibrary(package="graphframes"))

def make_cluster(key, min_w=2, max_w=4, num_w=None, single_node=False):
    spec = compute.ClusterSpec(
        spark_version=spark_version,
        node_type_id=node_type,
        data_security_mode=compute.DataSecurityMode.SINGLE_USER,
    )
    if policy_id:
        spec.policy_id = policy_id
    if single_node:
        spec.num_workers = 0
        spec.spark_conf = {
            "spark.master": "local[*, 4]",
            "spark.databricks.cluster.profile": "singleNode",
        }
        spec.custom_tags = {"ResourceClass": "SingleNode"}
    elif num_w is not None:
        spec.num_workers = num_w
    else:
        spec.autoscale = compute.AutoScale(min_workers=min_w, max_workers=max_w)
    return jobs.JobCluster(job_cluster_key=key, new_cluster=spec)

def _nb_path(notebook):
    return f"{notebooks_path}/{notebook.removesuffix('.py')}"

def nb_task(key, notebook, params, deps=None, extra_libs=None):
    libs = [whl_lib] + (extra_libs or [])
    return jobs.Task(
        task_key=key,
        max_retries=1,
        job_cluster_key="cluster",
        notebook_task=jobs.NotebookTask(
            notebook_path=_nb_path(notebook),
            base_parameters=params),
        libraries=libs,
        depends_on=[jobs.TaskDependency(task_key=d) for d in (deps or [])])

def serverless_task(key, notebook, params, deps=None):
    return jobs.Task(
        task_key=key,
        max_retries=1,
        environment_key="default",
        notebook_task=jobs.NotebookTask(
            notebook_path=_nb_path(notebook),
            base_parameters=params),
        depends_on=[jobs.TaskDependency(task_key=d) for d in (deps or [])])

def jp(name, default):
    return jobs.JobParameterDefinition(name=name, default=str(default))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job 1: Metadata Generator (single mode)

# COMMAND ----------

def build_metadata_generator_job():
    params = [
        jp("table_names", var("job_table_names", "none")), jp("mode", "comment"),
        jp("catalog_name", catalog_name), jp("schema_name", schema_name),
        jp("current_user", current_user), jp("permission_groups", "none"),
        jp("permission_users", "none"), jp("run_id", "{{job.run_id}}"),
        jp("include_previously_failed_tables", "false"),
        jp("apply_ddl", var("apply_ddl", "false")),
        jp("use_kb_comments", "false"),
        jp("ontology_bundle", var("ontology_bundle", "")),
        jp("domain_config_path", ""),
        jp("sample_size", var("sample_size", "5")),
        jp("include_lineage", var("include_lineage", "false")),
        jp("model", var("model", "databricks-claude-sonnet-4-6")),
    ]
    base = {p.name: f"{{{{job.parameters.{p.name}}}}}" for p in params}
    tasks = [nb_task("generate_metadata", "generate_metadata.py", base)]
    return (f"{app_name}_metadata_job", tasks, params,
            [make_cluster("cluster")], "metadata_generator_job", 5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job 2: Parallel Modes (comment -> PI + domain)

# COMMAND ----------

def build_parallel_modes_job():
    params = [
        jp("table_names", var("job_table_names", "none")),
        jp("catalog_name", catalog_name),
        jp("schema_name", schema_name),
        jp("apply_ddl", var("apply_ddl", "false")),
        jp("current_user", current_user), jp("permission_groups", "none"),
        jp("permission_users", "none"), jp("run_id", "{{job.run_id}}"),
        jp("include_previously_failed_tables", "false"),
        jp("use_kb_comments", "false"),
        jp("ontology_bundle", var("ontology_bundle", "")),
        jp("domain_config_path", ""),
        jp("sample_size", var("sample_size", "5")),
        jp("include_lineage", var("include_lineage", "false")),
        jp("model", var("model", "databricks-claude-sonnet-4-6")),
    ]
    ref = lambda n: f"{{{{job.parameters.{n}}}}}"
    common = {
        "table_names": ref("table_names"),
        "catalog_name": ref("catalog_name"),
        "schema_name": ref("schema_name"),
        "apply_ddl": ref("apply_ddl"),
        "current_user": ref("current_user"),
        "permission_groups": ref("permission_groups"),
        "permission_users": ref("permission_users"),
        "run_id": ref("run_id"),
        "cleanup_control_table": "false",
        "include_previously_failed_tables": ref("include_previously_failed_tables"),
        "ontology_bundle": ref("ontology_bundle"),
        "domain_config_path": ref("domain_config_path"),
        "sample_size": ref("sample_size"),
        "include_lineage": ref("include_lineage"),
        "model": ref("model"),
    }
    tasks = [
        nb_task("generate_comments", "generate_metadata.py",
                {**common, "mode": "comment"}),
        nb_task("generate_pi", "generate_metadata.py",
                {**common, "mode": "pi", "include_existing_table_comment": "true",
                 "use_kb_comments": ref("use_kb_comments")},
                deps=["generate_comments"]),
        nb_task("generate_domain", "generate_metadata.py",
                {**common, "mode": "domain", "include_existing_table_comment": "true",
                 "use_kb_comments": ref("use_kb_comments")},
                deps=["generate_comments"]),
    ]
    return (f"{app_name}_parallel_modes_job", tasks, params,
            [make_cluster("cluster")], "metadata_parallel_modes_job", 5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job 3: Full Analytics Pipeline (18 tasks, 7 stages)

# COMMAND ----------

def build_analytics_pipeline_job():
    params = [
        jp("catalog_name", catalog_name), jp("schema_name", schema_name),
        jp("similarity_threshold", var("similarity_threshold", "0.8")),
        jp("max_edges_per_node", var("max_edges_per_node", "10")),
        jp("use_ann_similarity", var("use_ann_similarity", "true")),
        jp("generate_comments", "true"),
        jp("cluster_min_k", var("cluster_min_k", "2")),
        jp("cluster_max_k", var("cluster_max_k", "15")),
        jp("cluster_node_types", var("cluster_node_types", "table")),
        jp("ontology_bundle", var("ontology_bundle", "general")),
        jp("domain_config_path", ""),
        jp("incremental", var("incremental", "true")),
        jp("model", var("model", "databricks-claude-sonnet-4-6")),
        jp("table_names", ""),
        jp("max_ai_candidates", var("max_ai_candidates", "200")),
        jp("rule_score_min_for_ai", var("rule_score_min_for_ai", "0.50")),
        jp("max_candidates_per_table_pair", var("max_candidates_per_table_pair", "5")),
        jp("apply_ddl", var("apply_ddl", "false")),
    ]
    ref = lambda n: f"{{{{job.parameters.{n}}}}}"
    cat_sch = {"catalog_name": ref("catalog_name"), "schema_name": ref("schema_name")}
    cat_sch_tbl = {**cat_sch, "table_names": ref("table_names")}

    tasks = [
        nb_task("build_knowledge_base", "build_knowledge_base.py", cat_sch_tbl),
        nb_task("build_column_kb", "build_column_kb.py", cat_sch_tbl),
        nb_task("build_schema_kb", "build_schema_kb.py",
                {**cat_sch_tbl, "generate_comments": ref("generate_comments")},
                deps=["build_knowledge_base"]),
        nb_task("extract_extended_metadata", "extract_extended_metadata.py",
                {**cat_sch_tbl, "incremental": ref("incremental")},
                deps=["build_knowledge_base"]),
        nb_task("build_knowledge_graph", "build_knowledge_graph.py", cat_sch_tbl,
                deps=["build_column_kb", "build_schema_kb", "extract_extended_metadata"],
                extra_libs=[graphframes_lib]),
        nb_task("run_profiling", "run_profiling.py",
                {**cat_sch_tbl, "incremental": ref("incremental")},
                deps=["build_knowledge_base"]),
        nb_task("build_ontology", "build_ontology.py",
                {**cat_sch_tbl, "ontology_bundle": ref("ontology_bundle"),
                 "domain_config_path": ref("domain_config_path"),
                 "incremental": ref("incremental"), "model": ref("model"),
                 "apply_ddl": ref("apply_ddl")},
                deps=["build_knowledge_base", "build_column_kb"]),
        nb_task("build_ontology_vector_index", "build_ontology_vector_index.py",
                {**cat_sch, "ontology_bundle": ref("ontology_bundle"),
                 "endpoint_name": "dbxmetagen-vs"},
                deps=["build_ontology"]),
        nb_task("generate_embeddings", "generate_embeddings.py", cat_sch,
                deps=["build_knowledge_graph", "build_ontology"]),
        nb_task("build_similarity_edges", "build_similarity_edges.py",
                {**cat_sch, "similarity_threshold": ref("similarity_threshold"),
                 "max_edges_per_node": ref("max_edges_per_node"),
                 "use_ann_similarity": ref("use_ann_similarity")},
                deps=["generate_embeddings"]),
        nb_task("cluster_analysis", "cluster_analysis.py",
                {**cat_sch, "min_k": ref("cluster_min_k"),
                 "max_k": ref("cluster_max_k"),
                 "node_types": ref("cluster_node_types")},
                deps=["generate_embeddings"]),
        nb_task("compute_data_quality", "compute_data_quality.py", cat_sch,
                deps=["run_profiling"]),
        nb_task("validate_ontology", "validate_ontology.py", cat_sch,
                deps=["build_ontology"]),
        nb_task("predict_foreign_keys", "predict_foreign_keys.py",
                {**cat_sch, "column_similarity_threshold": "0.75",
                 "table_similarity_threshold": "0.7", "confidence_threshold": "0.7",
                 "apply_ddl": ref("apply_ddl"), "dry_run": "false",
                 "incremental": ref("incremental"),
                 "max_ai_candidates": ref("max_ai_candidates"),
                 "rule_score_min_for_ai": ref("rule_score_min_for_ai"),
                 "max_candidates_per_table_pair": ref("max_candidates_per_table_pair")},
                deps=["build_similarity_edges"]),
        nb_task("refresh_ontology_edges", "refresh_ontology_edges.py",
                {**cat_sch_tbl, "ontology_bundle": ref("ontology_bundle"),
                 "model": ref("model")},
                deps=["predict_foreign_keys", "build_ontology"]),
        nb_task("final_analysis", "final_analysis.py", cat_sch,
                deps=["refresh_ontology_edges", "cluster_analysis",
                      "compute_data_quality", "validate_ontology"]),
        nb_task("build_community_summaries", "build_community_summaries.py",
                {**cat_sch, "model": ref("model")},
                deps=["final_analysis"]),
        nb_task("build_vector_index", "build_vector_index.py",
                {**cat_sch, "endpoint_name": "dbxmetagen-vs"},
                deps=["final_analysis", "build_community_summaries"]),
    ]
    return (f"{app_name}_full_analytics_pipeline", tasks, params,
            [make_cluster("cluster")], "full_analytics_pipeline_job", 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job 4: Knowledge Base Builder

# COMMAND ----------

def build_kb_job():
    params = [jp("catalog_name", catalog_name), jp("schema_name", schema_name)]
    ref = lambda n: f"{{{{job.parameters.{n}}}}}"
    cat_sch = {"catalog_name": ref("catalog_name"), "schema_name": ref("schema_name")}
    tasks = [
        nb_task("build_knowledge_base", "build_knowledge_base.py", cat_sch),
        nb_task("build_knowledge_graph", "build_knowledge_graph.py", cat_sch,
                deps=["build_knowledge_base"], extra_libs=[graphframes_lib]),
    ]
    return (f"{app_name}_knowledge_base_job", tasks, params,
            [make_cluster("cluster", num_w=1)], "knowledge_base_builder_job", 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job 5: Sync Reviewed DDL

# COMMAND ----------

def build_sync_ddl_job():
    params = [
        jp("reviewed_file_name", ""),
        jp("mode", "comment"),
        jp("current_user_override", current_user),
    ]
    ref = lambda n: f"{{{{job.parameters.{n}}}}}"
    tasks = [
        nb_task("sync_reviewed_ddl", "sync_reviewed_ddl.py", {
            "reviewed_file_name": ref("reviewed_file_name"),
            "mode": ref("mode"),
            "current_user_override": ref("current_user_override"),
        }),
    ]
    return (f"{app_name}_sync_ddl_job", tasks, params,
            [make_cluster("cluster", min_w=1, max_w=2)], "sync_ddl_job", 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job 6: Metadata Serverless (single mode + KB)

# COMMAND ----------

def build_metadata_serverless_job():
    params = [
        jp("table_names", var("job_table_names", "none")), jp("mode", "comment"),
        jp("catalog_name", catalog_name), jp("schema_name", schema_name),
        jp("current_user", current_user), jp("permission_groups", "none"),
        jp("permission_users", "none"), jp("run_id", "{{job.run_id}}"),
        jp("include_previously_failed_tables", "false"),
        jp("apply_ddl", var("apply_ddl", "false")),
        jp("use_kb_comments", "false"),
        jp("ontology_bundle", var("ontology_bundle", "")),
        jp("domain_config_path", ""),
        jp("sample_size", "50"),
        jp("include_lineage", var("include_lineage", "false")),
        jp("model", var("model", "databricks-claude-sonnet-4-6")),
    ]
    ref = lambda n: f"{{{{job.parameters.{n}}}}}"
    base = {p.name: ref(p.name) for p in params}
    cat_sch = {"catalog_name": ref("catalog_name"), "schema_name": ref("schema_name")}
    tasks = [
        serverless_task("generate_metadata", "generate_metadata.py", base),
        serverless_task("build_knowledge_base", "build_knowledge_base.py",
                        cat_sch, deps=["generate_metadata"]),
        serverless_task("build_column_kb", "build_column_kb.py",
                        cat_sch, deps=["generate_metadata"]),
    ]
    return (f"{app_name}_metadata_serverless_job", tasks, params,
            None, "metadata_serverless_job", 5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job 7: Parallel Serverless (all 3 modes + KB)

# COMMAND ----------

def build_parallel_serverless_job():
    params = [
        jp("table_names", var("job_table_names", "none")),
        jp("catalog_name", catalog_name),
        jp("schema_name", schema_name),
        jp("apply_ddl", var("apply_ddl", "false")),
        jp("current_user", current_user), jp("permission_groups", "none"),
        jp("permission_users", "none"), jp("run_id", "{{job.run_id}}"),
        jp("include_previously_failed_tables", "false"),
        jp("use_kb_comments", "false"),
        jp("ontology_bundle", var("ontology_bundle", "")),
        jp("domain_config_path", ""),
        jp("sample_size", var("sample_size", "5")),
        jp("include_lineage", var("include_lineage", "false")),
        jp("model", var("model", "databricks-claude-sonnet-4-6")),
    ]
    ref = lambda n: f"{{{{job.parameters.{n}}}}}"
    common = {
        "table_names": ref("table_names"),
        "catalog_name": ref("catalog_name"),
        "schema_name": ref("schema_name"),
        "apply_ddl": ref("apply_ddl"),
        "current_user": ref("current_user"),
        "permission_groups": ref("permission_groups"),
        "permission_users": ref("permission_users"),
        "run_id": ref("run_id"),
        "cleanup_control_table": "false",
        "include_previously_failed_tables": ref("include_previously_failed_tables"),
        "ontology_bundle": ref("ontology_bundle"),
        "domain_config_path": ref("domain_config_path"),
        "sample_size": ref("sample_size"),
        "include_lineage": ref("include_lineage"),
        "model": ref("model"),
    }
    cat_sch = {"catalog_name": ref("catalog_name"), "schema_name": ref("schema_name")}
    tasks = [
        serverless_task("generate_comments", "generate_metadata.py",
                        {**common, "mode": "comment"}),
        serverless_task("generate_pi", "generate_metadata.py",
                        {**common, "mode": "pi", "include_existing_table_comment": "true",
                         "use_kb_comments": ref("use_kb_comments")},
                        deps=["generate_comments"]),
        serverless_task("generate_domain", "generate_metadata.py",
                        {**common, "mode": "domain", "include_existing_table_comment": "true",
                         "use_kb_comments": ref("use_kb_comments")},
                        deps=["generate_comments"]),
        serverless_task("build_knowledge_base", "build_knowledge_base.py",
                        cat_sch, deps=["generate_pi", "generate_domain"]),
        serverless_task("build_column_kb", "build_column_kb.py",
                        cat_sch, deps=["generate_pi", "generate_domain"]),
    ]
    return (f"{app_name}_parallel_serverless_job", tasks, params,
            None, "metadata_parallel_serverless_job", 5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job 8: Setup MCP Servers

# COMMAND ----------

def build_setup_mcp_servers_job():
    params = [
        jp("catalog_name", catalog_name), jp("schema_name", schema_name),
        jp("vs_index_name", "metadata_vs_index"), jp("drop_existing", "false"),
    ]
    ref = lambda n: f"{{{{job.parameters.{n}}}}}"
    tasks = [nb_task("setup_mcp", "setup_mcp_servers.py",
                     {p.name: ref(p.name) for p in params})]
    return (f"{app_name}_setup_mcp_servers", tasks, params,
            [make_cluster("cluster", single_node=True)], "setup_mcp_servers_job", 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or Update Jobs
# MAGIC
# MAGIC Jobs are created **without** SPN ACLs -- those are granted by Notebook 02
# MAGIC after the app (and its service principal) exist.

# COMMAND ----------

ESSENTIAL_JOBS = [
    build_metadata_generator_job,
    build_parallel_modes_job,
    build_analytics_pipeline_job,
    build_kb_job,
    build_sync_ddl_job,
    build_metadata_serverless_job,
    build_parallel_serverless_job,
    build_setup_mcp_servers_job,
]

if mode == "teardown":
    print("=== Teardown mode -- skipping job creation ===")
else:
    existing_jobs = {j.settings.name: j for j in w.jobs.list()
                     if j.settings and j.settings.name}
    created_jobs = {}

    serverless_env = [jobs.JobEnvironment(
        environment_key="default",
        spec=compute.Environment(
            client="1",
            dependencies=[whl_volume_path] if whl_volume_path else []))]

    for builder in ESSENTIAL_JOBS:
        name, tasks, params, clusters, res_name, max_concurrent = builder()
        is_serverless = clusters is None
        settings = jobs.JobSettings(
            name=name, tasks=tasks, parameters=params,
            max_concurrent_runs=max_concurrent)
        if is_serverless:
            settings.environments = serverless_env
            if budget_policy_id:
                settings.budget_policy_id = budget_policy_id
        else:
            settings.job_clusters = clusters
        if name in existing_jobs:
            jid = existing_jobs[name].job_id
            w.jobs.reset(job_id=jid, new_settings=settings)
            created_jobs[res_name] = jid
            print(f"  Updated: {name} (id={jid})")
        else:
            result = w.jobs.create(settings=settings)
            created_jobs[res_name] = result.job_id
            print(f"  Created: {name} (id={result.job_id})")

    print(f"\n{len(created_jobs)} essential jobs ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vector Search Endpoint

# COMMAND ----------

if mode == "setup":
    try:
        ep = w.vector_search_endpoints.get_endpoint(vs_endpoint_name)
        print(f"VS endpoint '{vs_endpoint_name}' exists (state={ep.endpoint_status.state})")
    except NotFound:
        print(f"Creating VS endpoint '{vs_endpoint_name}'...")
        w.vector_search_endpoints.create_endpoint(
            name=vs_endpoint_name, endpoint_type=vs_svc.EndpointType.STANDARD)
        print("Creation requested -- may take a few minutes to come online")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teardown: Delete Jobs

# COMMAND ----------

if mode == "teardown":
    print("=== Deleting essential jobs ===")
    existing_jobs = {j.settings.name: j for j in w.jobs.list()
                     if j.settings and j.settings.name}
    for builder in ESSENTIAL_JOBS:
        jname = builder()[0]
        if jname in existing_jobs:
            w.jobs.delete(job_id=existing_jobs[jname].job_id)
            print(f"  Deleted: {jname}")
        else:
            print(f"  Not found: {jname}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teardown: Vector Search Endpoint

# COMMAND ----------

if mode == "teardown":
    try:
        w.vector_search_endpoints.delete_endpoint(vs_endpoint_name)
        print(f"Deleted VS endpoint '{vs_endpoint_name}'")
    except NotFound:
        print(f"VS endpoint '{vs_endpoint_name}' not found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teardown: Drop Schema (optional)

# COMMAND ----------

if mode == "teardown":
    dbutils.widgets.dropdown("confirm_drop_schema", "no", ["no", "yes"],
                             "DROP SCHEMA CASCADE? (destroys all tables)")
    if dbutils.widgets.get("confirm_drop_schema") == "yes":
        from databricks.sdk.service.sql import StatementState
        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"DROP SCHEMA IF EXISTS `{catalog_name}`.`{schema_name}` CASCADE",
            wait_timeout="30s")
        print(f"Schema dropped ({resp.status.state.value})")
    else:
        print("Schema preserved (set confirm_drop_schema=yes to drop)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

if mode == "setup":
    print("=" * 60)
    print(f"Wheel:       {whl_volume_path}")
    print(f"Jobs:        {len(created_jobs)} created/updated")
    for res_name, jid in created_jobs.items():
        print(f"  {res_name} -> {jid}")
    print(f"VS endpoint: {vs_endpoint_name}")
    print("=" * 60)
    print("\nNext: Run Notebook 02 to deploy the app and grant permissions.")
elif mode == "teardown":
    print("Teardown complete. Run Notebook 02 with mode=destroy to delete the app.")
