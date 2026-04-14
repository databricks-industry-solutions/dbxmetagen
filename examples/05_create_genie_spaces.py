# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5: Create Genie Spaces
# MAGIC
# MAGIC This is the payoff -- creates Genie spaces from all the metadata, knowledge bases,
# MAGIC FK predictions, ontology, and metric views built in previous steps.
# MAGIC
# MAGIC The `build_genie_space` function:
# MAGIC 1. Assembles context from your metadata artifacts (KB, FKs, ontology, metrics)
# MAGIC 2. Runs a 3-phase LLM agent to generate instructions, SQL snippets, and synonyms
# MAGIC 3. Creates the Genie space via REST API
# MAGIC 4. Exports the space JSON to a UC Volume for backup/versioning
# MAGIC
# MAGIC **Auto-splitting:** Genie spaces degrade above ~25 tables. When the table count
# MAGIC exceeds the limit, this notebook groups tables by ontology entity type and creates
# MAGIC one space per group. FK-connected groups are merged to keep joinable tables together.
# MAGIC
# MAGIC **Prerequisites:** Steps 1-4 and a SQL warehouse ID in the widget.

# COMMAND ----------

# MAGIC %pip install -qqq https://github.com/databricks-industry-solutions/dbxmetagen/archive/refs/heads/main.zip
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./helpers/common

# COMMAND ----------

assert WAREHOUSE_ID, "warehouse_id is required -- fill in the widget above (SQL Warehouse ID)"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve Tables
# MAGIC
# MAGIC If `table_names` is set, uses that. Otherwise reads the source tables that were
# MAGIC processed in step 01 from the `table_knowledge_base`.

# COMMAND ----------

if TABLE_NAMES:
    tables = resolve_tables(TABLE_NAMES, CATALOG, SCHEMA)
else:
    print("No table_names widget set -- reading source tables from table_knowledge_base")
    tables = resolve_source_tables_from_kb(CATALOG, SCHEMA)

print(f"Tables for Genie: {len(tables)}")
for t in tables:
    print(f"  {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Group Tables for Multiple Spaces
# MAGIC
# MAGIC If there are more tables than fit in a single space, we group by ontology
# MAGIC entity type and merge FK-connected groups. This keeps related tables together
# MAGIC and gives each space a coherent topic.

# COMMAND ----------

import math
from collections import defaultdict

MAX_TABLES_PER_SPACE = 25


def group_by_ontology(all_tables: list[str], max_per_space: int) -> dict[str, list[str]] | None:
    """Group tables using ontology entity_type, merge FK-connected groups."""
    table_set = set(all_tables)
    groups = defaultdict(set)

    try:
        ent_rows = spark.sql(
            f"SELECT entity_type, source_tables FROM {fqn('ontology_entities')}"
        ).collect()
    except Exception:
        return None

    for row in ent_rows:
        etype = row.entity_type or "uncategorized"
        for t in (row.source_tables or []):
            if t in table_set:
                groups[etype].add(t)

    assigned = set().union(*groups.values()) if groups else set()
    unassigned = table_set - assigned
    if unassigned:
        groups["uncategorized"].update(unassigned)

    try:
        fk_rows = spark.sql(
            f"SELECT source_table, target_table FROM {fqn('fk_predictions')}"
        ).collect()
        fk_pairs = [(r.source_table, r.target_table) for r in fk_rows]
    except Exception:
        fk_pairs = []

    if fk_pairs:
        table_to_group = {}
        for g, tbls in groups.items():
            for t in tbls:
                table_to_group[t] = g
        for src, tgt in fk_pairs:
            g1, g2 = table_to_group.get(src), table_to_group.get(tgt)
            if g1 and g2 and g1 != g2:
                merged = groups[g1] | groups[g2]
                if len(merged) <= max_per_space:
                    groups[g1] = merged
                    for t in groups[g2]:
                        table_to_group[t] = g1
                    del groups[g2]

    final = {}
    for name, tbls in groups.items():
        tbl_list = sorted(tbls)
        if len(tbl_list) <= max_per_space:
            final[name] = tbl_list
        else:
            n_chunks = math.ceil(len(tbl_list) / max_per_space)
            chunk_size = math.ceil(len(tbl_list) / n_chunks)
            for i in range(n_chunks):
                final[f"{name} ({i + 1})"] = tbl_list[i * chunk_size : (i + 1) * chunk_size]
    return final


if len(tables) <= MAX_TABLES_PER_SPACE:
    space_groups = {"all": tables}
    print(f"Table count ({len(tables)}) within limit -- creating single space")
else:
    print(f"Table count ({len(tables)}) exceeds {MAX_TABLES_PER_SPACE} -- grouping for multiple spaces")
    space_groups = group_by_ontology(tables, MAX_TABLES_PER_SPACE)
    if space_groups is None:
        print("  ontology_entities not found -- falling back to even batches")
        sorted_t = sorted(tables)
        n = math.ceil(len(sorted_t) / MAX_TABLES_PER_SPACE)
        cs = math.ceil(len(sorted_t) / n)
        space_groups = {f"batch {i+1}": sorted_t[i*cs:(i+1)*cs] for i in range(n)}
    else:
        print(f"  Grouped into {len(space_groups)} spaces using ontology")

print(f"\nSpace plan ({len(space_groups)} spaces):")
for name, tbls in space_groups.items():
    print(f"  [{name}] {len(tbls)} tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build and Create Genie Spaces
# MAGIC
# MAGIC Each group gets its own Genie space. The 3-phase LLM agent generates:
# MAGIC 1. **Core config:** title, description, instructions
# MAGIC 2. **SQL snippets:** pre-built queries for common questions
# MAGIC 3. **Synonyms and joins:** column aliases and join specifications from FK predictions

# COMMAND ----------

from dbxmetagen.genie import build_genie_space

results = []

for group_name, group_tables in space_groups.items():
    if len(space_groups) == 1 and group_name == "all":
        title = f"{CATALOG}.{SCHEMA} Analytics"
    else:
        title = f"{CATALOG}.{SCHEMA} - {group_name}"

    print(f"\nCreating space: {title} ({len(group_tables)} tables)...")
    result = build_genie_space(
        warehouse_id=WAREHOUSE_ID,
        catalog=CATALOG,
        schema=SCHEMA,
        table_identifiers=group_tables,
        title=title,
        model_endpoint=MODEL,
        create=True,
    )
    space_id = result.get("space_id", "unknown")
    print(f"  Created: {space_id}")
    results.append({"group": group_name, "result": result, "tables": group_tables})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Space JSONs to Volume
# MAGIC
# MAGIC Saves each space's full definition and serialized config to the UC Volume.
# MAGIC Useful for version control, backup, or deploying the same space to another workspace.

# COMMAND ----------

import json, re, pathlib
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
outdir = pathlib.Path(f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/genie_exports")
outdir.mkdir(exist_ok=True, parents=True)

for entry in results:
    r = entry["result"]
    sid = r.get("space_id", "unknown")
    safe_title = re.sub(r"[^a-zA-Z0-9_]", "_", r["title"])[:50]
    base = f"{sid}__{safe_title}"

    full = ws.api_client.do("GET", f"/api/2.0/genie/spaces/{sid}", query={"include_serialized_space": "true"})
    (outdir / f"{base}.space.json").write_text(json.dumps(full, indent=2), encoding="utf-8")
    (outdir / f"{base}.serialized.json").write_text(json.dumps(r["serialized_space"], indent=2), encoding="utf-8")
    print(f"Exported: {base}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"\nGenie spaces created: {len(results)}")
for entry in results:
    r = entry["result"]
    print(f"  [{entry['group']}] {r.get('space_id','?')} -- {r['title']} ({len(entry['tables'])} tables)")
print(f"\nExport dir: {outdir}")
print("\nOpen any space in the Databricks UI to start asking questions about your data.")
