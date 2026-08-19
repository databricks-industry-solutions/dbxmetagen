# Databricks notebook source
# One-off validation of the generic-column-name FK guard (federation 'id'
# over-matching fix). Builds a synthetic knowledge graph with the pathogenic
# pattern -- several tables each carrying a bare `id` column -- plus one
# legitimate customer_id -> dim_customer.id relationship, then runs the DEPLOYED
# FKPredictor candidate generators and checks:
#   * id <-> id (both-generic, uncorroborated) pairs are DROPPED
#   * a legit customer_id -> dim_customer.id classic pair is KEPT
#   * disabling the guard (generic_column_names=()) reproduces the explosion
# Prints a PASS/FAIL summary. Safe: writes only to a scratch schema it creates.

import json
from pyspark.sql import SparkSession
from dbxmetagen.fk_prediction import FKPredictionConfig, FKPredictor

spark = SparkSession.builder.getOrCreate()

SCRATCH = "dbxmetagen_fkguard_validation"

catalog = "eswanson_demo"
try:
    catalog = dbutils.widgets.get("catalog_name") or catalog  # noqa: F821
except Exception:
    pass

fqschema = f"{catalog}.{SCRATCH}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqschema}")

nodes_t = f"{fqschema}.graph_nodes"
edges_t = f"{fqschema}.graph_edges"

# COMMAND ----------

# Synthetic tables:
#   orders(id, customer_id, amount)
#   customers(id, region)           -- referenced by orders.customer_id
#   products(id, sku)
#   shipments(id, status)
# Bare `id` exists on all four -> id<->id fan-out is the bug.
# customer_id (orders) -> customers is the ONE legit FK (classic strategy).
tbl = lambda t: f"{catalog}.{SCRATCH}.{t}"
cols = [
    # (col fq id,                     parent table fq,      dtype)
    (f"{tbl('orders')}.id",           tbl("orders"),        "bigint"),
    (f"{tbl('orders')}.customer_id",  tbl("orders"),        "bigint"),
    (f"{tbl('orders')}.amount",       tbl("orders"),        "double"),
    (f"{tbl('customers')}.id",        tbl("customers"),     "bigint"),
    (f"{tbl('customers')}.region",    tbl("customers"),     "string"),
    (f"{tbl('products')}.id",         tbl("products"),      "bigint"),
    (f"{tbl('products')}.sku",        tbl("products"),      "string"),
    (f"{tbl('shipments')}.id",        tbl("shipments"),     "bigint"),
    (f"{tbl('shipments')}.status",    tbl("shipments"),     "string"),
]
node_rows = [(cid, parent, dt, "column") for (cid, parent, dt) in cols]
spark.createDataFrame(node_rows, "id string, parent_id string, data_type string, node_type string") \
    .write.mode("overwrite").saveAsTable(nodes_t)

# Embedding edges: identical column names embed near-identically. Give every
# bare `id` <-> `id` pair a high similarity (the explosion source), plus
# customer_id <-> customers.id (the legit link).
id_ids = [c for (c, _, _) in cols if c.endswith(".id")]
edge_rows = []
for i in range(len(id_ids)):
    for j in range(i + 1, len(id_ids)):
        edge_rows.append((id_ids[i], id_ids[j], 0.98, "similar_embedding"))
# legit: orders.customer_id ~ customers.id
edge_rows.append((f"{tbl('orders')}.customer_id", f"{tbl('customers')}.id", 0.95, "similar_embedding"))
spark.createDataFrame(edge_rows, "src string, dst string, weight double, relationship string") \
    .write.mode("overwrite").saveAsTable(edges_t)

print(f"Synthetic graph: {len(node_rows)} nodes, {len(edge_rows)} embedding edges")
print(f"  bare-id columns: {len(id_ids)} -> id<->id pairs possible: {len(id_ids)*(len(id_ids)-1)//2}")

# COMMAND ----------

def _run(generic_names):
    cfg = FKPredictionConfig(
        catalog_name=catalog, schema_name=SCRATCH,
        incremental=False,
    )
    if generic_names is not None:
        cfg.generic_column_names = generic_names
    p = FKPredictor(spark, cfg)
    p._changed_tables = None
    emb = p.get_candidates()
    name = p.get_name_based_candidates()
    def _pairs(df):
        out = []
        for r in df.select("col_a", "col_b").collect():
            a = r.col_a.split(".")[-1].lower()
            b = r.col_b.split(".")[-1].lower()
            out.append((a, b, r.col_a.split(".")[-2], r.col_b.split(".")[-2]))
        return out
    return _pairs(emb), _pairs(name)

# Guard ON (default generic list)
emb_on, name_on = _run(None)
# Guard OFF (empty generic list = old behavior)
emb_off, name_off = _run(())

def _both_id(pairs):
    return [p for p in pairs if p[0] == "id" and p[1] == "id"]

def _has_legit(pairs):
    # orders.customer_id <-> customers.id (order-normalized col_a<=col_b)
    return any({p[0], p[1]} == {"customer_id", "id"} for p in pairs)

print("\n=== GUARD OFF (old behavior) ===")
print(f"embedding id<->id pairs: {len(_both_id(emb_off))}")
print(f"name      id<->id pairs: {len(_both_id(name_off))}")

print("\n=== GUARD ON (fix) ===")
print(f"embedding id<->id pairs: {len(_both_id(emb_on))}  (expect 0)")
print(f"name      id<->id pairs: {len(_both_id(name_on))}  (expect 0)")
print(f"legit customer_id<->id present (embedding): {_has_legit(emb_on)}  (expect True)")

# COMMAND ----------

checks = {
    "guard_off_shows_explosion": len(_both_id(emb_off)) >= 3,
    "guard_on_drops_id_x_id_embedding": len(_both_id(emb_on)) == 0,
    "guard_on_drops_id_x_id_name": len(_both_id(name_on)) == 0,
    "guard_on_keeps_legit_customer_id": _has_legit(emb_on),
}
print("\n=== RESULT ===")
for k, v in checks.items():
    print(f"  [{'PASS' if v else 'FAIL'}] {k}")
allpass = all(checks.values())
print(f"\nOVERALL: {'PASS' if allpass else 'FAIL'}")

# cleanup
spark.sql(f"DROP SCHEMA IF EXISTS {fqschema} CASCADE")
print(f"cleaned up {fqschema}")

dbutils.notebook.exit(json.dumps({"pass": allpass, "checks": checks,  # noqa: F821
                                  "guard_off_id_pairs": len(_both_id(emb_off)),
                                  "guard_on_id_pairs": len(_both_id(emb_on))}))
