# Databricks notebook source
# MAGIC %md
# MAGIC # Graph Node Clustering Analysis
# MAGIC
# MAGIC Discovers natural groupings among graph node embeddings (tables, columns, etc.)
# MAGIC using K-means clustering. Clusters reveal which entities are semantically similar
# MAGIC based on their metadata descriptions, powering downstream reporting, ontology
# MAGIC refinement, and anomaly detection.
# MAGIC
# MAGIC ## Approach: Three-Phase Optimal K Selection
# MAGIC
# MAGIC Finding the right number of clusters (K) is the core challenge. This notebook
# MAGIC automates it with a three-phase strategy:
# MAGIC
# MAGIC 1. **Broad Pass** -- Fast coarse scan over the K range using only WSSSE (O(n) per K)
# MAGIC    on a 25% sample. Finds the elbow region via second-derivative analysis.
# MAGIC 2. **Narrow Pass** -- Accurate refined search around the elbow using silhouette scoring
# MAGIC    (sampled to 10K nodes for O(n^2) feasibility) on a 60% sample. Selects K by
# MAGIC    stability-weighted silhouette: `mean - 0.5 * std`.
# MAGIC 3. **Final Clustering** -- Full-data K-means with 5 random seeds, keeping the best.
# MAGIC
# MAGIC ## Scaling to Large Datasets
# MAGIC
# MAGIC This notebook handles 1M+ nodes without code changes:
# MAGIC - K-means training is distributed via Spark MLlib (classic) or sklearn (serverless).
# MAGIC - Silhouette is always computed on a fixed-size sample (default 10K).
# MAGIC - DataFrame checkpointing auto-enables above 50K nodes to truncate lineage (classic only).
# MAGIC - Broad/narrow phases use percentage-based sampling, not full scans.
# MAGIC
# MAGIC ### Serverless Compute
# MAGIC
# MAGIC On serverless, Spark MLlib is unavailable. The notebook auto-detects this and
# MAGIC uses scikit-learn on the driver instead. All embeddings are collected to a numpy
# MAGIC array, so driver memory is the constraint:
# MAGIC
# MAGIC | Nodes | Memory (768-dim) | Status |
# MAGIC |---|---|---|
# MAGIC | 10K | ~30 MB | Safe |
# MAGIC | 100K | ~300 MB | Safe |
# MAGIC | 200K (default limit) | ~600 MB | Safe |
# MAGIC | 500K | ~1.5 GB | Tight -- raise limit only with sufficient driver memory |
# MAGIC | 1M+ | ~3 GB+ | Use classic compute with MLlib |
# MAGIC
# MAGIC The limit is controlled by `_SERVERLESS_NODE_LIMIT` in the setup cell.
# MAGIC "Nodes" means graph_nodes matching the `node_types` filter (default: tables only).
# MAGIC Clustering results are directionally equivalent between MLlib and sklearn.
# MAGIC
# MAGIC For tuning at extreme scale, see `docs/KMEANS_CLUSTERING.md`.
# MAGIC
# MAGIC ## Output Tables
# MAGIC
# MAGIC | Table | Contents |
# MAGIC |---|---|
# MAGIC | `node_cluster_assignments` | Node ID, cluster label, K, silhouette score |
# MAGIC | `clustering_metrics` | Full WSSSE + silhouette history per K per phase |

# COMMAND ----------

# MAGIC # Uncomment below when running outside of a DAB-deployed job
# MAGIC # %pip install /Workspace/Users/<your_username>/.bundle/dbxmetagen/dev/artifacts/.internal/dbxmetagen-*.whl
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("min_k", "2", "Minimum K")
dbutils.widgets.text("max_k", "20", "Maximum K")
dbutils.widgets.text("node_types", "table", "Node types to cluster (comma-separated)")
dbutils.widgets.text("table_names", "", "Table Names (comma-separated, empty for all)")
dbutils.widgets.text("incremental", "true", "Incremental")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
min_k = int(dbutils.widgets.get("min_k"))
max_k = int(dbutils.widgets.get("max_k"))
node_types = [t.strip() for t in dbutils.widgets.get("node_types").split(",")]

if not catalog_name or not schema_name:
    raise ValueError("catalog_name and schema_name are required")

print(f"Clustering nodes in {catalog_name}.{schema_name}")
print(f"K range: {min_k} to {max_k}")
print(f"Node types: {node_types}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup and Data Loading

# COMMAND ----------

import sys
sys.path.append("../src")  # For git-clone or DAB deployment; pip-installed package works without this

from dbxmetagen.table_filter import parse_table_names
table_names = parse_table_names(dbutils.widgets.get("table_names").strip()) or None

if table_names:
    print("Skipping clustering -- table_names scoping active, run full refresh to re-cluster")
    dbutils.notebook.exit("skipped")

incremental = dbutils.widgets.get("incremental").strip().lower() in ("true", "1", "yes")
if incremental:
    nodes_table = f"{catalog_name}.{schema_name}.graph_nodes"
    assign_table = f"{catalog_name}.{schema_name}.node_cluster_assignments"
    try:
        max_emb = spark.sql(
            f"SELECT COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01') AS mu "
            f"FROM {nodes_table} WHERE embedding IS NOT NULL"
        ).collect()[0].mu
        max_assign = spark.sql(
            f"SELECT COALESCE(MAX(created_at), TIMESTAMP '1970-01-01') AS mu FROM {assign_table}"
        ).collect()[0].mu
        if max_emb <= max_assign:
            print(f"Incremental: no new embeddings since last cluster run ({max_assign} >= {max_emb}), skipping")
            dbutils.notebook.exit("skipped_incremental")
    except Exception as e:
        print(f"Watermark check failed ({e}), running full cluster analysis")

import mlflow
mlflow.autolog(disable=True)

import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from datetime import datetime
import json

SILHOUETTE_SAMPLE_THRESHOLD = 10000
CHECKPOINT_THRESHOLD = 50000

# Serverless (Spark Connect) doesn't support MLlib or .cache()/.persist().
# Detect once and route to sklearn (serverless) or MLlib (classic).
try:
    spark.sparkContext
    _SERVERLESS = False
except Exception:
    _SERVERLESS = True

if _SERVERLESS:
    from sklearn.cluster import KMeans as SKLearnKMeans
    from sklearn.metrics import silhouette_score as sklearn_silhouette_score
else:
    from pyspark.ml.clustering import KMeans as SparkKMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    from pyspark.ml.linalg import VectorUDT, DenseVector

print(f"Compute mode: {'serverless (sklearn)' if _SERVERLESS else 'classic (MLlib)'}")

# COMMAND ----------

# Load nodes with embeddings
nodes_table = f"{catalog_name}.{schema_name}.graph_nodes"

# Filter to specified node types with valid embeddings
node_type_filter = " OR ".join([f"node_type = '{nt}'" for nt in node_types])

nodes_df = spark.sql(
    f"""
    SELECT 
        id,
        node_type,
        domain,
        security_level,
        comment,
        embedding
    FROM {nodes_table}
    WHERE embedding IS NOT NULL
      AND SIZE(embedding) > 0
      AND ({node_type_filter})
"""
)

total_nodes = nodes_df.count()
print(f"Found {total_nodes} nodes with embeddings")

# Serverless clustering collects all embeddings to the driver as a numpy array.
# Memory cost: total_nodes * embedding_dim * 4 bytes (float32).
#   - 200K nodes * 768 dims = ~600 MB (safe for 8-16 GB serverless driver)
#   - 500K nodes * 768 dims = ~1.5 GB (tight; may OOM with other driver overhead)
# "Nodes" here means graph_nodes matching the node_types filter -- typically
# tables only (default), but includes columns if node_types=table,column.
# To raise the limit: increase _SERVERLESS_NODE_LIMIT below and ensure the
# serverless cluster type has sufficient driver memory. Above ~500K, consider
# using classic compute with MLlib (distributed) instead.
_SERVERLESS_NODE_LIMIT = 200_000

total_table_nodes = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM {nodes_table} WHERE ({node_type_filter})"
).collect()[0].cnt

if total_table_nodes < min_k:
    print(f"Only {total_table_nodes} node(s) of type {node_types} in graph -- clustering requires >= {min_k}. Skipping.")
    dbutils.notebook.exit("skipped_insufficient_nodes")

if total_nodes < min_k:
    raise ValueError(
        f"Found {total_table_nodes} node(s) of type {node_types} but only {total_nodes} have embeddings. "
        f"Embedding generation may have failed -- check the generate_embeddings task output."
    )

if _SERVERLESS and total_nodes > _SERVERLESS_NODE_LIMIT:
    raise ValueError(
        f"Too many nodes ({total_nodes:,}) for serverless clustering "
        f"(limit: {_SERVERLESS_NODE_LIMIT:,}). On serverless, all embeddings are "
        f"collected to the driver (~{total_nodes * 768 * 4 / 1e9:.1f} GB for 768-dim). "
        f"Options: (1) raise _SERVERLESS_NODE_LIMIT if driver memory allows, "
        f"(2) filter node_types to reduce scope, or "
        f"(3) use classic compute with MLlib for distributed clustering."
    )

# Adjust max_k if we have fewer nodes
effective_max_k = min(max_k, total_nodes - 1)
print(f"Effective K range: {min_k} to {effective_max_k}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Prepare Embeddings

# COMMAND ----------

if _SERVERLESS:
    # Collect to driver for sklearn clustering
    _pdf = nodes_df.select(
        "id", "node_type", "domain", "security_level", "comment", "embedding"
    ).toPandas()
    _embeddings = np.array(_pdf["embedding"].tolist(), dtype=np.float32)
    _node_meta = _pdf.drop(columns=["embedding"])
    vector_count = len(_embeddings)
    embedding_dim = _embeddings.shape[1]
    _full_data = _embeddings
    print(f"Prepared {vector_count} nodes (collected to driver for sklearn)")
    print(f"Embedding dimension: {embedding_dim}")
else:
    @F.udf(returnType=VectorUDT())
    def array_to_vector(arr):
        if arr is None or len(arr) == 0:
            return None
        return DenseVector([float(x) for x in arr])

    nodes_with_vectors = (
        nodes_df.withColumn("features", array_to_vector(F.col("embedding")))
        .filter(F.col("features").isNotNull())
        .cache()
    )
    vector_count = nodes_with_vectors.count()
    print(f"Prepared {vector_count} nodes with valid vectors")

    if vector_count > CHECKPOINT_THRESHOLD:
        print(f"Large dataset ({vector_count} > {CHECKPOINT_THRESHOLD}), checkpointing...")
        spark.sparkContext.setCheckpointDir("/tmp/clustering_checkpoint")
        nodes_with_vectors = nodes_with_vectors.checkpoint()
        print("Checkpoint complete -- lineage truncated")

    sample_embedding = nodes_with_vectors.select("features").first()
    embedding_dim = len(sample_embedding.features)
    _full_data = nodes_with_vectors
    print(f"Embedding dimension: {embedding_dim}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## K-Means Clustering Functions

# COMMAND ----------


def _compute_silhouette(data, labels, sample_threshold=SILHOUETTE_SAMPLE_THRESHOLD):
    """Compute silhouette score, sampling for large datasets."""
    if _SERVERLESS:
        n = len(labels)
        if n <= 1 or len(set(labels)) <= 1:
            return 0.0
        if n > sample_threshold:
            idx = np.random.RandomState(42).choice(n, sample_threshold, replace=False)
            return float(sklearn_silhouette_score(data[idx], labels[idx]))
        return float(sklearn_silhouette_score(data, labels))
    else:
        evaluator = ClusteringEvaluator(
            featuresCol="features", predictionCol="cluster",
            metricName="silhouette", distanceMeasure="squaredEuclidean",
        )
        count = data.count()
        if count > sample_threshold:
            return evaluator.evaluate(data.sample(fraction=sample_threshold / count, seed=42))
        return evaluator.evaluate(data)


def _run_kmeans_wssse(data, k, seed=42, max_iter=30):
    """Run KMeans, return (model, wssse)."""
    if _SERVERLESS:
        km = SKLearnKMeans(n_clusters=k, random_state=seed, max_iter=max_iter, n_init=1)
        km.fit(data)
        return km, float(km.inertia_)
    else:
        kmeans = SparkKMeans(
            k=k, seed=seed, maxIter=max_iter, initMode="k-means||",
            featuresCol="features", predictionCol="cluster",
        )
        model = kmeans.fit(data)
        return model, float(model.summary.trainingCost)


def _run_kmeans_full(data, k, seed=42, max_iter=50):
    """Run KMeans, return (model, predictions_or_labels, silhouette, wssse)."""
    if _SERVERLESS:
        km = SKLearnKMeans(n_clusters=k, random_state=seed, max_iter=max_iter, n_init=1)
        labels = km.fit_predict(data)
        wssse = float(km.inertia_)
        sil = _compute_silhouette(data, labels)
        return km, labels, sil, wssse
    else:
        kmeans = SparkKMeans(
            k=k, seed=seed, maxIter=max_iter, initMode="k-means||",
            featuresCol="features", predictionCol="cluster",
        )
        model = kmeans.fit(data)
        wssse = float(model.summary.trainingCost)
        predictions = model.transform(data)
        sil = _compute_silhouette(predictions, None)
        return model, predictions, sil, wssse


def _sample_data(data, fraction, seed):
    """Sample: numpy slice on serverless, DF.sample on classic."""
    if _SERVERLESS:
        n = max(1, int(len(data) * fraction))
        idx = np.random.RandomState(seed).choice(len(data), n, replace=False)
        return data[idx]
    return data.sample(fraction=fraction, seed=seed)


def _data_count(data):
    return len(data) if _SERVERLESS else data.count()


def evaluate_k_range_wssse_only(data, k_range, n_seeds=2, max_iter=20):
    """Phase 1: evaluate K values using WSSSE only."""
    results = []
    for k in k_range:
        wssses = []
        for seed_i in range(n_seeds):
            try:
                _, wssse = _run_kmeans_wssse(data, k, seed=seed_i * 42, max_iter=max_iter)
                wssses.append(wssse)
            except Exception as e:
                print(f"  K={k}, seed={seed_i}: Error - {e}")
        if wssses:
            results.append({
                "k": k, "wssse_mean": float(np.mean(wssses)),
                "wssse_std": float(np.std(wssses)), "n_runs": len(wssses),
            })
            print(f"  K={k}: WSSSE={np.mean(wssses):.2f} (+-{np.std(wssses):.2f})")
    return results


def evaluate_k_range_with_silhouette(data, k_range, n_seeds=3, max_iter=30):
    """Phase 2: evaluate K values with WSSSE and silhouette."""
    results = []
    for k in k_range:
        silhouettes, wssses = [], []
        for seed_i in range(n_seeds):
            try:
                _, _, sil, wssse = _run_kmeans_full(
                    data, k, seed=seed_i * 42, max_iter=max_iter,
                )
                silhouettes.append(sil)
                wssses.append(wssse)
            except Exception as e:
                print(f"  K={k}, seed={seed_i}: Error - {e}")
        if silhouettes:
            results.append({
                "k": k, "silhouette_mean": float(np.mean(silhouettes)),
                "silhouette_std": float(np.std(silhouettes)),
                "wssse_mean": float(np.mean(wssses)),
                "wssse_std": float(np.std(wssses)), "n_runs": len(silhouettes),
            })
            print(
                f"  K={k}: silhouette={np.mean(silhouettes):.4f} (+-{np.std(silhouettes):.4f})"
            )
    return results


# COMMAND ----------
# MAGIC %md
# MAGIC ## Phase 1: Broad Search (WSSSE Only)
# MAGIC
# MAGIC Fast coarse search using only WSSSE (O(n) per K) to identify promising regions.
# MAGIC
# MAGIC **Sampling strategy**: We use 25% of the data (minimum 500 nodes) for speed.
# MAGIC At 1M nodes this means ~250K points, which is still fast because WSSSE only
# MAGIC computes each point's distance to its assigned center. We test K values in
# MAGIC coarse steps and find the elbow via second-derivative analysis of the WSSSE
# MAGIC curve -- the point where adding more clusters stops giving meaningful
# MAGIC improvement.

# COMMAND ----------

print("=" * 60)
print("PHASE 1: Broad Search (WSSSE only for speed)")
print("=" * 60)

# Sample for broad search (25% or at least 500 nodes)
sample_fraction_broad = min(0.25, max(500 / vector_count, 0.1))
broad_sample = _sample_data(_full_data, sample_fraction_broad, seed=42)
broad_sample_count = _data_count(broad_sample)
if not _SERVERLESS:
    broad_sample = broad_sample.cache()
print(
    f"Broad search sample: {broad_sample_count} nodes ({sample_fraction_broad*100:.1f}%)"
)

if broad_sample_count < min_k:
    print(f"Sample too small ({broad_sample_count} < {min_k}), using full dataset")
    if not _SERVERLESS:
        broad_sample.unpersist()
    broad_sample = _full_data
    broad_sample_count = vector_count

# Test K values in steps
k_step = max(1, (effective_max_k - min_k) // 10)
broad_k_range = list(range(min_k, effective_max_k + 1, k_step))
if effective_max_k not in broad_k_range:
    broad_k_range.append(effective_max_k)
broad_k_range = [k for k in broad_k_range if k < broad_sample_count]
if not broad_k_range:
    broad_k_range = [min_k]

print(f"Testing K values: {broad_k_range}")

# WSSSE-only evaluation (fast)
broad_results = evaluate_k_range_wssse_only(
    broad_sample, broad_k_range, n_seeds=2, max_iter=20
)

# Find elbow via second derivative of WSSSE curve.
# The second derivative is highest at the K where the rate of WSSSE decrease
# changes most -- i.e., the "elbow". This is more robust than eyeballing the
# curve and works without user intervention.
wssse_values = [r["wssse_mean"] for r in broad_results]
k_values = [r["k"] for r in broad_results]

# Calculate second derivative to find elbow
if len(wssse_values) >= 3:
    second_deriv = []
    for i in range(1, len(wssse_values) - 1):
        d2 = wssse_values[i - 1] - 2 * wssse_values[i] + wssse_values[i + 1]
        second_deriv.append((k_values[i], d2))
    best_broad_k = max(second_deriv, key=lambda x: x[1])[0]
else:
    # Fallback: pick middle K
    best_broad_k = k_values[len(k_values) // 2]

print(f"\nBest K from broad search (elbow): {best_broad_k}")

if not _SERVERLESS:
    broad_sample.unpersist()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Phase 2: Narrow Search (with Silhouette)
# MAGIC
# MAGIC Refined search around the elbow region with silhouette scoring.
# MAGIC
# MAGIC **Why silhouette?** WSSSE always decreases with K and doesn't tell you when
# MAGIC clusters stop being meaningful. Silhouette compares intra-cluster compactness
# MAGIC to inter-cluster separation -- it peaks at the K where clusters are most
# MAGIC distinct.
# MAGIC
# MAGIC **Stability weighting**: We select K by `mean_silhouette - 0.5 * std_silhouette`
# MAGIC rather than raw mean. This penalizes K values that score well on one seed but
# MAGIC poorly on another, favoring K values that produce consistent clusters.

# COMMAND ----------

print("=" * 60)
print("PHASE 2: Narrow Search (with silhouette scoring)")
print("=" * 60)

# Define narrow range around best K
narrow_radius = max(2, k_step)
narrow_min_k = max(min_k, best_broad_k - narrow_radius)
narrow_max_k = min(effective_max_k, best_broad_k + narrow_radius)
narrow_k_range = list(range(narrow_min_k, narrow_max_k + 1))

print(f"Narrow search range: {narrow_k_range}")

# Larger sample for narrow search (60% or at least 1000 nodes)
sample_fraction_narrow = min(0.6, max(1000 / vector_count, 0.3))
narrow_sample = _sample_data(_full_data, sample_fraction_narrow, seed=123)
narrow_sample_count = _data_count(narrow_sample)
if not _SERVERLESS:
    narrow_sample = narrow_sample.cache()
print(
    f"Narrow search sample: {narrow_sample_count} nodes ({sample_fraction_narrow*100:.1f}%)"
)

if narrow_sample_count < min_k:
    print(f"Narrow sample too small ({narrow_sample_count} < {min_k}), using full dataset")
    if not _SERVERLESS:
        narrow_sample.unpersist()
    narrow_sample = _full_data
    narrow_sample_count = vector_count

narrow_k_range = [k for k in narrow_k_range if k < narrow_sample_count]
if not narrow_k_range:
    narrow_k_range = [min_k]

# Full evaluation with silhouette
narrow_results = evaluate_k_range_with_silhouette(
    narrow_sample, narrow_k_range, n_seeds=5, max_iter=30
)

# Find best K considering both mean and stability
for r in narrow_results:
    r["stability_score"] = r["silhouette_mean"] - 0.5 * r["silhouette_std"]

best_narrow = max(narrow_results, key=lambda x: x["stability_score"])
print(f"\nBest K from narrow search: {best_narrow['k']}")
print(
    f"  Silhouette: {best_narrow['silhouette_mean']:.4f} (+-{best_narrow['silhouette_std']:.4f})"
)
print(f"  Stability score: {best_narrow['stability_score']:.4f}")

if not _SERVERLESS:
    narrow_sample.unpersist()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Phase 3: Final Clustering
# MAGIC
# MAGIC Train the final model on **100% of the data** with the chosen K.
# MAGIC
# MAGIC We run 5 random initializations (`n_final_runs`) and keep the one with the
# MAGIC highest silhouette. This ensemble approach is critical because K-means is
# MAGIC non-convex -- different starting centroids can produce very different clusters.
# MAGIC More iterations (`max_iter=100`) ensure each run converges fully.

# COMMAND ----------

print("=" * 60)
print("PHASE 3: Final Clustering")
print("=" * 60)

optimal_k = best_narrow["k"]
print(f"Final clustering with K={optimal_k} on {vector_count} nodes")

# Run multiple initializations and keep best
best_model = None
best_final_silhouette = -1
best_preds = None
final_results = []

n_final_runs = 5
print(f"Running {n_final_runs} initializations...")

for i in range(n_final_runs):
    model, preds, sil, wssse = _run_kmeans_full(
        _full_data, optimal_k, seed=i * 100 + 42, max_iter=100,
    )
    final_results.append({"run": i, "silhouette": sil, "wssse": wssse})
    print(f"  Run {i+1}: silhouette={sil:.4f}, WSSSE={wssse:.2f}")

    if sil > best_final_silhouette:
        best_final_silhouette = sil
        best_model = model
        best_preds = preds

print(f"\nBest final model: silhouette={best_final_silhouette:.4f}")

# Build final_predictions Spark DF
if _SERVERLESS:
    result_pdf = _node_meta.copy()
    result_pdf["cluster"] = best_preds.astype(int)
    final_predictions = spark.createDataFrame(result_pdf)
else:
    final_predictions = best_preds

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cluster Analysis and Statistics

# COMMAND ----------

# Cluster size distribution
print("=" * 60)
print("CLUSTER STATISTICS")
print("=" * 60)

cluster_stats = (
    final_predictions.groupBy("cluster")
    .agg(
        F.count("*").alias("node_count"),
        F.countDistinct("domain").alias("unique_domains"),
        F.sum(F.when(F.col("security_level") == "PHI", 1).otherwise(0)).alias(
            "phi_count"
        ),
        F.sum(F.when(F.col("security_level") == "PII", 1).otherwise(0)).alias(
            "pii_count"
        ),
    )
    .orderBy("cluster")
)

print("\nCluster sizes and characteristics:")
cluster_stats.display()

# COMMAND ----------

# Domain distribution per cluster
print("\nTop domains per cluster:")
domain_per_cluster = (
    final_predictions.groupBy("cluster", "domain")
    .count()
    .orderBy("cluster", F.desc("count"))
)
domain_per_cluster.display()

# COMMAND ----------

# Cluster centroids (for interpretation)
centroids = best_model.cluster_centers_ if _SERVERLESS else best_model.clusterCenters()
print(
    f"\nCluster centroids shape: {len(centroids)} clusters x {len(centroids[0])} dimensions"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

# Save cluster assignments to a new table
cluster_assignments_table = f"{catalog_name}.{schema_name}.node_cluster_assignments"

# Prepare output
cluster_output = (
    final_predictions.select("id", "node_type", "domain", "security_level", "cluster")
    .withColumn("cluster", F.col("cluster").cast("int"))
    .withColumn("k_value", F.lit(optimal_k))
    .withColumn("silhouette_score", F.lit(best_final_silhouette))
    .withColumn("created_at", F.current_timestamp())
)

# Create or replace table
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {cluster_assignments_table} (
        id STRING,
        node_type STRING,
        domain STRING,
        security_level STRING,
        cluster INT,
        k_value INT,
        silhouette_score DOUBLE,
        created_at TIMESTAMP
    )
    COMMENT 'K-means cluster assignments for graph nodes'
"""
)

# OVERWRITE: Replaces entire `node_cluster_assignments` Delta table with one row per clustered node: `id`, `node_type`,
#   `domain`, `security_level`, `cluster` label, constants `k_value` (chosen K), `silhouette_score`, `created_at`.
# WHY: Downstream analysts and dashboards need the current authoritative partition of nodes — not stale assignments from prior runs or K.
# TRADEOFFS: Full overwrite is simple and fast to query but loses historical assignment lineage (use clustering_metrics append for audit);
#   concurrent runs would race — last writer wins unless external locking is added.
cluster_output.write.mode("overwrite").saveAsTable(cluster_assignments_table)
print(f"\nSaved cluster assignments to {cluster_assignments_table}")

# COMMAND ----------

# Save clustering metrics history
metrics_table = f"{catalog_name}.{schema_name}.clustering_metrics"

# Define explicit schema to avoid type inference issues
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
)

METRICS_SCHEMA = StructType(
    [
        StructField("k", IntegerType(), False),
        StructField("silhouette_mean", DoubleType(), True),
        StructField("silhouette_std", DoubleType(), True),
        StructField("wssse_mean", DoubleType(), True),
        StructField("wssse_std", DoubleType(), True),
        StructField("n_runs", IntegerType(), True),
        StructField("phase", StringType(), True),
        StructField("sample_size", IntegerType(), True),
    ]
)

# Combine all phase results as tuples matching schema order
all_metrics = []
for r in broad_results:
    all_metrics.append(
        (
            int(r["k"]),
            None,  # silhouette_mean not computed in broad phase
            None,  # silhouette_std
            float(r["wssse_mean"]),
            float(r["wssse_std"]),
            int(r["n_runs"]),
            "broad",
            int(broad_sample_count),
        )
    )
for r in narrow_results:
    all_metrics.append(
        (
            int(r["k"]),
            float(r["silhouette_mean"]),
            float(r["silhouette_std"]),
            float(r["wssse_mean"]),
            float(r["wssse_std"]),
            int(r["n_runs"]),
            "narrow",
            int(narrow_sample_count),
        )
    )
for r in final_results:
    all_metrics.append(
        (
            int(optimal_k),
            float(r["silhouette"]),
            0.0,
            float(r["wssse"]),
            0.0,
            1,
            "final",
            int(vector_count),
        )
    )

metrics_df = spark.createDataFrame(all_metrics, schema=METRICS_SCHEMA)
metrics_df = metrics_df.withColumn("run_timestamp", F.current_timestamp())

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {metrics_table} (
        k INT,
        silhouette_mean DOUBLE,
        silhouette_std DOUBLE,
        wssse_mean DOUBLE,
        wssse_std DOUBLE,
        n_runs INT,
        phase STRING,
        sample_size INT,
        run_timestamp TIMESTAMP
    )
    COMMENT 'K-means clustering metrics history'
"""
)

# APPEND: Adds rows to `clustering_metrics` for broad/narrow/final phases (k, silhouette/wssse stats, phase label, sample_size, run_timestamp);
#   `mergeSchema=true` evolves schema if new columns appear.
# WHY: Preserves exploratory history (elbow scans, finalist seeds) alongside the single current assignment snapshot for trending and reproducibility.
# TRADEOFFS: Table grows without automatic retention pruning; mergeSchema relaxes enforcement and can widen types implicitly — favors flexibility over strict contracts.
# Dedup guard: delete metrics from prior runs for the same (k, phase) combos
dedup_pairs = set()
for row in all_metrics:
    dedup_pairs.add((row[0], row[6]))  # (k, phase)
if dedup_pairs:
    pair_clauses = " OR ".join(f"(k = {k} AND phase = '{p}')" for k, p in dedup_pairs)
    try:
        spark.sql(f"DELETE FROM {metrics_table} WHERE {pair_clauses}")
    except Exception as e:
        print(f"Dedup DELETE failed for clustering_metrics, proceeding with append: {e}")
metrics_df.write.mode("append").option("mergeSchema", "true").saveAsTable(metrics_table)
print(f"Saved metrics to {metrics_table}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Visualization: Elbow and Silhouette Plots

# COMMAND ----------

import matplotlib.pyplot as plt

# Get metrics for visualization (narrow results have both metrics)
viz_metrics = [r for r in narrow_results]

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Elbow plot (WSSSE)
k_values = [r["k"] for r in viz_metrics]
wssse_values = [r["wssse_mean"] for r in viz_metrics]
wssse_stds = [r["wssse_std"] for r in viz_metrics]

axes[0].errorbar(k_values, wssse_values, yerr=wssse_stds, marker="o", capsize=5)
axes[0].axvline(x=optimal_k, color="r", linestyle="--", label=f"Optimal K={optimal_k}")
axes[0].set_xlabel("K (Number of Clusters)")
axes[0].set_ylabel("Within-Cluster Sum of Squares")
axes[0].set_title("Elbow Method")
axes[0].legend()
axes[0].grid(True, alpha=0.3)

# Silhouette plot
sil_values = [r["silhouette_mean"] for r in viz_metrics]
sil_stds = [r["silhouette_std"] for r in viz_metrics]

axes[1].errorbar(
    k_values, sil_values, yerr=sil_stds, marker="o", capsize=5, color="green"
)
axes[1].axvline(x=optimal_k, color="r", linestyle="--", label=f"Optimal K={optimal_k}")
axes[1].set_xlabel("K (Number of Clusters)")
axes[1].set_ylabel("Silhouette Score")
axes[1].set_title("Silhouette Analysis")
axes[1].legend()
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig("/tmp/clustering_analysis.png", dpi=150, bbox_inches="tight")
plt.show()

print(f"\nPlot saved to /tmp/clustering_analysis.png")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("CLUSTERING SUMMARY")
print("=" * 60)
print(f"Total nodes clustered: {vector_count}")
print(f"Node types: {node_types}")
print(f"Embedding dimension: {embedding_dim}")
print(f"")
print(f"Optimal K: {optimal_k}")
print(f"Final silhouette score: {best_final_silhouette:.4f}")
print(f"")
print(f"Tables created:")
print(f"  - {cluster_assignments_table}")
print(f"  - {metrics_table}")

# Clean up
if not _SERVERLESS:
    nodes_with_vectors.unpersist()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cluster Interpretation Helper
# MAGIC
# MAGIC Sample comments from each cluster using efficient window functions.

# COMMAND ----------

# Efficient cluster interpretation using window function (replaces Python loop)
# This avoids multiple collect() calls and processes everything in Spark
print("Sample nodes from each cluster:")

# Register temp view for SQL query
final_predictions.createOrReplaceTempView("cluster_predictions")

# Single query with window function to get top 3 per cluster
cluster_samples = spark.sql(
    f"""
    SELECT 
        cluster,
        id,
        domain,
        CASE 
            WHEN LENGTH(comment) > 100 THEN CONCAT(SUBSTRING(comment, 1, 100), '...')
            ELSE comment
        END as comment_preview
    FROM (
        SELECT 
            cluster,
            id,
            domain,
            comment,
            ROW_NUMBER() OVER (PARTITION BY cluster ORDER BY id) as rn
        FROM cluster_predictions
    )
    WHERE rn <= 3
    ORDER BY cluster, rn
"""
)

# Display results (single collect at the end)
cluster_samples.display()

# Also print formatted output
print("\nFormatted cluster samples:")
for row in cluster_samples.collect():
    if row.rn == 1 if hasattr(row, "rn") else True:
        print(f"\n--- Cluster {row.cluster} ---")
    print(f"  [{row.domain or 'N/A'}] {row.id}")
    if row.comment_preview:
        print(f"    {row.comment_preview}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Scaling Notes
# MAGIC
# MAGIC **Classic compute (MLlib):** Handles **1M+ nodes without code changes**. All
# MAGIC expensive operations are guarded:
# MAGIC
# MAGIC | Concern | How it's handled |
# MAGIC |---|---|
# MAGIC | K-means training | Distributed via Spark MLlib |
# MAGIC | Silhouette (O(n^2)) | Sampled to 10K via `SILHOUETTE_SAMPLE_THRESHOLD` |
# MAGIC | DAG lineage growth | Checkpointed above `CHECKPOINT_THRESHOLD` (50K) |
# MAGIC | Broad/narrow search | Percentage-based sampling, not full scans |
# MAGIC
# MAGIC **Serverless compute (sklearn):** Handles up to **200K nodes** by default
# MAGIC (configurable via `_SERVERLESS_NODE_LIMIT`). All data is collected to the driver
# MAGIC as a numpy array. Silhouette sampling still applies. For larger datasets, use
# MAGIC classic compute.
# MAGIC
# MAGIC **You do NOT need to resample the input data.** The same code works for 1K
# MAGIC and 1M nodes. For tuning suggestions at extreme scale (adjusting thresholds,
# MAGIC reducing seeds, or applying PCA for very high-dimensional embeddings), see
# MAGIC `docs/KMEANS_CLUSTERING.md`.
