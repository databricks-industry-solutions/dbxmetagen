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
# MAGIC - K-means training is distributed via Spark MLlib.
# MAGIC - Silhouette is always computed on a fixed-size sample (default 10K).
# MAGIC - DataFrame checkpointing auto-enables above 50K nodes to truncate lineage.
# MAGIC - Broad/narrow phases use percentage-based sampling, not full scans.
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

import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.linalg import Vectors, VectorUDT, DenseVector
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    StructType,
    StructField,
    StringType,
    IntegerType,
)
from datetime import datetime
import json

# Performance thresholds -- these are the main knobs for scaling.
# SILHOUETTE_SAMPLE_THRESHOLD: Silhouette is O(n^2). Above this count, we
#   compute it on a random sample instead of the full dataset. 10K is a good
#   default; increase to 50-100K for tighter estimates if cluster resources allow.
# CHECKPOINT_THRESHOLD: Spark's DAG grows with each transformation. Above this
#   count, we checkpoint the DataFrame to disk to truncate lineage and prevent
#   stack overflows or excessive planning time in the Catalyst optimizer.
SILHOUETTE_SAMPLE_THRESHOLD = 10000
CHECKPOINT_THRESHOLD = 50000

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

if total_nodes < min_k:
    raise ValueError(
        f"Not enough nodes ({total_nodes}) for clustering. Need at least {min_k}."
    )

# Adjust max_k if we have fewer nodes
effective_max_k = min(max_k, total_nodes - 1)
print(f"Effective K range: {min_k} to {effective_max_k}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Convert Embeddings to MLlib Vectors

# COMMAND ----------


# Spark MLlib expects DenseVector, but graph_nodes stores embeddings as
# array<float>. This UDF bridges the two. It runs once per node and is cached.
@F.udf(returnType=VectorUDT())
def array_to_vector(arr):
    if arr is None or len(arr) == 0:
        return None
    return DenseVector([float(x) for x in arr])


# Materialize vectors and cache -- everything downstream reads from this.
nodes_with_vectors = (
    nodes_df.withColumn("features", array_to_vector(F.col("embedding")))
    .filter(F.col("features").isNotNull())
    .cache()
)

vector_count = nodes_with_vectors.count()
print(f"Prepared {vector_count} nodes with valid vectors")

# For large datasets, checkpoint to disk so that Spark doesn't carry the full
# DAG (read -> UDF -> filter) through every downstream K-means fit. Without
# this, the Catalyst optimizer can choke on plan complexity at ~100K+ nodes.
if vector_count > CHECKPOINT_THRESHOLD:
    print(f"Large dataset ({vector_count} > {CHECKPOINT_THRESHOLD}), checkpointing...")
    spark.sparkContext.setCheckpointDir("/tmp/clustering_checkpoint")
    nodes_with_vectors = nodes_with_vectors.checkpoint()
    print("Checkpoint complete -- lineage truncated")

sample_embedding = nodes_with_vectors.select("features").first()
embedding_dim = len(sample_embedding.features)
print(f"Embedding dimension: {embedding_dim}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## K-Means Clustering Functions

# COMMAND ----------


def compute_silhouette_scaled(
    predictions_df, sample_threshold=SILHOUETTE_SAMPLE_THRESHOLD
):
    """Compute silhouette score, sampling for large datasets.

    Silhouette measures cluster quality on [-1, 1]:
      +1 = point is well-matched to its cluster and poorly-matched to neighbors
       0 = point is on the border between clusters
      -1 = point is likely in the wrong cluster

    Because silhouette requires pairwise distances (O(n^2)), we sample down to
    `sample_threshold` rows for datasets that exceed it. The sample is drawn
    uniformly at random so all clusters are represented proportionally.
    """
    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
    )

    count = predictions_df.count()
    if count > sample_threshold:
        sample_fraction = sample_threshold / count
        sample_df = predictions_df.sample(fraction=sample_fraction, seed=42).cache()
        silhouette = evaluator.evaluate(sample_df)
        sample_df.unpersist()
        return silhouette
    else:
        return evaluator.evaluate(predictions_df)


def run_kmeans_wssse_only(data_df, k, seed=42, max_iter=30):
    """Run K-means returning only WSSSE (Within-Cluster Sum of Squared Errors).

    WSSSE is the sum of squared distances from each point to its assigned cluster
    center. It always decreases as K grows (more clusters = tighter fit), so we
    look for the "elbow" where the rate of decrease slows sharply.

    Used in Phase 1 because WSSSE is O(n) -- it only needs each point's distance
    to its own center, not pairwise distances like silhouette.
    """
    kmeans = KMeans(
        k=k,
        seed=seed,
        maxIter=max_iter,
        initMode="k-means||",
        featuresCol="features",
        predictionCol="cluster",
    )
    model = kmeans.fit(data_df)
    wssse = model.summary.trainingCost
    return model, wssse


def run_kmeans_with_evaluation(
    data_df, k, seed=42, max_iter=50, compute_silhouette=True
):
    """
    Run K-means and return model with metrics.

    Args:
        data_df: DataFrame with 'features' column
        k: Number of clusters
        seed: Random seed
        max_iter: Maximum iterations
        compute_silhouette: Whether to compute silhouette (expensive)

    Returns:
        Tuple of (model, silhouette_score, wssse)
    """
    kmeans = KMeans(
        k=k,
        seed=seed,
        maxIter=max_iter,
        initMode="k-means||",
        featuresCol="features",
        predictionCol="cluster",
    )

    model = kmeans.fit(data_df)
    wssse = model.summary.trainingCost

    if compute_silhouette:
        predictions = model.transform(data_df)
        silhouette = compute_silhouette_scaled(predictions)
    else:
        silhouette = None

    return model, silhouette, wssse


def evaluate_k_range_wssse_only(data_df, k_range, n_seeds=2, max_iter=20):
    """Evaluate multiple K values using WSSSE only (Phase 1 broad search).

    Runs each K with `n_seeds` random initializations and reports mean/std.
    Multiple seeds guard against poor initialization -- K-means is sensitive to
    starting centroids and can converge to local minima.
    """
    results = []

    for k in k_range:
        wssses = []

        for seed in range(n_seeds):
            try:
                _, wssse = run_kmeans_wssse_only(
                    data_df, k, seed=seed * 42, max_iter=max_iter
                )
                wssses.append(wssse)
            except Exception as e:
                print(f"  K={k}, seed={seed}: Error - {e}")
                continue

        if wssses:
            results.append(
                {
                    "k": k,
                    "wssse_mean": float(np.mean(wssses)),
                    "wssse_std": float(np.std(wssses)),
                    "n_runs": len(wssses),
                }
            )
            print(f"  K={k}: WSSSE={np.mean(wssses):.2f} (+-{np.std(wssses):.2f})")

    return results


def evaluate_k_range_with_silhouette(data_df, k_range, n_seeds=3, max_iter=30):
    """Evaluate K values with both WSSSE and silhouette (Phase 2 narrow search).

    More expensive than WSSSE-only but gives a direct measure of cluster
    separation. Silhouette is auto-sampled for large datasets via
    `compute_silhouette_scaled`. Uses more seeds (default 3) and iterations
    (default 30) than the broad phase for higher accuracy.
    """
    results = []

    for k in k_range:
        silhouettes = []
        wssses = []

        for seed in range(n_seeds):
            try:
                _, sil, wssse = run_kmeans_with_evaluation(
                    data_df,
                    k,
                    seed=seed * 42,
                    max_iter=max_iter,
                    compute_silhouette=True,
                )
                silhouettes.append(sil)
                wssses.append(wssse)
            except Exception as e:
                print(f"  K={k}, seed={seed}: Error - {e}")
                continue

        if silhouettes:
            results.append(
                {
                    "k": k,
                    "silhouette_mean": float(np.mean(silhouettes)),
                    "silhouette_std": float(np.std(silhouettes)),
                    "wssse_mean": float(np.mean(wssses)),
                    "wssse_std": float(np.std(wssses)),
                    "n_runs": len(silhouettes),
                }
            )
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
broad_sample = nodes_with_vectors.sample(
    fraction=sample_fraction_broad, seed=42
).cache()
broad_sample_count = broad_sample.count()
print(
    f"Broad search sample: {broad_sample_count} nodes ({sample_fraction_broad*100:.1f}%)"
)

if broad_sample_count < min_k:
    print(f"Sample too small ({broad_sample_count} < {min_k}), using full dataset")
    broad_sample.unpersist()
    broad_sample = nodes_with_vectors
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
narrow_sample = nodes_with_vectors.sample(
    fraction=sample_fraction_narrow, seed=123
).cache()
narrow_sample_count = narrow_sample.count()
print(
    f"Narrow search sample: {narrow_sample_count} nodes ({sample_fraction_narrow*100:.1f}%)"
)

if narrow_sample_count < min_k:
    print(f"Narrow sample too small ({narrow_sample_count} < {min_k}), using full dataset")
    narrow_sample.unpersist()
    narrow_sample = nodes_with_vectors
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
final_results = []

n_final_runs = 5
print(f"Running {n_final_runs} initializations...")

for i in range(n_final_runs):
    model, sil, wssse = run_kmeans_with_evaluation(
        nodes_with_vectors,
        optimal_k,
        seed=i * 100 + 42,
        max_iter=100,
        compute_silhouette=True,
    )
    final_results.append({"run": i, "silhouette": sil, "wssse": wssse})
    print(f"  Run {i+1}: silhouette={sil:.4f}, WSSSE={wssse:.2f}")

    if sil > best_final_silhouette:
        best_final_silhouette = sil
        best_model = model

print(f"\nBest final model: silhouette={best_final_silhouette:.4f}")

# Get cluster assignments
final_predictions = best_model.transform(nodes_with_vectors)

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
centroids = best_model.clusterCenters()
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

# Overwrite with new assignments
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
# MAGIC This notebook handles **1M+ nodes without code changes**. All expensive
# MAGIC operations are already guarded:
# MAGIC
# MAGIC | Concern | How it's handled |
# MAGIC |---|---|
# MAGIC | K-means training | Distributed via Spark MLlib |
# MAGIC | Silhouette (O(n^2)) | Sampled to 10K via `SILHOUETTE_SAMPLE_THRESHOLD` |
# MAGIC | DAG lineage growth | Checkpointed above `CHECKPOINT_THRESHOLD` (50K) |
# MAGIC | Broad/narrow search | Percentage-based sampling, not full scans |
# MAGIC
# MAGIC **You do NOT need to resample the input data.** The same code works for 1K
# MAGIC and 1M nodes. For tuning suggestions at extreme scale (adjusting thresholds,
# MAGIC reducing seeds, or applying PCA for very high-dimensional embeddings), see
# MAGIC `docs/KMEANS_CLUSTERING.md`.
