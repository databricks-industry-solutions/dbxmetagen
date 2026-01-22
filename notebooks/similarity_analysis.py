# Databricks notebook source
# MAGIC %md
# MAGIC # Similarity Analysis
# MAGIC 
# MAGIC This notebook analyzes similarity between tables, columns, and schemas using
# MAGIC embedding-based similarity from the knowledge graph.
# MAGIC 
# MAGIC ## Features
# MAGIC - Identify potential duplicate tables
# MAGIC - Find similar tables across different domains
# MAGIC - Cluster related columns
# MAGIC - Generate analytics views

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("similarity_threshold", "0.8", "Similarity Threshold")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
similarity_threshold = float(dbutils.widgets.get("similarity_threshold"))

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Similarity Threshold: {similarity_threshold}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Table Similarity Analysis

# COMMAND ----------

# Find similar tables based on embeddings
similar_tables_df = spark.sql(f"""
SELECT 
    e.src as table_a,
    e.dst as table_b,
    e.weight as similarity,
    n1.domain as domain_a,
    n2.domain as domain_b,
    n1.comment as comment_a,
    n2.comment as comment_b
FROM {catalog_name}.{schema_name}.graph_edges e
JOIN {catalog_name}.{schema_name}.graph_nodes n1 ON e.src = n1.id
JOIN {catalog_name}.{schema_name}.graph_nodes n2 ON e.dst = n2.id
WHERE e.relationship = 'similar_embedding'
  AND n1.node_type = 'table'
  AND n2.node_type = 'table'
ORDER BY e.weight DESC
""")

display(similar_tables_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Potential Duplicates (High Similarity)

# COMMAND ----------

# Tables with very high similarity might be duplicates
potential_duplicates = similar_tables_df.filter(f"similarity >= 0.95")
print(f"Found {potential_duplicates.count()} potential duplicate table pairs")
display(potential_duplicates)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cross-Domain Similarities

# COMMAND ----------

# Find tables that are similar but in different domains
cross_domain_similar = similar_tables_df.filter("domain_a != domain_b AND domain_a IS NOT NULL AND domain_b IS NOT NULL")
print(f"Found {cross_domain_similar.count()} cross-domain similar table pairs")
display(cross_domain_similar)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Table Clustering

# COMMAND ----------

# Create clusters based on connected components
# Tables connected by similarity edges are in the same cluster

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW table_clusters AS
WITH RECURSIVE clusters AS (
    -- Start with each table as its own cluster
    SELECT DISTINCT id as table_id, id as cluster_id
    FROM {catalog_name}.{schema_name}.graph_nodes
    WHERE node_type = 'table'
),
-- Find minimum cluster_id for each table through similarity edges
connected AS (
    SELECT 
        n.id as table_id,
        COALESCE(
            LEAST(
                MIN(e1.src),
                MIN(e1.dst),
                MIN(e2.src),
                MIN(e2.dst)
            ),
            n.id
        ) as cluster_id
    FROM {catalog_name}.{schema_name}.graph_nodes n
    LEFT JOIN {catalog_name}.{schema_name}.graph_edges e1 
        ON n.id = e1.src AND e1.relationship = 'similar_embedding'
    LEFT JOIN {catalog_name}.{schema_name}.graph_edges e2 
        ON n.id = e2.dst AND e2.relationship = 'similar_embedding'
    WHERE n.node_type = 'table'
    GROUP BY n.id
)
SELECT * FROM connected
""")

# Show cluster sizes
cluster_sizes = spark.sql("""
SELECT cluster_id, COUNT(*) as cluster_size
FROM table_clusters
GROUP BY cluster_id
HAVING COUNT(*) > 1
ORDER BY cluster_size DESC
""")

display(cluster_sizes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Column Similarity Within Tables

# COMMAND ----------

# Find similar columns (potentially redundant columns)
similar_columns = spark.sql(f"""
SELECT 
    e.src as column_a,
    e.dst as column_b,
    e.weight as similarity,
    n1.parent_id as table_a,
    n2.parent_id as table_b,
    n1.data_type as type_a,
    n2.data_type as type_b
FROM {catalog_name}.{schema_name}.graph_edges e
JOIN {catalog_name}.{schema_name}.graph_nodes n1 ON e.src = n1.id
JOIN {catalog_name}.{schema_name}.graph_nodes n2 ON e.dst = n2.id
WHERE e.relationship = 'similar_embedding'
  AND n1.node_type = 'column'
  AND n2.node_type = 'column'
  AND n1.parent_id = n2.parent_id  -- Same table
ORDER BY e.weight DESC
LIMIT 100
""")

display(similar_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Analytics Tables

# COMMAND ----------

# Create a summary table of table clusters
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.table_clusters AS
WITH cluster_assignments AS (
    SELECT 
        n.id as table_name,
        n.domain,
        n.comment,
        COALESCE(
            LEAST(
                MIN(e1.src),
                MIN(e1.dst),
                MIN(e2.src),
                MIN(e2.dst)
            ),
            n.id
        ) as cluster_id
    FROM {catalog_name}.{schema_name}.graph_nodes n
    LEFT JOIN {catalog_name}.{schema_name}.graph_edges e1 
        ON n.id = e1.src AND e1.relationship = 'similar_embedding'
    LEFT JOIN {catalog_name}.{schema_name}.graph_edges e2 
        ON n.id = e2.dst AND e2.relationship = 'similar_embedding'
    WHERE n.node_type = 'table'
    GROUP BY n.id, n.domain, n.comment
)
SELECT 
    cluster_id,
    COLLECT_LIST(table_name) as tables_in_cluster,
    COUNT(*) as cluster_size,
    COLLECT_SET(domain) as domains,
    current_timestamp() as created_at
FROM cluster_assignments
GROUP BY cluster_id
""")

print("Created table_clusters analytics table")

# COMMAND ----------

# Create cross-domain similarities summary
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.cross_domain_similarities AS
SELECT 
    n1.domain as domain_a,
    n2.domain as domain_b,
    COUNT(*) as pair_count,
    AVG(e.weight) as avg_similarity,
    MAX(e.weight) as max_similarity,
    COLLECT_LIST(STRUCT(e.src, e.dst, e.weight)) as table_pairs,
    current_timestamp() as created_at
FROM {catalog_name}.{schema_name}.graph_edges e
JOIN {catalog_name}.{schema_name}.graph_nodes n1 ON e.src = n1.id
JOIN {catalog_name}.{schema_name}.graph_nodes n2 ON e.dst = n2.id
WHERE e.relationship = 'similar_embedding'
  AND n1.node_type = 'table'
  AND n2.node_type = 'table'
  AND n1.domain IS NOT NULL
  AND n2.domain IS NOT NULL
  AND n1.domain != n2.domain
GROUP BY n1.domain, n2.domain
ORDER BY pair_count DESC
""")

print("Created cross_domain_similarities analytics table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary Statistics

# COMMAND ----------

summary = spark.sql(f"""
SELECT
    'Total Tables' as metric,
    COUNT(*) as value
FROM {catalog_name}.{schema_name}.graph_nodes
WHERE node_type = 'table'

UNION ALL

SELECT
    'Tables with Embeddings' as metric,
    COUNT(*) as value
FROM {catalog_name}.{schema_name}.graph_nodes
WHERE node_type = 'table' AND embedding IS NOT NULL

UNION ALL

SELECT
    'Similarity Edges' as metric,
    COUNT(*) as value
FROM {catalog_name}.{schema_name}.graph_edges
WHERE relationship = 'similar_embedding'

UNION ALL

SELECT
    'Table Clusters (size > 1)' as metric,
    COUNT(DISTINCT cluster_id) as value
FROM {catalog_name}.{schema_name}.table_clusters
WHERE cluster_size > 1
""")

display(summary)

# COMMAND ----------

print("Similarity analysis complete!")

