# Databricks notebook source
# MAGIC %md
# MAGIC # Final Analysis Dashboard
# MAGIC 
# MAGIC Comprehensive analytics and visualizations for the knowledge graph,
# MAGIC profiling results, similarity analysis, and ontology.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

if not catalog_name or not schema_name:
    raise ValueError("Both catalog_name and schema_name are required")

print(f"Analyzing {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Graph Overview

# COMMAND ----------

# Graph statistics
graph_stats = spark.sql(f"""
SELECT
    'Total Nodes' as metric, COUNT(*) as value
FROM {catalog_name}.{schema_name}.graph_nodes
UNION ALL
SELECT 'Table Nodes', COUNT(*) FROM {catalog_name}.{schema_name}.graph_nodes WHERE node_type = 'table'
UNION ALL
SELECT 'Column Nodes', COUNT(*) FROM {catalog_name}.{schema_name}.graph_nodes WHERE node_type = 'column'
UNION ALL
SELECT 'Schema Nodes', COUNT(*) FROM {catalog_name}.{schema_name}.graph_nodes WHERE node_type = 'schema'
UNION ALL
SELECT 'Total Edges', COUNT(*) FROM {catalog_name}.{schema_name}.graph_edges
UNION ALL
SELECT 'Similarity Edges', COUNT(*) FROM {catalog_name}.{schema_name}.graph_edges WHERE relationship = 'similar_embedding'
UNION ALL
SELECT 'Nodes with Embeddings', COUNT(*) FROM {catalog_name}.{schema_name}.graph_nodes WHERE embedding IS NOT NULL
""")

display(graph_stats)

# COMMAND ----------

# Edge types distribution
edge_dist = spark.sql(f"""
SELECT relationship, COUNT(*) as count
FROM {catalog_name}.{schema_name}.graph_edges
GROUP BY relationship
ORDER BY count DESC
""")

display(edge_dist)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Similarity Analysis

# COMMAND ----------

# Top similar table pairs
print("Top 15 Most Similar Table Pairs:")
similar_tables = spark.sql(f"""
SELECT 
    e.src as table_a,
    e.dst as table_b,
    ROUND(e.weight, 3) as similarity,
    n1.domain as domain_a,
    n2.domain as domain_b,
    CASE WHEN n1.domain = n2.domain THEN 'Same' ELSE 'Cross-Domain' END as domain_relation
FROM {catalog_name}.{schema_name}.graph_edges e
JOIN {catalog_name}.{schema_name}.graph_nodes n1 ON e.src = n1.id
JOIN {catalog_name}.{schema_name}.graph_nodes n2 ON e.dst = n2.id
WHERE e.relationship = 'similar_embedding'
  AND n1.node_type = 'table'
  AND n2.node_type = 'table'
ORDER BY e.weight DESC
LIMIT 15
""")

display(similar_tables)

# COMMAND ----------

# Potential duplicates (similarity >= 0.95)
print("Potential Duplicate Tables (similarity >= 0.95):")
duplicates = spark.sql(f"""
SELECT 
    e.src as table_a,
    e.dst as table_b,
    ROUND(e.weight, 3) as similarity,
    SUBSTRING(n1.comment, 1, 100) as comment_a,
    SUBSTRING(n2.comment, 1, 100) as comment_b
FROM {catalog_name}.{schema_name}.graph_edges e
JOIN {catalog_name}.{schema_name}.graph_nodes n1 ON e.src = n1.id
JOIN {catalog_name}.{schema_name}.graph_nodes n2 ON e.dst = n2.id
WHERE e.relationship = 'similar_embedding'
  AND e.weight >= 0.95
  AND n1.node_type = 'table'
ORDER BY e.weight DESC
""")

dup_count = duplicates.count()
print(f"Found {dup_count} potential duplicate pairs")
if dup_count > 0:
    display(duplicates)

# COMMAND ----------

# Cross-domain similarities
print("Cross-Domain Similar Tables:")
cross_domain = spark.sql(f"""
SELECT 
    n1.domain as domain_a,
    n2.domain as domain_b,
    COUNT(*) as pair_count,
    ROUND(AVG(e.weight), 3) as avg_similarity
FROM {catalog_name}.{schema_name}.graph_edges e
JOIN {catalog_name}.{schema_name}.graph_nodes n1 ON e.src = n1.id
JOIN {catalog_name}.{schema_name}.graph_nodes n2 ON e.dst = n2.id
WHERE e.relationship = 'similar_embedding'
  AND n1.node_type = 'table'
  AND n1.domain IS NOT NULL
  AND n2.domain IS NOT NULL
  AND n1.domain != n2.domain
GROUP BY n1.domain, n2.domain
ORDER BY pair_count DESC
LIMIT 10
""")

display(cross_domain)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Summary

# COMMAND ----------

# Quality score distribution
try:
    quality_summary = spark.sql(f"""
    SELECT 
        CASE 
            WHEN overall_score >= 90 THEN 'Excellent (90+)'
            WHEN overall_score >= 70 THEN 'Good (70-89)'
            WHEN overall_score >= 50 THEN 'Fair (50-69)'
            ELSE 'Poor (<50)'
        END as quality_tier,
        COUNT(*) as table_count,
        ROUND(AVG(overall_score), 1) as avg_score
    FROM {catalog_name}.{schema_name}.data_quality_scores
    GROUP BY 1
    ORDER BY avg_score DESC
    """)
    
    display(quality_summary)
except Exception as e:
    print(f"Data quality scores not available: {e}")

# COMMAND ----------

# Low quality tables needing attention
try:
    low_quality = spark.sql(f"""
    SELECT 
        table_name,
        ROUND(overall_score, 1) as overall_score,
        ROUND(completeness_score, 1) as completeness,
        ROUND(freshness_score, 1) as freshness,
        SIZE(quality_issues) as issue_count
    FROM {catalog_name}.{schema_name}.data_quality_scores
    WHERE overall_score < 70
    ORDER BY overall_score ASC
    LIMIT 15
    """)
    
    print(f"Tables with Low Quality Scores (<70):")
    display(low_quality)
except Exception as e:
    print(f"Could not retrieve low quality tables: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ontology Summary

# COMMAND ----------

# Entity type distribution
try:
    entity_summary = spark.sql(f"""
    SELECT 
        entity_type,
        COUNT(*) as entity_count,
        ROUND(AVG(confidence), 2) as avg_confidence,
        SUM(CASE WHEN validated THEN 1 ELSE 0 END) as validated_count
    FROM {catalog_name}.{schema_name}.ontology_entities
    GROUP BY entity_type
    ORDER BY entity_count DESC
    """)
    
    print("Ontology Entities by Type:")
    display(entity_summary)
except Exception as e:
    print(f"Ontology entities not available: {e}")

# COMMAND ----------

# Top entities by confidence
try:
    top_entities = spark.sql(f"""
    SELECT 
        entity_name,
        entity_type,
        ROUND(confidence, 2) as confidence,
        validated,
        SIZE(source_tables) as source_table_count
    FROM {catalog_name}.{schema_name}.ontology_entities
    ORDER BY confidence DESC
    LIMIT 15
    """)
    
    print("Top Entities by Confidence:")
    display(top_entities)
except Exception as e:
    print(f"Could not retrieve entities: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Domain Distribution

# COMMAND ----------

# Tables by domain
domain_dist = spark.sql(f"""
SELECT 
    COALESCE(domain, 'Unclassified') as domain,
    COUNT(*) as table_count,
    SUM(CASE WHEN has_pii THEN 1 ELSE 0 END) as pii_tables,
    SUM(CASE WHEN has_phi THEN 1 ELSE 0 END) as phi_tables
FROM {catalog_name}.{schema_name}.graph_nodes
WHERE node_type = 'table'
GROUP BY domain
ORDER BY table_count DESC
""")

display(domain_dist)

# COMMAND ----------

# Security level distribution
security_dist = spark.sql(f"""
SELECT 
    security_level,
    COUNT(*) as table_count
FROM {catalog_name}.{schema_name}.graph_nodes
WHERE node_type = 'table'
GROUP BY security_level
ORDER BY table_count DESC
""")

display(security_dist)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Lineage Overview

# COMMAND ----------

# Tables with most upstream dependencies
try:
    upstream_deps = spark.sql(f"""
    SELECT 
        table_name,
        SIZE(upstream_tables) as upstream_count,
        SIZE(downstream_tables) as downstream_count
    FROM {catalog_name}.{schema_name}.extended_table_metadata
    WHERE upstream_tables IS NOT NULL
    ORDER BY SIZE(upstream_tables) DESC
    LIMIT 15
    """)
    
    print("Tables with Most Upstream Dependencies:")
    display(upstream_deps)
except Exception as e:
    print(f"Extended metadata not available: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cluster Analysis and Network Visualization

# COMMAND ----------

# Build network visualization of table similarities
import matplotlib.pyplot as plt
import numpy as np

# Get similarity edges for network
similarity_network = spark.sql(f"""
SELECT 
    SPLIT(e.src, '\\\\.')[2] as src_table,
    SPLIT(e.dst, '\\\\.')[2] as dst_table,
    e.weight as similarity,
    n1.domain as src_domain,
    n2.domain as dst_domain
FROM {catalog_name}.{schema_name}.graph_edges e
JOIN {catalog_name}.{schema_name}.graph_nodes n1 ON e.src = n1.id
JOIN {catalog_name}.{schema_name}.graph_nodes n2 ON e.dst = n2.id
WHERE e.relationship = 'similar_embedding'
  AND n1.node_type = 'table'
  AND n2.node_type = 'table'
  AND e.weight >= 0.6
ORDER BY e.weight DESC
LIMIT 100
""").toPandas()

if len(similarity_network) > 0:
    try:
        import networkx as nx
        
        # Create graph
        G = nx.Graph()
        
        # Add edges with weights
        for _, row in similarity_network.iterrows():
            G.add_edge(row['src_table'], row['dst_table'], weight=row['similarity'])
        
        # Get connected components (clusters)
        clusters = list(nx.connected_components(G))
        print(f"Found {len(clusters)} table clusters based on similarity")
        
        # Cluster summary
        cluster_summary = []
        for i, cluster in enumerate(sorted(clusters, key=len, reverse=True)[:10]):
            cluster_summary.append({
                "cluster_id": i + 1,
                "size": len(cluster),
                "tables": ", ".join(list(cluster)[:5]) + ("..." if len(cluster) > 5 else "")
            })
        
        cluster_df = spark.createDataFrame(cluster_summary)
        print("\nTop 10 Table Clusters by Size:")
        display(cluster_df)
        
        # Create visualization
        fig, ax = plt.subplots(1, 1, figsize=(14, 10))
        
        # Layout
        pos = nx.spring_layout(G, k=2, iterations=50, seed=42)
        
        # Get unique domains for coloring
        domains = set()
        for _, row in similarity_network.iterrows():
            if row['src_domain']:
                domains.add(row['src_domain'])
            if row['dst_domain']:
                domains.add(row['dst_domain'])
        
        domain_colors = {d: plt.cm.Set3(i / max(len(domains), 1)) for i, d in enumerate(sorted(domains))}
        domain_colors[None] = (0.7, 0.7, 0.7, 1.0)  # Gray for unclassified
        
        # Assign colors to nodes
        node_colors = []
        domain_map = {}
        for _, row in similarity_network.iterrows():
            domain_map[row['src_table']] = row['src_domain']
            domain_map[row['dst_table']] = row['dst_domain']
        
        for node in G.nodes():
            node_colors.append(domain_colors.get(domain_map.get(node), (0.7, 0.7, 0.7, 1.0)))
        
        # Draw edges with width based on similarity
        edge_weights = [G[u][v]['weight'] * 3 for u, v in G.edges()]
        nx.draw_networkx_edges(G, pos, alpha=0.4, width=edge_weights, edge_color='gray', ax=ax)
        
        # Draw nodes
        nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=300, ax=ax)
        
        # Draw labels (smaller font for readability)
        nx.draw_networkx_labels(G, pos, font_size=7, ax=ax)
        
        ax.set_title(f"Table Similarity Network\n{len(G.nodes())} tables, {len(G.edges())} similarity edges, {len(clusters)} clusters", fontsize=12)
        ax.axis('off')
        
        # Legend for domains
        legend_elements = [plt.Line2D([0], [0], marker='o', color='w', 
                                       markerfacecolor=domain_colors[d], markersize=10, label=d or 'Unclassified')
                         for d in sorted(domains)[:8]]
        ax.legend(handles=legend_elements, loc='upper left', fontsize=8)
        
        plt.tight_layout()
        display(fig)
        plt.close()
        
    except ImportError:
        print("NetworkX not available - install with: pip install networkx")
        print("\nSimilarity network data:")
        display(spark.createDataFrame(similarity_network))
else:
    print("No similarity edges found above threshold (0.6)")

# COMMAND ----------

# Similarity heatmap (top tables)
if len(similarity_network) > 0:
    try:
        # Build adjacency matrix for top tables
        top_tables = list(set(similarity_network['src_table'].tolist()[:15] + 
                             similarity_network['dst_table'].tolist()[:15]))[:20]
        
        # Create similarity matrix
        sim_matrix = np.zeros((len(top_tables), len(top_tables)))
        for _, row in similarity_network.iterrows():
            if row['src_table'] in top_tables and row['dst_table'] in top_tables:
                i = top_tables.index(row['src_table'])
                j = top_tables.index(row['dst_table'])
                sim_matrix[i, j] = row['similarity']
                sim_matrix[j, i] = row['similarity']
        
        # Fill diagonal
        np.fill_diagonal(sim_matrix, 1.0)
        
        # Create heatmap
        fig, ax = plt.subplots(figsize=(12, 10))
        im = ax.imshow(sim_matrix, cmap='RdYlGn', vmin=0, vmax=1)
        
        # Labels
        ax.set_xticks(np.arange(len(top_tables)))
        ax.set_yticks(np.arange(len(top_tables)))
        ax.set_xticklabels(top_tables, rotation=45, ha='right', fontsize=8)
        ax.set_yticklabels(top_tables, fontsize=8)
        
        # Colorbar
        cbar = ax.figure.colorbar(im, ax=ax)
        cbar.ax.set_ylabel("Similarity", rotation=-90, va="bottom")
        
        ax.set_title("Table Similarity Heatmap (Top Tables)", fontsize=12)
        
        plt.tight_layout()
        display(fig)
        plt.close()
        
    except Exception as e:
        print(f"Could not create heatmap: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Summary Statistics

# COMMAND ----------

# Final summary
print("=" * 60)
print("PIPELINE ANALYSIS COMPLETE")
print("=" * 60)

summary_stats = spark.sql(f"""
SELECT 
    (SELECT COUNT(*) FROM {catalog_name}.{schema_name}.table_knowledge_base) as tables_in_kb,
    (SELECT COUNT(*) FROM {catalog_name}.{schema_name}.graph_nodes) as graph_nodes,
    (SELECT COUNT(*) FROM {catalog_name}.{schema_name}.graph_edges) as graph_edges,
    (SELECT COUNT(*) FROM {catalog_name}.{schema_name}.graph_nodes WHERE embedding IS NOT NULL) as nodes_with_embeddings,
    (SELECT COUNT(*) FROM {catalog_name}.{schema_name}.graph_edges WHERE relationship = 'similar_embedding') as similarity_edges
""").collect()[0]

print(f"Tables in Knowledge Base: {summary_stats.tables_in_kb}")
print(f"Graph Nodes: {summary_stats.graph_nodes}")
print(f"Graph Edges: {summary_stats.graph_edges}")
print(f"Nodes with Embeddings: {summary_stats.nodes_with_embeddings}")
print(f"Similarity Edges: {summary_stats.similarity_edges}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Analysis Summary Output
# MAGIC 
# MAGIC This analysis has covered:
# MAGIC - Graph structure and relationship types
# MAGIC - Table similarity analysis with potential duplicates and cross-domain matches
# MAGIC - Data quality scores and tables needing attention
# MAGIC - Ontology entity discovery results
# MAGIC - Domain and security level distribution
# MAGIC - Lineage dependencies
# MAGIC - Network visualization with clustering
# MAGIC
# MAGIC For deeper exploration, query the underlying tables directly or use the dashboard queries.

