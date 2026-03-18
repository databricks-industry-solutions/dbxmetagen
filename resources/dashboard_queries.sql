-- Dashboard SQL Queries for DBXMetagen Analytics
-- These queries can be used to create a Databricks SQL Dashboard
-- Replace {{catalog}} and {{schema}} with your actual catalog/schema names

-- =============================================================================
-- QUERY 1: Graph Overview Card Metrics
-- Widget Type: Counter
-- =============================================================================
SELECT 
    COUNT(*) as total_nodes,
    SUM(CASE WHEN node_type = 'table' THEN 1 ELSE 0 END) as table_nodes,
    SUM(CASE WHEN node_type = 'column' THEN 1 ELSE 0 END) as column_nodes,
    SUM(CASE WHEN embedding IS NOT NULL THEN 1 ELSE 0 END) as nodes_with_embeddings
FROM {{catalog}}.{{schema}}.graph_nodes;

-- =============================================================================
-- QUERY 2: Edge Types Distribution
-- Widget Type: Pie Chart
-- =============================================================================
SELECT 
    relationship,
    COUNT(*) as edge_count
FROM {{catalog}}.{{schema}}.graph_edges
GROUP BY relationship
ORDER BY edge_count DESC;

-- =============================================================================
-- QUERY 3: Top Similar Table Pairs
-- Widget Type: Table
-- =============================================================================
SELECT 
    SPLIT_PART(e.src, '.', 3) as table_a,
    SPLIT_PART(e.dst, '.', 3) as table_b,
    ROUND(e.weight, 3) as similarity,
    n1.domain as domain_a,
    n2.domain as domain_b
FROM {{catalog}}.{{schema}}.graph_edges e
JOIN {{catalog}}.{{schema}}.graph_nodes n1 ON e.src = n1.id
JOIN {{catalog}}.{{schema}}.graph_nodes n2 ON e.dst = n2.id
WHERE e.relationship = 'similar_embedding'
  AND n1.node_type = 'table'
ORDER BY e.weight DESC
LIMIT 20;

-- =============================================================================
-- QUERY 4: Data Quality Score Distribution
-- Widget Type: Bar Chart
-- =============================================================================
SELECT 
    CASE 
        WHEN overall_score >= 90 THEN '1. Excellent (90+)'
        WHEN overall_score >= 70 THEN '2. Good (70-89)'
        WHEN overall_score >= 50 THEN '3. Fair (50-69)'
        ELSE '4. Poor (<50)'
    END as quality_tier,
    COUNT(*) as table_count
FROM {{catalog}}.{{schema}}.data_quality_scores
GROUP BY 1
ORDER BY 1;

-- =============================================================================
-- QUERY 5: Quality Dimension Scores (Average)
-- Widget Type: Bar Chart (horizontal)
-- =============================================================================
SELECT 
    'Completeness' as dimension, ROUND(AVG(completeness_score), 1) as avg_score
FROM {{catalog}}.{{schema}}.data_quality_scores
UNION ALL
SELECT 'Uniqueness', ROUND(AVG(uniqueness_score), 1)
FROM {{catalog}}.{{schema}}.data_quality_scores
UNION ALL
SELECT 'Freshness', ROUND(AVG(freshness_score), 1)
FROM {{catalog}}.{{schema}}.data_quality_scores
UNION ALL
SELECT 'Consistency', ROUND(AVG(consistency_score), 1)
FROM {{catalog}}.{{schema}}.data_quality_scores;

-- =============================================================================
-- QUERY 6: Domain Distribution
-- Widget Type: Pie Chart
-- =============================================================================
SELECT 
    COALESCE(domain, 'Unclassified') as domain,
    COUNT(*) as table_count
FROM {{catalog}}.{{schema}}.graph_nodes
WHERE node_type = 'table'
GROUP BY 1
ORDER BY table_count DESC;

-- =============================================================================
-- QUERY 7: PII/PHI Coverage
-- Widget Type: Stacked Bar Chart
-- =============================================================================
SELECT 
    'Has PII' as category, 
    SUM(CASE WHEN has_pii THEN 1 ELSE 0 END) as count,
    SUM(CASE WHEN NOT has_pii THEN 1 ELSE 0 END) as no_count
FROM {{catalog}}.{{schema}}.graph_nodes
WHERE node_type = 'table'
UNION ALL
SELECT 'Has PHI',
    SUM(CASE WHEN has_phi THEN 1 ELSE 0 END),
    SUM(CASE WHEN NOT has_phi THEN 1 ELSE 0 END)
FROM {{catalog}}.{{schema}}.graph_nodes
WHERE node_type = 'table';

-- =============================================================================
-- QUERY 8: Ontology Entity Types
-- Widget Type: Bar Chart
-- =============================================================================
SELECT 
    entity_type,
    COUNT(*) as entity_count,
    ROUND(AVG(confidence), 2) as avg_confidence
FROM {{catalog}}.{{schema}}.ontology_entities
GROUP BY entity_type
ORDER BY entity_count DESC;

-- =============================================================================
-- QUERY 9: Low Quality Tables (Needs Attention)
-- Widget Type: Table
-- =============================================================================
SELECT 
    SPLIT_PART(table_name, '.', 3) as table_short_name,
    ROUND(overall_score, 1) as overall,
    ROUND(completeness_score, 1) as completeness,
    ROUND(freshness_score, 1) as freshness,
    SIZE(quality_issues) as issue_count
FROM {{catalog}}.{{schema}}.data_quality_scores
WHERE overall_score < 70
ORDER BY overall_score ASC
LIMIT 15;

-- =============================================================================
-- QUERY 10: Profiling Stats Summary
-- Widget Type: Counter/Table
-- =============================================================================
SELECT 
    COUNT(DISTINCT table_name) as tables_profiled,
    SUM(row_count) as total_rows,
    SUM(table_size_bytes) / (1024*1024*1024) as total_size_gb,
    MAX(snapshot_time) as last_profiled
FROM {{catalog}}.{{schema}}.profiling_snapshots;

-- =============================================================================
-- QUERY 11: Cross-Domain Similarity Matrix
-- Widget Type: Table/Heatmap
-- =============================================================================
SELECT 
    n1.domain as domain_a,
    n2.domain as domain_b,
    COUNT(*) as similar_pairs,
    ROUND(AVG(e.weight), 3) as avg_similarity
FROM {{catalog}}.{{schema}}.graph_edges e
JOIN {{catalog}}.{{schema}}.graph_nodes n1 ON e.src = n1.id
JOIN {{catalog}}.{{schema}}.graph_nodes n2 ON e.dst = n2.id
WHERE e.relationship = 'similar_embedding'
  AND n1.node_type = 'table'
  AND n1.domain IS NOT NULL
  AND n2.domain IS NOT NULL
GROUP BY n1.domain, n2.domain
HAVING COUNT(*) >= 2
ORDER BY similar_pairs DESC;

-- =============================================================================
-- QUERY 12: Potential Duplicate Tables
-- Widget Type: Table (Alert)
-- =============================================================================
SELECT 
    SPLIT_PART(e.src, '.', 3) as table_a,
    SPLIT_PART(e.dst, '.', 3) as table_b,
    ROUND(e.weight, 3) as similarity,
    n1.domain as domain
FROM {{catalog}}.{{schema}}.graph_edges e
JOIN {{catalog}}.{{schema}}.graph_nodes n1 ON e.src = n1.id
JOIN {{catalog}}.{{schema}}.graph_nodes n2 ON e.dst = n2.id
WHERE e.relationship = 'similar_embedding'
  AND e.weight >= 0.95
  AND n1.node_type = 'table'
ORDER BY e.weight DESC;

-- =============================================================================
-- QUERY 13: Column Null Rate Distribution
-- Widget Type: Histogram
-- =============================================================================
SELECT 
    CASE 
        WHEN null_rate = 0 THEN '0%'
        WHEN null_rate < 0.1 THEN '1-10%'
        WHEN null_rate < 0.25 THEN '10-25%'
        WHEN null_rate < 0.5 THEN '25-50%'
        WHEN null_rate < 0.75 THEN '50-75%'
        ELSE '75-100%'
    END as null_rate_bucket,
    COUNT(*) as column_count
FROM {{catalog}}.{{schema}}.column_profiling_stats
WHERE null_rate IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- =============================================================================
-- QUERY 14: Security Level Distribution
-- Widget Type: Pie Chart
-- =============================================================================
SELECT 
    COALESCE(security_level, 'Unspecified') as security_level,
    COUNT(*) as table_count
FROM {{catalog}}.{{schema}}.graph_nodes
WHERE node_type = 'table'
GROUP BY 1
ORDER BY table_count DESC;

-- =============================================================================
-- QUERY 15: Daily Quality Score Trend (if historical data exists)
-- Widget Type: Line Chart
-- =============================================================================
SELECT 
    DATE(created_at) as score_date,
    ROUND(AVG(overall_score), 1) as avg_overall,
    ROUND(AVG(completeness_score), 1) as avg_completeness,
    ROUND(AVG(freshness_score), 1) as avg_freshness
FROM {{catalog}}.{{schema}}.data_quality_scores
GROUP BY DATE(created_at)
ORDER BY score_date DESC
LIMIT 30;

