# Knowledge Graph Query Examples

The knowledge graph provides a graph-based view of your metadata with nodes (tables, columns) and edges (relationships). This enables powerful queries to discover patterns and relationships across your data estate.

## Tables

- **`metadata_knowledge_base_nodes`**: Nodes representing tables, columns, and other entities
- **`metadata_knowledge_base_edges`**: Relationships between nodes

## Node Schema

```sql
node_id                  STRING   -- Primary key (SHA256 hash)
node_type                STRING   -- 'table' or 'column'
object_name              STRING   -- Fully qualified name
catalog                  STRING
schema                   STRING  
table_name               STRING
column_name              STRING   -- NULL for tables
comment                  STRING   -- From comment mode
pi_classification        STRING   -- From PI mode
pi_type                  STRING   -- PII, PHI, PCI, None
pi_confidence            DOUBLE
domain                   STRING   -- From domain mode
subdomain                STRING
domain_confidence        DOUBLE
data_type                STRING
num_queries_last_7_days  INT      -- Query count from system.access.table_lineage (tables only)
metadata                 STRING   -- Raw input metadata including lineage (JSON stored as string)
tags                     MAP      -- Additional tags
created_at               TIMESTAMP
updated_at               TIMESTAMP
```

## Edge Schema

```sql
edge_id              STRING   -- Primary key
source_node_id       STRING   -- References nodes.node_id
target_node_id       STRING   -- References nodes.node_id
relationship_type    STRING   -- Type of relationship
metadata             STRING   -- Additional edge properties (JSON stored as string)
created_at           TIMESTAMP
updated_at           TIMESTAMP
```

## Relationship Types

| Type | Description | Example |
|------|-------------|---------|
| `IS_COLUMN_OF` | Column belongs to table | `customers.email IS_COLUMN_OF customers` |
| `IN_SAME_TABLE` | Columns in same table | `first_name IN_SAME_TABLE last_name` |
| `IN_SAME_SCHEMA` | Tables in same schema | `orders IN_SAME_SCHEMA customers` |
| `HAS_SAME_DOMAIN` | Entities share domain | `table1 HAS_SAME_DOMAIN table2` |
| `IS_UPSTREAM_OF` | Table lineage (source→target) | `raw_data IS_UPSTREAM_OF processed_data` |

## Common Queries

### Find most popular tables by query count

```sql
SELECT 
    object_name,
    num_queries_last_7_days,
    domain,
    pi_type,
    comment
FROM metadata_knowledge_base_nodes
WHERE node_type = 'table'
  AND num_queries_last_7_days IS NOT NULL
ORDER BY num_queries_last_7_days DESC
LIMIT 20
```

### Find all PII/PHI tables

```sql
SELECT 
    object_name,
    pi_type,
    pi_confidence,
    comment
FROM metadata_knowledge_base_nodes
WHERE node_type = 'table'
  AND pi_type IN ('PII', 'PHI', 'PCI')
ORDER BY pi_confidence DESC
```

### Find all columns with PII in a schema

```sql
SELECT 
    object_name,
    pi_type,
    pi_confidence,
    comment
FROM metadata_knowledge_base_nodes
WHERE node_type = 'column'
  AND schema = 'my_schema'
  AND pi_type IN ('PII', 'PHI')
ORDER BY table_name, column_name
```

### Find tables in the same domain

```sql
SELECT 
    domain,
    subdomain,
    COUNT(*) as table_count,
    COLLECT_LIST(object_name) as tables
FROM metadata_knowledge_base_nodes
WHERE node_type = 'table'
  AND domain IS NOT NULL
GROUP BY domain, subdomain
ORDER BY table_count DESC
```

### Find all columns of a table with their metadata

```sql
SELECT 
    n.object_name as column_name,
    n.data_type,
    n.pi_type,
    n.comment
FROM metadata_knowledge_base_nodes n
JOIN metadata_knowledge_base_edges e 
    ON n.node_id = e.source_node_id
JOIN metadata_knowledge_base_nodes t 
    ON e.target_node_id = t.node_id
WHERE e.relationship_type = 'IS_COLUMN_OF'
  AND t.object_name = 'my_catalog.my_schema.my_table'
ORDER BY n.column_name
```

### Find columns with similar names across tables

```sql
SELECT 
    c1.table_name as table1,
    c2.table_name as table2,
    c1.column_name,
    c1.pi_type as table1_pi_type,
    c2.pi_type as table2_pi_type
FROM metadata_knowledge_base_nodes c1
JOIN metadata_knowledge_base_nodes c2
    ON c1.column_name = c2.column_name
    AND c1.node_id < c2.node_id  -- Avoid duplicates
    AND c1.table_name != c2.table_name
WHERE c1.node_type = 'column'
  AND c2.node_type = 'column'
  AND c1.schema = 'my_schema'
  AND c2.schema = 'my_schema'
```

### Find tables connected by shared domain

```sql
SELECT 
    t1.object_name as table1,
    t2.object_name as table2,
    t1.domain,
    t1.subdomain
FROM metadata_knowledge_base_nodes t1
JOIN metadata_knowledge_base_edges e 
    ON t1.node_id = e.source_node_id
JOIN metadata_knowledge_base_nodes t2 
    ON e.target_node_id = t2.node_id
WHERE e.relationship_type = 'HAS_SAME_DOMAIN'
  AND t1.domain = 'Finance'
```

## Lineage Queries

### Find direct upstream tables (dependencies)

```sql
-- Find tables that a specific table depends on
SELECT 
    upstream.object_name as upstream_table,
    upstream.domain as upstream_domain,
    upstream.num_queries_last_7_days as upstream_popularity,
    target.object_name as target_table
FROM metadata_knowledge_base_edges e
JOIN metadata_knowledge_base_nodes upstream 
    ON e.source_node_id = upstream.node_id
JOIN metadata_knowledge_base_nodes target 
    ON e.target_node_id = target.node_id
WHERE e.relationship_type = 'IS_UPSTREAM_OF'
  AND target.object_name = 'my_catalog.my_schema.my_table'
```

### Find direct downstream tables (dependents)

```sql
-- Find tables that depend on a specific table
SELECT 
    source.object_name as source_table,
    downstream.object_name as downstream_table,
    downstream.domain as downstream_domain,
    downstream.num_queries_last_7_days as downstream_popularity
FROM metadata_knowledge_base_edges e
JOIN metadata_knowledge_base_nodes source 
    ON e.source_node_id = source.node_id
JOIN metadata_knowledge_base_nodes downstream 
    ON e.target_node_id = downstream.node_id
WHERE e.relationship_type = 'IS_UPSTREAM_OF'
  AND source.object_name = 'my_catalog.my_schema.my_table'
```

### Find full lineage chains (multi-hop)

```sql
-- Find all tables in the lineage chain (up to 3 levels)
WITH RECURSIVE lineage AS (
  -- Base case: start table
  SELECT 
    node_id,
    object_name,
    0 as level
  FROM metadata_knowledge_base_nodes
  WHERE object_name = 'my_catalog.my_schema.my_table'
  
  UNION ALL
  
  -- Recursive case: follow IS_UPSTREAM_OF edges
  SELECT 
    n.node_id,
    n.object_name,
    l.level + 1
  FROM lineage l
  JOIN metadata_knowledge_base_edges e 
    ON l.node_id = e.target_node_id
  JOIN metadata_knowledge_base_nodes n 
    ON e.source_node_id = n.node_id
  WHERE e.relationship_type = 'IS_UPSTREAM_OF'
    AND l.level < 3
)
SELECT 
    level,
    object_name,
    n.domain,
    n.num_queries_last_7_days
FROM lineage
JOIN metadata_knowledge_base_nodes n 
  ON lineage.node_id = n.node_id
ORDER BY level, object_name
```

### Find lineage impact: Popular tables affected by changes

```sql
-- Find highly-used downstream tables that would be affected
WITH downstream_tables AS (
  SELECT DISTINCT
    downstream.node_id,
    downstream.object_name,
    downstream.num_queries_last_7_days,
    downstream.domain,
    downstream.pi_type
  FROM metadata_knowledge_base_nodes source
  JOIN metadata_knowledge_base_edges e 
    ON source.node_id = e.source_node_id
  JOIN metadata_knowledge_base_nodes downstream 
    ON e.target_node_id = downstream.node_id
  WHERE e.relationship_type = 'IS_UPSTREAM_OF'
    AND source.object_name = 'my_catalog.my_schema.source_table'
)
SELECT 
    object_name,
    num_queries_last_7_days,
    domain,
    pi_type
FROM downstream_tables
WHERE num_queries_last_7_days > 100  -- High usage threshold
ORDER BY num_queries_last_7_days DESC
```

### Find PII lineage: Track PII propagation

```sql
-- Find if PII data flows from source to targets
SELECT 
    source.object_name as source_table,
    source.pi_type as source_pi_type,
    target.object_name as target_table,
    target.pi_type as target_pi_type,
    CASE 
        WHEN source.pi_type IN ('PII', 'PHI', 'PCI') 
         AND target.pi_type NOT IN ('PII', 'PHI', 'PCI')
        THEN 'POTENTIAL_PII_LEAK'
        ELSE 'CONSISTENT'
    END as pii_status
FROM metadata_knowledge_base_edges e
JOIN metadata_knowledge_base_nodes source 
    ON e.source_node_id = source.node_id
JOIN metadata_knowledge_base_nodes target 
    ON e.target_node_id = target.node_id
WHERE e.relationship_type = 'IS_UPSTREAM_OF'
  AND source.pi_type IN ('PII', 'PHI', 'PCI')
```

### Find orphan tables (no lineage)

```sql
-- Find tables with no upstream or downstream dependencies
SELECT 
    n.object_name,
    n.domain,
    n.num_queries_last_7_days
FROM metadata_knowledge_base_nodes n
WHERE n.node_type = 'table'
  AND NOT EXISTS (
    SELECT 1 FROM metadata_knowledge_base_edges e
    WHERE e.relationship_type = 'IS_UPSTREAM_OF'
      AND (e.source_node_id = n.node_id OR e.target_node_id = n.node_id)
  )
ORDER BY n.num_queries_last_7_days DESC NULLS LAST
```

### Find columns that appear in multiple tables (potential join keys)

```sql
SELECT 
    column_name,
    COUNT(DISTINCT table_name) as table_count,
    COLLECT_SET(table_name) as tables,
    COLLECT_SET(pi_type) as classifications
FROM metadata_knowledge_base_nodes
WHERE node_type = 'column'
  AND schema = 'my_schema'
GROUP BY column_name
HAVING COUNT(DISTINCT table_name) > 1
ORDER BY table_count DESC
```

### Analyze PII distribution by domain

```sql
SELECT 
    domain,
    subdomain,
    pi_type,
    COUNT(*) as column_count,
    AVG(pi_confidence) as avg_confidence
FROM metadata_knowledge_base_nodes
WHERE node_type = 'column'
  AND pi_type IS NOT NULL
  AND domain IS NOT NULL
GROUP BY domain, subdomain, pi_type
ORDER BY domain, subdomain, pi_type
```

### Find tables with no comments

```sql
SELECT 
    object_name,
    domain,
    pi_type
FROM metadata_knowledge_base_nodes
WHERE node_type = 'table'
  AND (comment IS NULL OR comment = '')
ORDER BY object_name
```

### Graph traversal: Find all columns in tables of a specific domain

```sql
SELECT 
    t.object_name as table_name,
    c.object_name as column_name,
    c.pi_type,
    c.comment
FROM metadata_knowledge_base_nodes t
JOIN metadata_knowledge_base_edges e 
    ON t.node_id = e.target_node_id
JOIN metadata_knowledge_base_nodes c 
    ON e.source_node_id = c.node_id
WHERE e.relationship_type = 'IS_COLUMN_OF'
  AND t.node_type = 'table'
  AND t.domain = 'Healthcare'
ORDER BY t.object_name, c.column_name
```

## Advanced Pattern: Multi-hop Queries

### Find tables that share both schema and domain

```sql
SELECT 
    t1.object_name as table1,
    t2.object_name as table2,
    t1.schema,
    t1.domain
FROM metadata_knowledge_base_nodes t1
JOIN metadata_knowledge_base_edges e1 
    ON t1.node_id = e1.source_node_id
JOIN metadata_knowledge_base_nodes t2 
    ON e1.target_node_id = t2.node_id
    AND e1.relationship_type = 'IN_SAME_SCHEMA'
JOIN metadata_knowledge_base_edges e2 
    ON t1.node_id = e2.source_node_id
    AND t2.node_id = e2.target_node_id
    AND e2.relationship_type = 'HAS_SAME_DOMAIN'
WHERE t1.node_type = 'table'
  AND t2.node_type = 'table'
```

## Incremental Processing

The knowledge graph builder runs incrementally based on the `_created_at` timestamp in `metadata_generation_log`. Each run:

1. Finds the max `updated_at` from existing nodes
2. Processes only new metadata entries after that timestamp
3. Upserts nodes (updates existing, inserts new)
4. Rebuilds all edges (edges are derived, not incremental)

To force a full rebuild, drop the tables:

```sql
DROP TABLE IF EXISTS metadata_knowledge_base_nodes;
DROP TABLE IF EXISTS metadata_knowledge_base_edges;
```

Then run metadata generation with `build_knowledge_graph: true`.

## GraphFrames Queries

GraphFrames provides graph algorithms and pattern matching on top of Spark DataFrames.

### Setup

Install GraphFrames:
```python
%pip install graphframes
```

Load the knowledge graph:
```python
from graphframes import GraphFrame

# Load nodes as vertices (requires "id" column)
vertices = spark.table("metadata_knowledge_base_nodes").selectExpr(
    "node_id as id",
    "node_type",
    "object_name",
    "catalog",
    "schema",
    "table_name",
    "column_name",
    "pi_type",
    "domain"
)

# Load edges (requires "src" and "dst" columns)
edges = spark.table("metadata_knowledge_base_edges").selectExpr(
    "edge_id as id",
    "source_node_id as src",
    "target_node_id as dst",
    "relationship_type"
)

# Create GraphFrame
g = GraphFrame(vertices, edges)
```

### Basic Graph Queries

**Count nodes and edges:**
```python
print(f"Nodes: {g.vertices.count()}")
print(f"Edges: {g.edges.count()}")
```

**Find node degrees:**
```python
# In-degree (how many edges point TO this node)
in_degrees = g.inDegrees
in_degrees.orderBy("inDegree", ascending=False).show()

# Out-degree (how many edges point FROM this node)  
out_degrees = g.outDegrees
out_degrees.orderBy("outDegree", ascending=False).show()

# Total degree
degrees = g.degrees
degrees.orderBy("degree", ascending=False).show()
```

### Graph Algorithms

**PageRank - Find most important nodes:**
```python
# Run PageRank
results = g.pageRank(resetProbability=0.15, maxIter=10)

# Show top nodes by importance
results.vertices.select("id", "object_name", "node_type", "pagerank") \
    .orderBy("pagerank", ascending=False) \
    .show(20, False)
```

**Connected Components - Find isolated clusters:**
```python
# Find connected components
cc = g.connectedComponents()

# Count tables per component
cc.groupBy("component").count().orderBy("count", ascending=False).show()

# Show tables in largest component
largest_component = cc.groupBy("component").count() \
    .orderBy("count", ascending=False) \
    .first()["component"]

cc.filter(f"component = {largest_component}") \
    .select("object_name", "node_type") \
    .show(50, False)
```

**Shortest Paths - Find relationships between entities:**
```python
# Define landmarks (nodes to find paths to)
table1_id = spark.table("metadata_knowledge_base_nodes") \
    .filter("object_name = 'my_catalog.my_schema.table1'") \
    .select("node_id").first()["node_id"]

table2_id = spark.table("metadata_knowledge_base_nodes") \
    .filter("object_name = 'my_catalog.my_schema.table2'") \
    .select("node_id").first()["node_id"]

landmarks = [table1_id, table2_id]

# Find shortest paths
paths = g.shortestPaths(landmarks=landmarks)
paths.show(False)
```

**Triangle Count - Find tightly connected groups:**
```python
# Count triangles (groups of 3 nodes all connected)
triangles = g.triangleCount()
triangles.select("id", "object_name", "count") \
    .orderBy("count", ascending=False) \
    .show()
```

**Label Propagation - Community detection:**
```python
# Find communities (groups of related entities)
communities = g.labelPropagation(maxIter=5)

# Count entities per community
communities.groupBy("label").count() \
    .orderBy("count", ascending=False) \
    .show()

# Show entities in a specific community
community_id = communities.groupBy("label").count() \
    .orderBy("count", ascending=False) \
    .first()["label"]

communities.filter(f"label = {community_id}") \
    .select("object_name", "node_type", "domain") \
    .show(50, False)
```

### Motif Finding (Pattern Matching)

**Find columns connected through their table:**
```python
# Pattern: column1 -> table <- column2
motif_pattern = "(c1)-[e1]->(t); (t)<-[e2]-(c2)"

motifs = g.find(motif_pattern) \
    .filter("c1.node_type = 'column'") \
    .filter("c2.node_type = 'column'") \
    .filter("t.node_type = 'table'") \
    .filter("e1.relationship_type = 'IS_COLUMN_OF'") \
    .filter("e2.relationship_type = 'IS_COLUMN_OF'") \
    .filter("c1.id != c2.id")

motifs.select(
    "c1.object_name as column1",
    "c2.object_name as column2", 
    "t.object_name as shared_table"
).show(20, False)
```

**Find tables in same schema with same domain:**
```python
# Pattern: table1 -> table2 (both relationships)
motif_pattern = "(t1)-[e1]->(t2); (t1)-[e2]->(t2)"

motifs = g.find(motif_pattern) \
    .filter("e1.relationship_type = 'IN_SAME_SCHEMA'") \
    .filter("e2.relationship_type = 'HAS_SAME_DOMAIN'")

motifs.select(
    "t1.object_name as table1",
    "t2.object_name as table2",
    "t1.domain",
    "t1.schema"
).show(20, False)
```

**Find PII columns in related tables:**
```python
# Pattern: pii_column1 -> table1 -> table2 <- pii_column2
motif_pattern = "(c1)-[e1]->(t1)-[e2]->(t2)<-[e3]-(c2)"

motifs = g.find(motif_pattern) \
    .filter("c1.pi_type IN ('PII', 'PHI')") \
    .filter("c2.pi_type IN ('PII', 'PHI')") \
    .filter("e1.relationship_type = 'IS_COLUMN_OF'") \
    .filter("e2.relationship_type = 'IN_SAME_SCHEMA'") \
    .filter("e3.relationship_type = 'IS_COLUMN_OF'")

motifs.select(
    "c1.object_name as pii_column1",
    "c2.object_name as pii_column2",
    "t1.object_name as table1",
    "t2.object_name as table2"
).show(20, False)
```

### BFS (Breadth-First Search)

**Find all nodes within N hops:**
```python
# Find all entities within 2 hops of a specific table
start_node = spark.table("metadata_knowledge_base_nodes") \
    .filter("object_name = 'my_catalog.my_schema.my_table'") \
    .select("node_id").first()["node_id"]

# BFS from start node, max 2 edges away
bfs_result = g.bfs(
    fromExpr=f"id = '{start_node}'",
    toExpr="node_type = 'column'",  # Find columns
    maxPathLength=2
)

bfs_result.select("from.object_name", "to.object_name", "to.pi_type").show(False)
```

**Find paths between PII and specific domain:**
```python
# Find paths from PII columns to Healthcare domain tables
bfs_result = g.bfs(
    fromExpr="pi_type = 'PII'",
    toExpr="domain = 'Healthcare'",
    maxPathLength=3
)

bfs_result.select(
    "from.object_name as pii_source",
    "to.object_name as healthcare_table"
).show(False)
```

### Advanced: Subgraph Queries

**Extract subgraph for a specific schema:**
```python
# Filter to nodes in specific schema
schema_vertices = vertices.filter("schema = 'my_schema'")

# Filter to edges between those nodes
schema_node_ids = [row.id for row in schema_vertices.select("id").collect()]
schema_edges = edges.filter(
    col("src").isin(schema_node_ids) & col("dst").isin(schema_node_ids)
)

# Create subgraph
schema_graph = GraphFrame(schema_vertices, schema_edges)

# Run algorithms on subgraph
schema_pagerank = schema_graph.pageRank(resetProbability=0.15, maxIter=10)
schema_pagerank.vertices.orderBy("pagerank", ascending=False).show()
```

### Practical Examples

**Find most connected PII columns:**
```python
# Load graph
g = GraphFrame(vertices, edges)

# Filter to PII columns
pii_columns = g.vertices.filter("node_type = 'column' AND pi_type IN ('PII', 'PHI')")

# Get their degrees
pii_degrees = g.degrees.join(pii_columns, "id")

# Show most connected PII columns (potential privacy risks)
pii_degrees.select("object_name", "pi_type", "degree") \
    .orderBy("degree", ascending=False) \
    .show(20, False)
```

**Identify domain hubs (tables that connect multiple domains):**
```python
# Find tables that link to other tables with different domains
cross_domain = g.find("(t1)-[e]->(t2)") \
    .filter("t1.node_type = 'table'") \
    .filter("t2.node_type = 'table'") \
    .filter("t1.domain != t2.domain") \
    .filter("t1.domain IS NOT NULL") \
    .filter("t2.domain IS NOT NULL")

# Count cross-domain connections per table
cross_domain.groupBy("t1.object_name", "t1.domain") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(20, False)
```

**Find most popular tables and their dependents:**
```python
# Find highly-used tables with downstream dependencies
popular_with_lineage = g.find("(source)-[e]->(target)") \
    .filter("e.relationship_type = 'IS_UPSTREAM_OF'") \
    .filter("source.num_queries_last_7_days > 100") \
    .select(
        "source.object_name as popular_source",
        "source.num_queries_last_7_days as source_queries",
        "target.object_name as dependent_table",
        "target.num_queries_last_7_days as dependent_queries"
    ) \
    .orderBy("source_queries", ascending=False)

popular_with_lineage.show(20, False)
```

**Lineage impact analysis with PageRank:**
```python
# Run PageRank to find most influential tables in lineage
results = g.pageRank(resetProbability=0.15, maxIter=10)

# Combine with popularity metrics
influential_tables = results.vertices \
    .filter("node_type = 'table'") \
    .select(
        "object_name",
        "pagerank",
        "num_queries_last_7_days",
        "domain",
        "pi_type"
    ) \
    .orderBy("pagerank", ascending=False)

influential_tables.show(20, False)

# Find high PageRank tables with PII
influential_pii = influential_tables \
    .filter("pi_type IN ('PII', 'PHI', 'PCI')") \
    .filter("pagerank > 1.0")

influential_pii.show(20, False)
```

**Find lineage chains with motifs:**
```python
# Find 2-hop lineage chains: source -> intermediate -> target
lineage_chain = g.find("(source)-[e1]->(intermediate)-[e2]->(target)") \
    .filter("e1.relationship_type = 'IS_UPSTREAM_OF'") \
    .filter("e2.relationship_type = 'IS_UPSTREAM_OF'") \
    .select(
        "source.object_name as source_table",
        "intermediate.object_name as intermediate_table",
        "target.object_name as target_table",
        "source.num_queries_last_7_days as source_popularity",
        "target.num_queries_last_7_days as target_popularity"
    )

lineage_chain.show(20, False)
```

**Track PII propagation through lineage:**
```python
# Find if PII tables have downstream non-PII tables (potential data leaks)
pii_propagation = g.find("(pii_source)-[e]->(downstream)") \
    .filter("e.relationship_type = 'IS_UPSTREAM_OF'") \
    .filter("pii_source.pi_type IN ('PII', 'PHI', 'PCI')") \
    .filter("downstream.pi_type NOT IN ('PII', 'PHI', 'PCI') OR downstream.pi_type IS NULL") \
    .select(
        "pii_source.object_name as pii_table",
        "pii_source.pi_type as source_classification",
        "downstream.object_name as downstream_table",
        "downstream.pi_type as downstream_classification"
    )

print("Potential PII leaks (PII sources with non-PII downstream tables):")
pii_propagation.show(50, False)
```

**Find tables with no dependencies (orphans) by popularity:**
```python
# Get all table nodes
all_tables = g.vertices.filter("node_type = 'table'")

# Get tables with lineage edges
tables_with_lineage = g.edges \
    .filter("relationship_type = 'IS_UPSTREAM_OF'") \
    .select("source_node_id", "target_node_id") \
    .distinct()

# Find tables with no lineage connections
all_table_ids = all_tables.select(col("id").alias("table_id"))
has_lineage = tables_with_lineage.select(col("source_node_id").alias("table_id")) \
    .union(tables_with_lineage.select(col("target_node_id").alias("table_id"))) \
    .distinct()

orphan_tables = all_table_ids.join(has_lineage, "table_id", "left_anti")

# Join back to get full table info
orphans_with_info = orphan_tables.join(
    all_tables.select("id", "object_name", "num_queries_last_7_days", "domain"),
    orphan_tables["table_id"] == all_tables["id"]
).select("object_name", "num_queries_last_7_days", "domain") \
 .orderBy("num_queries_last_7_days", ascending=False)

print("Tables with no lineage connections:")
orphans_with_info.show(50, False)
```

## Configuration

Enable in `variables.yml`:

```yaml
build_knowledge_graph:
  description: Build knowledge graph from metadata logs
  default: true  # Set to true to enable
```

The knowledge graph builds automatically after successful metadata generation.

