# Databricks notebook source
# MAGIC %md
# MAGIC # Setup MCP Servers for dbxmetagen
# MAGIC
# MAGIC Creates Databricks **Managed MCP** servers that expose dbxmetagen's knowledge
# MAGIC base, knowledge graph, and vector index to any MCP client (Cursor, Claude Code,
# MAGIC AI Playground, custom agents).
# MAGIC
# MAGIC **What gets created:**
# MAGIC 1. **UC Functions MCP** -- SQL table-valued functions for KB + graph queries
# MAGIC 2. **Vector Search MCP** -- validates the existing VS index (no setup needed)
# MAGIC
# MAGIC **Usage:** Set widgets, Run All. Copy the output config into your MCP client.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name (required)")
dbutils.widgets.text("schema_name", "metadata_results", "Schema Name")
dbutils.widgets.text("vs_index_name", "metadata_vs_index", "Vector Search Index Name")
dbutils.widgets.dropdown("drop_existing", "false", ["true", "false"], "Recreate Functions")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog_name").strip()
schema = dbutils.widgets.get("schema_name").strip()
vs_index = dbutils.widgets.get("vs_index_name").strip()
drop_existing = dbutils.widgets.get("drop_existing").lower() == "true"

assert catalog, "catalog_name is required"

fq = lambda t: f"`{catalog}`.`{schema}`.`{t}`"
print(f"Target: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validate Prerequisites

# COMMAND ----------

required_tables = [
    "table_knowledge_base",
    "column_knowledge_base",
    "graph_nodes",
    "graph_edges",
    "fk_predictions",
    "ontology_entities",
]

missing = []
for t in required_tables:
    try:
        spark.sql(f"DESCRIBE TABLE {fq(t)}").limit(1).collect()
    except Exception:
        missing.append(t)

if missing:
    msg = f"Missing tables in {catalog}.{schema}: {missing}"
    print(f"WARNING: {msg}")
    print("Some MCP functions will fail until these tables are populated by the dbxmetagen pipeline.")
else:
    print("All required tables exist.")

# Check VS index
vs_fq = f"{catalog}.{schema}.{vs_index}"
vs_exists = False
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    idx_info = w.vector_search_indexes.get_index(vs_fq)
    vs_exists = True
    print(f"Vector Search index '{vs_fq}' exists (status: {idx_info.status})")
except Exception as e:
    print(f"WARNING: Vector Search index '{vs_fq}' not found: {e}")
    print("The VS MCP server URL will not work until the index is built.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Knowledge Base Functions

# COMMAND ----------

or_replace = "OR REPLACE" if drop_existing else ""

kb_functions = [
    f"""
CREATE {or_replace} FUNCTION {fq('mcp_get_table_metadata')}(table_name_param STRING)
RETURNS TABLE(table_name STRING, comment STRING, domain STRING, subdomain STRING, has_pii BOOLEAN, has_phi BOOLEAN, row_count BIGINT)
COMMENT 'Look up table metadata from the knowledge base. Accepts a full or partial table name.'
RETURN
  SELECT table_name, comment, domain, subdomain, has_pii, has_phi, row_count
  FROM {fq('table_knowledge_base')}
  WHERE table_name LIKE CONCAT('%', table_name_param, '%')
  ORDER BY table_name LIMIT 10
""",
    f"""
CREATE {or_replace} FUNCTION {fq('mcp_get_column_metadata')}(
  table_name_param STRING, column_name_param STRING DEFAULT NULL
)
RETURNS TABLE(table_name STRING, column_name STRING, comment STRING, data_type STRING, classification STRING, classification_type STRING)
COMMENT 'Look up column metadata from the knowledge base. Filter by table name and optionally column name.'
RETURN
  SELECT table_name, column_name, comment, data_type, classification, classification_type
  FROM {fq('column_knowledge_base')}
  WHERE table_name LIKE CONCAT('%', table_name_param, '%')
    AND (column_name_param IS NULL OR column_name LIKE CONCAT('%', column_name_param, '%'))
  ORDER BY table_name, column_name LIMIT 50
""",
    f"""
CREATE {or_replace} FUNCTION {fq('mcp_search_knowledge_base')}(
  search_term STRING, domain_filter STRING DEFAULT NULL, max_results INT DEFAULT 20
)
RETURNS TABLE(source STRING, table_name STRING, column_name STRING, comment STRING, domain STRING)
COMMENT 'Search across the table and column knowledge bases by keyword. Optionally filter by domain.'
RETURN
  SELECT * FROM (
    SELECT 'table' AS source, table_name, NULL AS column_name, comment, domain
    FROM {fq('table_knowledge_base')}
    WHERE (comment LIKE CONCAT('%', search_term, '%') OR table_name LIKE CONCAT('%', search_term, '%'))
      AND (domain_filter IS NULL OR domain = domain_filter)
    UNION ALL
    SELECT 'column' AS source, c.table_name, c.column_name, c.comment, t.domain
    FROM {fq('column_knowledge_base')} c
    LEFT JOIN {fq('table_knowledge_base')} t ON c.table_name = t.table_name
    WHERE (c.comment LIKE CONCAT('%', search_term, '%') OR c.column_name LIKE CONCAT('%', search_term, '%'))
      AND (domain_filter IS NULL OR t.domain = domain_filter)
  )
  ORDER BY source, table_name LIMIT max_results
""",
    f"""
CREATE {or_replace} FUNCTION {fq('mcp_get_fk_predictions')}(
  table_name_param STRING, min_confidence DOUBLE DEFAULT 0.5
)
RETURNS TABLE(src_table STRING, src_column STRING, dst_table STRING, dst_column STRING, confidence DOUBLE, ai_reasoning STRING)
COMMENT 'Get predicted foreign key relationships for a table. Shows both outgoing and incoming FK edges above a confidence threshold.'
RETURN
  SELECT src_table, src_column, dst_table, dst_column,
         ROUND(final_confidence, 3) AS confidence, ai_reasoning
  FROM {fq('fk_predictions')}
  WHERE (src_table LIKE CONCAT('%', table_name_param, '%')
      OR dst_table LIKE CONCAT('%', table_name_param, '%'))
    AND final_confidence >= min_confidence
  ORDER BY final_confidence DESC LIMIT 30
""",
    f"""
CREATE {or_replace} FUNCTION {fq('mcp_get_ontology_entities')}(table_name_param STRING DEFAULT NULL)
RETURNS TABLE(entity_name STRING, entity_type STRING, description STRING, source_tables_csv STRING, confidence DOUBLE, entity_uri STRING, source_ontology STRING)
COMMENT 'Get ontology entity mappings. Optionally filter by source table name.'
RETURN
  SELECT entity_name, entity_type, description,
         ARRAY_JOIN(source_tables, ', ') AS source_tables_csv,
         confidence, entity_uri, source_ontology
  FROM {fq('ontology_entities')}
  WHERE table_name_param IS NULL
     OR EXISTS(source_tables, t -> t LIKE CONCAT('%', table_name_param, '%'))
  ORDER BY entity_name LIMIT 50
""",
]

for ddl in kb_functions:
    fname = ddl.split("FUNCTION")[1].split("(")[0].strip()
    try:
        spark.sql(ddl)
        print(f"Created: {fname}")
    except Exception as e:
        if "already exists" in str(e).lower() and not drop_existing:
            print(f"Exists (skip): {fname} -- set drop_existing=true to recreate")
        else:
            print(f"ERROR creating {fname}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Knowledge Graph Functions

# COMMAND ----------

graph_functions = [
    f"""
CREATE {or_replace} FUNCTION {fq('mcp_query_graph_nodes')}(
  node_type_param STRING DEFAULT NULL,
  domain_param STRING DEFAULT NULL,
  search_term STRING DEFAULT NULL,
  max_results INT DEFAULT 20
)
RETURNS TABLE(id STRING, node_type STRING, domain STRING, display_name STRING, short_description STRING, security_level STRING, sensitivity STRING, ontology_type STRING)
COMMENT 'Search the knowledge graph nodes. Filter by node_type (table, column, schema, entity), domain, or keyword in id/comment.'
RETURN
  SELECT id, node_type, domain, display_name, short_description, security_level, sensitivity, ontology_type
  FROM {fq('graph_nodes')}
  WHERE (node_type_param IS NULL OR node_type = node_type_param)
    AND (domain_param IS NULL OR domain = domain_param)
    AND (search_term IS NULL OR id LIKE CONCAT('%', search_term, '%') OR comment LIKE CONCAT('%', search_term, '%'))
  ORDER BY display_name LIMIT max_results
""",
    f"""
CREATE {or_replace} FUNCTION {fq('mcp_get_node_details')}(node_id_param STRING)
RETURNS TABLE(id STRING, table_name STRING, node_type STRING, domain STRING, subdomain STRING,
              display_name STRING, short_description STRING, comment STRING,
              has_pii BOOLEAN, has_phi BOOLEAN, security_level STRING, sensitivity STRING,
              ontology_id STRING, ontology_type STRING, data_type STRING, parent_id STRING,
              source_system STRING, status STRING)
COMMENT 'Get full details for a specific graph node by its ID.'
RETURN
  SELECT id, table_name, node_type, domain, subdomain,
         display_name, short_description, comment,
         has_pii, has_phi, security_level, sensitivity,
         ontology_id, ontology_type, data_type, parent_id,
         source_system, status
  FROM {fq('graph_nodes')}
  WHERE id = node_id_param
""",
    f"""
CREATE {or_replace} FUNCTION {fq('mcp_find_related_nodes')}(
  node_id_param STRING,
  edge_type_param STRING DEFAULT NULL,
  min_weight DOUBLE DEFAULT 0.0,
  max_results INT DEFAULT 20
)
RETURNS TABLE(neighbor_id STRING, relationship STRING, edge_type STRING, weight DOUBLE,
              join_expression STRING, neighbor_type STRING, neighbor_domain STRING,
              neighbor_name STRING, neighbor_description STRING)
COMMENT 'Find nodes directly connected to a given node (1-hop). Optionally filter by edge_type (contains, references, similar_to, similar_embedding, derives_from) and minimum weight.'
RETURN
  SELECT e.dst AS neighbor_id, e.relationship, e.edge_type, e.weight,
         e.join_expression,
         n.node_type AS neighbor_type, n.domain AS neighbor_domain,
         n.display_name AS neighbor_name, n.short_description AS neighbor_description
  FROM {fq('graph_edges')} e
  JOIN {fq('graph_nodes')} n ON e.dst = n.id
  WHERE e.src = node_id_param
    AND (edge_type_param IS NULL OR e.edge_type = edge_type_param)
    AND e.weight >= min_weight
  UNION ALL
  SELECT e.src AS neighbor_id, e.relationship, e.edge_type, e.weight,
         e.join_expression,
         n.node_type AS neighbor_type, n.domain AS neighbor_domain,
         n.display_name AS neighbor_name, n.short_description AS neighbor_description
  FROM {fq('graph_edges')} e
  JOIN {fq('graph_nodes')} n ON e.src = n.id
  WHERE e.dst = node_id_param
    AND (edge_type_param IS NULL OR e.edge_type = edge_type_param)
    AND e.weight >= min_weight
  ORDER BY weight DESC LIMIT max_results
""",
    f"""
CREATE {or_replace} FUNCTION {fq('mcp_traverse_graph')}(
  start_node_param STRING,
  max_hops_param INT DEFAULT 2,
  edge_type_param STRING DEFAULT NULL
)
RETURNS TABLE(node_id STRING, node_type STRING, domain STRING, display_name STRING, short_description STRING, hop INT)
COMMENT 'Multi-hop graph traversal from a starting node using recursive expansion. Returns all reachable nodes within max_hops (1-4). Optionally filter by edge type.'
RETURN
  WITH RECURSIVE traversal(nid, hop, path) AS (
    SELECT start_node_param, 0, ARRAY(start_node_param)
    UNION ALL
    SELECT e.dst, t.hop + 1, ARRAY_APPEND(t.path, e.dst)
    FROM traversal t
    JOIN {fq('graph_edges')} e ON e.src = t.nid
    WHERE t.hop < LEAST(max_hops_param, 4)
      AND NOT ARRAY_CONTAINS(t.path, e.dst)
      AND (edge_type_param IS NULL OR e.edge_type = edge_type_param)
    UNION ALL
    SELECT e.src, t.hop + 1, ARRAY_APPEND(t.path, e.src)
    FROM traversal t
    JOIN {fq('graph_edges')} e ON e.dst = t.nid
    WHERE t.hop < LEAST(max_hops_param, 4)
      AND NOT ARRAY_CONTAINS(t.path, e.src)
      AND (edge_type_param IS NULL OR e.edge_type = edge_type_param)
  )
  SELECT DISTINCT n.id AS node_id, n.node_type, n.domain, n.display_name, n.short_description, t.hop
  FROM traversal t
  JOIN {fq('graph_nodes')} n ON n.id = t.nid
  WHERE t.hop > 0
  ORDER BY t.hop, n.display_name
  LIMIT 100
""",
]

for ddl in graph_functions:
    fname = ddl.split("FUNCTION")[1].split("(")[0].strip()
    try:
        spark.sql(ddl)
        print(f"Created: {fname}")
    except Exception as e:
        if "already exists" in str(e).lower() and not drop_existing:
            print(f"Exists (skip): {fname} -- set drop_existing=true to recreate")
        else:
            print(f"ERROR creating {fname}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MCP Server Configuration
# MAGIC
# MAGIC Copy the JSON below into your MCP client configuration.

# COMMAND ----------

import json

host = spark.conf.get("spark.databricks.workspaceUrl", "")
if not host.startswith("https://"):
    host = f"https://{host}"

uc_functions_url = f"{host}/api/2.0/mcp/functions/{catalog}/{schema}"
vs_mcp_url = f"{host}/api/2.0/mcp/vector-search/{catalog}/{schema}/{vs_index}"

cursor_config = {
    "mcpServers": {
        "dbxmetagen-knowledge": {
            "type": "streamable-http",
            "url": uc_functions_url,
            "headers": {"Authorization": "Bearer <YOUR_PAT_OR_OAUTH_TOKEN>"},
        },
    }
}

if vs_exists:
    cursor_config["mcpServers"]["dbxmetagen-vector-search"] = {
        "type": "streamable-http",
        "url": vs_mcp_url,
        "headers": {"Authorization": "Bearer <YOUR_PAT_OR_OAUTH_TOKEN>"},
    }

print("=" * 70)
print("MCP SERVER URLS")
print("=" * 70)
print(f"\nUC Functions (KB + Graph): {uc_functions_url}")
if vs_exists:
    print(f"Vector Search:             {vs_mcp_url}")
else:
    print("Vector Search:             (not available -- build the VS index first)")

print("\n" + "=" * 70)
print("CURSOR / CLAUDE CODE CONFIG  (.cursor/mcp.json)")
print("=" * 70)
print(json.dumps(cursor_config, indent=2))

print("\n" + "=" * 70)
print("FUNCTIONS CREATED")
print("=" * 70)
all_funcs = [
    "mcp_get_table_metadata", "mcp_get_column_metadata", "mcp_search_knowledge_base",
    "mcp_get_fk_predictions", "mcp_get_ontology_entities",
    "mcp_query_graph_nodes", "mcp_get_node_details", "mcp_find_related_nodes",
    "mcp_traverse_graph",
]
for f in all_funcs:
    print(f"  {catalog}.{schema}.{f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Smoke Test

# COMMAND ----------

print("--- mcp_get_table_metadata ---")
display(spark.sql(f"SELECT * FROM {fq('mcp_get_table_metadata')}('') LIMIT 5"))

# COMMAND ----------

print("--- mcp_get_column_metadata ---")
display(spark.sql(f"SELECT * FROM {fq('mcp_get_column_metadata')}('', NULL) LIMIT 5"))

# COMMAND ----------

print("--- mcp_search_knowledge_base ---")
display(spark.sql(f"SELECT * FROM {fq('mcp_search_knowledge_base')}('', NULL, 5)"))

# COMMAND ----------

print("--- mcp_get_fk_predictions ---")
display(spark.sql(f"SELECT * FROM {fq('mcp_get_fk_predictions')}('', 0.0) LIMIT 5"))

# COMMAND ----------

print("--- mcp_get_ontology_entities ---")
display(spark.sql(f"SELECT * FROM {fq('mcp_get_ontology_entities')}(NULL) LIMIT 5"))

# COMMAND ----------

print("--- mcp_query_graph_nodes ---")
display(spark.sql(f"SELECT * FROM {fq('mcp_query_graph_nodes')}(NULL, NULL, NULL, 5)"))

# COMMAND ----------

print("--- mcp_get_node_details (first node) ---")
try:
    first_node = spark.sql(f"SELECT id FROM {fq('graph_nodes')} LIMIT 1").collect()
    if first_node:
        nid = first_node[0]["id"]
        display(spark.sql(f"SELECT * FROM {fq('mcp_get_node_details')}('{nid}')"))
    else:
        print("No graph nodes found -- skipping")
except Exception as e:
    print(f"Skipped: {e}")

# COMMAND ----------

print("--- mcp_find_related_nodes (first node) ---")
try:
    if first_node:
        display(spark.sql(f"SELECT * FROM {fq('mcp_find_related_nodes')}('{nid}', NULL, 0.0, 5)"))
    else:
        print("No graph nodes found -- skipping")
except Exception as e:
    print(f"Skipped: {e}")

# COMMAND ----------

print("--- mcp_traverse_graph (first node, 2 hops) ---")
try:
    if first_node:
        display(spark.sql(f"SELECT * FROM {fq('mcp_traverse_graph')}('{nid}', 2, NULL)"))
    else:
        print("No graph nodes found -- skipping")
except Exception as e:
    print(f"Skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Your MCP servers are ready. Next steps:
# MAGIC 1. Copy the Cursor config from Section 4 into `.cursor/mcp.json`
# MAGIC 2. Replace `<YOUR_PAT_OR_OAUTH_TOKEN>` with a Databricks PAT or set up OAuth
# MAGIC 3. The UC Functions MCP server exposes all 9 functions as tools
# MAGIC 4. The Vector Search MCP server exposes hybrid similarity search
# MAGIC 5. Both servers are visible in your workspace under **Agents > MCP Servers**
