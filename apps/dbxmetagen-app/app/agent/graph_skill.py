"""Graph schema context for LLM prompts in the multi-agent pipeline.

Provides GRAPH_SCHEMA_CONTEXT -- a structured description of node types,
edge types, properties, and effective query patterns so that the planner
and retrieval agents can reason about the knowledge graph.
"""

GRAPH_SCHEMA_CONTEXT = """
=== KNOWLEDGE GRAPH SCHEMA ===

TABLE: public.graph_nodes
  Primary key: id (STRING) -- fully qualified identifier, e.g. "catalog.schema.table" or "catalog.schema.table.column"
  Key columns:
    node_type     -- one of: table, column, schema, entity
    domain        -- business domain classification
    parent_id     -- parent node id (column -> table, table -> schema; NULL for entities)
    display_name  -- human-readable label
    short_description -- AI-generated one-line summary
    ontology_id   -- linked ontology entity id (if classified)
    ontology_type -- entity type classification (for entity nodes)
    sensitivity   -- data sensitivity level (public, PII, PHI)
    status        -- lifecycle status or entity role (discovered, primary, referenced)
    source_system -- origin: "knowledge_graph" or "ontology"
    comment       -- full AI-generated description
    has_pii, has_phi -- boolean flags
    security_level, data_type, quality_score -- additional metadata

TABLE: public.graph_edges
  Composite key: (src, dst, relationship)
  Key columns:
    src            -- source node id
    dst            -- destination node id
    relationship   -- relationship type (always matches edge_type)
    edge_type      -- same as relationship
    direction      -- "out" or "undirected"
    weight         -- numeric weight/confidence (0-1)
    join_expression -- SQL join expression for FK edges (e.g. "a.patient_id = b.patient_id")
    join_confidence -- confidence score for the join prediction (0-1)
    ontology_rel   -- ontology relationship name if sourced from ontology
    source_system  -- origin: "knowledge_graph", "ontology", "fk_predictions", "embedding_similarity"
    status         -- "active" or "candidate"

EDGE TYPES (relationship and edge_type are always equal):
  contains            -- schema contains table, table contains column (direction: out)
  references          -- FK relationship between tables; check join_expression and join_confidence
  instance_of         -- table is an instance of an ontology entity
  has_property        -- ontology entity has a property mapped to a column
  is_a                -- entity type hierarchy (child -> parent)
  same_domain         -- tables share the same business domain
  same_subdomain      -- tables share the same subdomain
  same_catalog        -- tables share the same catalog
  same_schema         -- tables share the same database schema
  same_security_level -- tables share the same PII/PHI classification
  same_classification -- columns share the same data classification
  similar_embedding   -- embedding similarity between nodes; weight = cosine similarity
  derives_from        -- lineage: one table derives from another
  <named_rel>         -- ontology-declared relationships (e.g. "treated_by", "prescribed_to")

VS-GRAPH BRIDGE:
  The metadata_documents table (source for Databricks Vector Search) includes a
  node_id column that maps directly to graph_nodes.id.
  Pattern: VS semantic search -> extract node_ids from results -> use those as
  starting points for graph traversal to discover structural context.

EFFECTIVE PATTERNS:
  1. Semantic discovery: Use search_metadata(query) to find relevant nodes by meaning.
     Extract node_ids from VS results to bridge into graph traversal.
  2. Structural exploration: Use traverse_graph(start_node, edge_type=...) for multi-hop
     BFS through typed relationships.
  3. Hybrid: Combine VS for fuzzy matching with graph traversal for precise structural
     lookups (FKs, containment, lineage).
  4. Reference joins: For FK analysis, filter edges by relationship='references' and
     inspect join_expression and join_confidence.
  5. Ontology mapping: Filter edges by relationship='instance_of' or 'has_property' to
     discover how columns map to business entities.
  6. Similarity clusters: Use find_similar_nodes or relationship='similar_embedding' to
     discover semantically related tables/columns.
""".strip()
