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
    parent_id     -- parent node id (column -> table, table -> schema)
    display_name  -- human-readable label
    short_description -- AI-generated one-line summary
    ontology_id   -- linked ontology entity id (if classified)
    ontology_type -- linked ontology entity type
    sensitivity   -- data sensitivity level (public, internal, confidential, restricted)
    status        -- lifecycle status (active, candidate, deprecated)
    source_system -- origin of the node record
    comment       -- full AI-generated description
    has_pii, has_phi -- boolean flags
    security_level, data_type, quality_score -- additional metadata

TABLE: public.graph_edges
  Composite key: (src, dst, relationship)
  Key columns:
    src            -- source node id
    dst            -- destination node id
    relationship   -- legacy relationship label
    edge_type      -- typed relationship category (see below)
    direction      -- "forward" or "bidirectional"
    weight         -- numeric weight/confidence (0-1)
    join_expression -- SQL join expression for reference edges (e.g. "a.patient_id = b.patient_id")
    join_confidence -- confidence score for the join prediction (0-1)
    ontology_rel   -- ontology relationship name if sourced from ontology
    source_system  -- origin: "knowledge_graph", "ontology", "fk_inference", "embedding_similarity"
    status         -- "active" or "candidate"

EDGE TYPES:
  contains       -- schema contains table, table contains column (direction: forward)
  references     -- FK relationship between tables/columns; check join_expression and join_confidence
  instance_of    -- column/table is an instance of an ontology entity
  has_property   -- ontology entity has a property mapped to a column
  same_domain    -- tables share the same business domain
  same_schema    -- tables share the same database schema
  same_classification -- columns share the same data classification
  similar_to     -- embedding similarity between nodes; weight = cosine similarity
  derives_from   -- lineage: one table derives from another

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
  4. Reference joins: For FK analysis, filter edges by edge_type='references' and
     inspect join_expression and join_confidence.
  5. Ontology mapping: Filter edges by edge_type='instance_of' or 'has_property' to
     discover how columns map to business entities.
  6. Similarity clusters: Use find_similar_nodes or edge_type='similar_to' to discover
     semantically related tables/columns.
""".strip()
