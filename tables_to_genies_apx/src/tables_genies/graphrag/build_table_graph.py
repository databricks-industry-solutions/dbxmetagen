"""
GraphRAG-based table relationship graph builder.

Uses concepts from Microsoft GraphRAG:
- Entity extraction from table/column metadata
- Community detection for clustering related tables
- Semantic relationship discovery
"""
from typing import List, Dict, Any, Callable, Optional, Awaitable
import networkx as nx
from collections import defaultdict
import json
import asyncio


class GraphRAGTableGraphBuilder:
    """
    Builds table relationship graph using GraphRAG concepts.
    
    Implementation supports:
    - Structural analysis: Schema/catalog co-location, column overlap, FK hints
    - LLM-based semantic analysis: Entity extraction, semantic relationship detection
    - Community detection (Louvain algorithm)
    - Domain clustering
    """
    
    def __init__(self):
        self.graph = None
        self.communities = {}
        self.semantic_entities = {}
    
    def extract_entities(self, enriched_tables: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract entities from table metadata.
        
        Entities include:
        - Table names
        - Column names (as potential FK indicators)
        - Data types patterns
        - Schema/catalog groupings
        """
        entities = {
            'tables': {},
            'columns': defaultdict(list),
            'schemas': defaultdict(list),
            'catalogs': defaultdict(list),
        }
        
        for table_data in enriched_tables:
            if not table_data.get('enriched'):
                continue
            
            fqn = table_data['fqn']
            catalog = table_data.get('catalog', fqn.split('.')[0])
            schema = table_data.get('schema', fqn.split('.')[1])
            table_name = table_data.get('table', fqn.split('.')[-1])
            
            # Table entity with rich metadata
            entities['tables'][fqn] = {
                'fqn': fqn,
                'catalog': catalog,
                'schema': schema,
                'table': table_name,
                'column_count': table_data.get('column_count', 0),
                'columns': [col.get('name') for col in table_data.get('columns', [])],
                'table_description': table_data.get('table_description', ''),
                'enriched_columns': table_data.get('enriched_columns', []),
            }
            
            # Column entities (for FK detection)
            for col in table_data.get('columns', []):
                col_name = col.get('name')
                if col_name:
                    entities['columns'][col_name].append(fqn)
            
            # Schema/catalog groupings
            entities['schemas'][schema].append(fqn)
            entities['catalogs'][catalog].append(fqn)
        
        return entities
    
    def detect_relationships(self, entities: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Detect relationships between tables.
        
        Relationships:
        - Same schema (strong relationship)
        - Same catalog (moderate relationship)
        - Shared column names (potential FK)
        - Similar column patterns
        """
        relationships = []
        
        tables = entities['tables']
        table_fqns = list(tables.keys())
        
        for i, fqn1 in enumerate(table_fqns):
            for fqn2 in table_fqns[i+1:]:
                table1 = tables[fqn1]
                table2 = tables[fqn2]
                
                weight = 0
                rel_types = []
                
                # Same schema = strong relationship (5 points)
                if table1['schema'] == table2['schema']:
                    weight += 5
                    rel_types.append('same_schema')
                
                # Same catalog = moderate relationship (2 points)
                elif table1['catalog'] == table2['catalog']:
                    weight += 2
                    rel_types.append('same_catalog')
                
                # Column name overlap (1 point per shared column)
                cols1 = set(table1['columns'])
                cols2 = set(table2['columns'])
                overlap = cols1 & cols2
                if overlap:
                    weight += len(overlap)
                    rel_types.append(f'column_overlap_{len(overlap)}')
                
                # FK hints (column names ending with _id)
                id_cols1 = {c for c in cols1 if c.endswith('_id')}
                id_cols2 = {c for c in cols2 if c.endswith('_id')}
                if id_cols1 & id_cols2:
                    weight += 3
                    rel_types.append('fk_hint')
                
                if weight > 0:
                    relationships.append({
                        'source': fqn1,
                        'target': fqn2,
                        'weight': weight,
                        'types': rel_types
                    })
        
        return relationships
    
    async def extract_entities_with_llm(
        self, 
        enriched_tables: List[Dict[str, Any]], 
        llm_func: Callable[[str], Awaitable[str]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Extract semantic entities using LLM.
        
        Args:
            enriched_tables: List of enriched table metadata with table_description
            llm_func: Async function that takes a prompt and returns LLM response
            
        Returns:
            Dict mapping table FQN to extracted entities
        """
        entities_by_table = {}
        
        # Build prompt for batch entity extraction
        table_summaries = []
        for table in enriched_tables:
            if not table.get('enriched'):
                continue
            
            fqn = table['fqn']
            description = table.get('table_description', '')
            columns = table.get('enriched_columns', [])
            
            # Create a concise summary
            col_summary = ', '.join([
                f"{col.get('column_name')} ({col.get('data_type')})"
                for col in columns[:10]
            ])
            
            table_summaries.append({
                'fqn': fqn,
                'table_name': table['table'],
                'description': description,
                'columns': col_summary
            })
        
        prompt = f"""Analyze these database tables and extract semantic entities for each.

Tables:
{json.dumps(table_summaries, indent=2)}

For each table, identify:
1. Primary domain (e.g., "healthcare", "finance", "e-commerce")
2. Key business concepts (e.g., ["patient", "claim", "insurance"])
3. Data themes (e.g., ["transactional", "regulatory", "analytical"])

Return a JSON object mapping each FQN to its entities. Format:
{{
  "catalog.schema.table": {{
    "domain": "...",
    "concepts": ["...", "..."],
    "themes": ["...", "..."]
  }}
}}

Only return valid JSON, no explanations."""

        try:
            response = await llm_func(prompt)
            # Clean response
            response_clean = response.replace('```json', '').replace('```', '').strip()
            
            # Try to parse as JSON
            try:
                entities_by_table = json.loads(response_clean)
            except json.JSONDecodeError as je:
                print(f"Failed to parse LLM response as JSON: {je}")
                print(f"Response preview: {response_clean[:200]}...")
                return {}
            
            self.semantic_entities = entities_by_table
            return entities_by_table
        except Exception as e:
            print(f"LLM entity extraction failed: {e}")
            import traceback
            traceback.print_exc()
            return {}
    
    async def detect_semantic_relationships(
        self,
        entities_by_table: Dict[str, Dict[str, Any]],
        llm_func: Callable[[str], Awaitable[str]]
    ) -> List[Dict[str, Any]]:
        """
        Detect semantic relationships using LLM.
        
        Args:
            entities_by_table: Extracted entities per table
            llm_func: Async function that takes a prompt and returns LLM response
            
        Returns:
            List of semantic relationship dicts with source, target, weight, reason
        """
        if not entities_by_table:
            return []
        
        prompt = f"""Analyze these table entities and identify semantic relationships.

Table Entities:
{json.dumps(entities_by_table, indent=2)}

Identify pairs of tables that are semantically related based on:
- Shared domain or business concepts
- Complementary data themes (e.g., transactional + regulatory)
- Part of the same business process

For each related pair, provide:
- source: First table FQN
- target: Second table FQN
- confidence: Score from 1-10 indicating relationship strength
- reason: Brief explanation of the relationship

Return a JSON array of relationships. Format:
[
  {{
    "source": "catalog.schema.table1",
    "target": "catalog.schema.table2",
    "confidence": 8,
    "reason": "Both handle insurance claims lifecycle"
  }}
]

Only identify meaningful relationships (confidence >= 5). Return valid JSON only."""

        try:
            response = await llm_func(prompt)
            # Clean response
            response_clean = response.replace('```json', '').replace('```', '').strip()
            
            # Try to parse as JSON
            try:
                relationships = json.loads(response_clean)
            except json.JSONDecodeError as je:
                print(f"Failed to parse LLM response as JSON: {je}")
                print(f"Response preview: {response_clean[:200]}...")
                return []
            
            # Convert to standard format
            semantic_rels = []
            for rel in relationships:
                semantic_rels.append({
                    'source': rel['source'],
                    'target': rel['target'],
                    'weight': rel.get('confidence', 5),
                    'types': ['semantic'],
                    'reason': rel.get('reason', '')
                })
            
            return semantic_rels
        except Exception as e:
            print(f"LLM semantic relationship detection failed: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def detect_communities(self, G: nx.Graph) -> Dict[str, int]:
        """
        Detect communities using Louvain algorithm.
        
        Returns mapping of node -> community_id
        """
        try:
            import community as community_louvain
            communities = community_louvain.best_partition(G)
            return communities
        except ImportError:
            # Fallback: use schema as community
            print("community package not available, using schema-based communities")
            communities = {}
            for node, data in G.nodes(data=True):
                communities[node] = hash(data.get('schema', 'default')) % 10
            return communities
    
    async def build_graph(
        self,
        enriched_tables: List[Dict[str, Any]],
        llm_func: Optional[Callable[[str], Awaitable[str]]] = None
    ) -> nx.Graph:
        """
        Build NetworkX graph with GraphRAG-detected relationships.
        
        Args:
            enriched_tables: List of enriched table metadata dicts
            llm_func: Optional async LLM function for semantic analysis
        
        Returns:
            NetworkX Graph with nodes (tables) and edges (relationships)
        """
        # Extract structural entities
        entities = self.extract_entities(enriched_tables)
        
        # Create graph
        G = nx.Graph()
        
        # Add nodes
        for fqn, table_data in entities['tables'].items():
            G.add_node(fqn, **table_data)
        
        # Add structural edges
        relationships = self.detect_relationships(entities)
        for rel in relationships:
            G.add_edge(
                rel['source'],
                rel['target'],
                weight=rel['weight'],
                types=','.join(rel['types'])
            )
        
        # LLM-based semantic analysis (if available)
        if llm_func:
            try:
                # Extract entities with LLM
                semantic_entities = await self.extract_entities_with_llm(enriched_tables, llm_func)
                
                # Detect semantic relationships
                semantic_rels = await self.detect_semantic_relationships(semantic_entities, llm_func)
                
                # Add semantic edges to graph
                for rel in semantic_rels:
                    # Check if edge already exists
                    if G.has_edge(rel['source'], rel['target']):
                        # Augment existing edge
                        existing_types = G[rel['source']][rel['target']].get('types', '')
                        G[rel['source']][rel['target']]['types'] = existing_types + ',semantic'
                        G[rel['source']][rel['target']]['weight'] += rel['weight']
                        G[rel['source']][rel['target']]['semantic_reason'] = rel.get('reason', '')
                    else:
                        # Add new semantic edge
                        G.add_edge(
                            rel['source'],
                            rel['target'],
                            weight=rel['weight'],
                            types='semantic',
                            semantic_reason=rel.get('reason', '')
                        )
            except Exception as e:
                print(f"LLM analysis failed, continuing with structural graph: {e}")
        
        # Detect communities
        if G.number_of_nodes() > 0:
            communities = self.detect_communities(G)
            for node, community_id in communities.items():
                G.nodes[node]['community'] = community_id
        
        self.graph = G
        self.communities = self._build_community_hierarchy(G)
        
        return G
    
    def _build_community_hierarchy(self, G: nx.Graph) -> Dict[int, List[str]]:
        """Build hierarchy of communities."""
        hierarchy = defaultdict(list)
        for node, data in G.nodes(data=True):
            community_id = data.get('community', 0)
            hierarchy[community_id].append(node)
        return dict(hierarchy)
    
    def to_cytoscape_format(self) -> Dict[str, Any]:
        """
        Convert graph to Cytoscape.js format with rich metadata.
        
        Returns:
            Dict with elements list for Cytoscape.js
        """
        if not self.graph:
            return {'elements': [], 'node_count': 0, 'edge_count': 0}
        
        elements = []
        
        # Nodes with full metadata
        for node, data in self.graph.nodes(data=True):
            # Build simplified columns list for hover panel
            columns_detail = []
            enriched_cols = data.get('enriched_columns', [])
            for col in enriched_cols:
                columns_detail.append({
                    'name': col.get('column_name', ''),
                    'type': col.get('data_type', ''),
                    'comment': col.get('enhanced_comment', col.get('comment', '')),
                    'sample_values': col.get('sample_values', [])[:3]  # Only first 3 samples
                })
            
            elements.append({
                'data': {
                    'id': node,
                    'label': data.get('table', node.split('.')[-1]),
                    'catalog': data.get('catalog', ''),
                    'schema': data.get('schema', ''),
                    'column_count': data.get('column_count', 0),
                    'community': data.get('community', 0),
                    'table_description': data.get('table_description', ''),
                    'columns': columns_detail,
                },
                'classes': f"community-{data.get('community', 0)}"
            })
        
        # Edges
        for source, target, data in self.graph.edges(data=True):
            edge_data = {
                'source': source,
                'target': target,
                'weight': data.get('weight', 1),
                'types': data.get('types', ''),
            }
            
            # Add semantic reason if available
            if 'semantic_reason' in data:
                edge_data['semantic_reason'] = data['semantic_reason']
            
            elements.append({'data': edge_data})
        
        return {
            'elements': elements,
            'node_count': self.graph.number_of_nodes(),
            'edge_count': self.graph.number_of_edges(),
            'communities': self.communities
        }
    
    def get_node_details(self, table_fqn: str) -> Dict[str, Any]:
        """Get detailed information for a specific node."""
        if self.graph and table_fqn in self.graph.nodes:
            return self.graph.nodes[table_fqn]
        return {}
