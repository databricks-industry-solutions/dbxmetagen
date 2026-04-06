"""Embedded Oxigraph triplestore for SPARQL over ontology graphs.

Requires pyoxigraph. Install with: pip install 'dbxmetagen[ontology]'

Loaded from Turtle files produced by ontology_turtle.py. Supports
multi-hop traversal, join validation, and relationship discovery
that vector search alone cannot provide.
"""

import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_AVAILABLE = None


def is_available() -> bool:
    """Check if pyoxigraph is installed."""
    global _AVAILABLE
    if _AVAILABLE is None:
        try:
            import pyoxigraph  # noqa: F401
            _AVAILABLE = True
        except ImportError:
            _AVAILABLE = False
    return _AVAILABLE


class OntologyGraphStore:
    """Embedded in-process triplestore backed by pyoxigraph.

    No JVM, no separate server -- pure Python via Rust bindings.
    """

    def __init__(
        self,
        turtle_paths: Optional[List[str]] = None,
        persist_path: Optional[str] = None,
    ):
        if not is_available():
            raise ImportError(
                "pyoxigraph is required for OntologyGraphStore. "
                "Install with: pip install 'dbxmetagen[ontology]'"
            )
        import pyoxigraph as og

        self._og = og
        self.store = og.Store(path=persist_path)
        for path in (turtle_paths or []):
            self.load_turtle(path)

    def load_turtle(self, path: str) -> int:
        """Load a Turtle file into the store. Returns number of triples added."""
        before = len(self.store)
        with open(path, "rb") as f:
            self.store.load(f, format=self._og.RdfFormat.TURTLE)
        added = len(self.store) - before
        logger.info("Loaded %s: %d new triples (total: %d)", path, added, len(self.store))
        return added

    def sparql(self, query: str) -> List[Dict[str, str]]:
        """Execute a SPARQL SELECT query. Returns list of {variable: value} dicts."""
        results = self.store.query(query)
        rows = []
        variables = [str(v) for v in results.variables]
        for row in results:
            rows.append({
                variables[i]: str(val) if val is not None else None
                for i, val in enumerate(row)
            })
        return rows

    def get_related_tables(self, table_uri: str) -> List[Dict[str, str]]:
        """Find tables connected to the given table via ObjectProperties.

        build_turtle encodes edges as (edge, rdfs:domain, tableA) and
        (edge, rdfs:range, tableB), so we query domain/range patterns.
        """
        return self.sparql(f"""
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?related ?edge ?label WHERE {{
                {{
                    ?edge a owl:ObjectProperty ;
                          rdfs:domain <{table_uri}> ;
                          rdfs:range ?related .
                }}
                UNION
                {{
                    ?edge a owl:ObjectProperty ;
                          rdfs:domain ?related ;
                          rdfs:range <{table_uri}> .
                }}
                OPTIONAL {{ ?edge rdfs:label ?label }}
            }}
        """)

    def get_join_path(self, from_entity: str, to_entity: str) -> List[Dict[str, str]]:
        """Find edge paths between two entity types (1-2 hops)."""
        hbo = "https://ontology.databricks.com/dbxmetagen#"
        return self.sparql(f"""
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX hbo: <{hbo}>
            SELECT ?edge ?intermediate ?edge2 WHERE {{
                {{
                    ?edge a owl:ObjectProperty ;
                          rdfs:domain ?src ;
                          rdfs:range ?dst .
                    ?src rdfs:subClassOf hbo:{from_entity} .
                    ?dst rdfs:subClassOf hbo:{to_entity} .
                }}
                UNION
                {{
                    ?edge a owl:ObjectProperty ;
                           rdfs:domain ?src ;
                           rdfs:range ?mid .
                    ?edge2 a owl:ObjectProperty ;
                            rdfs:domain ?mid ;
                            rdfs:range ?dst .
                    ?src rdfs:subClassOf hbo:{from_entity} .
                    ?dst rdfs:subClassOf hbo:{to_entity} .
                    ?mid rdfs:label ?intermediate .
                }}
            }}
        """)

    def validate_join(self, from_entity: str, to_entity: str, edge_name: str) -> bool:
        """Check if a join is valid according to the ontology."""
        hbo = "https://ontology.databricks.com/dbxmetagen#"
        query = f"""
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX hbo: <{hbo}>
            ASK {{
                hbo:{edge_name} a owl:ObjectProperty ;
                    rdfs:domain ?src ;
                    rdfs:range ?dst .
                ?src rdfs:subClassOf* hbo:{from_entity} .
                ?dst rdfs:subClassOf* hbo:{to_entity} .
            }}
        """
        return bool(self.store.query(query))

    @property
    def triple_count(self) -> int:
        return len(self.store)
