__version__ = "0.5.3"

from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.overrides import (
    build_condition,
    apply_overrides_with_loop,
    apply_overrides_with_joins,
    override_metadata_from_csv,
)
from src.dbxmetagen.knowledge_base import build_knowledge_base
from src.dbxmetagen.column_knowledge_base import build_column_knowledge_base
from src.dbxmetagen.schema_knowledge_base import build_schema_knowledge_base
from src.dbxmetagen.extended_metadata import extract_extended_metadata
from src.dbxmetagen.knowledge_graph import build_knowledge_graph, build_extended_knowledge_graph
from src.dbxmetagen.profiling import run_profiling
from src.dbxmetagen.data_quality import compute_data_quality
from src.dbxmetagen.embeddings import generate_embeddings, find_similar_nodes
from src.dbxmetagen.similarity_edges import build_similarity_edges
from src.dbxmetagen.ontology import build_ontology
from src.dbxmetagen.ontology_validator import validate_ontology

__all__ = [
    "MetadataConfig",
    "build_condition",
    "apply_overrides_with_loop",
    "apply_overrides_with_joins",
    "override_metadata_from_csv",
    # Knowledge base
    "build_knowledge_base",
    "build_column_knowledge_base",
    "build_schema_knowledge_base",
    "extract_extended_metadata",
    # Knowledge graph
    "build_knowledge_graph",
    "build_extended_knowledge_graph",
    # Profiling
    "run_profiling",
    "compute_data_quality",
    # Embeddings & similarity
    "generate_embeddings",
    "find_similar_nodes",
    "build_similarity_edges",
    # Ontology
    "build_ontology",
    "validate_ontology",
]
