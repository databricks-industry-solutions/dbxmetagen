__version__ = "0.0.3"

from dbxmetagen.config import MetadataConfig
from dbxmetagen.overrides import (
    build_condition,
    apply_overrides_with_loop,
    apply_overrides_with_joins,
    override_metadata_from_csv,
)
from dbxmetagen.knowledge_base import build_knowledge_base
from dbxmetagen.column_knowledge_base import build_column_knowledge_base
from dbxmetagen.schema_knowledge_base import build_schema_knowledge_base
from dbxmetagen.extended_metadata import extract_extended_metadata
from dbxmetagen.knowledge_graph import (
    build_knowledge_graph,
    build_extended_knowledge_graph,
)
from dbxmetagen.profiling import run_profiling
from dbxmetagen.data_quality import compute_data_quality
from dbxmetagen.embeddings import generate_embeddings, find_similar_nodes
from dbxmetagen.similarity_edges import build_similarity_edges
from dbxmetagen.ontology import (
    build_ontology,
    list_available_bundles,
    resolve_bundle_path,
)
from dbxmetagen.ontology_validator import validate_ontology
from dbxmetagen.fk_prediction import predict_foreign_keys
from dbxmetagen.semantic_layer import generate_semantic_layer
from dbxmetagen.vector_index import build_vector_index
from dbxmetagen.geo_classifier import classify_columns_geo

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
    "list_available_bundles",
    "resolve_bundle_path",
    "validate_ontology",
    # FK prediction
    "predict_foreign_keys",
    # Semantic layer
    "generate_semantic_layer",
    # Geo classification
    "classify_columns_geo",
]
