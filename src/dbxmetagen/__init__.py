__version__ = "0.8.9"

from dbxmetagen.config import MetadataConfig

_LAZY_IMPORTS = {
    "build_condition": "dbxmetagen.overrides",
    "apply_overrides_with_loop": "dbxmetagen.overrides",
    "apply_overrides_with_joins": "dbxmetagen.overrides",
    "override_metadata_from_csv": "dbxmetagen.overrides",
    "build_knowledge_base": "dbxmetagen.knowledge_base",
    "build_column_knowledge_base": "dbxmetagen.column_knowledge_base",
    "build_schema_knowledge_base": "dbxmetagen.schema_knowledge_base",
    "extract_extended_metadata": "dbxmetagen.extended_metadata",
    "build_knowledge_graph": "dbxmetagen.knowledge_graph",
    "build_extended_knowledge_graph": "dbxmetagen.knowledge_graph",
    "run_profiling": "dbxmetagen.profiling",
    "compute_data_quality": "dbxmetagen.data_quality",
    "generate_embeddings": "dbxmetagen.embeddings",
    "find_similar_nodes": "dbxmetagen.embeddings",
    "build_similarity_edges": "dbxmetagen.similarity_edges",
    "build_ontology": "dbxmetagen.ontology",
    "list_available_bundles": "dbxmetagen.ontology",
    "resolve_bundle_path": "dbxmetagen.ontology",
    "validate_ontology": "dbxmetagen.ontology_validator",
    "predict_foreign_keys": "dbxmetagen.fk_prediction",
    "generate_semantic_layer": "dbxmetagen.semantic_layer",
    "build_vector_index": "dbxmetagen.vector_index",
    "classify_columns_geo": "dbxmetagen.geo_classifier",
    "build_genie_space": "dbxmetagen.genie",
    "assemble_genie_context": "dbxmetagen.genie",
}


def __getattr__(name):
    if name in _LAZY_IMPORTS:
        import importlib
        module = importlib.import_module(_LAZY_IMPORTS[name])
        return getattr(module, name)
    raise AttributeError(f"module 'dbxmetagen' has no attribute {name!r}")


__all__ = [
    "MetadataConfig",
    "build_condition",
    "apply_overrides_with_loop",
    "apply_overrides_with_joins",
    "override_metadata_from_csv",
    "build_knowledge_base",
    "build_column_knowledge_base",
    "build_schema_knowledge_base",
    "extract_extended_metadata",
    "build_knowledge_graph",
    "build_extended_knowledge_graph",
    "run_profiling",
    "compute_data_quality",
    "generate_embeddings",
    "find_similar_nodes",
    "build_similarity_edges",
    "build_ontology",
    "list_available_bundles",
    "resolve_bundle_path",
    "validate_ontology",
    "predict_foreign_keys",
    "generate_semantic_layer",
    "build_vector_index",
    "classify_columns_geo",
    "build_genie_space",
    "assemble_genie_context",
]
