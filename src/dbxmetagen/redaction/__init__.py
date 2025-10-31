"""
PHI/PII Redaction Library for DBXMetagen

This module provides tools for detecting, evaluating, and redacting Protected Health Information (PHI)
and Personally Identifiable Information (PII) in medical text.

Main components:
- Presidio-based detection
- AI-based detection
- Entity alignment between different detection methods
- Evaluation and metrics
"""

from .config import (
    ELIGIBLE_ENTITY_TYPES,
    LABEL_ENUMS,
    ENTITIES_TO_IGNORE,
    PHI_PROMPT_SKELETON,
)

from .utils import (
    is_fuzzy_match,
    is_overlap,
    calculate_overlap,
    calculate_string_overlap,
)

from .presidio import (
    format_presidio_batch_results,
    make_presidio_batch_udf,
)

from .ai_detector import (
    make_prompt,
    format_entity_response_object_udf,
)

from .alignment import (
    align_entities_row,
    align_entities_udf,
)

from .evaluation import (
    evaluate_detection,
    calculate_metrics,
)

__all__ = [
    # Config
    "ELIGIBLE_ENTITY_TYPES",
    "LABEL_ENUMS",
    "ENTITIES_TO_IGNORE",
    "PHI_PROMPT_SKELETON",
    # Utils
    "is_fuzzy_match",
    "is_overlap",
    "calculate_overlap",
    "calculate_string_overlap",
    # Presidio
    "format_presidio_batch_results",
    "make_presidio_batch_udf",
    # AI Detection
    "make_prompt",
    "format_entity_response_object_udf",
    # Alignment
    "align_entities_row",
    "align_entities_udf",
    # Evaluation
    "evaluate_detection",
    "calculate_metrics",
]

