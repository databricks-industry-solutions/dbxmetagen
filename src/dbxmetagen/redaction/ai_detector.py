"""
AI-based PHI/PII detection functions.

This module provides functions for detecting PHI/PII using AI/LLM-based approaches,
particularly leveraging Databricks AI endpoints.
"""

import json
import re
from typing import List, Union
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

from .config import PHI_PROMPT_SKELETON, LABEL_ENUMS


def make_prompt(
    prompt_skeleton: str = PHI_PROMPT_SKELETON,
    labels: Union[List[str], str] = LABEL_ENUMS,
) -> str:
    """
    Create a PHI detection prompt with specified entity labels.

    Args:
        prompt_skeleton: Template prompt with {label_enums} placeholder
        labels: List of entity type labels or JSON string of labels

    Returns:
        Formatted prompt string ready to use with LLM

    Example:
        >>> prompt = make_prompt(labels=["PERSON", "EMAIL", "PHONE"])
        >>> # Use prompt with ai_query or similar
    """
    if isinstance(labels, list):
        labels_str = json.dumps(labels)
    else:
        labels_str = labels

    return prompt_skeleton.format(label_enums=labels_str)


@pandas_udf(StringType())
def format_entity_response_object_udf(
    identified_entities_series: pd.Series, sentences: pd.Series
) -> pd.Series:
    """
    Format AI-detected entities with position information.

    This UDF takes the entity list from AI responses and enhances it with
    precise position information (start/end indices) by finding all occurrences
    in the original text. Note that if the same entity is identified under differing entity types,
    the resulting object will have duplicate positions for those types.

    Args:
        identified_entities_series: Series of JSON strings with entity lists
        sentences: Series of original text strings

    Returns:
        Series of JSON strings with enhanced entity objects including positions

    Notes:
        - Handles multiple occurrences of the same entity
        - Uses regex for exact matching
        - Deduplicates entities before position matching

    Example:
        >>> df = df.withColumn("formatted_entities",
        ...     format_entity_response_object_udf(col("ai_response"), col("text")))
    """
    new_entity_series = []

    for entity_list, sentence in zip(identified_entities_series, sentences):
        try:
            entities = json.loads(entity_list)
        except (json.JSONDecodeError, TypeError):
            new_entity_series.append(json.dumps([]))
            continue

        unique_entities_set = set(
            [
                (entity["entity"], entity["entity_type"])
                for entity in entities
                if "entity" in entity and "entity_type" in entity
            ]
        )

        new_entity_list = []

        for entity_text, entity_type in unique_entities_set:
            # Use case-insensitive regex to improve recall
            pattern = re.escape(entity_text)
            positions = [
                (m.start(), m.end() - 1)
                for m in re.finditer(pattern, sentence, re.IGNORECASE)
            ]

            for position in positions:
                # Extract the actual text from the sentence to preserve original case
                actual_text = sentence[position[0] : position[1] + 1]
                new_entity_list.append(
                    {
                        "entity": actual_text,  # Use actual text from sentence, not entity_text
                        "entity_type": entity_type,
                        "start": position[0],
                        "end": position[1],
                        "score": None,
                        "analysis_explanation": None,
                        "recognition_metadata": {},
                    }
                )

        new_entity_series.append(json.dumps(new_entity_list))

    return pd.Series(new_entity_series)
