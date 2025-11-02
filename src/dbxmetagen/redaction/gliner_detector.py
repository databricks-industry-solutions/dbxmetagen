"""
GLiNER-based PHI/PII detection functions.

This module provides functions for detecting PHI/PII using GLiNER models,
a generalist NER model that can be applied to biomedical text.
"""

from typing import List, Dict, Any
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StringType
import json


def run_gliner_detection(
    df: DataFrame,
    doc_id_column: str,
    text_column: str,
    model_name: str = "urchade/gliner_medium-v2.1",
    num_cores: int = 10,
) -> DataFrame:
    """
    Run GLiNER-based PHI detection on a DataFrame.

    Uses GLiNER model for entity extraction with biomedical labels.

    Args:
        df: Input DataFrame with text to analyze
        doc_id_column: Name of document ID column
        text_column: Name of text column to analyze
        model_name: HuggingFace model identifier for GLiNER
        num_cores: Number of cores for repartitioning

    Returns:
        DataFrame with 'gliner_results' and 'gliner_results_struct' columns

    Example:
        >>> result_df = run_gliner_detection(
        ...     df,
        ...     doc_id_column="doc_id",
        ...     text_column="text",
        ...     model_name="urchade/gliner_medium-v2.1"
        ... )
    """
    # Define entity labels for biomedical text
    labels = [
        "person",
        "location",
        "organization",
        "date",
        "phone number",
        "email",
        "medical record number",
        "social security number",
        "address",
    ]

    @pandas_udf(StringType())
    def gliner_udf(doc_ids: pd.Series, texts: pd.Series) -> pd.Series:
        """
        Apply GLiNER model to extract entities from text.
        """
        try:
            from gliner import GLiNER
        except ImportError:
            raise ImportError("GLiNER not installed. Install with: pip install gliner")

        # Load model once per executor
        model = GLiNER.from_pretrained(model_name)

        results = []
        for doc_id, text in zip(doc_ids, texts):
            if not text or pd.isna(text):
                results.append(json.dumps([]))
                continue

            # Run prediction
            entities = model.predict_entities(text, labels, threshold=0.3)

            # Format results
            formatted_entities = []
            for entity in entities:
                formatted_entities.append(
                    {
                        "entity": entity["text"],
                        "entity_type": entity["label"].upper().replace(" ", "_"),
                        "start": entity["start"],
                        "end": entity["end"]
                        - 1,  # GLiNER uses exclusive end, we use inclusive
                        "score": entity["score"],
                        "doc_id": str(doc_id),
                    }
                )

            results.append(json.dumps(formatted_entities))

        return pd.Series(results)

    # Apply GLiNER detection
    result_df = df.repartition(num_cores).withColumn(
        "gliner_results", gliner_udf(col(doc_id_column), col(text_column))
    )

    # Parse JSON to struct
    from pyspark.sql.functions import from_json

    result_df = result_df.withColumn(
        "gliner_results_struct",
        from_json(
            "gliner_results",
            "array<struct<entity:string, entity_type:string, start:integer, end:integer, score:double, doc_id:string>>",
        ),
    )

    return result_df
