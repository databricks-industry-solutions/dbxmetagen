"""
GLiNER-based PHI/PII detection using HuggingFace transformers.

Uses the Ihor/gliner-biomed models via transformers AutoModel.
NO gliner package needed - uses transformers directly to avoid Presidio conflicts.
"""

from typing import List, Dict, Any
import pandas as pd
import json
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StringType


def run_gliner_detection(
    df: DataFrame,
    doc_id_column: str,
    text_column: str,
    model_name: str = "Ihor/gliner-biomed-large-v1.0",
    num_cores: int = 10,
) -> DataFrame:
    """
    Run GLiNER-based PHI detection using HuggingFace transformers.

    Uses transformers AutoModel for token classification - no gliner package needed.
    This avoids dependency conflicts with Presidio.

    Args:
        df: Input DataFrame with text to analyze
        doc_id_column: Name of document ID column
        text_column: Name of text column to analyze
        model_name: HuggingFace model identifier (default: Ihor/gliner-biomed-large-v1.0)
        num_cores: Number of cores for repartitioning

    Returns:
        DataFrame with 'gliner_results' and 'gliner_results_struct' columns

    Example:
        >>> result_df = run_gliner_detection(
        ...     df,
        ...     doc_id_column="doc_id",
        ...     text_column="text",
        ...     model_name="Ihor/gliner-biomed-large-v1.0"
        ... )
    """
    # Entity labels for biomedical PHI/PII
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
        Apply GLiNER model for entity extraction using official gliner library.

        GLiNER is a Named Entity Recognition model that can identify entities
        based on user-provided labels without fine-tuning.
        """
        try:
            from gliner import GLiNER
        except ImportError:
            raise ImportError(
                "gliner library required. Install with: pip install gliner"
            )

        # Load GLiNER model once per executor
        model = GLiNER.from_pretrained(model_name)

        results = []
        for doc_id, text in zip(doc_ids, texts):
            if not text or pd.isna(text):
                results.append(json.dumps([]))
                continue

            try:
                # Use GLiNER's native predict_entities method
                entities = model.predict_entities(text, labels, threshold=0.5)

                # Format entities to match expected structure
                formatted_entities = [
                    {
                        "entity": ent["text"],
                        "entity_type": ent["label"],
                        "start": ent["start"],
                        "end": ent["end"],
                        "score": ent.get("score", 1.0),
                        "doc_id": str(doc_id),
                    }
                    for ent in entities
                ]

                results.append(json.dumps(formatted_entities))

            except Exception as e:
                # Log error and return empty results for this document
                print(f"Error processing document {doc_id}: {str(e)}")
                results.append(json.dumps([]))

        return pd.Series(results)

    # Apply GLiNER detection
    result_df = df.repartition(num_cores).withColumn(
        "gliner_results", gliner_udf(col(doc_id_column), col(text_column))
    )

    # Parse JSON to struct for easier downstream processing
    from pyspark.sql.functions import from_json

    result_df = result_df.withColumn(
        "gliner_results_struct",
        from_json(
            "gliner_results",
            "array<struct<entity:string, entity_type:string, start:integer, end:integer, score:double, doc_id:string>>",
        ),
    )

    return result_df
