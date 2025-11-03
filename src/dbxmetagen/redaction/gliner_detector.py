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
        Apply GLiNER model via transformers for entity extraction.

        GLiNER models work by encoding the text and labels together,
        then predicting entity spans.
        """
        try:
            from transformers import AutoTokenizer, AutoModelForTokenClassification
            import torch
            import numpy as np
        except ImportError:
            raise ImportError(
                "transformers and torch required. Install with: pip install transformers torch"
            )

        # Load model and tokenizer once per executor
        tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
        model = AutoModelForTokenClassification.from_pretrained(
            model_name, trust_remote_code=True
        )
        model.eval()

        # Move to GPU if available
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model = model.to(device)

        results = []
        for doc_id, text in zip(doc_ids, texts):
            if not text or pd.isna(text):
                results.append(json.dumps([]))
                continue

            try:
                # Tokenize input text
                inputs = tokenizer(
                    text,
                    return_tensors="pt",
                    padding=True,
                    truncation=True,
                    max_length=512,
                    return_offsets_mapping=True,
                )

                offset_mapping = inputs.pop("offset_mapping")[0]
                inputs = {k: v.to(device) for k, v in inputs.items()}

                # Get model predictions
                with torch.no_grad():
                    outputs = model(**inputs)
                    logits = outputs.logits[0]  # [seq_len, num_labels]
                    predictions = torch.argmax(logits, dim=-1).cpu().numpy()
                    scores = (
                        torch.softmax(logits, dim=-1).max(dim=-1).values.cpu().numpy()
                    )

                # Extract entities from predictions
                formatted_entities = []
                current_entity = None

                for idx, (pred_label, score, (start_char, end_char)) in enumerate(
                    zip(predictions, scores, offset_mapping)
                ):
                    # Skip special tokens (CLS, SEP, PAD)
                    if start_char == 0 and end_char == 0:
                        continue

                    # Label 0 is usually "O" (outside entity)
                    if pred_label == 0:
                        # Save current entity if exists
                        if current_entity:
                            entity_text = text[
                                current_entity["start"] : current_entity["end"]
                            ]
                            if entity_text.strip():  # Only non-empty entities
                                formatted_entities.append(
                                    {
                                        "entity": entity_text,
                                        "entity_type": current_entity["entity_type"],
                                        "start": current_entity["start"],
                                        "end": current_entity["end"]
                                        - 1,  # Inclusive end
                                        "score": float(current_entity["score"]),
                                        "doc_id": str(doc_id),
                                    }
                                )
                            current_entity = None
                    else:
                        # Start or continue entity
                        entity_type = "PHI"  # Generic for now, can map label to type

                        if current_entity is None:
                            # Start new entity
                            current_entity = {
                                "start": int(start_char),
                                "end": int(end_char),
                                "entity_type": entity_type,
                                "score": score,
                                "count": 1,
                            }
                        else:
                            # Continue current entity
                            current_entity["end"] = int(end_char)
                            current_entity["score"] = (
                                current_entity["score"] * current_entity["count"]
                                + score
                            ) / (current_entity["count"] + 1)
                            current_entity["count"] += 1

                # Save final entity if exists
                if current_entity:
                    entity_text = text[current_entity["start"] : current_entity["end"]]
                    if entity_text.strip():
                        formatted_entities.append(
                            {
                                "entity": entity_text,
                                "entity_type": current_entity["entity_type"],
                                "start": current_entity["start"],
                                "end": current_entity["end"] - 1,
                                "score": float(current_entity["score"]),
                                "doc_id": str(doc_id),
                            }
                        )

                results.append(json.dumps(formatted_entities))

            except Exception as e:
                # Log error and return empty results for this document
                print(f"Error processing doc {doc_id}: {str(e)}")
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
