"""
Entity alignment functions for combining multiple PHI detection methods.

This module provides functions to align and merge entities detected by different
methods (e.g., Presidio and AI-based detection) to create a consensus view.
"""

from typing import List, Dict, Any, Optional
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DoubleType

from .utils import is_fuzzy_match, is_overlap
from .config import DEFAULT_FUZZY_MATCH_THRESHOLD, HIGH_CONFIDENCE_THRESHOLD


def align_entities_row(
    ai_entities: List[Dict[str, Any]],
    presidio_entities: List[Dict[str, Any]],
    doc_id: str,
    fuzzy_threshold: int = DEFAULT_FUZZY_MATCH_THRESHOLD
) -> List[Dict[str, Any]]:
    """
    Align entities from AI and Presidio detection for a single document.
    
    This function matches entities detected by AI with those detected by Presidio,
    assigning confidence scores based on the level of agreement.
    
    Matching logic:
    - High confidence: Exact match (entity text, start, end positions)
    - Medium confidence: Overlapping positions with fuzzy text match
    - Low confidence: Only detected by one method
    
    Args:
        ai_entities: List of entities detected by AI
        presidio_entities: List of entities detected by Presidio
        doc_id: Document identifier
        fuzzy_threshold: Threshold for fuzzy string matching (0-100)
        
    Returns:
        List of aligned entities with confidence scores
        
    Example:
        >>> ai_ents = [{"entity": "John", "start": 0, "end": 3}]
        >>> pres_ents = [{"entity": "John", "start": 0, "end": 3, "score": 0.9}]
        >>> aligned = align_entities_row(ai_ents, pres_ents, "doc1")
    """
    results = []
    used_presidio = set()
    
    for ai_ent in ai_entities:
        best_match = None
        presidio_score = None
        confidence = "low"

        for i, pres_ent in enumerate(presidio_entities):
            if str(pres_ent.get('doc_id')) != str(doc_id):
                continue
            
            if (str(ai_ent.get('entity')) == str(pres_ent.get('entity')) and
                int(ai_ent.get('start')) == int(pres_ent.get('start')) and
                int(ai_ent.get('end')) == int(pres_ent.get('end'))):
                
                best_match = pres_ent
                presidio_score = float(pres_ent.get('score')) if pres_ent.get('score') is not None else None
                confidence = "high"
                used_presidio.add(i)
                break
            
            # Check for overlapping match with fuzzy text similarity
            elif (is_overlap(
                    int(ai_ent.get('start')), int(ai_ent.get('end')),
                    int(pres_ent.get('start')), int(pres_ent.get('end'))
                ) and 
                is_fuzzy_match(
                    str(ai_ent.get('entity')), 
                    str(pres_ent.get('entity')),
                    threshold=fuzzy_threshold
                )):
                
                presidio_score = float(pres_ent.get('score')) if pres_ent.get('score') is not None else None
                
                # Prefer the longer entity (more specific)
                if len(str(pres_ent.get('entity'))) > len(str(ai_ent.get('entity'))):
                    best_match = pres_ent
                    confidence = "medium" if presidio_score and presidio_score >= HIGH_CONFIDENCE_THRESHOLD else "low"
                    used_presidio.add(i)
                else:
                    confidence = "medium"
                    used_presidio.add(i)
                
        # Add the aligned entity
        results.append({
            "entity": str(best_match['entity']) if best_match else str(ai_ent.get('entity')),
            "start": int(best_match['start']) if best_match else int(ai_ent.get('start')),
            "end": int(best_match['end']) if best_match else int(ai_ent.get('end')),
            "doc_id": str(doc_id) if doc_id is not None else None,
            "presidio_score": float(presidio_score) if presidio_score is not None else None,
            "confidence": str(confidence)
        })
    
    # Second pass: Add Presidio entities that weren't matched
    for i, pres_ent in enumerate(presidio_entities):
        if i not in used_presidio and str(pres_ent.get('doc_id')) == str(doc_id):
            score = float(pres_ent.get('score')) if pres_ent.get('score') is not None else None
            results.append({
                "entity": str(pres_ent.get('entity')),
                "start": int(pres_ent.get('start')),
                "end": int(pres_ent.get('end')),
                "doc_id": str(doc_id) if doc_id is not None else None,
                "presidio_score": score,
                "confidence": "low" if score is not None and score < HIGH_CONFIDENCE_THRESHOLD else "medium"
            })
    
    return results


def align_entities_udf(fuzzy_threshold: int = DEFAULT_FUZZY_MATCH_THRESHOLD):
    """
    Create a Pandas UDF for aligning entities from AI and Presidio detection.
    
    Args:
        fuzzy_threshold: Threshold for fuzzy string matching (0-100)
        
    Returns:
        A Pandas UDF that aligns entities from two detection methods
        
    Example:
        >>> align_udf = align_entities_udf(fuzzy_threshold=50)
        >>> df = df.withColumn("aligned",
        ...     align_udf(col("ai_entities"), col("presidio_entities"), col("doc_id")))
    """
    # Define the schema for the output
    entity_struct = StructType([
        StructField("entity", StringType()),
        StructField("start", IntegerType()),
        StructField("end", IntegerType()),
        StructField("doc_id", StringType()),
        StructField("presidio_score", DoubleType()),
        StructField("confidence", StringType())
    ])
    result_type = ArrayType(entity_struct)
    
    @pandas_udf(result_type)
    def _align_udf(
        ai_col: pd.Series,
        presidio_col: pd.Series,
        doc_id_col: pd.Series
    ) -> pd.Series:
        """Align entities for each row in the batch."""
        return pd.Series([
            align_entities_row(ai, presidio, doc_id, fuzzy_threshold=fuzzy_threshold)
            for ai, presidio, doc_id in zip(ai_col, presidio_col, doc_id_col)
        ])
    
    return _align_udf

