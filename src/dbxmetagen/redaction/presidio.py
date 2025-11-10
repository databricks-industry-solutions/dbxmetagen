"""
Presidio-based PHI/PII detection functions.

This module provides functions for detecting PHI/PII using Microsoft Presidio,
a data protection and anonymization SDK.
"""

import json
from typing import Iterator, Tuple
import pandas as pd
from pyspark.sql.functions import pandas_udf
from presidio_analyzer import BatchAnalyzerEngine
from presidio_analyzer.dict_analyzer_result import DictAnalyzerResult

from .config import DEFAULT_PRESIDIO_SCORE_THRESHOLD


def format_presidio_batch_results(
    results: Iterator[DictAnalyzerResult],
    score_threshold: float = DEFAULT_PRESIDIO_SCORE_THRESHOLD,
) -> list:
    """
    Format Presidio batch analysis results into a list of JSON strings.

    Args:
        results: Iterator of DictAnalyzerResult from Presidio batch analysis
        score_threshold: Minimum confidence score to include results (0.0-1.0)

    Returns:
        List of JSON-serialized findings for each document

    Example:
        >>> results = batch_analyzer.analyze_dict(text_dict)
        >>> formatted = format_presidio_batch_results(results, score_threshold=0.5)
    """
    col1, col2 = tuple(results)
    doc_ids = col1.value
    original_texts = col2.value
    recognizer_results = col2.recognizer_results

    output = []
    for i, res_doc in enumerate(recognizer_results):
        findings = []
        for res_ent in res_doc:
            ans = res_ent.to_dict()
            ans["doc_id"] = doc_ids[i]
            ans["entity"] = original_texts[i][res_ent.start : res_ent.end]

            if ans.get("score", 0) > score_threshold:
                findings.append(ans)

        output.append(json.dumps(findings))

    return output


def make_presidio_batch_udf(
    score_threshold: float = DEFAULT_PRESIDIO_SCORE_THRESHOLD, add_pci: bool = False
):
    """
    Create a Pandas UDF for batch PHI detection using Presidio.

    **IMPORTANT FOR DEVELOPMENT:** When using in Databricks Repos without installed package,
    this UDF may fail with serialization errors because workers cannot import the module.

    **Recommended approach:** Define the UDF inline in your notebook to capture helper
    functions in the closure. See notebooks/redaction/1. Benchmarking Detection.py for example.

    **For production:** This works fine when the package is properly installed as a wheel/library.

    This function creates a reusable UDF that can be applied to PySpark DataFrames
    for efficient batch processing of text documents.

    Args:
        score_threshold: Minimum confidence score to include results (0.0-1.0)
        add_pci: Whether to add PCI (Payment Card Industry) recognizers

    Returns:
        A Pandas UDF that takes (doc_ids, texts) and returns JSON-serialized results

    Example:
        >>> analyze_udf = make_presidio_batch_udf(score_threshold=0.5)
        >>> df = df.withColumn("phi_results", analyze_udf(col("doc_id"), col("text")))
    """

    @pandas_udf("string")
    def analyze_udf(
        batch_iter: Iterator[Tuple[pd.Series, pd.Series]],
    ) -> Iterator[pd.Series]:
        # Lazy import to avoid serialization issues with heavy dependencies
        from dbxmetagen.deterministic_pi import get_analyzer_engine

        analyzer = get_analyzer_engine(
            add_pci=add_pci, default_score_threshold=score_threshold
        )
        batch_analyzer = BatchAnalyzerEngine(analyzer_engine=analyzer)

        for doc_ids, texts in batch_iter:
            text_dict = pd.DataFrame({"doc_id": doc_ids, "text": texts}).to_dict(
                orient="list"
            )

            results = batch_analyzer.analyze_dict(
                text_dict,
                language="en",
                keys_to_skip=["doc_id"],
                score_threshold=score_threshold,
                batch_size=20,
                n_process=1,
            )

            output = format_presidio_batch_results(
                results, score_threshold=score_threshold
            )
            yield pd.Series(output)

    return analyze_udf
