"""
Example usage patterns for the refactored alignment module.

This file demonstrates various ways to use the alignment functionality
with different source combinations.
"""

from dbxmetagen.redaction.alignment import (
    align_entities_multi_source,
    MultiSourceAligner,
)


def example_two_sources_presidio_ai():
    """Example: Align entities from Presidio and AI detection."""

    # Sample entities from Presidio
    presidio_entities = [
        {
            "entity": "John Smith",
            "start": 8,
            "end": 18,
            "entity_type": "PERSON",
            "score": 0.95,
            "doc_id": "doc1",
        },
        {
            "entity": "test@email.com",
            "start": 30,
            "end": 44,
            "entity_type": "EMAIL",
            "score": 0.9,
            "doc_id": "doc1",
        },
    ]

    # Sample entities from AI detection
    ai_entities = [
        {
            "entity": "John Smith",
            "start": 8,
            "end": 18,
            "entity_type": "PERSON",
            "doc_id": "doc1",
        },
        {
            "entity": "555-1234",
            "start": 60,
            "end": 68,
            "entity_type": "PHONE_NUMBER",
            "doc_id": "doc1",
        },
    ]

    # Align entities
    aligned = align_entities_multi_source(
        presidio_entities=presidio_entities,
        gliner_entities=None,
        ai_entities=ai_entities,
        doc_id="doc1",
    )

    print("Two-source alignment (Presidio + AI):")
    for entity in aligned:
        print(f"  - {entity['entity']} ({entity['entity_type']})")
        print(f"    Confidence: {entity['confidence']}")
        print(f"    Presidio score: {entity['presidio_score']}")
        print(f"    AI score: {entity['ai_score']}")
        print()

    return aligned


def example_three_sources():
    """Example: Align entities from all three sources."""

    # Sample entities from Presidio
    presidio_entities = [
        {
            "entity": "John Smith",
            "start": 0,
            "end": 10,
            "entity_type": "PERSON",
            "score": 0.95,
        }
    ]

    # Sample entities from GLiNER
    gliner_entities = [
        {
            "entity": "John Smith",
            "start": 0,
            "end": 10,
            "entity_type": "person",
            "score": 0.85,
        }
    ]

    # Sample entities from AI
    ai_entities = [
        {"entity": "John Smith", "start": 0, "end": 10, "entity_type": "PERSON"}
    ]

    # Align entities
    aligned = align_entities_multi_source(
        presidio_entities=presidio_entities,
        gliner_entities=gliner_entities,
        ai_entities=ai_entities,
        doc_id="doc2",
    )

    print("Three-source alignment (Presidio + GLiNER + AI):")
    for entity in aligned:
        print(f"  - {entity['entity']} ({entity['entity_type']})")
        print(f"    Confidence: {entity['confidence']}")
        print(f"    Presidio: {entity['presidio_score']}")
        print(f"    GLiNER: {entity['gliner_score']}")
        print(f"    AI: {entity['ai_score']}")
        print()

    return aligned


def example_single_source():
    """Example: Process entities from a single source."""

    # Only Presidio entities
    presidio_entities = [
        {
            "entity": "Alice Johnson",
            "start": 0,
            "end": 13,
            "entity_type": "PERSON",
            "score": 0.9,
        }
    ]

    # Align (even with single source)
    aligned = align_entities_multi_source(
        presidio_entities=presidio_entities,
        gliner_entities=None,
        ai_entities=None,
        doc_id="doc3",
    )

    print("Single-source alignment (Presidio only):")
    for entity in aligned:
        print(f"  - {entity['entity']} ({entity['entity_type']})")
        print(f"    Confidence: {entity['confidence']}")
        print()

    return aligned


def example_with_extra_fields():
    """Example: Entities with extra fields are handled gracefully."""

    ai_entities = [
        {
            "entity": "John",
            "start": 0,
            "end": 4,
            "entity_type": "PERSON",
            # Extra fields that won't cause errors
            "extra_metadata": {"source": "llm"},
            "custom_field": "custom_value",
            "analysis_explanation": "This is a person name",
        }
    ]

    aligned = align_entities_multi_source(
        presidio_entities=None,
        gliner_entities=None,
        ai_entities=ai_entities,
        doc_id="doc4",
    )

    print("Alignment with extra fields (no errors):")
    for entity in aligned:
        print(f"  - {entity['entity']} ({entity['entity_type']})")
        print(f"    Confidence: {entity['confidence']}")
        print()

    return aligned


def example_using_aligner_class():
    """Example: Using MultiSourceAligner class directly."""

    aligner = MultiSourceAligner(fuzzy_threshold=60)

    presidio_entities = [
        {
            "entity": "John Smith",
            "start": 0,
            "end": 10,
            "entity_type": "PERSON",
            "score": 0.9,
        }
    ]
    ai_entities = [
        {"entity": "Smith, John", "start": 0, "end": 11, "entity_type": "PERSON"}
    ]

    aligned = aligner.align(
        doc_id="doc5",
        presidio_entities=presidio_entities,
        gliner_entities=None,
        ai_entities=ai_entities,
    )

    print("Using MultiSourceAligner class:")
    for entity in aligned:
        print(f"  - {entity['entity']} ({entity['entity_type']})")
        print(f"    Confidence: {entity['confidence']}")
        print()

    return aligned


def example_pyspark_udf_usage():
    """
    Example: How to use the UDF in PySpark (pseudocode).

    Note: This is pseudocode showing the pattern. Requires actual Spark context.

    Pseudocode for PySpark usage:
        from pyspark.sql.functions import col
        from dbxmetagen.redaction.alignment import align_entities_udf

        # Create UDF for two sources (Presidio + AI)
        align_udf = align_entities_udf(
            fuzzy_threshold=50,
            include_presidio=True,
            include_gliner=False,
            include_ai=True
        )

        # Apply to DataFrame
        df = df.withColumn(
            "aligned_entities",
            align_udf(
                col("ai_entities"),
                col("presidio_entities"),
                col("gliner_entities"),  # Pass even if not used
                col("doc_id")
            )
        )

        # For all three sources:
        align_udf_three = align_entities_udf(
            fuzzy_threshold=50,
            include_presidio=True,
            include_gliner=True,
            include_ai=True
        )

        df = df.withColumn(
            "aligned_entities",
            align_udf_three(
                col("ai_entities"),
                col("presidio_entities"),
                col("gliner_entities"),
                col("doc_id")
            )
        )
    """

    print("PySpark UDF usage example (see docstring)")
    return None


if __name__ == "__main__":
    print("=" * 60)
    print("Entity Alignment Examples")
    print("=" * 60)
    print()

    example_two_sources_presidio_ai()
    print("-" * 60)
    print()

    example_three_sources()
    print("-" * 60)
    print()

    example_single_source()
    print("-" * 60)
    print()

    example_with_extra_fields()
    print("-" * 60)
    print()

    example_using_aligner_class()
    print("-" * 60)
    print()

    example_pyspark_udf_usage()
    print("-" * 60)
    print()

    print("All examples completed!")
