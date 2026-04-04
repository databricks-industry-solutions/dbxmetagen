"""Unit tests for FK prediction source ranking constants."""
from dbxmetagen.fk_prediction import (
    SR_DECLARED,
    SR_EMBEDDING,
    SR_NAME,
    SR_ONTOLOGY,
    SR_QUERY,
)


def test_source_rank_ordering():
    """Lower number = higher trust for dedup."""
    assert SR_DECLARED < SR_QUERY < SR_NAME < SR_ONTOLOGY < SR_EMBEDDING


def test_dedup_sort_key_tuple():
    """Document expected (source_rank, col_similarity, query_hit_count) ordering."""
    rows = [
        (SR_EMBEDDING, 0.9, 0),
        (SR_DECLARED, 0.0, 0),
        (SR_QUERY, 0.5, 5),
    ]
    rows_sorted = sorted(rows, key=lambda r: (r[0], -r[1], -r[2]))
    assert rows_sorted[0][0] == SR_DECLARED
