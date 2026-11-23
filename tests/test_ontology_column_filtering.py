"""Column entity discovery skips technical columns and honors a 'None' answer."""

import types
from unittest.mock import MagicMock

import pytest

from dbxmetagen.ontology_roles import is_technical_column


@pytest.mark.parametrize("col", [
    "_source_file", "_source_table", "_rescued_data", "_metadata",
    "_fivetran_synced", "_airbyte_extracted_at", "etl_batch_id", "ingest_ts", "row_hash",
])
def test_technical_columns(col):
    assert is_technical_column(col)


@pytest.mark.parametrize("col", [
    "study_id", "usubjid", "record_id", "created_at", "updated_at",
    "source_value", "data_source", "exdose", "visitnum", "country",
])
def test_business_columns_kept(col):
    # Conservative: only the leading-underscore / audit-list / etl_ cases are skipped.
    assert not is_technical_column(col)


def _discoverer():
    from dbxmetagen.ontology import EntityDiscoverer
    d = EntityDiscoverer.__new__(EntityDiscoverer)
    d._entity_names = ["Person", "CdmSource", "DrugExposure"]
    d._stats = {"column_ai_classifications": 0, "column_no_entity": 0}
    d.entity_definitions = []
    d.CONFIDENCE_PENALTY_SNAP = 0.1
    return d


def _item(col, et, conf=0.9):
    return types.SimpleNamespace(column_name=col, entity_type=et, confidence=conf)


def test_batch_classifier_drops_none_answers():
    d = _discoverer()
    llm = MagicMock()
    llm.invoke.return_value = types.SimpleNamespace(classifications=[
        _item("usubjid", "Person"), _item("study_id", "None"), _item("x", "N/A"),
    ])
    d._get_batch_column_llm = lambda: llm
    cols = [types.SimpleNamespace(column_name=c, data_type="string", comment="")
            for c in ("usubjid", "study_id", "x")]
    out = d._classify_column_chunk("bronze_edc_ds", cols)
    assert out == [("usubjid", "Person", 0.9)]
    assert d._stats["column_no_entity"] == 2
    prompt = llm.invoke.call_args[0][0][0]["content"]
    assert "answer None" in prompt


def test_none_is_kept_if_bundle_defines_it():
    from dbxmetagen.ontology import _is_no_entity
    assert _is_no_entity("None", ["Person"])
    assert not _is_no_entity("None", ["Person", "None"])
    assert not _is_no_entity("Person", ["Person"])
