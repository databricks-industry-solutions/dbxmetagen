"""Regression tests: Pass-0 shortlist covers curated gold entities (healthcare bundle)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dbxmetagen.ontology_index import OntologyIndexLoader
from dbxmetagen.ontology_pass0 import pass0_keyword_candidates

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "tests" / "eval_data" / "ontology_gold.jsonl"


def _gold_rows():
    lines = GOLD.read_text(encoding="utf-8").strip().splitlines()
    return [json.loads(line) for line in lines if line.strip()]


@pytest.mark.parametrize("row", _gold_rows())
def test_pass0_contains_gold_entity_topk(row):
    if row.get("eval") != "pass0":
        pytest.skip("non-pass0 row")
    bundle = row["bundle"]
    loader = OntologyIndexLoader(bundle)
    if not loader.has_tier_indexes:
        pytest.skip(f"no tier indexes for {bundle}")
    tier1 = loader.get_entities_tier1()
    cands = pass0_keyword_candidates(
        table_name=row["table_name"],
        columns=row["columns"],
        sample=row["sample"],
        tier1=tier1,
        max_candidates=48,
        min_candidates=12,
    )
    names = [c["name"] for c in cands[:12]]
    gold = row["gold_entity"]
    assert gold in names, f"gold {gold!r} not in top-12 pass0 names {names}"


def test_gold_dataset_versioned():
    rows = _gold_rows()
    assert rows
    assert all(r.get("dataset_version") for r in rows)
