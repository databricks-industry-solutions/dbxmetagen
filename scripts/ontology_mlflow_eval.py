#!/usr/bin/env python3
"""Offline ontology eval: Pass-0 top-k vs gold set; log metrics to MLflow (optional tracking URI).

Usage:
  uv run python scripts/ontology_mlflow_eval.py
  MLFLOW_TRACKING_URI=http://localhost:5000 uv run python scripts/ontology_mlflow_eval.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from dbxmetagen.ontology_index import OntologyIndexLoader
from dbxmetagen.ontology_pass0 import pass0_keyword_candidates


def main() -> int:
    gold_path = ROOT / "tests" / "eval_data" / "ontology_gold.jsonl"
    lines = [json.loads(l) for l in gold_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    rows = [r for r in lines if r.get("eval") == "pass0"]

    top1 = 0
    top12 = 0
    for row in rows:
        loader = OntologyIndexLoader(row["bundle"])
        if not loader.has_tier_indexes:
            continue
        tier1 = loader.get_entities_tier1()
        cands = pass0_keyword_candidates(
            table_name=row["table_name"],
            columns=row["columns"],
            sample=row["sample"],
            tier1=tier1,
            max_candidates=48,
            min_candidates=12,
        )
        names = [c["name"] for c in cands]
        g = row["gold_entity"]
        if names and names[0] == g:
            top1 += 1
        if g in names[:12]:
            top12 += 1

    n = len(rows) or 1
    m_top1 = top1 / n
    m_top12 = top12 / n

    try:
        import mlflow

        mlflow.set_experiment("dbxmetagen_ontology_eval")
        with mlflow.start_run(run_name="pass0_keyword"):
            mlflow.log_param("gold_file", str(gold_path))
            mlflow.log_param("dataset_version", rows[0].get("dataset_version", "unknown") if rows else "")
            mlflow.log_metric("entity_pass0_top1", m_top1)
            mlflow.log_metric("entity_pass0_top12", m_top12)
            mlflow.log_metric("gold_rows", float(len(rows)))
    except Exception as e:
        print("MLflow logging skipped:", e)

    print(f"pass0_top1={m_top1:.3f} pass0_top12={m_top12:.3f} (n={len(rows)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
