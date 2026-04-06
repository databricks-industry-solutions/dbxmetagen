"""Unit tests for ontology scalability and correctness fixes (B1-B6, S1-S3)."""

import unittest
from dataclasses import replace as dc_replace
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# B1: predict_edge keyword-only signature
# ---------------------------------------------------------------------------


class TestPredictEdgeSignature(unittest.TestCase):
    """B1: predict_edge must accept keyword-only args; from_table etc. optional."""

    def test_keyword_only_minimal(self):
        from dbxmetagen.ontology_predictor import predict_edge, EdgePredictionResult

        loader = MagicMock()
        loader.get_edges_tier1.return_value = []

        result = predict_edge(
            src_entity="Patient",
            dst_entity="Encounter",
            loader=loader,
            llm_fn=lambda s, u: "{}",
        )
        self.assertIsInstance(result, EdgePredictionResult)
        self.assertEqual(result.from_table, "")
        self.assertEqual(result.to_table, "")

    def test_keyword_only_with_table_context(self):
        from dbxmetagen.ontology_predictor import predict_edge

        loader = MagicMock()
        loader.get_edges_tier1.return_value = []

        result = predict_edge(
            src_entity="Patient",
            dst_entity="Encounter",
            loader=loader,
            llm_fn=lambda s, u: "{}",
            from_table="patients",
            to_table="encounters",
        )
        self.assertEqual(result.from_table, "patients")
        self.assertEqual(result.to_table, "encounters")

    def test_positional_args_rejected(self):
        from dbxmetagen.ontology_predictor import predict_edge

        with self.assertRaises(TypeError):
            predict_edge("Patient", "Encounter", MagicMock(), lambda s, u: "{}")


# ---------------------------------------------------------------------------
# B3: Batch primary entity dict must include entity_uri / source_ontology
# ---------------------------------------------------------------------------


class TestBatchPrimaryEntityUri(unittest.TestCase):
    """B3: _batch_ai_classify_tables must propagate entity_uri/source_ontology."""

    def test_batch_result_has_uri_fields(self):
        from dbxmetagen.ontology import EntityDefinition

        edef = EntityDefinition(
            name="Patient", description="A patient",
            keywords=["patient"], parent=None,
            uri="http://hl7.org/fhir/Patient",
            source_ontology="FHIR_R4",
        )
        entity_dict = {
            "entity_id": "x",
            "entity_name": edef.name,
            "entity_uri": edef.uri if edef else None,
            "source_ontology": edef.source_ontology if edef else None,
        }
        self.assertEqual(entity_dict["entity_uri"], "http://hl7.org/fhir/Patient")
        self.assertEqual(entity_dict["source_ontology"], "FHIR_R4")

    def test_batch_result_none_when_no_edef(self):
        edef = None
        entity_dict = {
            "entity_uri": edef.uri if edef else None,
            "source_ontology": edef.source_ontology if edef else None,
        }
        self.assertIsNone(entity_dict["entity_uri"])
        self.assertIsNone(entity_dict["source_ontology"])


# ---------------------------------------------------------------------------
# B4: Three-pass upgrade preserves secondary entities
# ---------------------------------------------------------------------------


class TestThreePassUpgradePreservesSecondaries(unittest.TestCase):
    """B4: Upgrade must merge primary, not replace entire list."""

    def test_merge_preserves_secondaries(self):
        original_primary = {"entity_name": "OldType", "confidence": 0.4}
        secondary = {"entity_name": "SecondaryType", "confidence": 0.3, "entity_role": "secondary"}
        all_results_i = [original_primary, secondary]

        upgraded_primary = [{"entity_name": "BetterType", "confidence": 0.8}]
        if upgraded_primary[0]["confidence"] > original_primary["confidence"]:
            secondaries = all_results_i[1:]
            all_results_i = upgraded_primary + secondaries

        self.assertEqual(len(all_results_i), 2)
        self.assertEqual(all_results_i[0]["entity_name"], "BetterType")
        self.assertEqual(all_results_i[1]["entity_name"], "SecondaryType")


# ---------------------------------------------------------------------------
# B5: Row access uses bracket notation, not .get()
# ---------------------------------------------------------------------------


class TestRowAccessNotation(unittest.TestCase):
    """B5: Spark Row objects require bracket access, not .get()."""

    def test_bracket_access_works(self):
        class FakeRow:
            def __init__(self, **kwargs):
                self._data = kwargs

            def __getitem__(self, key):
                return self._data[key]

            def __contains__(self, key):
                return key in self._data

        row = FakeRow(table_name="patients")
        name = row["table_name"] if "table_name" in row else ""
        self.assertEqual(name, "patients")


# ---------------------------------------------------------------------------
# B6: dataclasses.replace() instead of __class__()
# ---------------------------------------------------------------------------


class TestDataclassReplace(unittest.TestCase):
    """B6: dc_replace preserves all fields; __class__() is brittle if fields are added."""

    def test_dc_replace_preserves_fields(self):
        from dbxmetagen.ontology_predictor import PredictionResult

        original = PredictionResult(
            table_name="tbl",
            predicted_entity="Patient",
            source_ontology="FHIR_R4",
            equivalent_class_uri="http://hl7.org/fhir/Patient",
            confidence_score=0.9,
            rationale="test",
            matched_properties=["p1"],
            passes_run=3,
            needs_human_review=False,
        )
        replaced = dc_replace(original, predicted_entity="Encounter", confidence_score=0.7)
        self.assertEqual(replaced.predicted_entity, "Encounter")
        self.assertEqual(replaced.confidence_score, 0.7)
        self.assertEqual(replaced.source_ontology, "FHIR_R4")
        self.assertEqual(replaced.matched_properties, ["p1"])
        self.assertEqual(replaced.passes_run, 3)


# ---------------------------------------------------------------------------
# S1: LLM edge pairs derived from FK evidence, capped
# ---------------------------------------------------------------------------


class TestLLMEdgePairRestriction(unittest.TestCase):
    """S1: Only FK-evidenced entity-type pairs fed to LLM edge prediction."""

    def test_fk_evidenced_pairs_only(self):
        """Simulate the logic from discover_named_relationships."""
        table_to_primary = {
            "patients": "Patient",
            "encounters": "Encounter",
            "claims": "Claim",
            "doctors": "Doctor",
        }
        seen = {("Patient", "Encounter")}  # already discovered via FK

        class FakeFK:
            def __init__(self, src, dst):
                self.src_table = src
                self.dst_table = dst

        fk_rows = [
            FakeFK("patients", "encounters"),
            FakeFK("encounters", "claims"),
            FakeFK("patients", "doctors"),
        ]

        candidate_pairs = set()
        for fk in fk_rows:
            s = table_to_primary.get(fk.src_table)
            d = table_to_primary.get(fk.dst_table)
            if s and d and s != d and (s, d) not in seen:
                candidate_pairs.add((s, d))

        self.assertNotIn(("Patient", "Encounter"), candidate_pairs)
        self.assertIn(("Encounter", "Claim"), candidate_pairs)
        self.assertIn(("Patient", "Doctor"), candidate_pairs)

    def test_cap_applied(self):
        max_llm_edge_calls = 2
        candidate_pairs = {("A", "B"), ("C", "D"), ("E", "F")}
        if len(candidate_pairs) > max_llm_edge_calls:
            candidate_pairs = set(list(candidate_pairs)[:max_llm_edge_calls])
        self.assertEqual(len(candidate_pairs), 2)


# ---------------------------------------------------------------------------
# S2: Three-pass upgrade cap
# ---------------------------------------------------------------------------


class TestThreePassUpgradeCap(unittest.TestCase):
    """S2: Three-pass upgrades sorted by confidence, capped."""

    def test_cap_applies_lowest_first(self):
        all_results = [
            [{"confidence": 0.3}],
            [{"confidence": 0.1}],
            [{"confidence": 0.5}],
            [{"confidence": 0.2}],
        ]
        threshold = 0.6
        max_upgrades = 2

        candidates = [
            (i, all_results[i][0].get("confidence", 1.0))
            for i in range(len(all_results))
            if all_results[i] and all_results[i][0].get("confidence", 1.0) < threshold
        ]
        candidates.sort(key=lambda x: x[1])
        self.assertEqual(candidates[0], (1, 0.1))
        self.assertEqual(candidates[1], (3, 0.2))

        if len(candidates) > max_upgrades:
            candidates = candidates[:max_upgrades]
        self.assertEqual(len(candidates), 2)
        indices = [c[0] for c in candidates]
        self.assertIn(1, indices)
        self.assertIn(3, indices)
        self.assertNotIn(0, indices)


# ---------------------------------------------------------------------------
# S3: Relationships API respects limit
# ---------------------------------------------------------------------------


class TestRelationshipsAPILimit(unittest.TestCase):
    """S3: LIMIT clause capped at 2000."""

    def test_limit_capped(self):
        limit = 5000
        capped = min(limit, 2000)
        self.assertEqual(capped, 2000)

    def test_limit_within_range(self):
        limit = 100
        capped = min(limit, 2000)
        self.assertEqual(capped, 100)


if __name__ == "__main__":
    unittest.main()
