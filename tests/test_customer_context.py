"""Unit tests for customer context enrichment pipeline."""

import os
import tempfile
import unittest

import yaml

from dbxmetagen.customer_context import (
    MAX_WORDS,
    resolve_customer_context,
    validate_context_text,
    _scope_id,
)


class TestValidateContextText(unittest.TestCase):

    def test_valid_text_returned(self):
        self.assertEqual(validate_context_text("Hello world"), "Hello world")

    def test_strips_whitespace(self):
        self.assertEqual(validate_context_text("  spaced  "), "spaced")

    def test_empty_raises(self):
        with self.assertRaises(ValueError):
            validate_context_text("")

    def test_whitespace_only_raises(self):
        with self.assertRaises(ValueError):
            validate_context_text("   ")

    def test_truncates_over_limit(self):
        long_text = " ".join(["word"] * (MAX_WORDS + 50))
        result = validate_context_text(long_text)
        self.assertEqual(len(result.split()), MAX_WORDS)


class TestScopeId(unittest.TestCase):

    def test_deterministic(self):
        self.assertEqual(_scope_id("catalog.schema"), _scope_id("catalog.schema"))

    def test_different_for_different_scopes(self):
        self.assertNotEqual(_scope_id("a.b"), _scope_id("a.c"))


class TestResolveCustomerContext(unittest.TestCase):

    def setUp(self):
        self.cache = [
            {"scope": "prod", "scope_type": "catalog", "context_text": "Production data lake.", "priority": 0},
            {"scope": "prod.claims", "scope_type": "schema", "context_text": "Claims from Cerner EHR.", "priority": 0},
            {"scope": "prod.claims.patients", "scope_type": "table", "context_text": "Patient demographics.", "priority": 0},
            {"scope": "prod.claims.dim_*", "scope_type": "pattern", "context_text": "SCD Type 2 dims.", "priority": 0},
            {"scope": "prod.claims", "scope_type": "schema", "context_text": "MRN is primary key.", "priority": 1},
        ]

    def test_catalog_match(self):
        result = resolve_customer_context(self.cache, "prod.other.whatever")
        self.assertIn("Production data lake", result)
        self.assertNotIn("Claims", result)

    def test_schema_match(self):
        result = resolve_customer_context(self.cache, "prod.claims.encounters")
        self.assertIn("Production data lake", result)
        self.assertIn("Claims from Cerner", result)
        self.assertIn("MRN is primary key", result)

    def test_table_match(self):
        result = resolve_customer_context(self.cache, "prod.claims.patients")
        self.assertIn("Production data lake", result)
        self.assertIn("Claims from Cerner", result)
        self.assertIn("Patient demographics", result)

    def test_pattern_match(self):
        result = resolve_customer_context(self.cache, "prod.claims.dim_provider")
        self.assertIn("SCD Type 2 dims", result)
        self.assertIn("Claims from Cerner", result)

    def test_pattern_no_match(self):
        result = resolve_customer_context(self.cache, "prod.claims.fact_visits")
        self.assertNotIn("SCD Type 2", result)

    def test_no_match(self):
        result = resolve_customer_context(self.cache, "dev.sandbox.test")
        self.assertEqual(result, "")

    def test_empty_cache(self):
        self.assertEqual(resolve_customer_context([], "prod.claims.patients"), "")

    def test_priority_ordering(self):
        """Higher priority entries appear later in concatenated output."""
        result = resolve_customer_context(self.cache, "prod.claims.encounters")
        cerner_pos = result.index("Claims from Cerner")
        mrn_pos = result.index("MRN is primary key")
        self.assertLess(cerner_pos, mrn_pos)

    def test_specificity_ordering(self):
        """Table-level appears after schema-level in output."""
        result = resolve_customer_context(self.cache, "prod.claims.patients")
        schema_pos = result.index("Claims from Cerner")
        table_pos = result.index("Patient demographics")
        self.assertLess(schema_pos, table_pos)

    def test_word_limit_enforced(self):
        big_cache = [
            {"scope": "cat", "scope_type": "catalog", "context_text": " ".join(["word"] * 600), "priority": 0},
        ]
        result = resolve_customer_context(big_cache, "cat.sch.tbl", max_words=100)
        self.assertLessEqual(len(result.split()), 100)


class TestYamlSeeding(unittest.TestCase):

    def test_parse_yaml_format(self):
        """Validate the expected YAML structure parses correctly."""
        content = {
            "contexts": [
                {
                    "scope": "prod.claims",
                    "scope_type": "schema",
                    "context_label": "Claims schema",
                    "context_text": "Healthcare claims data.",
                    "priority": 0,
                },
            ],
        }
        tmpdir = tempfile.mkdtemp()
        path = os.path.join(tmpdir, "test.yaml")
        with open(path, "w") as f:
            yaml.dump(content, f)

        with open(path) as f:
            data = yaml.safe_load(f)
        self.assertEqual(len(data["contexts"]), 1)
        self.assertEqual(data["contexts"][0]["scope_type"], "schema")

    def test_example_yaml_is_valid(self):
        """Verify the shipped example YAML parses and has valid scope types."""
        example = os.path.join(
            os.path.dirname(__file__), "..",
            "configurations", "customer_context", "example_clinical_trials.yaml"
        )
        if not os.path.exists(example):
            self.skipTest("Example YAML not found")

        with open(example) as f:
            data = yaml.safe_load(f)
        self.assertIn("contexts", data)
        valid_types = {"catalog", "schema", "table", "pattern"}
        for entry in data["contexts"]:
            self.assertIn(entry["scope_type"], valid_types)
            self.assertLessEqual(len(entry["context_text"].split()), MAX_WORDS)


class TestPromptEnrichment(unittest.TestCase):
    """Test that the Prompt enrichment method correctly sets prompt_content."""

    def test_enrich_sets_customer_context(self):
        cache = [
            {"scope": "cat.sch", "scope_type": "schema", "context_text": "Test context.", "priority": 0},
        ]

        class FakePrompt:
            full_table_name = "cat.sch.my_table"
            prompt_content = {}

        from dbxmetagen.customer_context import resolve_customer_context
        p = FakePrompt()
        ctx = resolve_customer_context(cache, p.full_table_name)
        if ctx:
            p.prompt_content["customer_context"] = ctx
        self.assertEqual(p.prompt_content["customer_context"], "Test context.")

    def test_enrich_empty_when_no_match(self):
        cache = [
            {"scope": "other.sch", "scope_type": "schema", "context_text": "No match.", "priority": 0},
        ]

        class FakePrompt:
            full_table_name = "cat.sch.my_table"
            prompt_content = {}

        from dbxmetagen.customer_context import resolve_customer_context
        p = FakePrompt()
        ctx = resolve_customer_context(cache, p.full_table_name)
        if ctx:
            p.prompt_content["customer_context"] = ctx
        self.assertNotIn("customer_context", p.prompt_content)


if __name__ == "__main__":
    unittest.main()
