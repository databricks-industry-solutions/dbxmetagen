"""Unit tests for customer context enrichment pipeline."""

import logging
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import yaml

from dbxmetagen.customer_context import (
    MAX_WORDS,
    MAX_WORDS_PER_ENTRY,
    MAX_TOTAL_WORDS,
    prefetch_customer_context,
    resolve_customer_context,
    resolve_customer_context_with_report,
    seed_customer_context_table,
    validate_context_text,
    _scope_id,
    _truncate_preserving_specificity,
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
        """Higher-priority entries appear FIRST within a scope, so they survive budget
        pressure (retention by value). Both schema entries here match `encounters`."""
        result = resolve_customer_context(self.cache, "prod.claims.encounters")
        cerner_pos = result.index("Claims from Cerner")   # priority 0
        mrn_pos = result.index("MRN is primary key")       # priority 1
        self.assertLess(mrn_pos, cerner_pos)

    def test_specificity_ordering(self):
        """Most-specific (table) appears FIRST so it survives truncation; the broad
        boilerplate is what gets dropped under budget pressure."""
        result = resolve_customer_context(self.cache, "prod.claims.patients")
        schema_pos = result.index("Claims from Cerner")
        table_pos = result.index("Patient demographics")
        self.assertLess(table_pos, schema_pos)

    def test_word_limit_enforced(self):
        big_cache = [
            {"scope": "cat", "scope_type": "catalog", "context_text": " ".join(["word"] * 600), "priority": 0},
        ]
        result = resolve_customer_context(big_cache, "cat.sch.tbl", max_words=100)
        self.assertLessEqual(len(result.split()), 100)

    def test_null_priority_does_not_crash(self):
        cache = [
            {"scope": "cat", "scope_type": "catalog", "context_text": "ctx", "priority": None},
            {"scope": "cat", "scope_type": "catalog", "context_text": "ctx2", "priority": 1},
        ]
        result = resolve_customer_context(cache, "cat.sch.tbl")
        self.assertIn("ctx", result)

    def test_null_context_text_does_not_crash(self):
        cache = [
            {"scope": "cat", "scope_type": "catalog", "context_text": None, "priority": 0},
        ]
        result = resolve_customer_context(cache, "cat.sch.tbl")
        self.assertEqual(result, "")


def _ctx_row(scope, scope_type, text, priority=0):
    return {"scope": scope, "scope_type": scope_type, "context_text": text, "priority": priority}


class TestTruncationPreservesSpecificity(unittest.TestCase):
    """Budget retention must keep the MOST-specific context (table), not the broadest
    boilerplate, and must not truncate silently."""

    def test_helper_orders_most_specific_first(self):
        matches = [
            _ctx_row("prod", "catalog", "CATALOG"),
            _ctx_row("prod.claims", "schema", "SCHEMA"),
            _ctx_row("prod.claims.*", "pattern", "PATTERN"),
            _ctx_row("prod.claims.patients", "table", "TABLE"),
        ]
        text, dropped = _truncate_preserving_specificity(matches, 1000)
        self.assertEqual(dropped, [])
        positions = [text.index(t) for t in ("TABLE", "PATTERN", "SCHEMA", "CATALOG")]
        self.assertEqual(positions, sorted(positions))  # table first ... catalog last

    def test_helper_priority_first_within_scope(self):
        matches = [
            _ctx_row("prod.claims.a_*", "pattern", "LOWPRIO", priority=0),
            _ctx_row("prod.claims.*", "pattern", "HIGHPRIO", priority=5),
        ]
        text, _ = _truncate_preserving_specificity(matches, 1000)
        self.assertLess(text.index("HIGHPRIO"), text.index("LOWPRIO"))

    def test_table_survives_budget_pressure(self):
        matches = [
            _ctx_row("prod", "catalog", " ".join(["boiler"] * 10)),
            _ctx_row("prod.claims.patients", "table", "MRN is PHI"),
        ]
        text, dropped = _truncate_preserving_specificity(matches, 4)
        self.assertIn("MRN is PHI", text)                              # specific survives
        self.assertNotIn("boiler", text)                              # broad dropped
        self.assertEqual([r["scope_type"] for r in dropped], ["catalog"])

    def test_over_backstop_keeps_specific_drops_broad(self):
        # 3 x 800 = 2400 > MAX_TOTAL_WORDS(2000): table+schema (1600) fit whole; the
        # broadest (catalog) is dropped whole rather than emitting a half-caveat.
        matches = [
            _ctx_row("prod", "catalog", " ".join(["c"] * 800)),
            _ctx_row("prod.claims", "schema", " ".join(["s"] * 800)),
            _ctx_row("prod.claims.patients", "table", " ".join(["t"] * 800)),
        ]
        text, dropped = _truncate_preserving_specificity(matches, MAX_TOTAL_WORDS)
        self.assertLessEqual(len(text.split()), MAX_TOTAL_WORDS)
        self.assertEqual(len(text.split()), 1600)                      # two whole entries kept
        self.assertIn(" ".join(["t"] * 800), text)                     # table fully retained
        self.assertNotIn("c", text.split())                            # catalog fully dropped
        self.assertEqual([r["scope_type"] for r in dropped], ["catalog"])

    def test_per_granularity_layers_coexist_within_backstop(self):
        # catalog+schema+table each 400 words (1200 total) < backstop -> nothing dropped.
        cache = [
            _ctx_row("prod", "catalog", " ".join(["c"] * 400)),
            _ctx_row("prod.claims", "schema", " ".join(["s"] * 400)),
            _ctx_row("prod.claims.patients", "table", " ".join(["t"] * 400)),
        ]
        text, dropped = resolve_customer_context_with_report(cache, "prod.claims.patients")
        self.assertEqual(dropped, [])
        self.assertEqual(len(text.split()), 1200)

    def test_report_lists_dropped_scopes(self):
        cache = [
            _ctx_row("prod", "catalog", " ".join(["c"] * 60)),
            _ctx_row("prod.claims.patients", "table", " ".join(["t"] * 60)),
        ]
        _, dropped = resolve_customer_context_with_report(cache, "prod.claims.patients", max_words=80)
        self.assertEqual([r["scope_type"] for r in dropped], ["catalog"])

    def test_resolve_warns_on_truncation(self):
        cache = [
            _ctx_row("prod", "catalog", " ".join(["c"] * 60)),
            _ctx_row("prod.claims.patients", "table", " ".join(["t"] * 60)),
        ]
        with self.assertLogs("dbxmetagen.customer_context", level="WARNING") as cm:
            resolve_customer_context(cache, "prod.claims.patients", max_words=80)
        joined = "\n".join(cm.output)
        self.assertIn("prod.claims.patients", joined)
        self.assertIn("truncated", joined)

    def test_resolve_silent_when_within_budget(self):
        cache = [_ctx_row("prod.claims.patients", "table", "short table context")]
        with self.assertNoLogs("dbxmetagen.customer_context", level="WARNING"):
            resolve_customer_context(cache, "prod.claims.patients")


class TestPrefetchCustomerContext(unittest.TestCase):

    def test_returns_empty_on_spark_error(self):
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("TABLE_OR_VIEW_NOT_FOUND")
        result = prefetch_customer_context(mock_spark, "cat", "sch")
        self.assertEqual(result, [])

    def test_logs_warning_on_error(self):
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = RuntimeError("permission denied")
        with self.assertLogs("dbxmetagen.customer_context", level="WARNING") as cm:
            prefetch_customer_context(mock_spark, "cat", "sch")
        self.assertTrue(any("permission denied" in msg for msg in cm.output))

    def test_returns_rows_on_success(self):
        mock_row = MagicMock()
        mock_row.asDict.return_value = {"scope": "cat", "scope_type": "catalog",
                                         "context_text": "hi", "priority": 0}
        mock_df = MagicMock()
        mock_df.collect.return_value = [mock_row]
        mock_spark = MagicMock()
        mock_spark.sql.return_value = mock_df
        result = prefetch_customer_context(mock_spark, "cat", "sch")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["context_text"], "hi")


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


class TestSeedCustomerContextTable(unittest.TestCase):

    def test_seeds_from_yaml_dir(self):
        content = {
            "contexts": [
                {"scope": "cat.sch", "scope_type": "schema",
                 "context_text": "Test context.", "priority": 0},
            ]
        }
        tmpdir = tempfile.mkdtemp()
        with open(os.path.join(tmpdir, "ctx.yaml"), "w") as f:
            yaml.dump(content, f)

        mock_spark = MagicMock()
        count = seed_customer_context_table(mock_spark, "cat", "sch", tmpdir)
        self.assertEqual(count, 1)
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS" in s for s in sql_calls))
        self.assertTrue(any("MERGE INTO" in s for s in sql_calls))

    def test_returns_zero_for_missing_dir(self):
        mock_spark = MagicMock()
        count = seed_customer_context_table(mock_spark, "cat", "sch", "/nonexistent/dir")
        self.assertEqual(count, 0)

    def test_skips_invalid_scope_type(self):
        content = {
            "contexts": [
                {"scope": "cat", "scope_type": "invalid_type", "context_text": "Bad.", "priority": 0},
            ]
        }
        tmpdir = tempfile.mkdtemp()
        with open(os.path.join(tmpdir, "bad.yaml"), "w") as f:
            yaml.dump(content, f)
        mock_spark = MagicMock()
        count = seed_customer_context_table(mock_spark, "cat", "sch", tmpdir)
        self.assertEqual(count, 0)


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


class TestExampleCustomerContextYaml(unittest.TestCase):
    """The pip-install example YAML must parse and use valid scope types."""

    def test_examples_yaml_parses(self):
        example = os.path.join(
            os.path.dirname(__file__), "..", "examples", "customer_context.yaml"
        )
        if not os.path.exists(example):
            self.skipTest("examples/customer_context.yaml not found")
        with open(example) as f:
            data = yaml.safe_load(f)
        self.assertIn("contexts", data)
        # If a user uncomments entries, types must be valid.
        valid_types = {"catalog", "schema", "table", "pattern"}
        for entry in (data.get("contexts") or []):
            self.assertIn(entry["scope_type"], valid_types)

    def test_shipped_example_seeds_without_crashing(self):
        """The all-commented shipped YAML (contexts: None) must NOT crash the
        seeder -- it's the exact happy path when a user first enables the flag."""
        example = os.path.join(
            os.path.dirname(__file__), "..", "examples", "customer_context.yaml"
        )
        if not os.path.exists(example):
            self.skipTest("examples/customer_context.yaml not found")
        tmpdir = tempfile.mkdtemp()
        import shutil
        shutil.copy(example, os.path.join(tmpdir, "customer_context.yaml"))
        # Should return 0 (no entries), not raise.
        n = seed_customer_context_table(MagicMock(), "cat", "sch", tmpdir)
        self.assertEqual(n, 0)

    def test_empty_and_comment_only_files_do_not_crash(self):
        tmpdir = tempfile.mkdtemp()
        with open(os.path.join(tmpdir, "empty.yaml"), "w") as f:
            f.write("# only a comment\n")
        with open(os.path.join(tmpdir, "nullkey.yaml"), "w") as f:
            f.write("contexts:\n")   # explicit key, no value -> None
        n = seed_customer_context_table(MagicMock(), "cat", "sch", tmpdir)
        self.assertEqual(n, 0)

    def test_malformed_priority_does_not_abort_seed(self):
        """A null or non-numeric priority defaults to 0 instead of crashing the
        whole seed (so other valid entries in the file still seed)."""
        content = (
            "contexts:\n"
            "  - scope: cat.sch\n"
            "    scope_type: schema\n"
            "    context_text: valid one\n"
            "    priority:\n"            # None
            "  - scope: cat.sch.t\n"
            "    scope_type: table\n"
            "    context_text: valid two\n"
            "    priority: high\n"       # non-numeric
        )
        tmpdir = tempfile.mkdtemp()
        with open(os.path.join(tmpdir, "c.yaml"), "w") as f:
            f.write(content)
        n = seed_customer_context_table(MagicMock(), "cat", "sch", tmpdir)
        self.assertEqual(n, 2)   # both entries seeded, priorities defaulted

    def test_merge_preserves_active_and_created_at(self):
        """The re-seed MERGE must not overwrite app-managed `active` / `created_at` /
        `created_by` (a UI soft-delete and operator provenance must survive a re-seed).
        The MATCHED branch updates ONLY the YAML-authored content fields + updated_at."""
        content = {
            "contexts": [
                {"scope": "cat.sch", "scope_type": "schema", "context_text": "hi"},
            ]
        }
        tmpdir = tempfile.mkdtemp()
        with open(os.path.join(tmpdir, "c.yaml"), "w") as f:
            yaml.dump(content, f)
        mock_spark = MagicMock()
        seed_customer_context_table(mock_spark, "cat", "sch", tmpdir)
        merge_sql = next(
            (str(c) for c in mock_spark.sql.call_args_list if "MERGE INTO" in str(c)), ""
        )
        # WHEN MATCHED updates content but NOT active/created_at/created_by.
        self.assertIn("context_text = src.context_text", merge_sql)
        self.assertIn("tgt.updated_at = src.updated_at", merge_sql)
        self.assertNotIn("tgt.active", merge_sql)
        self.assertNotIn("tgt.created_at", merge_sql)
        # created_by is operator provenance; a re-seed must not reset it to 'yaml_seed'.
        self.assertNotIn("tgt.created_by", merge_sql)


class TestSeedCustomerContextGating(unittest.TestCase):
    """main.seed_customer_context only seeds when BOTH the flag and dir are set."""

    def _import(self):
        try:
            from dbxmetagen.main import seed_customer_context
        except Exception as e:  # heavy deps unavailable under some harnesses
            self.skipTest(f"dbxmetagen.main not importable: {e}")
        return seed_customer_context

    def _config(self, **attrs):
        cfg = MagicMock()
        # MagicMock returns truthy mocks for any attr; set explicit values so
        # getattr(config, ..., default) reflects the test's intent.
        cfg.use_customer_context = attrs.get("use_customer_context", False)
        cfg.customer_context_yaml_dir = attrs.get("customer_context_yaml_dir", "")
        cfg.catalog_name = "cat"
        cfg.schema_name = "sch"
        return cfg

    def test_noop_when_flag_off(self):
        seed = self._import()
        with patch(
            "dbxmetagen.customer_context.seed_customer_context_table"
        ) as m:
            seed(self._config(use_customer_context=False,
                              customer_context_yaml_dir="/tmp/x"))
            m.assert_not_called()

    def test_noop_when_dir_missing(self):
        seed = self._import()
        with patch(
            "dbxmetagen.customer_context.seed_customer_context_table"
        ) as m:
            seed(self._config(use_customer_context=True,
                              customer_context_yaml_dir=""))
            m.assert_not_called()

    def test_seeds_when_both_set(self):
        seed = self._import()
        with patch(
            "dbxmetagen.customer_context.seed_customer_context_table",
            return_value=3,
        ) as m:
            seed(self._config(use_customer_context=True,
                              customer_context_yaml_dir="/tmp/ctx"))
            m.assert_called_once()
            # positional: (spark, catalog, schema, yaml_dir)
            args = m.call_args[0]
            self.assertEqual(args[1], "cat")
            self.assertEqual(args[2], "sch")
            self.assertEqual(args[3], "/tmp/ctx")

    def test_seed_failure_is_non_fatal(self):
        """A seeder exception must NOT propagate -- optional enrichment cannot
        abort core metadata generation."""
        seed = self._import()
        with patch(
            "dbxmetagen.customer_context.seed_customer_context_table",
            side_effect=RuntimeError("delta boom"),
        ):
            # Should swallow the error and return normally, not raise.
            seed(self._config(use_customer_context=True,
                              customer_context_yaml_dir="/tmp/ctx"))


if __name__ == "__main__":
    unittest.main()
