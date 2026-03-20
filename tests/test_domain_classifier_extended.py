"""Extended tests for domain_classifier -- enforce, user message, error result, pipeline.

These tests validate DESIRED behavior contracts, not just current behavior.
They are critical CI/CD guards for the classification pipeline.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from unittest.mock import MagicMock, patch
from dbxmetagen.domain_classifier import (
    _enforce_value,
    _normalize,
    _trigram_overlap,
    _build_user_message,
    _error_result,
    generate_domain_only_prompt,
    generate_subdomain_prompt,
    classify_table_domain,
    DomainResult,
    SubdomainResult,
)


# ── _enforce_value: 4-tier matching contract ─────────────────────────────


class TestEnforceValueTiers:
    """Each tier must be tested independently and in priority order."""

    # Tier 1: Exact case-insensitive
    def test_tier1_exact_lowercase(self):
        val, exact = _enforce_value("clinical", ["clinical", "finance"])
        assert val == "clinical" and exact is True

    def test_tier1_exact_mixed_case(self):
        val, exact = _enforce_value("CLINICAL", ["clinical", "finance"])
        assert val == "clinical" and exact is True

    def test_tier1_exact_with_whitespace(self):
        val, exact = _enforce_value("  clinical  ", ["clinical", "finance"])
        assert val == "clinical" and exact is True

    # Tier 2: Substring containment
    def test_tier2_predicted_contains_allowed(self):
        val, exact = _enforce_value("clinical_extended_v2", ["clinical", "finance"])
        assert val == "clinical" and exact is False

    def test_tier2_allowed_contains_predicted(self):
        val, exact = _enforce_value("clin", ["clinical", "finance"])
        assert val == "clinical" and exact is False

    def test_tier2_ambiguous_substring_first_wins(self):
        """When two allowed values are substrings, the first one matched wins."""
        val, _ = _enforce_value("clinical_development_research", ["clinical", "research"])
        assert val in ("clinical", "research")

    # Tier 3: Normalized match (strip separators)
    def test_tier3_underscore_to_none(self):
        val, exact = _enforce_value("real_world_evidence", ["realworldevidence", "genomics"])
        assert val == "realworldevidence" and exact is False

    def test_tier3_hyphen_to_underscore(self):
        val, exact = _enforce_value("real-world-evidence", ["real_world_evidence", "genomics"])
        assert val == "real_world_evidence" and exact is False

    def test_tier3_space_to_underscore(self):
        val, exact = _enforce_value("patient care", ["patient_care", "billing"])
        assert val == "patient_care" and exact is False

    # Tier 4: Trigram overlap
    def test_tier4_typo_correction(self):
        val, exact = _enforce_value("pharmaceutcal", ["pharmaceutical", "clinical"])
        assert val == "pharmaceutical" and exact is False

    def test_tier4_close_miss(self):
        val, exact = _enforce_value("clinicl", ["clinical", "zzzzzz"])
        assert val == "clinical" and exact is False

    # Fallback
    def test_fallback_completely_unrelated(self):
        val, exact = _enforce_value("xyzzy_foobar_baz", ["clinical", "finance"])
        assert val == "unknown" and exact is False

    def test_fallback_custom(self):
        val, exact = _enforce_value("xyzzy", ["clinical"], fallback="unclassified")
        assert val == "unclassified" and exact is False


class TestEnforceValueEdgeCases:
    """Edge cases that could break in production."""

    def test_empty_allowed_list(self):
        val, exact = _enforce_value("clinical", [])
        assert val == "unknown" and exact is False

    def test_empty_predicted(self):
        val, exact = _enforce_value("", ["clinical", "finance"])
        assert exact is False

    def test_predicted_is_only_separators(self):
        val, exact = _enforce_value("___---   ", ["clinical"])
        assert exact is False

    def test_single_char_allowed(self):
        val, exact = _enforce_value("a", ["a", "b"])
        assert val == "a" and exact is True

    def test_allowed_with_duplicates_casefolded(self):
        """If allowed list has case-variant duplicates, one should still match."""
        val, exact = _enforce_value("Clinical", ["clinical", "Clinical"])
        assert val.lower() == "clinical" and exact is True

    def test_never_returns_none(self):
        """Contract: _enforce_value ALWAYS returns (str, bool), never None."""
        val, exact = _enforce_value("anything", ["x", "y"])
        assert isinstance(val, str)
        assert isinstance(exact, bool)


# ── _normalize / _trigram_overlap ────────────────────────────────────────


class TestNormalizeContract:
    def test_idempotent(self):
        assert _normalize(_normalize("Real_World-Evidence")) == _normalize("Real_World-Evidence")

    def test_preserves_digits(self):
        assert _normalize("icd_10_code") == "icd10code"


class TestTrigramContract:
    def test_symmetry(self):
        assert _trigram_overlap("abc", "bca") == _trigram_overlap("bca", "abc")

    def test_short_returns_zero(self):
        assert _trigram_overlap("ab", "ab") == 0.0

    def test_identical_is_1(self):
        assert _trigram_overlap("clinical", "clinical") == 1.0

    def test_completely_different_is_0(self):
        assert _trigram_overlap("aaa", "zzz") == 0.0

    def test_partial_overlap_between_0_and_1(self):
        score = _trigram_overlap("clinical", "clinicl")
        assert 0.0 < score < 1.0


# ── _build_user_message: ordering & content contracts ────────────────────


class TestBuildUserMessage:

    def test_includes_table_name(self):
        msg = _build_user_message("cat.sch.orders", {})
        assert "cat.sch.orders" in msg

    def test_includes_column_contents(self):
        meta = {"column_contents": {"order_id": "bigint", "total": "decimal"}}
        msg = _build_user_message("t", meta)
        assert "order_id" in msg and "total" in msg

    def test_includes_tags_when_present(self):
        meta = {"table_tags": "pii=true, domain=finance"}
        msg = _build_user_message("t", meta)
        assert "pii=true" in msg

    def test_includes_constraints_when_present(self):
        meta = {"table_constraints": "PRIMARY KEY (id)"}
        msg = _build_user_message("t", meta)
        assert "PRIMARY KEY" in msg

    def test_comment_appears_before_columns(self):
        """Critical: comment is the strongest domain signal and must come first."""
        meta = {
            "column_contents": {"col_a": "string"},
            "table_comments": "IQVIA pharmaceutical data feed",
        }
        msg = _build_user_message("cat.sch.tbl", meta)
        assert msg.index("IQVIA") < msg.index("Column Information")

    def test_lineage_appears_before_columns(self):
        meta = {
            "column_contents": {"col_a": "int"},
            "lineage": {"upstream_tables": ["cat.sch.source"]},
        }
        msg = _build_user_message("cat.sch.tbl", meta)
        assert msg.index("Upstream Tables") < msg.index("Column Information")

    def test_no_duplicate_lineage_sections(self):
        meta = {
            "column_contents": {},
            "lineage": {
                "upstream_tables": ["a.b.c"],
                "downstream_tables": ["d.e.f"],
            },
        }
        msg = _build_user_message("cat.sch.tbl", meta)
        assert msg.count("Upstream Tables") == 1
        assert msg.count("Downstream Tables") == 1

    def test_lineage_key_in_column_contents_is_stripped(self):
        """column_contents may contain 'lineage' as a key; it should be removed to avoid duplication."""
        meta = {
            "column_contents": {"lineage": "should_be_stripped", "real_col": "int"},
            "lineage": {"upstream_tables": ["x.y.z"]},
        }
        msg = _build_user_message("cat.sch.tbl", meta)
        assert "should_be_stripped" not in msg
        assert "real_col" in msg

    def test_handles_empty_metadata_gracefully(self):
        msg = _build_user_message("cat.sch.tbl", {})
        assert "cat.sch.tbl" in msg
        assert "Column Information" in msg

    def test_column_metadata_included_when_present(self):
        meta = {"column_metadata": {"col_a": {"type": "STRING", "pii": True}}}
        msg = _build_user_message("t", meta)
        assert "Column Metadata" in msg


# ── _error_result ────────────────────────────────────────────────────────


class TestErrorResult:

    def test_structure_complete(self):
        result = _error_result("cat.sch.orders", Exception("timeout"))
        assert result["catalog"] == "cat"
        assert result["schema"] == "sch"
        assert result["table"] == "orders"
        assert result["domain"] == "unknown"
        assert result["subdomain"] is None
        assert result["confidence"] == 0.0
        assert "timeout" in result["reasoning"]
        assert "metadata_summary" in result

    def test_preserves_error_message(self):
        result = _error_result("a.b.c", ValueError("bad value"))
        assert "bad value" in result["reasoning"]


# ── generate_domain_only_prompt: structure contracts ─────────────────────


class TestDomainPromptStructure:

    SAMPLE_CONFIG = {
        "domains": {
            "clinical": {
                "name": "Clinical",
                "description": "Patient care",
                "keywords": ["patient", "encounter"],
                "subdomains": {
                    "patient_care": {"name": "Patient Care", "description": "Demographics"},
                    "diagnosis": {"name": "Diagnosis", "description": "ICD coding"},
                },
            },
            "finance": {
                "name": "Finance",
                "description": "Billing",
                "keywords": ["revenue"],
            },
        }
    }

    def test_contains_all_requested_domains(self):
        prompt = generate_domain_only_prompt(self.SAMPLE_CONFIG, ["clinical", "finance"])
        assert "**clinical**" in prompt
        assert "**finance**" in prompt

    def test_contains_subdomain_names_when_present(self):
        prompt = generate_domain_only_prompt(self.SAMPLE_CONFIG, ["clinical"])
        assert "Subdomains:" in prompt
        assert "patient_care" in prompt
        assert "diagnosis" in prompt

    def test_no_subdomain_line_when_absent(self):
        prompt = generate_domain_only_prompt(self.SAMPLE_CONFIG, ["finance"])
        lines_for_finance = [l for l in prompt.split("\n") if "finance" in l.lower()]
        assert not any("Subdomains:" in l for l in lines_for_finance)

    def test_contains_reflection_instruction(self):
        prompt = generate_domain_only_prompt(self.SAMPLE_CONFIG, ["clinical", "finance"])
        assert "top 3 candidate" in prompt

    def test_asks_for_second_choice(self):
        prompt = generate_domain_only_prompt(self.SAMPLE_CONFIG, ["clinical", "finance"])
        assert "second_choice_domain" in prompt

    def test_contains_keywords(self):
        prompt = generate_domain_only_prompt(self.SAMPLE_CONFIG, ["clinical"])
        assert "patient" in prompt
        assert "Keywords:" in prompt

    def test_contains_comment_emphasis(self):
        prompt = generate_domain_only_prompt(self.SAMPLE_CONFIG, ["clinical"])
        assert "table comment" in prompt.lower() or "comment" in prompt.lower()

    def test_only_requested_candidates_appear(self):
        """Domains not in candidates should NOT appear in the prompt."""
        prompt = generate_domain_only_prompt(self.SAMPLE_CONFIG, ["finance"])
        assert "**clinical**" not in prompt
        assert "**finance**" in prompt


# ── generate_subdomain_prompt ────────────────────────────────────────────


class TestSubdomainPromptStructure:

    SAMPLE_CONFIG = {
        "domains": {
            "clinical": {
                "name": "Clinical",
                "description": "Patient care",
                "keywords": ["patient"],
                "subdomains": {
                    "patient_care": {"name": "Patient Care", "description": "Demographics", "keywords": ["patient"]},
                    "diagnosis": {"name": "Diagnosis", "description": "ICD", "keywords": ["icd"]},
                },
            },
            "finance": {
                "name": "Finance",
                "description": "Billing",
                "keywords": ["revenue"],
                "subdomains": {
                    "accounting": {"name": "Accounting", "description": "GL", "keywords": ["ledger"]},
                },
            },
        }
    }

    def test_contains_primary_domain_subdomains(self):
        prompt = generate_subdomain_prompt(self.SAMPLE_CONFIG, "clinical")
        assert "patient_care" in prompt
        assert "diagnosis" in prompt

    def test_secondary_domain_included_when_specified(self):
        prompt = generate_subdomain_prompt(self.SAMPLE_CONFIG, "clinical", include_secondary="finance")
        assert "accounting" in prompt

    def test_secondary_domain_excluded_when_none(self):
        prompt = generate_subdomain_prompt(self.SAMPLE_CONFIG, "clinical", include_secondary=None)
        assert "accounting" not in prompt


# ── Pydantic model contracts ─────────────────────────────────────────────


class TestPydanticModels:

    def test_domain_result_second_choice_optional(self):
        r = DomainResult(domain="clinical", confidence=0.8, reasoning="test")
        assert r.second_choice_domain is None

    def test_domain_result_accepts_second_choice(self):
        r = DomainResult(domain="clinical", confidence=0.8, reasoning="test", second_choice_domain="finance")
        assert r.second_choice_domain == "finance"

    def test_domain_result_confidence_bounds(self):
        with pytest.raises(Exception):
            DomainResult(domain="x", confidence=1.5, reasoning="over max")
        with pytest.raises(Exception):
            DomainResult(domain="x", confidence=-0.1, reasoning="under min")

    def test_subdomain_result_confidence_bounds(self):
        with pytest.raises(Exception):
            SubdomainResult(subdomain="x", confidence=1.5, reasoning="over max")
        with pytest.raises(Exception):
            SubdomainResult(subdomain="x", confidence=-0.1, reasoning="under min")

    def test_domain_result_serializes_all_fields(self):
        r = DomainResult(
            domain="clinical",
            confidence=0.8,
            reasoning="test",
            recommended_domain="pharma",
            second_choice_domain="finance",
        )
        d = r.dict()
        assert "second_choice_domain" in d
        assert "recommended_domain" in d


# ── _classify_two_stage pipeline (mock LLM) ─────────────────────────────


class TestTwoStagePipeline:
    """Mock the LLM calls to test orchestration logic."""

    SAMPLE_CONFIG = {
        "domains": {
            "clinical": {
                "name": "Clinical",
                "description": "Patient care",
                "keywords": ["patient"],
                "subdomains": {
                    "patient_care": {"name": "Patient Care", "description": "Demographics", "keywords": ["patient"]},
                },
            },
            "finance": {
                "name": "Finance",
                "description": "Billing",
                "keywords": ["revenue"],
                "subdomains": {
                    "accounting": {"name": "Accounting", "description": "GL", "keywords": ["ledger"]},
                },
            },
        }
    }

    def _mock_stage1(self, domain="clinical", confidence=0.9, second_choice="finance"):
        return {
            "domain": domain,
            "confidence": confidence,
            "recommended_domain": None,
            "second_choice_domain": second_choice,
            "reasoning": "test reasoning",
        }

    def _mock_stage2(self, subdomain="patient_care", confidence=0.85):
        return {
            "subdomain": subdomain,
            "confidence": confidence,
            "recommended_subdomain": None,
            "reasoning": "subdomain reasoning",
            "metadata_summary": "summary",
        }

    @patch("dbxmetagen.domain_classifier.classify_subdomain_stage2")
    @patch("dbxmetagen.domain_classifier.classify_domain_stage1")
    def test_combined_confidence_is_min(self, mock_s1, mock_s2):
        mock_s1.return_value = self._mock_stage1(confidence=0.9)
        mock_s2.return_value = self._mock_stage2(confidence=0.7)
        result = classify_table_domain("cat.sch.tbl", {}, self.SAMPLE_CONFIG, two_stage=True)
        assert result["confidence"] == 0.7

    @patch("dbxmetagen.domain_classifier.classify_subdomain_stage2")
    @patch("dbxmetagen.domain_classifier.classify_domain_stage1")
    def test_enforce_snap_penalizes_confidence(self, mock_s1, mock_s2):
        """If stage-1 returns a domain that needs snapping, confidence drops by 0.1."""
        mock_s1.return_value = self._mock_stage1(domain="Clinical_Data", confidence=0.8)
        mock_s2.return_value = self._mock_stage2(confidence=0.85)
        result = classify_table_domain("cat.sch.tbl", {}, self.SAMPLE_CONFIG, two_stage=True)
        assert result["domain"] == "clinical"
        assert result["confidence"] <= 0.7

    @patch("dbxmetagen.domain_classifier.classify_subdomain_stage2")
    @patch("dbxmetagen.domain_classifier.classify_domain_stage1")
    def test_second_choice_passed_to_stage2(self, mock_s1, mock_s2):
        mock_s1.return_value = self._mock_stage1(confidence=0.3, second_choice="finance")
        mock_s2.return_value = self._mock_stage2()
        classify_table_domain(
            "cat.sch.tbl", {}, self.SAMPLE_CONFIG,
            two_stage=True, confidence_threshold=0.5,
        )
        _, kwargs = mock_s2.call_args
        assert kwargs["second_choice_domain"] == "finance"

    @patch("dbxmetagen.domain_classifier.classify_subdomain_stage2")
    @patch("dbxmetagen.domain_classifier.classify_domain_stage1")
    def test_result_contains_all_expected_keys(self, mock_s1, mock_s2):
        mock_s1.return_value = self._mock_stage1()
        mock_s2.return_value = self._mock_stage2()
        result = classify_table_domain("cat.sch.tbl", {}, self.SAMPLE_CONFIG, two_stage=True)
        for key in ["catalog", "schema", "table", "domain", "subdomain",
                     "confidence", "reasoning", "metadata_summary",
                     "recommended_domain", "second_choice_domain", "recommended_subdomain"]:
            assert key in result, f"Missing key: {key}"

    @patch("dbxmetagen.domain_classifier.classify_subdomain_stage2")
    @patch("dbxmetagen.domain_classifier.classify_domain_stage1")
    def test_catalog_schema_table_parsed(self, mock_s1, mock_s2):
        mock_s1.return_value = self._mock_stage1()
        mock_s2.return_value = self._mock_stage2()
        result = classify_table_domain("my_cat.my_sch.my_tbl", {}, self.SAMPLE_CONFIG, two_stage=True)
        assert result["catalog"] == "my_cat"
        assert result["schema"] == "my_sch"
        assert result["table"] == "my_tbl"


# ── classify_table_domain error handling ─────────────────────────────────


class TestClassifyErrorHandling:

    def test_returns_error_result_on_exception(self):
        """The public API must never raise -- it returns an error result."""
        with patch("dbxmetagen.domain_classifier._classify_two_stage", side_effect=RuntimeError("LLM down")):
            result = classify_table_domain("cat.sch.tbl", {}, {"domains": {}}, two_stage=True)
        assert result["domain"] == "unknown"
        assert result["confidence"] == 0.0
        assert "LLM down" in result["reasoning"]

    def test_returns_error_result_on_single_shot_exception(self):
        with patch("dbxmetagen.domain_classifier._classify_single_shot", side_effect=ValueError("parse fail")):
            result = classify_table_domain("cat.sch.tbl", {}, {"domains": {}}, two_stage=False)
        assert result["domain"] == "unknown"
        assert "parse fail" in result["reasoning"]
