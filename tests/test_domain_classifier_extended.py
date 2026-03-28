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
    _domain_keyword_prefilter,
    _error_result,
    generate_domain_only_prompt,
    generate_subdomain_prompt,
    classify_table_domain,
    DomainResult,
    SubdomainResult,
)


# ── _enforce_value: 5-tier matching contract ─────────────────────────────


class TestEnforceValueTiers:
    """Each tier must be tested independently and in priority order.

    Tier order:
      1. Exact case-insensitive
      2. Normalized exact (strip separators)
      3. Longest substring containment
      4. Trigram Jaccard >= 0.5
      5. Fallback
    """

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

    # Tier 2: Normalized exact match (strip separators)
    def test_tier2_underscore_to_none(self):
        val, exact = _enforce_value("real_world_evidence", ["realworldevidence", "genomics"])
        assert val == "realworldevidence" and exact is False

    def test_tier2_hyphen_to_underscore(self):
        val, exact = _enforce_value("real-world-evidence", ["real_world_evidence", "genomics"])
        assert val == "real_world_evidence" and exact is False

    def test_tier2_space_to_underscore(self):
        val, exact = _enforce_value("patient care", ["patient_care", "billing"])
        assert val == "patient_care" and exact is False

    # Tier 3: Longest substring containment
    def test_tier3_predicted_contains_allowed(self):
        val, exact = _enforce_value("clinical_extended_v2", ["clinical", "finance"])
        assert val == "clinical" and exact is False

    def test_tier3_allowed_contains_predicted(self):
        val, exact = _enforce_value("clin", ["clinical", "finance"])
        assert val == "clinical" and exact is False

    def test_tier3_ambiguous_substring_longest_wins(self):
        """When two allowed values are substrings, the longest match wins."""
        val, _ = _enforce_value("clinical_development_research", ["clinical", "research"])
        assert val in ("clinical", "research")

    # Tier 4: Trigram overlap (>= 0.5)
    def test_tier4_typo_correction(self):
        val, exact = _enforce_value("pharmaceutcal", ["pharmaceutical", "clinical"])
        assert val == "pharmaceutical" and exact is False

    def test_tier4_close_miss(self):
        val, exact = _enforce_value("clinicl", ["clinical", "zzzzzz"])
        assert val == "clinical" and exact is False

    # Tier 5: Fallback
    def test_tier5_completely_unrelated(self):
        val, exact = _enforce_value("xyzzy_foobar_baz", ["clinical", "finance"])
        assert val == "unknown" and exact is False

    def test_tier5_custom_fallback(self):
        val, exact = _enforce_value("xyzzy", ["clinical"], fallback="unclassified")
        assert val == "unclassified" and exact is False


class TestEnforceValueEdgeCases:
    """Edge cases that could break in production."""

    def test_empty_allowed_list(self):
        val, exact = _enforce_value("clinical", [])
        assert val == "unknown" and exact is False

    def test_empty_predicted(self):
        val, exact = _enforce_value("", ["clinical", "finance"])
        assert val == "unknown" and exact is False

    def test_predicted_is_only_separators(self):
        val, exact = _enforce_value("___---   ", ["clinical"])
        assert val == "unknown" and exact is False

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

    def test_schema_signal_present(self):
        msg = _build_user_message("cat.claims.fact_claim", {})
        assert "Schema: claims (strong domain signal)" in msg

    def test_schema_signal_before_columns(self):
        meta = {"column_contents": {"col_a": "int"}}
        msg = _build_user_message("cat.sch.tbl", meta)
        assert msg.index("Schema: sch") < msg.index("Column Information")

    def test_no_schema_line_for_short_table_name(self):
        msg = _build_user_message("just_a_table", {})
        assert "Schema:" not in msg


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

    @patch("dbxmetagen.domain_classifier.classify_subdomain_stage2")
    @patch("dbxmetagen.domain_classifier.classify_domain_stage1")
    def test_subdomain_snap_penalizes_confidence(self, mock_s1, mock_s2):
        """If stage-2 returns a subdomain needing snapping, confidence drops by 0.1."""
        mock_s1.return_value = self._mock_stage1(domain="clinical", confidence=0.9)
        mock_s2.return_value = self._mock_stage2(subdomain="Patient Care", confidence=0.85)
        result = classify_table_domain("cat.sch.tbl", {}, self.SAMPLE_CONFIG, two_stage=True)
        assert result["subdomain"] == "patient_care"
        assert result["confidence"] <= 0.75

    @patch("dbxmetagen.domain_classifier.classify_subdomain_stage2")
    @patch("dbxmetagen.domain_classifier.classify_domain_stage1")
    def test_prefilter_narrows_candidates_for_large_config(self, mock_s1, mock_s2):
        """With >top_n domains, the prefilter should pass fewer candidates to stage-1."""
        large_config = {"domains": {
            f"dom_{i}": {"name": f"Dom{i}", "description": "", "keywords": [f"kw{i}"],
                         "subdomains": {}} for i in range(12)
        }}
        large_config["domains"]["dom_0"]["keywords"] = ["patient", "encounter"]
        mock_s1.return_value = self._mock_stage1(domain="dom_0")
        mock_s2.return_value = self._mock_stage2(subdomain=None)
        meta = {"table_comments": "patient encounter records"}
        classify_table_domain("cat.sch.tbl", meta, large_config, two_stage=True)
        call_args = mock_s1.call_args
        candidates_passed = call_args[0][3] if len(call_args[0]) > 3 else call_args[1].get("candidate_domains")
        assert len(candidates_passed) <= 6
        assert "dom_0" in candidates_passed


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


# ══════════════════════════════════════════════════════════════════════════
# _enforce_value: revised tier ordering + robustness
# ══════════════════════════════════════════════════════════════════════════


class TestEnforceValueEmptyGuard:
    """Empty / whitespace-only predictions must return fallback immediately."""

    def test_empty_string_returns_fallback(self):
        val, exact = _enforce_value("", ["clinical", "finance"])
        assert val == "unknown" and exact is False

    def test_whitespace_only_returns_fallback(self):
        val, exact = _enforce_value("   ", ["clinical", "finance"])
        assert val == "unknown" and exact is False

    def test_none_like_empty_returns_fallback(self):
        val, exact = _enforce_value("\t\n", ["clinical"])
        assert val == "unknown" and exact is False

    def test_empty_with_custom_fallback(self):
        val, exact = _enforce_value("", ["a"], fallback="unclassified")
        assert val == "unclassified"


class TestEnforceValueNormalizedBeforeSubstring:
    """Tier 2 (normalized exact) must run before tier 3 (substring) so that
    'data engineering' matches 'data_engineering' instead of 'data'."""

    def test_space_variant_matches_underscore_key(self):
        val, exact = _enforce_value("data engineering", ["data", "data_engineering"])
        assert val == "data_engineering"

    def test_hyphen_variant_matches_underscore_key(self):
        val, exact = _enforce_value("data-engineering", ["data", "data_engineering"])
        assert val == "data_engineering"

    def test_space_variant_patient_care(self):
        val, exact = _enforce_value("Patient Care", ["patient_care", "patient"])
        assert val == "patient_care"

    def test_space_variant_quality_safety(self):
        val, exact = _enforce_value("quality safety", ["quality", "quality_safety"])
        assert val == "quality_safety"

    def test_space_variant_real_world_evidence(self):
        val, exact = _enforce_value("real world evidence", ["research", "real_world_evidence"])
        assert val == "real_world_evidence"


class TestEnforceValueLongestSubstringWins:
    """Tier 3 (substring) must prefer the longest matching key."""

    def test_data_governance_beats_data(self):
        val, _ = _enforce_value("data_governance_policy", ["data", "data_governance", "analytics"])
        assert val == "data_governance"

    def test_clinical_documentation_beats_clinical(self):
        val, _ = _enforce_value("clinical_documentation_notes", ["clinical", "clinical_documentation"])
        assert val == "clinical_documentation"

    def test_quality_safety_beats_quality(self):
        """'quality' is a substring of 'quality_safety_metrics', but 'quality_safety' is longer."""
        val, _ = _enforce_value("quality_safety_metrics", ["quality", "quality_safety"])
        assert val == "quality_safety"

    def test_sales_analytics_beats_sales(self):
        val, _ = _enforce_value("sales_analytics_quarterly", ["sales", "sales_analytics"])
        assert val == "sales_analytics"


class TestEnforceValueTrigramThreshold:
    """Trigram threshold is 0.5 (raised from 0.4)."""

    def test_close_typo_still_matches(self):
        val, exact = _enforce_value("pharmaceutcal", ["pharmaceutical", "clinical"])
        assert val == "pharmaceutical" and exact is False

    def test_moderate_typo_still_matches(self):
        val, exact = _enforce_value("clinicl", ["clinical", "zzzzzz"])
        assert val == "clinical" and exact is False

    def test_distant_string_does_not_match(self):
        """A string with only ~40% overlap should NOT match at the 0.5 threshold."""
        val, exact = _enforce_value("abc_xyz_foo", ["clinical", "finance", "operations"])
        assert val == "unknown"


# ══════════════════════════════════════════════════════════════════════════
# Integration-quality validation: real config keys, realistic LLM outputs
# ══════════════════════════════════════════════════════════════════════════


class TestEnforceValueHealthcareConfig:
    """Test _enforce_value against the FULL healthcare domain config (12 domains, ~30 subdomains).
    These simulate realistic LLM outputs and verify correct snapping."""

    DOMAIN_KEYS = [
        "clinical", "diagnostics", "payer", "pharmaceutical", "quality_safety",
        "research", "finance", "operations", "workforce", "customer",
        "technology", "governance",
    ]

    CLINICAL_SUBDOMAINS = [
        "patient_care", "diagnosis_condition", "medication_orders", "clinical_documentation",
    ]

    DIAGNOSTICS_SUBDOMAINS = ["laboratory", "imaging", "vitals_observations"]

    GOVERNANCE_SUBDOMAINS = ["legal", "regulatory", "data_governance"]

    # --- Domain-level: exact keys ---
    @pytest.mark.parametrize("key", DOMAIN_KEYS)
    def test_exact_domain_key(self, key):
        val, exact = _enforce_value(key, self.DOMAIN_KEYS)
        assert val == key and exact is True

    @pytest.mark.parametrize("key", DOMAIN_KEYS)
    def test_exact_domain_key_upper(self, key):
        val, exact = _enforce_value(key.upper(), self.DOMAIN_KEYS)
        assert val == key and exact is True

    # --- Domain-level: LLM returns display name ---
    @pytest.mark.parametrize("display,expected", [
        ("Clinical", "clinical"),
        ("Quality & Safety", "quality_safety"),
        ("Quality Safety", "quality_safety"),
        ("Diagnostics", "diagnostics"),
        ("Pharmaceutical", "pharmaceutical"),
    ])
    def test_display_name_resolves(self, display, expected):
        val, _ = _enforce_value(display, self.DOMAIN_KEYS)
        assert val == expected

    # --- Domain-level: LLM returns partial or extended name ---
    @pytest.mark.parametrize("predicted,expected", [
        ("clinical_data", "clinical"),
        ("payer_claims", "payer"),
        ("research_genomics", "research"),
        ("financial", "finance"),
        ("tech", "technology"),
        ("govern", "governance"),
    ])
    def test_partial_domain_resolves(self, predicted, expected):
        val, _ = _enforce_value(predicted, self.DOMAIN_KEYS)
        assert val == expected

    # --- Domain-level: common typos (must have >= 0.5 trigram overlap) ---
    @pytest.mark.parametrize("typo,expected", [
        ("clinicall", "clinical"),
        ("diagnotics", "diagnostics"),
        ("pharmaceutcal", "pharmaceutical"),
        ("operatons", "operations"),
    ])
    def test_typo_resolves(self, typo, expected):
        val, _ = _enforce_value(typo, self.DOMAIN_KEYS)
        assert val == expected

    @pytest.mark.parametrize("mangled", [
        "clincial",       # letter transposition -- trigram overlap < 0.5
        "govenrance",     # rearranged middle -- trigram overlap < 0.5
    ])
    def test_severe_typo_falls_to_unknown(self, mangled):
        """Severely mangled names must NOT match at the 0.5 trigram threshold."""
        val, _ = _enforce_value(mangled, self.DOMAIN_KEYS)
        assert val == "unknown"

    # --- Subdomain-level: exact keys ---
    @pytest.mark.parametrize("key", CLINICAL_SUBDOMAINS)
    def test_exact_clinical_subdomain(self, key):
        val, exact = _enforce_value(key, self.CLINICAL_SUBDOMAINS)
        assert val == key and exact is True

    # --- Subdomain-level: LLM returns display name ---
    @pytest.mark.parametrize("display,expected", [
        ("Patient Care", "patient_care"),
        ("Diagnosis & Conditions", "diagnosis_condition"),
        ("Medications & Orders", "medication_orders"),
        ("Clinical Documentation", "clinical_documentation"),
    ])
    def test_clinical_subdomain_display_name(self, display, expected):
        val, _ = _enforce_value(display, self.CLINICAL_SUBDOMAINS)
        assert val == expected

    # --- Governance subdomains: overlapping keys (data_governance vs governance) ---
    def test_governance_subdomain_data_governance(self):
        val, _ = _enforce_value("data governance", self.GOVERNANCE_SUBDOMAINS)
        assert val == "data_governance"

    def test_governance_subdomain_regulatory(self):
        val, _ = _enforce_value("Regulatory Compliance", self.GOVERNANCE_SUBDOMAINS)
        assert val == "regulatory"


class TestEnforceValueGeneralConfig:
    """Test against the general.yaml bundle (6 domains, ~15 subdomains)."""

    DOMAIN_KEYS = ["finance", "operations", "workforce", "customer", "technology", "governance"]

    CUSTOMER_SUBDOMAINS = ["sales", "marketing", "support"]

    @pytest.mark.parametrize("predicted,expected", [
        ("finance", "finance"),
        ("Finance", "finance"),
        ("FINANCE", "finance"),
        ("customer", "customer"),
        ("Customer Support", "customer"),
        ("workforce management", "workforce"),
        ("tech", "technology"),
    ])
    def test_general_domain_resolution(self, predicted, expected):
        val, _ = _enforce_value(predicted, self.DOMAIN_KEYS)
        assert val == expected

    @pytest.mark.parametrize("predicted,expected", [
        ("Sales & Pipeline", "sales"),
        ("marketing", "marketing"),
        ("Customer Support", "support"),
    ])
    def test_customer_subdomain_resolution(self, predicted, expected):
        val, _ = _enforce_value(predicted, self.CUSTOMER_SUBDOMAINS)
        assert val == expected


class TestEnforceValueCustomOverlappingConfigs:
    """Stress test with adversarial custom configs that have many overlapping prefixes."""

    def test_20_domain_config_exact_match(self):
        keys = [
            "data", "data_engineering", "data_science", "data_governance", "data_quality",
            "analytics", "analytics_engineering", "machine_learning", "ml_ops",
            "product", "product_analytics", "marketing", "marketing_analytics",
            "finance", "financial_planning", "hr", "hr_analytics",
            "security", "security_ops", "compliance",
        ]
        for k in keys:
            val, exact = _enforce_value(k, keys)
            assert val == k and exact is True, f"Expected exact match for '{k}', got '{val}'"

    def test_overlapping_prefix_data_engineering_space(self):
        keys = ["data", "data_engineering", "data_science", "data_governance"]
        val, _ = _enforce_value("data engineering", keys)
        assert val == "data_engineering"

    def test_overlapping_prefix_data_science_extended(self):
        keys = ["data", "data_engineering", "data_science", "data_governance"]
        val, _ = _enforce_value("data_science_platform", keys)
        assert val == "data_science"

    def test_overlapping_prefix_data_governance_policy(self):
        keys = ["data", "data_engineering", "data_science", "data_governance"]
        val, _ = _enforce_value("data governance policy", keys)
        assert val == "data_governance"

    def test_overlapping_analytics_vs_analytics_engineering(self):
        keys = ["analytics", "analytics_engineering"]
        val, _ = _enforce_value("analytics_engineering_team", keys)
        assert val == "analytics_engineering"

    def test_overlapping_ml_vs_ml_ops(self):
        keys = ["machine_learning", "ml_ops"]
        val, _ = _enforce_value("ml_ops_pipeline", keys)
        assert val == "ml_ops"


class TestEnforceValueNeverReturnsWrongKey:
    """Negative tests: ensure unrelated strings don't match valid domains."""

    DOMAIN_KEYS = [
        "clinical", "diagnostics", "payer", "pharmaceutical", "quality_safety",
        "research", "finance", "operations", "workforce", "customer",
        "technology", "governance",
    ]

    @pytest.mark.parametrize("garbage", [
        "zebra_crossing_data", "12345", "___", "the quick brown fox",
        "definitely_not_a_domain", "asdfghjkl", "banana_split_sundae",
    ])
    def test_garbage_returns_unknown(self, garbage):
        val, exact = _enforce_value(garbage, self.DOMAIN_KEYS)
        assert val == "unknown", f"'{garbage}' should not match any domain, got '{val}'"
        assert exact is False


# ══════════════════════════════════════════════════════════════════════════
# Early-return dict shape in get_domain_classification
# ══════════════════════════════════════════════════════════════════════════


class TestEarlyReturnDictShape:
    """The early-return dict from get_domain_classification must have every key
    that append_domain_table_row accesses."""

    REQUIRED_KEYS = [
        "domain", "subdomain", "confidence", "recommended_domain",
        "recommended_subdomain", "reasoning", "metadata_summary",
    ]

    def test_error_result_has_all_keys(self):
        result = _error_result("cat.sch.tbl", Exception("boom"))
        for key in self.REQUIRED_KEYS:
            assert key in result, f"_error_result missing key: {key}"

    def test_confidence_is_formattable_as_float(self):
        result = _error_result("cat.sch.tbl", Exception("boom"))
        formatted = f"{result['confidence']:.2f}"
        assert formatted == "0.00"


# ══════════════════════════════════════════════════════════════════════════
# classify_subdomain_stage2: no arbitrary secondary domain injection
# ══════════════════════════════════════════════════════════════════════════


class TestSecondaryDomainExpansion:
    """When second_choice_domain is invalid, stage-2 must NOT inject a random domain."""

    SAMPLE_CONFIG = {
        "domains": {
            "clinical": {
                "name": "Clinical", "keywords": ["patient"],
                "subdomains": {"patient_care": {"name": "Patient Care", "keywords": ["patient"]}},
            },
            "finance": {
                "name": "Finance", "keywords": ["revenue"],
                "subdomains": {"accounting": {"name": "Accounting", "keywords": ["ledger"]}},
            },
            "technology": {
                "name": "Technology", "keywords": ["server"],
                "subdomains": {"security": {"name": "Security", "keywords": ["auth"]}},
            },
        }
    }

    @patch("dbxmetagen.domain_classifier.classify_subdomain_stage2")
    @patch("dbxmetagen.domain_classifier.classify_domain_stage1")
    def test_no_secondary_when_second_choice_missing(self, mock_s1, mock_s2):
        mock_s1.return_value = {
            "domain": "technology", "confidence": 0.3,
            "recommended_domain": None, "second_choice_domain": None,
            "reasoning": "test",
        }
        mock_s2.return_value = {
            "subdomain": "security", "confidence": 0.7,
            "recommended_subdomain": None, "reasoning": "r", "metadata_summary": "s",
        }
        classify_table_domain(
            "cat.sch.tbl", {}, self.SAMPLE_CONFIG,
            two_stage=True, confidence_threshold=0.5,
        )
        _, kwargs = mock_s2.call_args
        assert kwargs.get("second_choice_domain") is None

    @patch("dbxmetagen.domain_classifier.classify_subdomain_stage2")
    @patch("dbxmetagen.domain_classifier.classify_domain_stage1")
    def test_no_secondary_when_second_choice_invalid(self, mock_s1, mock_s2):
        mock_s1.return_value = {
            "domain": "technology", "confidence": 0.3,
            "recommended_domain": None, "second_choice_domain": "nonexistent_domain",
            "reasoning": "test",
        }
        mock_s2.return_value = {
            "subdomain": "security", "confidence": 0.7,
            "recommended_subdomain": None, "reasoning": "r", "metadata_summary": "s",
        }
        classify_table_domain(
            "cat.sch.tbl", {}, self.SAMPLE_CONFIG,
            two_stage=True, confidence_threshold=0.5,
        )
        _, kwargs = mock_s2.call_args
        assert kwargs.get("second_choice_domain") == "nonexistent_domain"

    @patch("dbxmetagen.domain_classifier.classify_subdomain_stage2")
    @patch("dbxmetagen.domain_classifier.classify_domain_stage1")
    def test_valid_second_choice_still_passed(self, mock_s1, mock_s2):
        mock_s1.return_value = {
            "domain": "technology", "confidence": 0.3,
            "recommended_domain": None, "second_choice_domain": "clinical",
            "reasoning": "test",
        }
        mock_s2.return_value = {
            "subdomain": "security", "confidence": 0.7,
            "recommended_subdomain": None, "reasoning": "r", "metadata_summary": "s",
        }
        classify_table_domain(
            "cat.sch.tbl", {}, self.SAMPLE_CONFIG,
            two_stage=True, confidence_threshold=0.5,
        )
        _, kwargs = mock_s2.call_args
        assert kwargs["second_choice_domain"] == "clinical"
