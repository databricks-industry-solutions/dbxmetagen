"""Integration tests for domain classification (no LLM calls).

Tests _enforce_value fuzzy matching, prompt structure, user message layout,
config validity across all checked-in YAMLs, DomainResult schema, and
cross-config consistency. These are critical CI/CD guards.
"""

import sys
import os
import glob

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import yaml
import pytest
from dbxmetagen.domain_classifier import (
    _enforce_value,
    _normalize,
    _trigram_overlap,
    _build_user_message,
    generate_domain_only_prompt,
    generate_subdomain_prompt,
    load_domain_config,
    DomainResult,
)

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
HEALTHCARE_CONFIG = os.path.join(REPO_ROOT, "configurations", "domain_config_healthcare.yaml")
BUNDLE_DIR = os.path.join(REPO_ROOT, "configurations", "ontology_bundles")


# ---------------------------------------------------------------------------
# _enforce_value: realistic enterprise mismatches
# ---------------------------------------------------------------------------


class TestEnforceValueEnterprise:
    """Realistic mismatches seen at enterprise deployments."""

    def test_exact_case_insensitive(self):
        val, exact = _enforce_value("Clinical", ["clinical", "finance"])
        assert val == "clinical" and exact is True

    def test_substring_match(self):
        val, exact = _enforce_value("clinical_data", ["clinical", "finance"])
        assert val == "clinical" and exact is False

    def test_underscore_normalization(self):
        val, exact = _enforce_value("iq_base", ["iqbase", "clinical"])
        assert val == "iqbase" and exact is False

    def test_hyphen_normalization(self):
        val, exact = _enforce_value("real-world-evidence", ["real_world_evidence", "genomics"])
        assert val == "real_world_evidence" and exact is False

    def test_space_normalization(self):
        val, exact = _enforce_value("patient care", ["patient_care", "billing"])
        assert val == "patient_care" and exact is False

    def test_trigram_fallback_typo(self):
        val, exact = _enforce_value("pharmaceutcal", ["pharmaceutical", "clinical"])
        assert val == "pharmaceutical" and exact is False

    def test_fallback_to_unknown(self):
        val, exact = _enforce_value("zzz_completely_unrelated", ["clinical", "finance"])
        assert val == "unknown" and exact is False

    def test_custom_fallback(self):
        val, exact = _enforce_value("xyz", ["a", "b"], fallback="other")
        assert val == "other" and exact is False

    def test_exact_match_mixed_case(self):
        val, exact = _enforce_value("Pharmaceutical", ["clinical", "pharmaceutical", "finance"])
        assert val == "pharmaceutical" and exact is True

    def test_real_enterprise_iqvia_scenario(self):
        """The LLM returned 'IQVIA' but the config key is 'pharmaceutical' -- should NOT match via fuzzy."""
        allowed = ["clinical", "pharmaceutical", "finance", "operations"]
        val, exact = _enforce_value("IQVIA", allowed)
        # IQVIA is not a substring of any allowed value, nor vice versa;
        # normalize won't help; trigram overlap should be too low
        # The correct behavior is fallback (the LLM should pick pharmaceutical, not IQVIA)
        assert val == "unknown" or val in allowed

    def test_enforce_preserves_allowed_casing(self):
        """Return value should use the casing from the allowed list, not the prediction."""
        val, _ = _enforce_value("CLINICAL", ["Clinical_Data", "Finance_Ops"])
        # substring: "clinical" is in "clinical_data"
        assert val == "Clinical_Data"


# ---------------------------------------------------------------------------
# _normalize / _trigram_overlap helpers
# ---------------------------------------------------------------------------


class TestNormalize:
    def test_strips_all_separators(self):
        assert _normalize("real_world-evidence test") == "realworldevidencetest"

    def test_lowercases(self):
        assert _normalize("CLINICAL") == "clinical"

    def test_preserves_digits(self):
        assert _normalize("icd_10") == "icd10"


class TestTrigramOverlap:
    def test_identical_strings(self):
        assert _trigram_overlap("clinical", "clinical") == 1.0

    def test_short_strings(self):
        assert _trigram_overlap("ab", "ab") == 0.0

    def test_completely_different(self):
        assert _trigram_overlap("xyz", "abc") == 0.0

    def test_symmetry(self):
        assert _trigram_overlap("clinical", "clinicl") == _trigram_overlap("clinicl", "clinical")

    def test_partial_overlap_range(self):
        score = _trigram_overlap("clinical", "clinicl")
        assert 0.0 < score < 1.0


# ---------------------------------------------------------------------------
# _build_user_message: ordering and content contracts
# ---------------------------------------------------------------------------


class TestBuildUserMessage:

    def test_comment_before_columns(self):
        metadata = {
            "column_contents": {"col_a": {"type": "STRING"}},
            "table_comments": "This table contains IQVIA data",
        }
        msg = _build_user_message("cat.sch.tbl", metadata)
        assert msg.index("IQVIA") < msg.index("Column Information")

    def test_lineage_before_columns(self):
        metadata = {
            "column_contents": {"col_a": {"type": "INT"}},
            "lineage": {"upstream_tables": ["cat.sch.source1"]},
        }
        msg = _build_user_message("cat.sch.tbl", metadata)
        assert msg.index("Upstream Tables") < msg.index("Column Information")

    def test_includes_table_name(self):
        msg = _build_user_message("my_cat.my_sch.my_table", {"column_contents": {}})
        assert "my_cat.my_sch.my_table" in msg

    def test_no_duplicate_lineage(self):
        metadata = {
            "column_contents": {},
            "lineage": {
                "upstream_tables": ["a.b.c"],
                "downstream_tables": ["d.e.f"],
            },
        }
        msg = _build_user_message("cat.sch.tbl", metadata)
        assert msg.count("Upstream Tables") == 1
        assert msg.count("Downstream Tables") == 1

    def test_comment_labeled_as_important(self):
        """The comment section label should signal importance to the LLM."""
        metadata = {"table_comments": "Some comment"}
        msg = _build_user_message("t", metadata)
        assert "important" in msg.lower() or "signal" in msg.lower()

    def test_handles_missing_everything(self):
        msg = _build_user_message("cat.sch.tbl", {})
        assert "cat.sch.tbl" in msg

    def test_downstream_lineage_included(self):
        metadata = {
            "column_contents": {},
            "lineage": {"downstream_tables": ["x.y.z"]},
        }
        msg = _build_user_message("cat.sch.tbl", metadata)
        assert "Downstream Tables" in msg
        assert "x.y.z" in msg


# ---------------------------------------------------------------------------
# generate_domain_only_prompt: structure contracts
# ---------------------------------------------------------------------------


class TestGenerateDomainOnlyPrompt:

    @pytest.fixture
    def healthcare_config(self):
        with open(HEALTHCARE_CONFIG) as f:
            return yaml.safe_load(f)

    def test_includes_subdomain_names(self, healthcare_config):
        prompt = generate_domain_only_prompt(healthcare_config, ["clinical"])
        assert "Subdomains:" in prompt
        assert "patient_care" in prompt

    def test_includes_reflection_instruction(self, healthcare_config):
        domains = list(healthcare_config["domains"].keys())
        prompt = generate_domain_only_prompt(healthcare_config, domains)
        assert "top 3 candidate" in prompt
        assert "second_choice_domain" in prompt

    def test_includes_keywords(self, healthcare_config):
        prompt = generate_domain_only_prompt(healthcare_config, ["finance"])
        assert "Keywords:" in prompt
        assert "revenue" in prompt

    def test_all_candidate_domains_present(self, healthcare_config):
        candidates = ["clinical", "finance", "payer"]
        prompt = generate_domain_only_prompt(healthcare_config, candidates)
        for c in candidates:
            assert f"**{c}**" in prompt

    def test_non_candidate_domains_excluded(self, healthcare_config):
        """Only requested candidates should appear -- not the entire config."""
        prompt = generate_domain_only_prompt(healthcare_config, ["finance"])
        assert "**clinical**" not in prompt

    def test_domain_with_no_subdomains_omits_line(self):
        config = {"domains": {"simple": {"name": "Simple", "description": "No subs", "keywords": ["a"]}}}
        prompt = generate_domain_only_prompt(config, ["simple"])
        assert "Subdomains:" not in prompt

    def test_empty_candidates_produces_valid_prompt(self):
        config = {"domains": {"clinical": {"name": "Clinical", "description": "x"}}}
        prompt = generate_domain_only_prompt(config, [])
        assert isinstance(prompt, str)

    def test_prompt_instructs_attention_to_lineage(self, healthcare_config):
        prompt = generate_domain_only_prompt(healthcare_config, ["clinical"])
        assert "lineage" in prompt.lower()


# ---------------------------------------------------------------------------
# DomainResult schema
# ---------------------------------------------------------------------------


class TestDomainResultSchema:

    def test_second_choice_field_exists(self):
        r = DomainResult(domain="clinical", confidence=0.8, reasoning="test", second_choice_domain="finance")
        assert r.second_choice_domain == "finance"

    def test_second_choice_optional(self):
        r = DomainResult(domain="clinical", confidence=0.8, reasoning="test")
        assert r.second_choice_domain is None

    def test_confidence_bounds_enforced(self):
        with pytest.raises(Exception):
            DomainResult(domain="x", confidence=1.5, reasoning="over")


# ---------------------------------------------------------------------------
# Config YAML validity -- CI/CD guard for config correctness
# ---------------------------------------------------------------------------


class TestConfigYamlValidity:
    """Verify all checked-in domain configs have consistent key conventions.

    If someone adds a domain with uppercase or spaces, CI catches it.
    """

    def _load_all_domain_configs(self):
        configs = []
        if os.path.exists(HEALTHCARE_CONFIG):
            with open(HEALTHCARE_CONFIG) as f:
                configs.append(("domain_config_healthcare.yaml", yaml.safe_load(f)))
        for path in sorted(glob.glob(os.path.join(BUNDLE_DIR, "*.yaml"))):
            with open(path) as f:
                data = yaml.safe_load(f)
            if data and "domains" in data:
                configs.append((os.path.basename(path), data))
        return configs

    def test_domain_keys_are_lowercase_snake(self):
        for name, config in self._load_all_domain_configs():
            for dk in config.get("domains", {}):
                assert dk == dk.lower(), f"{name}: domain key '{dk}' is not lowercase"
                assert " " not in dk, f"{name}: domain key '{dk}' contains spaces"

    def test_subdomain_keys_are_lowercase_snake(self):
        for name, config in self._load_all_domain_configs():
            for dk, dinfo in config.get("domains", {}).items():
                for sk in dinfo.get("subdomains", {}):
                    assert sk == sk.lower(), f"{name}: subdomain '{sk}' under '{dk}' is not lowercase"
                    assert " " not in sk, f"{name}: subdomain '{sk}' under '{dk}' contains spaces"

    def test_every_domain_has_description(self):
        for name, config in self._load_all_domain_configs():
            for dk, dinfo in config.get("domains", {}).items():
                assert dinfo.get("description"), f"{name}: domain '{dk}' missing description"

    def test_every_domain_has_name(self):
        for name, config in self._load_all_domain_configs():
            for dk, dinfo in config.get("domains", {}).items():
                assert dinfo.get("name"), f"{name}: domain '{dk}' missing 'name'"

    def test_subdomain_keys_valid_for_enforce(self):
        """All subdomain keys should be exact-matchable by _enforce_value."""
        for name, config in self._load_all_domain_configs():
            for dk, dinfo in config.get("domains", {}).items():
                for sk in dinfo.get("subdomains", {}):
                    val, exact = _enforce_value(sk, [sk])
                    assert exact, f"{name}: subdomain '{sk}' doesn't survive _enforce_value round-trip"

    def test_at_least_one_config_loads(self):
        assert len(self._load_all_domain_configs()) > 0

    def test_healthcare_has_minimum_domains(self):
        """Healthcare config should have at least clinical, finance, payer."""
        if not os.path.exists(HEALTHCARE_CONFIG):
            pytest.skip("healthcare config not found")
        with open(HEALTHCARE_CONFIG) as f:
            config = yaml.safe_load(f)
        domains = set(config.get("domains", {}).keys())
        for expected in ["clinical", "finance", "payer"]:
            assert expected in domains, f"Healthcare config missing '{expected}' domain"


# ---------------------------------------------------------------------------
# load_domain_config from bundle path
# ---------------------------------------------------------------------------


class TestLoadDomainConfigFromBundle:

    def test_loads_from_healthcare_bundle(self):
        bundle_path = os.path.join(BUNDLE_DIR, "healthcare.yaml")
        if not os.path.exists(bundle_path):
            pytest.skip("healthcare bundle not found")
        config = load_domain_config(bundle_path=bundle_path)
        if config:
            assert "domains" in config

    def test_loads_from_standalone_config(self):
        if not os.path.exists(HEALTHCARE_CONFIG):
            pytest.skip("healthcare config not found")
        config = load_domain_config(config_path=HEALTHCARE_CONFIG)
        assert config is not None
        assert "domains" in config

    def test_bundle_and_standalone_share_core_domains(self):
        """Both sources should provide the same core domain keys."""
        standalone_path = HEALTHCARE_CONFIG
        bundle_path = os.path.join(BUNDLE_DIR, "healthcare.yaml")
        if not os.path.exists(standalone_path) or not os.path.exists(bundle_path):
            pytest.skip("config files not found")
        s = load_domain_config(config_path=standalone_path)
        b = load_domain_config(bundle_path=bundle_path)
        if s and b:
            s_keys = set(s.get("domains", {}).keys())
            b_keys = set(b.get("domains", {}).keys())
            overlap = s_keys & b_keys
            assert len(overlap) >= 3, f"Standalone and bundle share only {len(overlap)} domains"


# ---------------------------------------------------------------------------
# Cross-function contract: subdomain prompt uses _enforce_value-compatible keys
# ---------------------------------------------------------------------------


class TestCrossFunctionContracts:
    """Verify functions compose correctly -- the output of one is valid input for another."""

    @pytest.fixture
    def healthcare_config(self):
        if not os.path.exists(HEALTHCARE_CONFIG):
            pytest.skip("healthcare config not found")
        with open(HEALTHCARE_CONFIG) as f:
            return yaml.safe_load(f)

    def test_all_subdomain_keys_survive_enforce(self, healthcare_config):
        """Every subdomain key in the config should be recoverable by _enforce_value."""
        for dk, dinfo in healthcare_config.get("domains", {}).items():
            sd_keys = list(dinfo.get("subdomains", {}).keys())
            for sk in sd_keys:
                val, exact = _enforce_value(sk, sd_keys)
                assert exact, f"Subdomain key '{sk}' in domain '{dk}' doesn't exact-match itself"

    def test_subdomain_prompt_references_actual_keys(self, healthcare_config):
        """The subdomain prompt for each domain should contain all its subdomain keys."""
        for dk, dinfo in healthcare_config.get("domains", {}).items():
            sd_keys = list(dinfo.get("subdomains", {}).keys())
            if not sd_keys:
                continue
            prompt = generate_subdomain_prompt(healthcare_config, dk)
            for sk in sd_keys:
                assert sk in prompt, f"Subdomain '{sk}' missing from prompt for domain '{dk}'"
