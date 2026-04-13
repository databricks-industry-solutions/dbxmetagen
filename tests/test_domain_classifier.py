"""Tests for domain_classifier module."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from unittest.mock import MagicMock, patch
from dbxmetagen.domain_classifier import (
    load_domain_config,
    generate_domain_prompt_section,
    create_system_prompt,
    TableClassification,
    _domain_keyword_prefilter,
    _build_user_message,
)


class TestLoadDomainConfig:

    def test_returns_dict_with_valid_path(self, tmp_path):
        config_file = tmp_path / "domain_config.yaml"
        config_file.write_text("domains:\n  finance:\n    description: Financial data\n    keywords: [revenue, cost]\n")
        config = load_domain_config(str(config_file))
        assert isinstance(config, dict)
        assert "domains" in config

    def test_returns_defaults_with_missing_path(self):
        config = load_domain_config("/nonexistent/path.yaml")
        assert isinstance(config, dict)

    def test_returns_none_with_empty_file(self, tmp_path):
        config_file = tmp_path / "empty.yaml"
        config_file.write_text("")
        config = load_domain_config(str(config_file))
        # Empty YAML returns None; caller handles default
        assert config is None or isinstance(config, dict)


class TestGenerateDomainPromptSection:

    def test_generates_string_with_domains(self):
        config = {
            "domains": {
                "Finance": {"description": "Financial data", "keywords": ["revenue", "cost"]},
                "HR": {"description": "Human resources", "keywords": ["employee", "salary"]},
            }
        }
        result = generate_domain_prompt_section(config)
        assert isinstance(result, str)
        assert "Finance" in result
        assert "HR" in result

    def test_handles_empty_domains(self):
        config = {"domains": {}}
        result = generate_domain_prompt_section(config)
        assert isinstance(result, str)


class TestCreateSystemPrompt:

    def test_returns_nonempty_string(self):
        config = {"domains": {"Test": {"description": "Test domain", "keywords": ["test"]}}}
        result = create_system_prompt(config)
        assert isinstance(result, str)
        assert len(result) > 0


class TestTableClassification:

    def test_valid_classification(self):
        tc = TableClassification(
            catalog="test_catalog",
            schema="test_schema",
            table="revenue_table",
            domain="Finance",
            subdomain="Revenue",
            confidence=0.9,
            reasoning="Contains revenue data",
        )
        assert tc.domain == "Finance"
        assert tc.confidence == 0.9
        assert tc.catalog == "test_catalog"


# ---------------------------------------------------------------------------
# Fixtures for prefilter / schema-signal tests
# ---------------------------------------------------------------------------

MULTI_DOMAIN_CONFIG = {
    "domains": {
        "clinical": {
            "name": "Clinical",
            "keywords": ["patient", "encounter", "diagnosis"],
            "subdomains": {
                "patient_care": {
                    "keywords": ["admission", "visit", "discharge"],
                },
            },
        },
        "finance": {
            "name": "Finance",
            "keywords": ["revenue", "billing", "ledger", "payment"],
            "subdomains": {
                "accounting": {
                    "keywords": ["journal", "gl", "accounts_payable"],
                },
            },
        },
        "operations": {
            "name": "Operations",
            "keywords": ["inventory", "procurement", "logistics"],
            "subdomains": {},
        },
        "payer": {
            "name": "Payer",
            "keywords": ["claim", "insurance", "member", "benefit"],
            "subdomains": {
                "claims": {
                    "keywords": ["adjudication", "denial", "remittance"],
                },
            },
        },
        "technology": {
            "name": "Technology",
            "keywords": ["server", "security", "log", "monitoring"],
            "subdomains": {},
        },
        "workforce": {
            "name": "Workforce",
            "keywords": ["employee", "payroll", "recruitment"],
            "subdomains": {},
        },
        "governance": {
            "name": "Governance",
            "keywords": ["compliance", "contract", "governance"],
            "subdomains": {},
        },
        "customer": {
            "name": "Customer",
            "keywords": ["customer", "sales", "campaign"],
            "subdomains": {},
        },
    },
}


class TestDomainKeywordPrefilter:

    def test_top_n_filters_low_scoring_domains(self):
        metadata = {"table_comments": "patient encounter records", "column_metadata": []}
        result = _domain_keyword_prefilter(
            "cat.clinical_data.fact_encounters", metadata, MULTI_DOMAIN_CONFIG, top_n=3,
        )
        assert len(result) == 3
        assert "clinical" in result

    def test_returns_all_when_domains_lte_top_n(self):
        small_config = {
            "domains": {
                "a": {"keywords": ["x"], "subdomains": {}},
                "b": {"keywords": ["y"], "subdomains": {}},
            }
        }
        result = _domain_keyword_prefilter(
            "cat.sch.tbl", {}, small_config, top_n=6,
        )
        assert set(result) == {"a", "b"}

    def test_schema_name_boosts_domain(self):
        metadata = {"table_comments": "", "column_metadata": []}
        result = _domain_keyword_prefilter(
            "cat.claims_processing.dim_status", metadata, MULTI_DOMAIN_CONFIG, top_n=3,
        )
        assert "payer" in result

    def test_subdomain_keywords_contribute(self):
        metadata = {"table_comments": "journal entries and gl accounts", "column_metadata": []}
        result = _domain_keyword_prefilter(
            "cat.reporting.fact_transactions", metadata, MULTI_DOMAIN_CONFIG, top_n=3,
        )
        assert "finance" in result

    def test_column_names_contribute(self):
        metadata = {
            "table_comments": "",
            "column_metadata": [{"name": "patient_id"}, {"name": "encounter_date"}],
        }
        result = _domain_keyword_prefilter(
            "cat.raw.some_table", metadata, MULTI_DOMAIN_CONFIG, top_n=3,
        )
        assert "clinical" in result

    def test_table_name_keyword_match(self):
        metadata = {"table_comments": "", "column_metadata": []}
        result = _domain_keyword_prefilter(
            "cat.default.employee_payroll", metadata, MULTI_DOMAIN_CONFIG, top_n=3,
        )
        assert "workforce" in result

    def test_empty_metadata_returns_top_n(self):
        result = _domain_keyword_prefilter(
            "cat.sch.tbl", {}, MULTI_DOMAIN_CONFIG, top_n=4,
        )
        assert len(result) == 4

    def test_column_metadata_as_dict(self):
        metadata = {"column_metadata": {"patient_id": {"type": "BIGINT"}, "visit_date": {"type": "DATE"}}}
        result = _domain_keyword_prefilter(
            "cat.raw.some_table", metadata, MULTI_DOMAIN_CONFIG, top_n=3,
        )
        assert "clinical" in result

    def test_column_contents_fallback(self):
        metadata = {
            "column_contents": {
                "column_metadata": [{"name": "claim_id"}, {"name": "member_id"}],
            },
        }
        result = _domain_keyword_prefilter(
            "cat.raw.some_table", metadata, MULTI_DOMAIN_CONFIG, top_n=3,
        )
        assert "payer" in result

    def test_case_insensitive_matching(self):
        metadata = {"table_comments": "PATIENT Encounter RECORDS"}
        result = _domain_keyword_prefilter(
            "cat.CLINICAL.FACT_TABLE", metadata, MULTI_DOMAIN_CONFIG, top_n=3,
        )
        assert "clinical" in result

    def test_multiple_keyword_hits_stack(self):
        metadata = {"table_comments": "revenue billing payment ledger"}
        result = _domain_keyword_prefilter(
            "cat.sch.tbl", metadata, MULTI_DOMAIN_CONFIG, top_n=3,
        )
        assert result[0] == "finance"

    def test_no_keywords_domain_scores_zero(self):
        config = {"domains": {**MULTI_DOMAIN_CONFIG["domains"],
                              "empty_dom": {"keywords": [], "subdomains": {}}}}
        metadata = {"table_comments": "patient records"}
        result = _domain_keyword_prefilter(
            "cat.sch.tbl", metadata, config, top_n=3,
        )
        assert "empty_dom" not in result

    def test_default_top_n_is_six(self):
        result = _domain_keyword_prefilter("cat.sch.tbl", {}, MULTI_DOMAIN_CONFIG)
        assert len(result) == 6


class TestBuildUserMessageSchemaSignal:

    def test_contains_schema_line(self):
        msg = _build_user_message("cat.claims_processing.dim_status", {})
        assert "Schema: claims_processing (strong domain signal)" in msg

    def test_still_contains_table_line(self):
        msg = _build_user_message("cat.my_schema.my_table", {})
        assert "Table: cat.my_schema.my_table" in msg
        assert "Schema: my_schema" in msg

    def test_no_schema_for_non_dotted_name(self):
        msg = _build_user_message("single_name", {})
        assert "Schema:" not in msg

    def test_schema_before_comment(self):
        meta = {"table_comments": "some comment"}
        msg = _build_user_message("cat.sch.tbl", meta)
        assert msg.index("Schema: sch") < msg.index("some comment")


class TestClassifyTwoStageUsesPrefilter:

    @patch("dbxmetagen.domain_classifier._domain_keyword_prefilter")
    @patch("dbxmetagen.domain_classifier.classify_domain_stage1")
    @patch("dbxmetagen.domain_classifier.classify_subdomain_stage2")
    def test_two_stage_calls_prefilter(self, mock_s2, mock_s1, mock_pf):
        mock_pf.return_value = ["clinical", "payer"]
        mock_s1.return_value = {"domain": "clinical", "confidence": 0.9, "reasoning": "d"}
        mock_s2.return_value = {
            "domain": "clinical", "subdomain": "patient_care",
            "confidence": 0.9, "reasoning": "s",
        }
        from dbxmetagen.domain_classifier import _classify_two_stage
        _classify_two_stage(
            "cat.sch.tbl", {}, MULTI_DOMAIN_CONFIG,
        )
        mock_pf.assert_called_once_with("cat.sch.tbl", {}, MULTI_DOMAIN_CONFIG)


# ---------------------------------------------------------------------------
# Domain resolution from bundles
# ---------------------------------------------------------------------------

BUNDLES_DIR = os.path.join(
    os.path.dirname(__file__), "..", "configurations", "ontology_bundles"
)


class TestDomainResolutionFromBundles:
    """Verify load_domain_config correctly extracts domains from bundles."""

    def test_curated_healthcare_bundle(self):
        cfg = load_domain_config(bundle_path=os.path.join(BUNDLES_DIR, "healthcare.yaml"))
        domains = cfg["domains"]
        assert "clinical" in domains
        assert "diagnostics" in domains
        assert len(domains) >= 10

    def test_curated_general_bundle(self):
        cfg = load_domain_config(bundle_path=os.path.join(BUNDLES_DIR, "general.yaml"))
        domains = cfg["domains"]
        assert "finance" in domains
        assert "customer" in domains
        assert len(domains) >= 6

    def test_formal_fhir_has_healthcare_domains(self):
        cfg = load_domain_config(bundle_path=os.path.join(BUNDLES_DIR, "fhir_r4.yaml"))
        domains = cfg["domains"]
        assert "clinical" in domains
        assert "diagnostics" in domains
        assert "payer" in domains

    def test_formal_omop_has_healthcare_domains(self):
        cfg = load_domain_config(bundle_path=os.path.join(BUNDLES_DIR, "omop_cdm.yaml"))
        domains = cfg["domains"]
        assert "clinical" in domains
        assert len(domains) >= 10

    def test_formal_schema_org_has_general_domains(self):
        cfg = load_domain_config(bundle_path=os.path.join(BUNDLES_DIR, "schema_org.yaml"))
        domains = cfg["domains"]
        assert "finance" in domains
        assert "technology" in domains
        assert "clinical" not in domains

    def test_config_path_overrides_bundle(self, tmp_path):
        custom = tmp_path / "custom_domains.yaml"
        custom.write_text("domains:\n  custom_domain:\n    name: Custom\n    keywords: [test]\n")
        cfg = load_domain_config(config_path=str(custom))
        assert "custom_domain" in cfg["domains"]

    def test_bundle_without_domains_logs_warning(self, tmp_path, caplog):
        bundle = tmp_path / "empty_bundle.yaml"
        bundle.write_text("metadata:\n  name: Empty\nontology:\n  entities: {}\n")
        import logging
        with caplog.at_level(logging.WARNING, logger="dbxmetagen.domain_classifier"):
            load_domain_config(bundle_path=str(bundle))
        assert any("has no 'domains' section" in r.message for r in caplog.records)

    def test_bundle_name_shorthand(self):
        """Passing just 'healthcare' (no path) resolves to the bundle YAML."""
        cfg = load_domain_config(bundle_path="healthcare")
        domains = cfg.get("domains", {})
        assert "clinical" in domains
