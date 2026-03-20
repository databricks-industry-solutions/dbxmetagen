"""Tests for loading domains from EITHER an ontology bundle OR a standalone domain config file.

Both sources must produce a config dict with a top-level ``domains`` key whose
values contain ``name``, ``description``, ``keywords``, and ``subdomains``.
The downstream prompt generators and classifiers must work identically
regardless of the source.
"""

import os
import sys
import yaml
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dbxmetagen.domain_classifier import (
    load_domain_config,
    generate_domain_prompt_section,
    generate_domain_only_prompt,
    generate_subdomain_prompt,
    create_system_prompt,
)


# ── Fixtures: minimal domain configs ────────────────────────────────────


STANDALONE_YAML = {
    "version": "1.1",
    "domains": {
        "clinical": {
            "name": "Clinical",
            "description": "Patient care and encounters",
            "keywords": ["patient", "encounter", "diagnosis"],
            "subdomains": {
                "patient_care": {
                    "name": "Patient Care",
                    "description": "Demographics and admissions",
                    "keywords": ["patient", "admission"],
                },
                "diagnosis_condition": {
                    "name": "Diagnosis & Conditions",
                    "description": "Medical diagnoses and coding",
                    "keywords": ["diagnosis", "icd"],
                },
            },
        },
        "finance": {
            "name": "Finance",
            "description": "Billing, payments, and revenue",
            "keywords": ["billing", "payment", "ledger"],
            "subdomains": {
                "accounting": {
                    "name": "Accounting",
                    "description": "GL, AP/AR",
                    "keywords": ["ledger", "journal"],
                },
            },
        },
    },
}

BUNDLE_YAML = {
    "metadata": {
        "name": "test_bundle",
        "version": "3.0",
        "industry": "healthcare",
        "description": "Test bundle",
    },
    "ontology": {
        "version": "3.0",
        "name": "Test",
        "entities": {"definitions": {"Patient": {"description": "A patient"}}},
    },
    "domains": {
        "clinical": {
            "name": "Clinical",
            "description": "Patient care and encounters",
            "keywords": ["patient", "encounter", "diagnosis"],
            "subdomains": {
                "patient_care": {
                    "name": "Patient Care",
                    "description": "Demographics and admissions",
                    "keywords": ["patient", "admission"],
                },
                "diagnosis_condition": {
                    "name": "Diagnosis & Conditions",
                    "description": "Medical diagnoses and coding",
                    "keywords": ["diagnosis", "icd"],
                },
            },
        },
        "finance": {
            "name": "Finance",
            "description": "Billing, payments, and revenue",
            "keywords": ["billing", "payment", "ledger"],
            "subdomains": {
                "accounting": {
                    "name": "Accounting",
                    "description": "GL, AP/AR",
                    "keywords": ["ledger", "journal"],
                },
            },
        },
    },
}


@pytest.fixture
def standalone_config_path(tmp_path):
    p = tmp_path / "domain_config.yaml"
    p.write_text(yaml.dump(STANDALONE_YAML, default_flow_style=False))
    return str(p)


@pytest.fixture
def bundle_config_path(tmp_path):
    p = tmp_path / "bundle.yaml"
    p.write_text(yaml.dump(BUNDLE_YAML, default_flow_style=False))
    return str(p)


# ── load_domain_config: standalone file ──────────────────────────────────


class TestLoadFromStandaloneFile:

    def test_loads_domains(self, standalone_config_path):
        config = load_domain_config(config_path=standalone_config_path)
        assert "domains" in config
        assert "clinical" in config["domains"]
        assert "finance" in config["domains"]

    def test_preserves_subdomains(self, standalone_config_path):
        config = load_domain_config(config_path=standalone_config_path)
        clinical = config["domains"]["clinical"]
        assert "subdomains" in clinical
        assert "patient_care" in clinical["subdomains"]
        assert "diagnosis_condition" in clinical["subdomains"]

    def test_preserves_keywords(self, standalone_config_path):
        config = load_domain_config(config_path=standalone_config_path)
        assert "patient" in config["domains"]["clinical"]["keywords"]

    def test_preserves_descriptions(self, standalone_config_path):
        config = load_domain_config(config_path=standalone_config_path)
        assert config["domains"]["finance"]["description"] == "Billing, payments, and revenue"

    def test_subdomain_has_keywords(self, standalone_config_path):
        config = load_domain_config(config_path=standalone_config_path)
        sd = config["domains"]["clinical"]["subdomains"]["patient_care"]
        assert "patient" in sd["keywords"]


# ── load_domain_config: ontology bundle ──────────────────────────────────


class TestLoadFromBundle:

    def test_loads_domains_from_bundle(self, bundle_config_path):
        config = load_domain_config(bundle_path=bundle_config_path)
        assert "domains" in config
        assert "clinical" in config["domains"]
        assert "finance" in config["domains"]

    def test_preserves_subdomains_from_bundle(self, bundle_config_path):
        config = load_domain_config(bundle_path=bundle_config_path)
        clinical = config["domains"]["clinical"]
        assert "patient_care" in clinical["subdomains"]

    def test_preserves_keywords_from_bundle(self, bundle_config_path):
        config = load_domain_config(bundle_path=bundle_config_path)
        assert "billing" in config["domains"]["finance"]["keywords"]

    def test_preserves_descriptions_from_bundle(self, bundle_config_path):
        config = load_domain_config(bundle_path=bundle_config_path)
        assert "Patient care" in config["domains"]["clinical"]["description"]

    def test_bundle_version_propagated(self, bundle_config_path):
        config = load_domain_config(bundle_path=bundle_config_path)
        assert config.get("version") == "3.0"


# ── Both sources produce identical downstream output ─────────────────────


class TestOutputEquivalence:
    """The prompt generators must produce equivalent output regardless of source."""

    def _load_both(self, standalone_config_path, bundle_config_path):
        standalone = load_domain_config(config_path=standalone_config_path)
        bundle = load_domain_config(bundle_path=bundle_config_path)
        return standalone, bundle

    def test_same_domain_keys(self, standalone_config_path, bundle_config_path):
        s, b = self._load_both(standalone_config_path, bundle_config_path)
        assert set(s["domains"].keys()) == set(b["domains"].keys())

    def test_same_subdomain_keys(self, standalone_config_path, bundle_config_path):
        s, b = self._load_both(standalone_config_path, bundle_config_path)
        for domain_key in s["domains"]:
            s_subs = set(s["domains"][domain_key].get("subdomains", {}).keys())
            b_subs = set(b["domains"][domain_key].get("subdomains", {}).keys())
            assert s_subs == b_subs, f"Subdomain mismatch for {domain_key}"

    def test_prompt_section_mentions_all_domains(self, standalone_config_path, bundle_config_path):
        s, b = self._load_both(standalone_config_path, bundle_config_path)
        for config in [s, b]:
            prompt = generate_domain_prompt_section(config)
            assert "clinical" in prompt
            assert "finance" in prompt

    def test_system_prompt_mentions_all_domains(self, standalone_config_path, bundle_config_path):
        s, b = self._load_both(standalone_config_path, bundle_config_path)
        for config in [s, b]:
            prompt = create_system_prompt(config)
            assert "clinical" in prompt
            assert "finance" in prompt

    def test_domain_only_prompt_works_for_both(self, standalone_config_path, bundle_config_path):
        s, b = self._load_both(standalone_config_path, bundle_config_path)
        candidates = ["clinical", "finance"]
        for config in [s, b]:
            prompt = generate_domain_only_prompt(config, candidates)
            assert "Clinical" in prompt
            assert "Finance" in prompt

    def test_subdomain_prompt_works_for_both(self, standalone_config_path, bundle_config_path):
        s, b = self._load_both(standalone_config_path, bundle_config_path)
        for config in [s, b]:
            prompt = generate_subdomain_prompt(config, "clinical")
            assert "patient_care" in prompt
            assert "diagnosis_condition" in prompt


# ── Priority: bundle_path takes precedence over config_path ──────────────


class TestSourcePriority:

    def test_bundle_takes_precedence(self, tmp_path):
        """When both bundle_path and config_path are given, bundle wins."""
        standalone = tmp_path / "standalone.yaml"
        standalone.write_text(yaml.dump({
            "domains": {"only_standalone": {"name": "Standalone", "keywords": []}},
        }))
        bundle = tmp_path / "bundle.yaml"
        bundle.write_text(yaml.dump({
            "metadata": {"name": "b", "version": "3.0"},
            "domains": {"only_bundle": {"name": "Bundle", "keywords": []}},
        }))
        config = load_domain_config(
            config_path=str(standalone), bundle_path=str(bundle)
        )
        assert "only_bundle" in config["domains"]
        assert "only_standalone" not in config["domains"]

    def test_falls_back_to_standalone_when_bundle_missing(self, tmp_path):
        standalone = tmp_path / "standalone.yaml"
        standalone.write_text(yaml.dump({
            "domains": {"standalone_domain": {"name": "SD", "keywords": ["x"]}},
        }))
        config = load_domain_config(
            config_path=str(standalone),
            bundle_path="/nonexistent/bundle.yaml",
        )
        assert "standalone_domain" in config["domains"]

    def test_falls_back_to_standalone_when_bundle_has_no_domains(self, tmp_path):
        standalone = tmp_path / "standalone.yaml"
        standalone.write_text(yaml.dump({
            "domains": {"sd": {"name": "SD", "keywords": []}},
        }))
        bundle = tmp_path / "no_domains.yaml"
        bundle.write_text(yaml.dump({"metadata": {"name": "b"}, "ontology": {}}))
        config = load_domain_config(
            config_path=str(standalone), bundle_path=str(bundle)
        )
        assert "sd" in config["domains"]


# ── Edge cases ────────────────────────────────────────────────────────────


class TestEdgeCases:

    def test_bundle_with_empty_domains(self, tmp_path):
        p = tmp_path / "empty_domains.yaml"
        p.write_text(yaml.dump({"metadata": {"name": "b"}, "domains": {}}))
        config = load_domain_config(bundle_path=str(p))
        assert config["domains"] == {}

    def test_standalone_with_no_subdomains(self, tmp_path):
        p = tmp_path / "no_sub.yaml"
        p.write_text(yaml.dump({
            "domains": {"simple": {"name": "Simple", "keywords": ["a"]}},
        }))
        config = load_domain_config(config_path=str(p))
        assert "simple" in config["domains"]
        prompt = generate_subdomain_prompt(config, "simple")
        assert "no predefined subdomains" in prompt

    def test_neither_path_returns_fallback(self):
        config = load_domain_config(
            config_path="/nonexistent.yaml",
            bundle_path="/also_nonexistent.yaml",
        )
        assert "domains" in config

    def test_bundle_shortname_resolution(self, tmp_path, monkeypatch):
        """When bundle_path is a bare name like 'healthcare', it should resolve
        to configurations/ontology_bundles/healthcare.yaml if that exists."""
        bundles_dir = tmp_path / "configurations" / "ontology_bundles"
        bundles_dir.mkdir(parents=True)
        bundle_file = bundles_dir / "healthcare.yaml"
        bundle_file.write_text(yaml.dump({
            "metadata": {"name": "hc", "version": "3.0"},
            "domains": {"resolved": {"name": "Resolved", "keywords": []}},
        }))
        monkeypatch.chdir(tmp_path)
        config = load_domain_config(bundle_path="healthcare")
        assert "resolved" in config["domains"]


# ── Real file tests (against checked-in configs) ────────────────────────


class TestRealConfigs:
    """Validate that the actual checked-in configs load correctly."""

    _repo = os.path.join(os.path.dirname(__file__), "..")

    def test_healthcare_standalone_loads(self):
        path = os.path.join(self._repo, "configurations", "domain_config_healthcare.yaml")
        if not os.path.exists(path):
            pytest.skip("domain_config_healthcare.yaml not found")
        config = load_domain_config(config_path=path)
        assert "domains" in config
        assert len(config["domains"]) > 0
        for dk, dv in config["domains"].items():
            assert "name" in dv, f"Domain {dk} missing 'name'"
            assert "keywords" in dv, f"Domain {dk} missing 'keywords'"

    def test_healthcare_bundle_loads(self):
        path = os.path.join(self._repo, "configurations", "ontology_bundles", "healthcare.yaml")
        if not os.path.exists(path):
            pytest.skip("healthcare.yaml bundle not found")
        config = load_domain_config(bundle_path=path)
        assert "domains" in config
        assert len(config["domains"]) > 0

    def test_general_bundle_loads(self):
        path = os.path.join(self._repo, "configurations", "ontology_bundles", "general.yaml")
        if not os.path.exists(path):
            pytest.skip("general.yaml bundle not found")
        config = load_domain_config(bundle_path=path)
        assert "domains" in config
        assert "finance" in config["domains"]

    def test_real_standalone_and_bundle_both_have_clinical(self):
        """Healthcare standalone and healthcare bundle should both provide 'clinical'."""
        standalone_path = os.path.join(
            self._repo, "configurations", "domain_config_healthcare.yaml"
        )
        bundle_path = os.path.join(
            self._repo, "configurations", "ontology_bundles", "healthcare.yaml"
        )
        if not os.path.exists(standalone_path) or not os.path.exists(bundle_path):
            pytest.skip("Config files not found")
        s = load_domain_config(config_path=standalone_path)
        b = load_domain_config(bundle_path=bundle_path)
        assert "clinical" in s["domains"]
        assert "clinical" in b["domains"]
        assert "patient_care" in s["domains"]["clinical"]["subdomains"]
        assert "patient_care" in b["domains"]["clinical"]["subdomains"]
