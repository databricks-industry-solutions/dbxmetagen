"""Unit tests for knowledge_base module.

Tests focus on:
1. Pure function logic (table name parsing, classification)
2. SQL generation correctness
3. Configuration behavior
4. Edge cases and error handling
"""

import pytest
import sys
import os
from unittest.mock import MagicMock, patch

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.dbxmetagen.knowledge_base import (
    KnowledgeBaseConfig,
    parse_table_name_parts,
    classify_has_pii,
    classify_has_phi,
    KnowledgeBaseBuilder,
    build_knowledge_base,
)


class TestParseTableNameParts:
    """Test parse_table_name_parts function - core parsing logic."""

    def test_three_part_name_extracts_all_parts(self):
        """Standard case: catalog.schema.table extracts all three parts."""
        result = parse_table_name_parts("prod_catalog.analytics.user_events")
        assert result == {
            "catalog": "prod_catalog",
            "schema": "analytics", 
            "table_short_name": "user_events"
        }

    def test_two_part_name_uses_first_as_catalog(self):
        """Two parts: first is catalog, second is schema, short name is full string."""
        result = parse_table_name_parts("schema.table")
        assert result["catalog"] == "schema"
        assert result["schema"] == "table"
        # Edge case behavior: short name becomes full string when < 3 parts
        assert result["table_short_name"] == "schema.table"

    def test_single_part_name_becomes_catalog_and_short_name(self):
        """Single part: becomes both catalog and short name, schema is None."""
        result = parse_table_name_parts("my_table")
        assert result["catalog"] == "my_table"
        assert result["schema"] is None
        assert result["table_short_name"] == "my_table"

    def test_empty_string_returns_all_none(self):
        """Empty string should return all None values."""
        result = parse_table_name_parts("")
        assert result == {"catalog": None, "schema": None, "table_short_name": None}

    def test_none_input_returns_all_none(self):
        """None input should return all None values (no exception)."""
        result = parse_table_name_parts(None)
        assert result == {"catalog": None, "schema": None, "table_short_name": None}

    def test_four_or_more_parts_ignores_extras(self):
        """Extra parts beyond three are ignored (only first 3 used)."""
        result = parse_table_name_parts("a.b.c.d.e")
        assert result["catalog"] == "a"
        assert result["schema"] == "b"
        assert result["table_short_name"] == "c"

    def test_names_with_underscores_preserved(self):
        """Underscores in names are preserved correctly."""
        result = parse_table_name_parts("dev_catalog.test_schema.user_click_events")
        assert result["catalog"] == "dev_catalog"
        assert result["schema"] == "test_schema"
        assert result["table_short_name"] == "user_click_events"


class TestClassifyHasPii:
    """Test classify_has_pii - determines if data has any personal information."""

    # Valid PII classifications
    @pytest.mark.parametrize("classification", ["pii", "PII", "Pii", "pII"])
    def test_pii_in_any_case_returns_true(self, classification):
        """PII in any case should return True."""
        assert classify_has_pii(classification) is True

    @pytest.mark.parametrize("classification", ["phi", "PHI", "Phi"])
    def test_phi_returns_true_because_phi_is_pii(self, classification):
        """PHI (protected health info) is a subset of PII, so returns True."""
        assert classify_has_pii(classification) is True

    @pytest.mark.parametrize("classification", ["pci", "PCI", "Pci"])
    def test_pci_returns_true_payment_card_is_pii(self, classification):
        """PCI (payment card info) is PII, so returns True."""
        assert classify_has_pii(classification) is True

    # Invalid/missing classifications
    def test_none_returns_false(self):
        """None classification means no PII detected."""
        assert classify_has_pii(None) is False

    def test_empty_string_returns_false(self):
        """Empty string means no classification."""
        assert classify_has_pii("") is False

    @pytest.mark.parametrize("classification", [
        "non-pii", "public", "internal", "confidential", 
        "pii_related", "has_pii", "PIIX", "xpii"
    ])
    def test_other_values_return_false(self, classification):
        """Only exact matches (pii/phi/pci) return True, not partial matches."""
        assert classify_has_pii(classification) is False


class TestClassifyHasPhi:
    """Test classify_has_phi - determines if data has protected health information."""

    @pytest.mark.parametrize("classification", ["phi", "PHI", "Phi", "pHI"])
    def test_phi_in_any_case_returns_true(self, classification):
        """PHI in any case should return True."""
        assert classify_has_phi(classification) is True

    # PHI is stricter than PII - only PHI matches, not PII/PCI
    def test_pii_returns_false_pii_is_not_phi(self):
        """PII is NOT PHI (the reverse relationship)."""
        assert classify_has_phi("pii") is False
        assert classify_has_phi("PII") is False

    def test_pci_returns_false(self):
        """PCI (payment card) is not PHI (health info)."""
        assert classify_has_phi("pci") is False

    def test_none_returns_false(self):
        """None means no PHI."""
        assert classify_has_phi(None) is False

    def test_empty_string_returns_false(self):
        """Empty string means no classification."""
        assert classify_has_phi("") is False

    @pytest.mark.parametrize("classification", [
        "health_info", "medical", "hipaa", "phi_data", "contains_phi"
    ])
    def test_partial_matches_return_false(self, classification):
        """Only exact 'phi' matches, not related terms."""
        assert classify_has_phi(classification) is False


class TestKnowledgeBaseConfig:
    """Test configuration dataclass behavior."""

    def test_fully_qualified_paths_built_correctly(self):
        """Config should build correct fully qualified table paths."""
        config = KnowledgeBaseConfig(
            catalog_name="prod",
            schema_name="metadata"
        )
        assert config.fully_qualified_source == "prod.metadata.metadata_generation_log"
        assert config.fully_qualified_target == "prod.metadata.table_knowledge_base"

    def test_custom_table_names_override_defaults(self):
        """Custom source/target table names should override defaults."""
        config = KnowledgeBaseConfig(
            catalog_name="cat",
            schema_name="sch",
            source_table="my_log",
            target_table="my_kb"
        )
        assert config.fully_qualified_source == "cat.sch.my_log"
        assert config.fully_qualified_target == "cat.sch.my_kb"

    def test_special_characters_in_names_not_escaped(self):
        """Names with underscores should be preserved as-is."""
        config = KnowledgeBaseConfig(
            catalog_name="dev_catalog",
            schema_name="test_schema"
        )
        assert "dev_catalog.test_schema" in config.fully_qualified_source


class TestCreateTargetTableDDL:
    """Test that DDL generation produces correct SQL."""

    def setup_method(self):
        self.mock_spark = MagicMock()
        self.config = KnowledgeBaseConfig(
            catalog_name="test_cat",
            schema_name="test_sch"
        )

    def test_ddl_includes_required_columns(self):
        """DDL must include all required columns with correct types."""
        builder = KnowledgeBaseBuilder(self.mock_spark, self.config)
        builder.create_target_table()
        
        ddl = self.mock_spark.sql.call_args[0][0]
        
        # Required columns (note: `schema` is escaped because it's a reserved word)
        assert "table_name STRING NOT NULL" in ddl
        assert "catalog STRING" in ddl
        assert "`schema` STRING" in ddl  # Escaped reserved word
        assert "table_short_name STRING" in ddl
        assert "comment STRING" in ddl
        assert "domain STRING" in ddl
        assert "subdomain STRING" in ddl
        assert "has_pii BOOLEAN" in ddl
        assert "has_phi BOOLEAN" in ddl
        assert "created_at TIMESTAMP" in ddl
        assert "updated_at TIMESTAMP" in ddl

    def test_ddl_uses_liquid_clustering(self):
        """DDL should use CLUSTER BY for liquid clustering on DBR 17.4+."""
        builder = KnowledgeBaseBuilder(self.mock_spark, self.config)
        builder.create_target_table()
        
        ddl = self.mock_spark.sql.call_args[0][0]
        assert "CLUSTER BY (catalog, `schema`)" in ddl  # Escaped reserved word

    def test_ddl_uses_correct_table_name(self):
        """DDL should create table at the configured location."""
        builder = KnowledgeBaseBuilder(self.mock_spark, self.config)
        builder.create_target_table()
        
        ddl = self.mock_spark.sql.call_args[0][0]
        assert "test_cat.test_sch.table_knowledge_base" in ddl

    def test_ddl_is_idempotent(self):
        """DDL should use IF NOT EXISTS for idempotency."""
        builder = KnowledgeBaseBuilder(self.mock_spark, self.config)
        builder.create_target_table()
        
        ddl = self.mock_spark.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in ddl


class TestMergeSQLGeneration:
    """Test that MERGE SQL is generated correctly - this is critical business logic."""

    def setup_method(self):
        self.mock_spark = MagicMock()
        self.mock_spark.sql.return_value.collect.return_value = [{"cnt": 0}]
        self.config = KnowledgeBaseConfig(
            catalog_name="test_cat",
            schema_name="test_sch"
        )

    def _get_merge_sql(self):
        """Helper to extract MERGE SQL from mock calls."""
        builder = KnowledgeBaseBuilder(self.mock_spark, self.config)
        mock_df = MagicMock()
        builder.merge_to_target(mock_df)
        
        for call in self.mock_spark.sql.call_args_list:
            sql = call[0][0]
            if "MERGE INTO" in sql:
                return sql
        return None

    def test_merge_uses_coalesce_for_nullable_columns(self):
        """MERGE should use COALESCE to preserve existing values when new is NULL.
        
        This is critical: when domain runs after comment, we don't want to 
        overwrite comment with NULL.
        """
        merge_sql = self._get_merge_sql()
        
        # These columns should use COALESCE to preserve existing values
        assert "COALESCE(source.comment, target.comment)" in merge_sql
        assert "COALESCE(source.domain, target.domain)" in merge_sql
        assert "COALESCE(source.subdomain, target.subdomain)" in merge_sql

    def test_merge_uses_or_for_boolean_flags(self):
        """Boolean PII/PHI flags should use OR logic.
        
        Once a table is marked as having PII, it should stay marked even if
        a later run doesn't detect PII (could be different columns processed).
        """
        merge_sql = self._get_merge_sql()
        
        assert "source.has_pii OR target.has_pii" in merge_sql
        assert "source.has_phi OR target.has_phi" in merge_sql

    def test_merge_uses_greatest_for_updated_at(self):
        """updated_at should use GREATEST to keep the most recent timestamp."""
        merge_sql = self._get_merge_sql()
        
        assert "GREATEST(source.updated_at, target.updated_at)" in merge_sql

    def test_merge_matches_on_table_name(self):
        """MERGE should match on table_name as the unique key."""
        merge_sql = self._get_merge_sql()
        
        assert "ON target.table_name = source.table_name" in merge_sql

    def test_merge_inserts_all_columns_when_not_matched(self):
        """When table doesn't exist, all columns should be inserted."""
        merge_sql = self._get_merge_sql()
        
        assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
        # Verify key columns are in INSERT
        assert "table_name" in merge_sql
        assert "has_pii" in merge_sql
        assert "has_phi" in merge_sql
        assert "created_at" in merge_sql


class TestReadSourceDataQuery:
    """Test that source data query is correct."""

    def setup_method(self):
        self.mock_spark = MagicMock()
        self.config = KnowledgeBaseConfig(
            catalog_name="prod",
            schema_name="meta"
        )

    def test_query_reads_from_correct_source_table(self):
        """Query should read from the configured source table."""
        builder = KnowledgeBaseBuilder(self.mock_spark, self.config)
        builder.read_source_data()
        
        query = self.mock_spark.sql.call_args[0][0]
        assert "prod.meta.metadata_generation_log" in query

    def test_query_filters_null_tables(self):
        """Query should filter out rows with NULL table names."""
        builder = KnowledgeBaseBuilder(self.mock_spark, self.config)
        builder.read_source_data()
        
        query = self.mock_spark.sql.call_args[0][0]
        assert "WHERE `table` IS NOT NULL" in query

    def test_query_selects_required_columns(self):
        """Query should select columns needed for transformation."""
        builder = KnowledgeBaseBuilder(self.mock_spark, self.config)
        builder.read_source_data()
        
        query = self.mock_spark.sql.call_args[0][0]
        # Key columns for the transformation
        assert "metadata_type" in query
        assert "ddl_type" in query
        assert "column_content" in query
        assert "domain" in query
        assert "subdomain" in query
        assert "classification" in query
        assert "_created_at" in query


class TestBuildKnowledgeBaseFunction:
    """Test the convenience function."""

    @patch("src.dbxmetagen.knowledge_base.KnowledgeBaseBuilder")
    def test_function_creates_correct_config(self, mock_builder_class):
        """Function should create config with provided catalog/schema."""
        mock_builder = MagicMock()
        mock_builder.run.return_value = {"staged_count": 0, "total_records": 0}
        mock_builder_class.return_value = mock_builder
        
        build_knowledge_base(
            spark=MagicMock(),
            catalog_name="my_catalog",
            schema_name="my_schema"
        )
        
        # Get the config that was passed to the builder
        config = mock_builder_class.call_args[0][1]
        assert config.catalog_name == "my_catalog"
        assert config.schema_name == "my_schema"

    @patch("src.dbxmetagen.knowledge_base.KnowledgeBaseBuilder")
    def test_function_returns_run_result(self, mock_builder_class):
        """Function should return the result from builder.run()."""
        mock_builder = MagicMock()
        expected_result = {"staged_count": 42, "total_records": 100}
        mock_builder.run.return_value = expected_result
        mock_builder_class.return_value = mock_builder
        
        result = build_knowledge_base(
            spark=MagicMock(),
            catalog_name="cat",
            schema_name="sch"
        )
        
        assert result == expected_result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
