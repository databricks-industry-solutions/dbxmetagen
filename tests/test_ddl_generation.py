"""
Unit tests for DDL generation functions.
Focus on apostrophe handling and special characters in comments.
"""
import pytest
import os
from unittest.mock import patch
from src.dbxmetagen.ddl_generators import (
    _create_table_comment_ddl_func,
    _create_column_comment_ddl_func,
    _create_table_pi_information_ddl_func,
    _create_pi_information_ddl_func,
)


# Minimal config class for testing
class MinimalConfig:
    """Minimal config object for testing DDL generation."""
    def __init__(self):
        self.pi_classification_tag_name = "data_classification"
        self.pi_subclassification_tag_name = "data_subclassification"


# ===== Fixtures for comment tests =====

@pytest.fixture
def comment_fixtures():
    """Test comments with various apostrophe and special character scenarios."""
    return {
        "simple": "Patient demographics table",
        "single_apostrophe": "Patient's given name",
        "multiple_apostrophes": "Doctor's patient's emergency contact",
        "contraction": "This table contains patients' medical records that we've collected",
        "possessive_plural": "All patients' records from the doctors' notes",
        "quote_and_apostrophe": "The patient said \"I'm feeling better\"",
        "special_chars": "Table with data: semicolons; commas, periods. And more!",
        "newline": "First line\nSecond line",
        "empty": "",
        "unicode": "Patient données médicales",
    }


@pytest.fixture
def pi_fixtures():
    """Test PI classification scenarios."""
    return {
        "phi_high": {"classification": "PHI", "type": "Health Information", "confidence": 0.95},
        "pii_medium": {"classification": "PII", "type": "Personal Identifier", "confidence": 0.85},
        "pci_low": {"classification": "PCI", "type": "Payment Card", "confidence": 0.70},
        "none": {"classification": "None", "type": "Public", "confidence": 0.99},
    }


@pytest.fixture
def table_names():
    """Test table name scenarios."""
    return {
        "simple": "dev.schema1.patients",
        "with_numbers": "prod.schema2.table_v2",
        "long_name": "enterprise.healthcare_analytics.patient_diagnosis_codes_2024",
    }


# ===== Table Comment DDL Tests =====

class TestTableCommentDDL:
    """Tests for table-level comment DDL generation."""
    
    def test_simple_comment(self, table_names):
        """Test DDL generation with simple comment."""
        ddl_func = _create_table_comment_ddl_func()
        result = ddl_func(table_names["simple"], "Patient demographics table")
        
        assert 'COMMENT ON TABLE dev.schema1.patients IS "Patient demographics table";' == result
        assert result.startswith("COMMENT ON TABLE")
        assert result.endswith(";")
        assert '"Patient demographics table"' in result
    
    def test_single_apostrophe(self, table_names, comment_fixtures):
        """Test that single apostrophes work correctly with double quotes."""
        ddl_func = _create_table_comment_ddl_func()
        result = ddl_func(table_names["simple"], comment_fixtures["single_apostrophe"])
        
        # Should have single apostrophe, not doubled
        assert "Patient's given name" in result
        assert "Patient''s given name" not in result
        assert '"Patient\'s given name"' in result
    
    def test_multiple_apostrophes(self, table_names, comment_fixtures):
        """Test multiple apostrophes in one comment."""
        ddl_func = _create_table_comment_ddl_func()
        result = ddl_func(table_names["simple"], comment_fixtures["multiple_apostrophes"])
        
        assert "Doctor's patient's emergency contact" in result
        assert "Doctor''s patient''s" not in result
    
    def test_contraction(self, table_names, comment_fixtures):
        """Test contractions like we've, that's."""
        ddl_func = _create_table_comment_ddl_func()
        result = ddl_func(table_names["simple"], comment_fixtures["contraction"])
        
        assert "we've" in result
        assert "we''ve" not in result
    
    def test_possessive_plural(self, table_names, comment_fixtures):
        """Test possessive plural forms."""
        ddl_func = _create_table_comment_ddl_func()
        result = ddl_func(table_names["simple"], comment_fixtures["possessive_plural"])
        
        assert "patients' records" in result
        assert "doctors' notes" in result
        assert "patients'' records" not in result
    
    def test_special_characters(self, table_names, comment_fixtures):
        """Test semicolons, commas, periods in comments."""
        ddl_func = _create_table_comment_ddl_func()
        result = ddl_func(table_names["simple"], comment_fixtures["special_chars"])
        
        assert "semicolons;" in result
        assert "commas," in result
        assert "periods." in result
    
    def test_empty_comment(self, table_names, comment_fixtures):
        """Test empty comment string."""
        ddl_func = _create_table_comment_ddl_func()
        result = ddl_func(table_names["simple"], comment_fixtures["empty"])
        
        assert 'IS "";' in result
    
    def test_unicode_characters(self, table_names, comment_fixtures):
        """Test unicode/accented characters."""
        ddl_func = _create_table_comment_ddl_func()
        result = ddl_func(table_names["simple"], comment_fixtures["unicode"])
        
        assert "données médicales" in result


# ===== Column Comment DDL Tests =====

class TestColumnCommentDDL:
    """Tests for column-level comment DDL generation."""
    
    @patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "16.0"})
    def test_dbr_16_syntax(self, table_names):
        """Test DBR 16+ syntax with COMMENT ON COLUMN."""
        ddl_func = _create_column_comment_ddl_func()
        result = ddl_func(table_names["simple"], "patient_id", "Unique patient identifier")
        
        assert "COMMENT ON COLUMN" in result
        assert "`patient_id`" in result
        assert '"Unique patient identifier"' in result
        assert result.endswith(";")
    
    @patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "15.0"})
    def test_dbr_15_syntax(self, table_names):
        """Test DBR 14-15 syntax with ALTER TABLE ALTER COLUMN."""
        ddl_func = _create_column_comment_ddl_func()
        result = ddl_func(table_names["simple"], "first_name", "Patient's first name")
        
        assert "ALTER TABLE" in result
        assert "ALTER COLUMN" in result
        assert "COMMENT" in result
        assert "`first_name`" in result
        assert "Patient's first name" in result
        assert "Patient''s first name" not in result
    
    @patch.dict(os.environ, {}, clear=True)
    def test_no_dbr_version(self, table_names):
        """Test fallback when DBR version not available."""
        ddl_func = _create_column_comment_ddl_func()
        result = ddl_func(table_names["simple"], "ssn", "Patient's SSN")
        
        # Should default to newer syntax
        assert "COMMENT ON COLUMN" in result
        assert "Patient's SSN" in result
    
    @patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "16.0"})
    def test_column_with_apostrophe(self, table_names, comment_fixtures):
        """Test column comment with apostrophes."""
        ddl_func = _create_column_comment_ddl_func()
        result = ddl_func(table_names["simple"], "diagnosis", comment_fixtures["single_apostrophe"])
        
        assert "Patient's given name" in result
        assert "Patient''s given name" not in result
    
    @patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "16.0"})
    def test_column_with_special_chars(self, table_names):
        """Test column comment with special characters."""
        ddl_func = _create_column_comment_ddl_func()
        comment = "Measurement in mg/dL; normal range: 70-100"
        result = ddl_func(table_names["simple"], "glucose_level", comment)
        
        assert "mg/dL;" in result
        assert "70-100" in result


# ===== PI Information DDL Tests =====

class TestPIInformationDDL:
    """Tests for PI classification DDL generation."""
    
    def test_table_pi_tags(self, table_names, pi_fixtures):
        """Test table-level PI tagging DDL."""
        config = MinimalConfig()
        
        ddl_func = _create_table_pi_information_ddl_func(config)
        phi_case = pi_fixtures["phi_high"]
        result = ddl_func(
            table_names["simple"],
            phi_case["classification"],
            phi_case["type"]
        )
        
        assert "ALTER TABLE" in result
        assert "SET TAGS" in result
        assert "'data_classification' = 'PHI'" in result
        assert "'data_subclassification' = 'Health Information'" in result
        assert result.endswith(";")
    
    def test_column_pi_tags(self, table_names, pi_fixtures):
        """Test column-level PI tagging DDL."""
        config = MinimalConfig()
        
        ddl_func = _create_pi_information_ddl_func(config)
        pii_case = pi_fixtures["pii_medium"]
        result = ddl_func(
            table_names["simple"],
            "ssn",
            pii_case["classification"],
            pii_case["type"]
        )
        
        assert "ALTER TABLE" in result
        assert "ALTER COLUMN" in result
        assert "SET TAGS" in result
        assert "`ssn`" in result
        assert "'data_classification' = 'PII'" in result
        assert "'data_subclassification' = 'Personal Identifier'" in result
    
    def test_pci_classification(self, table_names, pi_fixtures):
        """Test PCI classification."""
        config = MinimalConfig()
        
        ddl_func = _create_table_pi_information_ddl_func(config)
        pci_case = pi_fixtures["pci_low"]
        result = ddl_func(
            table_names["simple"],
            pci_case["classification"],
            pci_case["type"]
        )
        
        assert "'data_classification' = 'PCI'" in result
        assert "'data_subclassification' = 'Payment Card'" in result
    
    def test_none_classification(self, table_names, pi_fixtures):
        """Test None/Public classification."""
        config = MinimalConfig()
        
        ddl_func = _create_table_pi_information_ddl_func(config)
        none_case = pi_fixtures["none"]
        result = ddl_func(
            table_names["simple"],
            none_case["classification"],
            none_case["type"]
        )
        
        assert "'data_classification' = 'None'" in result
        assert "'data_subclassification' = 'Public'" in result


# ===== Integration Tests =====

class TestDDLIntegration:
    """Integration tests combining multiple DDL generation scenarios."""
    
    @patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "16.0"})
    def test_table_and_column_comments_match_format(self, table_names):
        """Test that table and column DDLs use consistent quoting."""
        table_func = _create_table_comment_ddl_func()
        column_func = _create_column_comment_ddl_func()
        
        comment = "Doctor's notes from patient's visit"
        
        table_ddl = table_func(table_names["simple"], comment)
        column_ddl = column_func(table_names["simple"], "notes", comment)
        
        # Both should use double quotes
        assert '"Doctor\'s notes from patient\'s visit"' in table_ddl
        assert '"Doctor\'s notes from patient\'s visit"' in column_ddl
        
        # Neither should have escaped apostrophes
        assert "Doctor''s" not in table_ddl
        assert "Doctor''s" not in column_ddl
    
    def test_multiple_tables_same_comment(self, table_names, comment_fixtures):
        """Test same comment applied to multiple tables."""
        ddl_func = _create_table_comment_ddl_func()
        comment = comment_fixtures["multiple_apostrophes"]
        
        results = [ddl_func(table_name, comment) for table_name in table_names.values()]
        
        # All should have the same comment text format
        for result in results:
            assert "Doctor's patient's emergency contact" in result
            assert "Doctor''s patient''s" not in result
    
    def test_pi_and_comment_on_same_table(self, table_names):
        """Test that PI tags and comments can coexist."""
        config = MinimalConfig()
        
        comment_func = _create_table_comment_ddl_func()
        pi_func = _create_table_pi_information_ddl_func(config)
        
        comment_ddl = comment_func(table_names["simple"], "Patient's PHI data")
        pi_ddl = pi_func(table_names["simple"], "PHI", "Health Information")
        
        # Both should be valid SQL
        assert comment_ddl.startswith("COMMENT ON TABLE")
        assert pi_ddl.startswith("ALTER TABLE")
        
        # Comment should use double quotes
        assert '"Patient\'s PHI data"' in comment_ddl
        
        # PI tags should use single quotes
        assert "'data_classification' = 'PHI'" in pi_ddl


# ===== Edge Cases =====

class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_very_long_comment(self, table_names):
        """Test handling of very long comments."""
        ddl_func = _create_table_comment_ddl_func()
        long_comment = "Patient's " + ("medical " * 100) + "records"
        
        result = ddl_func(table_names["simple"], long_comment)
        
        assert long_comment in result
        assert "Patient''s" not in result
    
    def test_comment_with_sql_keywords(self, table_names):
        """Test comments containing SQL keywords."""
        ddl_func = _create_table_comment_ddl_func()
        comment = "Table with SELECT, DROP, and ALTER operations"
        
        result = ddl_func(table_names["simple"], comment)
        
        assert "SELECT" in result
        assert "DROP" in result
        assert "ALTER" in result
    
    @patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "16.0"})
    def test_column_name_with_special_chars(self, table_names):
        """Test column names that need backtick escaping."""
        ddl_func = _create_column_comment_ddl_func()
        
        # Column names with spaces or special chars get backticks
        result = ddl_func(table_names["simple"], "patient name", "Patient's full name")
        
        assert "`patient name`" in result
        assert "Patient's full name" in result
    
    def test_empty_table_name(self):
        """Test behavior with empty table name."""
        ddl_func = _create_table_comment_ddl_func()
        
        # Should still generate DDL, even if invalid
        result = ddl_func("", "Some comment")
        
        assert "COMMENT ON TABLE" in result
        assert "Some comment" in result

