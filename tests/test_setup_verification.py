"""
Simple verification test to ensure test infrastructure is working.
Run this first to verify pytest and imports are configured correctly.
"""
import pytest


def test_imports():
    """Verify core imports work."""
    from src.dbxmetagen.ddl_generators import _create_table_comment_ddl_func
    from src.dbxmetagen.config import MetadataConfig
    
    assert _create_table_comment_ddl_func is not None
    assert MetadataConfig is not None


def test_basic_table_comment():
    """Sanity check: verify basic table comment DDL generation."""
    from src.dbxmetagen.ddl_generators import _create_table_comment_ddl_func
    
    ddl_func = _create_table_comment_ddl_func()
    result = ddl_func("catalog.schema.table", "Test comment")
    
    assert "COMMENT ON TABLE" in result
    assert "catalog.schema.table" in result
    assert "Test comment" in result
    assert result.endswith(";")


def test_apostrophe_not_escaped():
    """Critical test: verify apostrophes are NOT escaped with double quotes."""
    from src.dbxmetagen.ddl_generators import _create_table_comment_ddl_func
    
    ddl_func = _create_table_comment_ddl_func()
    result = ddl_func("catalog.schema.table", "Patient's name")
    
    # Should have single apostrophe, not doubled
    assert "Patient's name" in result
    assert "Patient''s name" not in result
    
    # Should be wrapped in double quotes
    assert '"Patient\'s name"' in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

