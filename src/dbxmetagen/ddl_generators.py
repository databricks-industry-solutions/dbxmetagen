"""
Standalone DDL generation functions.
Extracted for easier testing without heavy dependencies.
"""
import os
from typing import Callable
from src.dbxmetagen.config import MetadataConfig


def _create_table_comment_ddl_func() -> Callable[[str, str], str]:
    """
    Create a function that generates table-level comment DDL.
    Uses double quotes to avoid escaping apostrophes.
    
    Returns:
        Function that takes (table_name, comment) and returns DDL string
    """
    def table_comment_ddl(full_table_name: str, comment: str) -> str:
        # Use double quotes for the string literal to avoid escaping apostrophes
        return f"""COMMENT ON TABLE {full_table_name} IS "{comment}";"""

    return table_comment_ddl


def _create_column_comment_ddl_func() -> Callable[[str, str, str], str]:
    """
    Create a function that generates column-level comment DDL.
    Uses double quotes to avoid escaping apostrophes.
    Handles different Databricks runtime versions.
    
    Returns:
        Function that takes (table_name, column_name, comment) and returns DDL string
    """
    def column_comment_ddl(full_table_name: str, column_name: str, comment: str) -> str:
        # Use double quotes for the string literal to avoid escaping apostrophes
        dbr_number = os.environ.get("DATABRICKS_RUNTIME_VERSION")

        if dbr_number is None:
            # Default to newer syntax for serverless (assumes DBR 15+)
            ddl_statement = f"""COMMENT ON COLUMN {full_table_name}.`{column_name}` IS "{comment}";"""
        else:
            try:
                dbr_version = float(dbr_number)
                if dbr_version is None:
                    raise ValueError(f"Databricks runtime version is None")
                if dbr_version >= 16:
                    ddl_statement = f"""COMMENT ON COLUMN {full_table_name}.`{column_name}` IS "{comment}";"""
                elif dbr_version >= 14 and dbr_version < 16:
                    ddl_statement = f"""ALTER TABLE {full_table_name} ALTER COLUMN `{column_name}` COMMENT "{comment}";"""
                else:
                    raise ValueError(
                        f"Unsupported Databricks runtime version: {dbr_number}"
                    )
            except ValueError:
                ddl_statement = f"""COMMENT ON COLUMN {full_table_name}.`{column_name}` IS "{comment}";"""
        return ddl_statement

    return column_comment_ddl


def _create_table_pi_information_ddl_func(config: MetadataConfig) -> Callable[[str, str, str], str]:
    """
    Create a function that generates table-level PI classification tags.
    
    Args:
        config: MetadataConfig with tag names
        
    Returns:
        Function that takes (table_name, classification, pi_type) and returns DDL string
    """
    pi_class_tag = getattr(config, "pi_classification_tag_name", "data_classification")
    pi_subclass_tag = getattr(
        config, "pi_subclassification_tag_name", "data_subclassification"
    )

    def table_pi_information_ddl(
        table_name: str, classification: str, pi_type: str
    ) -> str:
        return f"ALTER TABLE {table_name} SET TAGS ('{pi_class_tag}' = '{classification}', '{pi_subclass_tag}' = '{pi_type}');"

    return table_pi_information_ddl


def _create_pi_information_ddl_func(config: MetadataConfig) -> Callable[[str, str, str, str], str]:
    """
    Create a function that generates column-level PI classification tags.
    
    Args:
        config: MetadataConfig with tag names
        
    Returns:
        Function that takes (table_name, column_name, classification, pi_type) and returns DDL string
    """
    pi_class_tag = getattr(config, "pi_classification_tag_name", "data_classification")
    pi_subclass_tag = getattr(
        config, "pi_subclassification_tag_name", "data_subclassification"
    )

    def pi_information_ddl(
        table_name: str, column_name: str, classification: str, pi_type: str
    ) -> str:
        return f"ALTER TABLE {table_name} ALTER COLUMN `{column_name}` SET TAGS ('{pi_class_tag}' = '{classification}', '{pi_subclass_tag}' = '{pi_type}');"

    return pi_information_ddl

