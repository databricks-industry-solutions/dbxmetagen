"""
User utility functions for dbxmetagen.

This module contains utility functions for handling user identifiers,
separated to avoid circular imports.
"""

import re
from pyspark.sql import SparkSession


def get_current_user() -> str:
    """
    Retrieves the current user.

    Returns:
        str: The current user.
    """
    spark = SparkSession.builder.getOrCreate()
    # Use first() instead of collect() for single values - more efficient
    return spark.sql("SELECT current_user()").first()[0]


def sanitize_user_identifier(identifier: str) -> str:
    """
    Sanitizes user identifier for use in table/file names.

    Replaces all non-alphanumeric characters (except underscore) with underscores.
    Safe for both email addresses and service principal IDs.

    Args:
        identifier (str): Email address or service principal name to sanitize

    Returns:
        str: Sanitized identifier safe for file/table names

    Raises:
        ValueError: If identifier is empty or whitespace-only

    Examples:
        sanitize_user_identifier("user@example.com") -> "user_example_com"
        sanitize_user_identifier("first.last+tag@domain.com") -> "first_last_tag_domain_com"
        sanitize_user_identifier("12345678-1234-1234-1234-123456789abc") -> "12345678_1234_1234_1234_123456789abc"
    """
    if not identifier or not identifier.strip():
        raise ValueError(
            "User identifier cannot be empty. "
            "This is typically populated from the current user or job context."
        )

    # Replace all non-alphanumeric characters (except underscore) with underscore
    # This handles @, ., -, +, #, !, and any other special characters
    return re.sub(r"[^a-zA-Z0-9_]", "_", identifier)


def sanitize_email(email: str) -> str:
    """
    DEPRECATED: Use sanitize_user_identifier instead.
    Backward compatibility wrapper for sanitize_user_identifier.
    """
    return sanitize_user_identifier(email)
