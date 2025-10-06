"""
User utility functions for dbxmetagen.

This module contains utility functions for handling user identifiers,
separated to avoid circular imports.
"""

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
    Sanitizes user identifier - handles both emails and service principal names.

    Replaces all special characters (@, ., -) with underscores for consistent naming.

    Args:
        identifier (str): Email address or service principal name to sanitize

    Returns:
        str: Sanitized identifier safe for file/table names

    Examples:
        sanitize_user_identifier("user@example.com") -> "user_example_com"
        sanitize_user_identifier("12345678-1234-1234-1234-123456789abc") -> "12345678_1234_1234_1234_123456789abc"
    """
    return identifier.replace("@", "_").replace(".", "_").replace("-", "_")


def sanitize_email(email: str) -> str:
    """
    DEPRECATED: Use sanitize_user_identifier instead.
    Backward compatibility wrapper for sanitize_user_identifier.
    """
    return sanitize_user_identifier(email)
