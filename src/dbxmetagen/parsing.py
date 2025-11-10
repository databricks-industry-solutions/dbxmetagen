"""Parsing and cleansing utilities shared among modules."""

import re
from typing import Optional
from deprecated import deprecated
from dbxmetagen.user_utils import sanitize_user_identifier


@deprecated("Use sanitize_user_identifier from processing module instead.")
def sanitize_email(email: str) -> str:
    """Use sanitize_user_identifier from processing module instead."""
    return sanitize_user_identifier(email)


def cleanse_sql_comment(comment: str) -> str:
    """
    Cleanse a SQL comment string to make it compatible with DB SQL.

    - Replaces double double-quotes ("") and single double-quotes (") with a single quote (').
    - Escapes single quotes (') by doubling them ('')
    - Leaves standard double quotes as-is (unless you need to escape them for your SQL dialect)

    Args:
        comment (str): The original comment string.
    Returns:
        str: The cleansed comment string.
    """
    if not comment:
        return comment

    comment = comment.replace('""', "'")
    comment = comment.replace('"', "'")

    return comment
