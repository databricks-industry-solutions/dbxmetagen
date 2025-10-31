"""
Utility functions for PHI/PII detection and matching.
"""

from typing import Tuple
from rapidfuzz import fuzz


def is_fuzzy_match(str1: str, str2: str, threshold: int = 50) -> bool:
    """
    Check if two strings are similar using fuzzy matching.
    
    Uses token set ratio similarity score to handle word order variations
    and partial matches.
    
    Args:
        str1: First string to compare
        str2: Second string to compare
        threshold: Minimum similarity score (0-100) to consider a match
        
    Returns:
        True if the token set ratio similarity score is >= threshold
        
    Examples:
        >>> is_fuzzy_match("John Smith", "Smith John", threshold=80)
        True
        >>> is_fuzzy_match("Alice", "Bob")
        False
    """
    if not str1 or not str2:
        return False
    
    score = fuzz.token_set_ratio(str1, str2)
    return score >= threshold


def is_overlap(
    start1: int, 
    end1: int, 
    start2: int, 
    end2: int, 
    tolerance: int = 0
) -> bool:
    """
    Check if two intervals overlap.
    
    Args:
        start1: Start position of first interval
        end1: End position of first interval
        start2: Start position of second interval
        end2: End position of second interval
        tolerance: Optional tolerance for near-misses (default: 0)
        
    Returns:
        True if intervals [start1, end1] and [start2, end2] overlap
        
    Examples:
        >>> is_overlap(0, 5, 3, 8)
        True
        >>> is_overlap(0, 5, 6, 10)
        False
        >>> is_overlap(0, 5, 6, 10, tolerance=1)
        True
    """
    return max(start1, start2) <= min(end1, end2) + tolerance


def calculate_overlap(start1: int, end1: int, start2: int, end2: int) -> int:
    """
    Calculate the length of overlap between two intervals.
    
    Args:
        start1: Start position of first interval
        end1: End position of first interval
        start2: Start position of second interval
        end2: End position of second interval
        
    Returns:
        Length of the overlap (0 if no overlap)
        
    Examples:
        >>> calculate_overlap(0, 5, 3, 8)
        2
        >>> calculate_overlap(0, 5, 6, 10)
        -1
    """
    return min(end1, end2) - max(start1, start2)


def calculate_string_overlap(s1: str, s2: str) -> float:
    """
    Calculate the normalized overlap between two strings.
    
    Finds the maximum suffix-prefix overlap between the strings and
    normalizes by the length of the shorter string.
    
    Args:
        s1: First string
        s2: Second string
        
    Returns:
        Normalized overlap ratio (0.0 to 1.0)
        
    Examples:
        >>> calculate_string_overlap("hello", "lowing")
        0.4  # "lo" overlap / 5 chars
    """
    if not s1 or not s2:
        return 0.0
    
    max_overlap = min(len(s1), len(s2))
    for i in range(max_overlap, 0, -1):
        if s1[-i:] == s2[:i]:
            return i / max_overlap
    return 0.0

