"""
Unit tests for truncation and prompt size validation.

These tests verify:
1. Word-based truncation in truncate_value (prompts.py)
2. Binary truncation at source (processing.py)
3. max_prompt_length validation (metadata_generator.py)

Run with: pytest tests/test_truncation.py -v
"""

import pytest
import json
import base64
import pandas as pd


class TestTruncateValue:
    """Test the truncate_value function logic (word-based only)."""

    def truncate_value(self, value: str, word_limit: int) -> str:
        """Replicate the truncate_value logic for testing."""
        words = value.split()
        if len(words) > word_limit:
            return " ".join(words[:word_limit])
        return value

    def test_truncate_normal_text_under_limit(self):
        """Test that normal text under the word limit is not truncated."""
        text = "This is a short sentence"
        result = self.truncate_value(text, word_limit=10)
        assert result == text

    def test_truncate_normal_text_over_limit(self):
        """Test that normal text over the word limit is truncated by words."""
        text = "word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11"
        result = self.truncate_value(text, word_limit=5)
        assert result == "word1 word2 word3 word4 word5"

    def test_truncate_empty_string(self):
        """Test that empty strings are handled correctly."""
        result = self.truncate_value("", word_limit=100)
        assert result == ""

    def test_truncate_single_word_not_affected(self):
        """Test that single words (like URLs) are NOT truncated by word-based logic."""
        # URLs and other single-word strings should pass through unchanged
        long_url = (
            "https://example.com/very/long/path/to/resource?param=value&other=stuff"
        )
        result = self.truncate_value(long_url, word_limit=100)
        assert (
            result == long_url
        )  # Unchanged - word-based truncation doesn't affect single words


class TestBinaryTruncationAtSource:
    """Test that binary data is truncated at the source (SQL/Spark level)."""

    def test_binary_truncation_sql_expression(self):
        """Verify the SQL expression correctly truncates base64."""
        # This tests the logic that would be applied by:
        # substr(base64(`col_name`), 1, 50)
        large_binary = b"A" * 1000
        full_base64 = base64.b64encode(large_binary).decode("utf-8")

        # Simulate SQL substr(base64(...), 1, 50)
        truncated = full_base64[:50]

        assert len(full_base64) > 1000  # Original is large
        assert len(truncated) == 50  # Truncated to 50 chars
        # Truncated value is enough to identify as base64
        assert truncated.isalnum() or "+" in truncated or "/" in truncated

    def test_small_binary_not_affected(self):
        """Verify small binary data stays under the limit."""
        small_binary = b"Hello"
        base64_str = base64.b64encode(small_binary).decode("utf-8")

        # Small base64 is under 50 chars, so substr has no effect
        truncated = base64_str[:50]
        assert truncated == base64_str

    def test_truncated_base64_is_identifiable(self):
        """Verify that 50 chars of base64 is enough for LLM identification."""
        # Different binary content all produce identifiable base64 patterns
        # All test cases must be large enough to produce >50 chars of base64
        test_cases = [
            b"\x00" * 100,  # Null bytes
            b"\xff" * 100,  # Max bytes
            b"PDF-1.4 " * 20,  # PDF-like content repeated
            bytes(range(256)) * 4,  # All byte values
        ]

        for binary_data in test_cases:
            full_base64 = base64.b64encode(binary_data).decode("utf-8")
            truncated = full_base64[:50]

            # 50 chars is enough to see the base64 pattern
            assert len(truncated) == 50, f"Base64 too short: {len(full_base64)} chars"
            # All chars should be valid base64 characters
            valid_chars = set(
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/="
            )
            assert all(c in valid_chars for c in truncated)


class TestColumnContentsValidator:
    """Test the CommentResponse column_contents validator."""

    def validate_column_contents(self, v):
        """Replicate the validator logic for testing."""
        
        def parse_if_stringified_array(item):
            """Parse a string if it looks like a stringified JSON array."""
            if isinstance(item, str):
                stripped = item.strip()
                if stripped.startswith("[") and stripped.endswith("]"):
                    try:
                        parsed = json.loads(stripped)
                        if isinstance(parsed, list) and len(parsed) == 1:
                            return str(parsed[0]) if not isinstance(parsed[0], str) else parsed[0]
                        elif isinstance(parsed, list):
                            return str(parsed[0]) if not isinstance(parsed[0], str) else parsed[0]
                    except json.JSONDecodeError:
                        pass
            return str(item) if not isinstance(item, str) else item
        
        if isinstance(v, str):
            stripped = v.strip()
            if stripped.startswith('[') and stripped.endswith(']'):
                try:
                    parsed = json.loads(stripped)
                    if isinstance(parsed, list):
                        return [parse_if_stringified_array(item) for item in parsed]
                except json.JSONDecodeError:
                    pass
            return [v]
        elif isinstance(v, list):
            if len(v) == 1 and isinstance(v[0], list):
                return [parse_if_stringified_array(item) for item in v[0]]
            return [parse_if_stringified_array(item) for item in v]
        else:
            raise ValueError("column_contents must be either a string or a list of strings")

    def test_normal_list_unchanged(self):
        """Test that a normal list of strings is returned as-is."""
        result = self.validate_column_contents(["desc1", "desc2", "desc3"])
        assert result == ["desc1", "desc2", "desc3"]

    def test_stringified_array_is_parsed(self):
        """Test that a stringified JSON array is correctly parsed."""
        # This is what the LLM sometimes returns incorrectly
        stringified = '["Description for column 1", "Description for column 2"]'
        result = self.validate_column_contents(stringified)
        assert result == ["Description for column 1", "Description for column 2"]

    def test_stringified_array_with_whitespace(self):
        """Test parsing with leading/trailing whitespace."""
        stringified = '  ["desc1", "desc2"]  '
        result = self.validate_column_contents(stringified)
        assert result == ["desc1", "desc2"]

    def test_regular_string_wrapped_in_list(self):
        """Test that a regular string (not JSON array) is wrapped in a list."""
        result = self.validate_column_contents("Just a regular description")
        assert result == ["Just a regular description"]

    def test_invalid_json_treated_as_string(self):
        """Test that invalid JSON starting with [ is treated as regular string."""
        invalid = "[This is not valid JSON"
        result = self.validate_column_contents(invalid)
        assert result == ["[This is not valid JSON"]

    def test_nested_list_flattened(self):
        """Test that [[desc1, desc2]] is flattened to [desc1, desc2]."""
        result = self.validate_column_contents([["desc1", "desc2"]])
        assert result == ["desc1", "desc2"]

    def test_stringified_single_element_array(self):
        """Test parsing a stringified single-element array."""
        stringified = '["Single column description"]'
        result = self.validate_column_contents(stringified)
        assert result == ["Single column description"]

    def test_list_containing_stringified_array(self):
        """Test that stringified arrays inside list elements are parsed."""
        # LLM sometimes returns: {"column_contents": ["[\"actual description\"]"]}
        result = self.validate_column_contents(['["The actual description here"]'])
        assert result == ["The actual description here"]

    def test_list_with_mixed_stringified_and_normal(self):
        """Test list with both normal strings and stringified arrays."""
        result = self.validate_column_contents(['["stringified desc"]', 'normal desc'])
        assert result == ["stringified desc", "normal desc"]


class TestMaxPromptLengthValidation:
    """Test the max_prompt_length validation in metadata generators."""

    def check_prompt_size(self, prompt: dict, max_prompt_length: int) -> bool:
        """Replicate the validation logic for testing."""
        prompt_size = len(json.dumps(prompt))
        return prompt_size <= max_prompt_length * 5

    def test_small_prompt_passes(self):
        """Test that a small prompt passes validation."""
        prompt = {"comment": [{"role": "user", "content": "short message"}]}
        assert self.check_prompt_size(prompt, max_prompt_length=4096)

    def test_large_prompt_fails(self):
        """Test that a very large prompt fails validation."""
        # Create a prompt that exceeds max_prompt_length * 5 characters
        large_content = "x" * 25000  # > 4096 * 5 = 20480
        prompt = {"comment": [{"role": "user", "content": large_content}]}
        assert not self.check_prompt_size(prompt, max_prompt_length=4096)

    def test_prompt_with_truncated_base64_passes(self):
        """Test that prompts with truncated base64 (50 chars) easily pass."""
        # After binary truncation at source, base64 is only 50 chars
        truncated_base64 = "A" * 50  # Simulates truncated base64
        prompt = {
            "comment": [
                {"role": "user", "content": f"Binary column data: {truncated_base64}"}
            ]
        }
        assert self.check_prompt_size(prompt, max_prompt_length=4096)

    def test_old_broken_check_would_pass_incorrectly(self):
        """Demonstrate that the old len(prompt) check was broken."""
        # Old check: len(prompt) > max_prompt_length
        # This would check dict key count (always 1), not content size
        large_content = "x" * 100000
        prompt = {"comment": [{"role": "user", "content": large_content}]}

        # Old broken check would pass (len(prompt) == 1)
        assert len(prompt) == 1
        assert len(prompt) <= 4096  # Old check passes incorrectly!

        # New check correctly fails
        assert not self.check_prompt_size(prompt, max_prompt_length=4096)


class TestCalculateCellLengthIntegration:
    """Integration tests for the calculate_cell_length flow (word-based only)."""

    def calculate_cell_length(
        self, pandas_df: pd.DataFrame, word_limit: int
    ) -> pd.DataFrame:
        """Replicate the calculate_cell_length logic for testing."""

        def truncate_value(value: str, word_limit: int) -> str:
            words = value.split()
            if len(words) > word_limit:
                return " ".join(words[:word_limit])
            return value

        for column in pandas_df.columns:
            pandas_df[column] = pandas_df[column].astype(str)
            pandas_df[column] = pandas_df[column].apply(
                lambda x: truncate_value(x, word_limit)
            )
        return pandas_df

    def test_dataframe_with_text_content(self):
        """Test word-based truncation on a DataFrame with text content."""
        df = pd.DataFrame(
            {
                "text_col": ["short text", "word " * 200],  # One short, one long
                "numeric_col": [123, 456789],
            }
        )

        result = self.calculate_cell_length(df, word_limit=100)

        # Short text unchanged
        assert result["text_col"].iloc[0] == "short text"
        # Long text truncated to 100 words
        assert len(result["text_col"].iloc[1].split()) == 100

    def test_single_word_values_unchanged(self):
        """Test that single-word values (URLs, IDs) pass through unchanged."""
        df = pd.DataFrame(
            {
                "url": ["https://example.com/path?query=value"],
                "uuid": ["550e8400e29b41d4a716446655440000"],
                "hash": ["a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6"],
            }
        )

        original = df.copy()
        result = self.calculate_cell_length(df, word_limit=100)

        # All single-word values should be unchanged
        for col in result.columns:
            assert result[col].iloc[0] == original[col].iloc[0]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
