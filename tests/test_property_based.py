"""Property-based tests for Pydantic validators using hypothesis.

These tests generate random inputs and assert validators never crash with
an unhandled exception. Pydantic ValidationError and ValueError are acceptable
rejections -- unhandled TypeError, AttributeError, KeyError etc. are bugs.
"""

import pytest
from hypothesis import given, strategies as st, settings, HealthCheck
from pydantic import ValidationError


# ---------------------------------------------------------------------------
# CommentResponse: column_contents validator must never crash
# ---------------------------------------------------------------------------

from dbxmetagen.metadata_generator import CommentResponse

# Strategy: column_contents can be None, str, list[str], list[list[str]],
# list[None], int, dict, or deeply nested structures.
_column_contents_st = st.one_of(
    st.none(),
    st.text(max_size=200),
    st.lists(st.text(max_size=50), max_size=10),
    st.lists(st.lists(st.text(max_size=30), max_size=5), max_size=5),
    st.lists(st.none(), max_size=3),
    st.integers(),
    st.dictionaries(st.text(max_size=10), st.text(max_size=10), max_size=3),
)


class TestCommentResponsePropertyBased:
    @given(v=_column_contents_st)
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_validator_never_crashes(self, v):
        """The validator may reject input (ValidationError) but must not crash."""
        try:
            CommentResponse.model_validate({
                "table": "t", "columns": ["c"], "column_contents": v,
            })
        except (ValueError, ValidationError):
            pass

    @given(v=st.text(max_size=500))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_string_input_always_produces_list(self, v):
        """Any string input should be coerced to a list, never crash."""
        try:
            r = CommentResponse.model_validate({
                "table": "t", "columns": ["c"], "column_contents": v,
            })
            assert isinstance(r.column_contents, list)
        except (ValueError, ValidationError):
            pass

    @given(v=st.lists(st.text(max_size=50), min_size=1, max_size=20))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_list_input_always_produces_list(self, v):
        """Any list-of-strings input should remain a list."""
        r = CommentResponse.model_validate({
            "table": "t", "columns": ["c"], "column_contents": v,
        })
        assert isinstance(r.column_contents, list)


# ---------------------------------------------------------------------------
# PIColumnContent: normalize_classification must never crash
# ---------------------------------------------------------------------------

from dbxmetagen.metadata_generator import PIColumnContent

_VALID_PI = {"pi", "phi", "pci", "medical_information", "None"}


class TestPIClassificationPropertyBased:
    @given(v=st.text(max_size=100))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_classification_always_valid(self, v):
        """Any string classification should normalize to a valid value."""
        try:
            r = PIColumnContent.model_validate({
                "column_name": "col", "classification": v,
                "reason": "test", "type": "string", "confidence": 0.9,
            })
            assert r.classification in _VALID_PI, (
                f"Got unexpected classification: {r.classification!r}"
            )
        except (ValueError, ValidationError):
            pass

    @given(v=st.one_of(st.none(), st.integers(), st.floats(), st.booleans()))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_non_string_classification_handled(self, v):
        """Non-string classification should either normalize or raise ValidationError."""
        try:
            r = PIColumnContent.model_validate({
                "column_name": "col", "classification": v,
                "reason": "test", "type": "string", "confidence": 0.9,
            })
            assert r.classification in _VALID_PI
        except (ValueError, ValidationError):
            pass


# ---------------------------------------------------------------------------
# _enforce_value: property-based invariants
# ---------------------------------------------------------------------------

from dbxmetagen.domain_classifier import _enforce_value


class TestEnforceValuePropertyBased:
    @given(
        predicted=st.text(max_size=50),
        allowed=st.lists(st.text(min_size=1, max_size=20), min_size=1, max_size=10),
    )
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_always_returns_tuple(self, predicted, allowed):
        result = _enforce_value(predicted, allowed)
        assert isinstance(result, tuple) and len(result) == 2

    @given(
        predicted=st.text(max_size=50),
        allowed=st.lists(st.text(min_size=1, max_size=20), min_size=1, max_size=10),
    )
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_result_in_allowed_or_fallback(self, predicted, allowed):
        val, _ = _enforce_value(predicted, allowed, fallback="FALLBACK")
        assert val in allowed or val == "FALLBACK"

    @given(allowed=st.lists(
        st.from_regex(r"[a-zA-Z][a-zA-Z0-9_]{0,19}", fullmatch=True),
        min_size=1, max_size=10, unique_by=str.lower,
    ))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_exact_match_is_idempotent(self, allowed):
        """Passing an allowed value back in must return that value with exact=True.

        Allowed values must be unique when lowercased (mirrors real domain configs).
        """
        for a in allowed:
            val, exact = _enforce_value(a, allowed)
            assert val == a and exact is True
