"""Tests for URI-style primary key inference in ontology_roles."""

from dbxmetagen.ontology_roles import infer_role_from_column_name


class TestUriPrimaryKeyInference:
    def test_disease_uri_on_disease_entity(self):
        role = infer_role_from_column_name(
            "disease_uri", "Disease", frozenset(["Disease", "Patient"])
        )
        assert role == "primary_key"

    def test_page_uri_not_pk_without_entity_match(self):
        role = infer_role_from_column_name(
            "page_uri", "Patient", frozenset(["Patient", "Order"])
        )
        assert role != "primary_key"
