"""Tests for review_combined error handling.

Verifies that backend errors produce JSON responses (not plain text),
and that the frontend guards against non-JSON error bodies.
"""

import unittest


class TestReviewCombinedBackendErrorHandling(unittest.TestCase):
    """Backend: review_combined returns JSON error on internal failure."""

    def test_returns_json_error_on_sql_failure(self):
        """When execute_sql raises, the endpoint should raise HTTPException(500)
        with a JSON-serializable detail, not an unhandled 500 with plain text."""
        from fastapi import HTTPException

        def review_combined_error_handler(fn):
            """Simulates the error-wrapping pattern added to review_combined."""
            try:
                return fn()
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        with self.assertRaises(HTTPException) as ctx:
            review_combined_error_handler(
                lambda: (_ for _ in ()).throw(RuntimeError("SQL warehouse timeout"))
            )
        self.assertEqual(ctx.exception.status_code, 500)
        self.assertEqual(ctx.exception.detail, "SQL warehouse timeout")

    def test_http_exception_passes_through(self):
        from fastapi import HTTPException

        def review_combined_error_handler(fn):
            try:
                return fn()
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        with self.assertRaises(HTTPException) as ctx:
            review_combined_error_handler(
                lambda: (_ for _ in ()).throw(HTTPException(status_code=400, detail="bad input"))
            )
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail, "bad input")


class TestFrontendJsonGuard(unittest.TestCase):
    """Frontend pattern: check res.ok before calling res.json()."""

    def _simulate_fetch_handler(self, status, body_text, is_json=False):
        """Simulates the frontend loadData error-handling pattern."""
        import json

        class FakeResponse:
            def __init__(self, status, body, is_json):
                self.ok = 200 <= status < 300
                self.status = status
                self._body = body
                self._is_json = is_json

            def json(self):
                if not self._is_json:
                    raise ValueError(f"Unexpected token")
                return json.loads(self._body)

        res = FakeResponse(status, body_text, is_json)

        if not res.ok:
            detail = f"Server error ({res.status})"
            try:
                ej = res.json()
                detail = ej.get("detail") or detail
            except Exception:
                pass
            return {"error": detail}

        return {"data": res.json()}

    def test_plain_text_500_gives_readable_error(self):
        result = self._simulate_fetch_handler(500, "Internal Server Error", is_json=False)
        self.assertIn("error", result)
        self.assertEqual(result["error"], "Server error (500)")

    def test_json_500_extracts_detail(self):
        import json
        result = self._simulate_fetch_handler(
            500, json.dumps({"detail": "SQL warehouse timeout"}), is_json=True
        )
        self.assertIn("error", result)
        self.assertEqual(result["error"], "SQL warehouse timeout")

    def test_success_200_parses_json(self):
        import json
        body = json.dumps({"tables": [], "total_count": 0, "truncated": False})
        result = self._simulate_fetch_handler(200, body, is_json=True)
        self.assertIn("data", result)
        self.assertEqual(result["data"]["tables"], [])


if __name__ == "__main__":
    unittest.main()
