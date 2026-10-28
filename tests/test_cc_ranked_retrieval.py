"""Unit tests for the opt-in 'ranked' customer-context retrieval mode (lexical relevance
within the scope filter). Pure Python, no SQL/Spark."""

from dbxmetagen.customer_context import (
    _lexical_score,
    _select_ranked,
    resolve_customer_context,
    resolve_customer_context_with_report,
)


def _row(scope, scope_type, text, priority=0, label=""):
    return {"scope": scope, "scope_type": scope_type, "context_text": text,
            "context_label": label, "priority": priority}


class TestLexicalScore:
    def test_overlap_scores_higher(self):
        entry = _row("s", "schema", "trade blotter notional execution fills")
        hi = _lexical_score({"trade", "notional", "blotter"}, entry)
        lo = _lexical_score({"patient", "diagnosis"}, entry)
        assert hi > lo >= 0.0

    def test_empty_signature_is_zero(self):
        assert _lexical_score(set(), _row("s", "schema", "anything")) == 0.0


class TestSelectRanked:
    def _many(self):
        # 1 catalog directive + 4 schema entries, only some relevant to a "trades" table.
        return [
            _row("cat", "catalog", "Global: expand all domain acronyms."),
            _row("cat.sch", "schema", "trades blotter notional execution venue fills"),
            _row("cat.sch", "schema", "trade settlement clearing counterparty"),
            _row("cat.sch", "schema", "patient encounter diagnosis medication"),
            _row("cat.sch", "schema", "employee payroll benefits hr"),
        ]

    def test_keeps_catalog_and_topk_relevant(self):
        kept = _select_ranked(self._many(), "trades_blank trade_id notional ndc", top_k=2)
        texts = [k["context_text"] for k in kept]
        # catalog directive always retained, plus the 2 most trade-relevant schema entries
        assert "Global: expand all domain acronyms." in texts
        assert any("blotter notional" in t for t in texts)
        assert any("settlement clearing" in t for t in texts)
        assert not any("patient" in t for t in texts)      # irrelevant dropped
        assert not any("payroll" in t for t in texts)

    def test_noop_when_within_topk(self):
        rows = self._many()[:2]
        assert _select_ranked(rows, "trades", top_k=8) == rows

    def test_noop_when_no_signature(self):
        rows = self._many()
        assert _select_ranked(rows, "", top_k=2) == rows


class TestResolveRankedMode:
    def _cache(self):
        return [
            _row("cat.sch", "schema", "trades blotter notional execution"),
            _row("cat.sch", "schema", "patient encounter diagnosis"),
            _row("cat.sch", "schema", "employee payroll benefits"),
        ]

    def test_exact_mode_keeps_all(self):
        txt, _, _ = resolve_customer_context_with_report(
            self._cache(), "cat.sch.trades_blank", mode="exact")
        assert "patient" in txt and "payroll" in txt and "blotter" in txt

    def test_ranked_mode_thins_to_relevant(self):
        txt = resolve_customer_context(
            self._cache(), "cat.sch.trades_blank",
            mode="ranked", signature="trades_blank trade_id notional", top_k=1)
        assert "blotter notional" in txt
        assert "patient" not in txt and "payroll" not in txt

    def test_ranked_default_signature_empty_is_noop(self):
        # mode=ranked but no signature -> behaves like exact (keeps all)
        txt = resolve_customer_context(
            self._cache(), "cat.sch.trades_blank", mode="ranked", signature="", top_k=1)
        assert "patient" in txt and "payroll" in txt
