"""Unit tests for dbxmetagen.fk_eval (pure Python scoring of FK predictions)."""

from dbxmetagen.fk_eval import format_report, pooled, score_fk

CAT = "cat.uat_x"


def g(src, dst, fk=True):
    st, sc = src.split(".")
    dt, dc = dst.split(".")
    return {"src_table": st, "src_column": sc, "dst_table": dt, "dst_column": dc, "expected_is_fk": fk}


def p(src, dst, is_fk=True, conf=0.9):
    st, sc = src.split(".")
    dt, dc = dst.split(".")
    return {"src_table": f"{CAT}.{st}", "src_column": f"{CAT}.{st}.{sc}",
            "dst_table": f"{CAT}.{dt}", "dst_column": f"{CAT}.{dt}.{dc}",
            "is_fk": is_fk, "final_confidence": conf}


GOLD = [g("orders.customer_id", "customers.customer_id"),
        g("lines.order_id", "orders.order_id"),
        g("lines.order_id", "invoices.order_id", fk=False)]


def test_perfect_prediction():
    s = score_fk("x", GOLD, [p("orders.customer_id", "customers.customer_id"),
                             p("lines.order_id", "orders.order_id")])
    assert (s.tp, s.fn, s.fp, s.reversed) == (2, 0, 0, 0)
    assert s.precision == 1.0 and s.recall == 1.0 and s.direction_accuracy == 1.0


def test_fully_qualified_names_are_shortened_and_case_folded():
    pred = p("Orders.Customer_ID", "Customers.Customer_ID")
    s = score_fk("x", GOLD, [pred])
    assert s.tp == 1


def test_reversed_counts_as_found_but_hurts_direction():
    s = score_fk("x", GOLD, [p("customers.customer_id", "orders.customer_id"),
                             p("lines.order_id", "orders.order_id")])
    assert s.tp == 2 and s.reversed == 1 and s.direction_accuracy == 0.5
    assert s.recall == 1.0


def test_miss_trap_and_unlabeled_fp():
    s = score_fk("x", GOLD, [p("lines.order_id", "invoices.order_id"),
                             p("orders.status", "lines.status")])
    assert s.tp == 0 and s.fn == 2 and s.fp == 2 and s.trap_fp == 1
    assert s.precision == 0.0 and s.recall == 0.0


def test_ambiguous_pairs_excluded():
    s = score_fk("x", GOLD, [p("orders.status", "lines.status")],
                 ambiguous=[("orders.status", "lines.status")])
    assert s.fp == 0


def test_high_confidence_non_fk_rows_are_reported():
    s = score_fk("x", GOLD, [p("lines.order_id", "orders.order_id"),
                             p("lines.order_id", "invoices.order_id", is_fk=False, conf=0.88),
                             p("orders.status", "lines.status", is_fk=False, conf=0.4)])
    assert s.high_conf_non_fk == 1 and s.high_conf_pairs[0][2] == 0.88
    assert s.fp == 0  # is_fk=false never counts as a predicted FK


def test_empty_inputs():
    s = score_fk("x", [], [])
    assert s.precision is None and s.recall is None and s.direction_accuracy is None


def test_pooled_and_report():
    a = score_fk("a", GOLD, [p("lines.order_id", "orders.order_id")])
    b = score_fk("b", GOLD, [p("orders.customer_id", "customers.customer_id"),
                             p("lines.order_id", "orders.order_id")])
    tot = pooled([a, b])
    assert (tot.gold, tot.tp, tot.fn) == (4, 3, 1)
    rep = format_report([a, b])
    assert "ALL" in rep and "MISS  orders.customer_id -> customers.customer_id" in rep


def test_null_gold_label_means_ambiguous():
    gold = GOLD + [g("orders.status", "lines.status", fk=None)]
    s = score_fk("x", gold, [p("orders.status", "lines.status"),
                             p("lines.order_id", "orders.order_id")])
    assert s.fp == 0 and s.trap_fp == 0      # ambiguous prediction not penalized
    assert s.gold == 2 and s.tp == 1         # nor counted as a recall target
