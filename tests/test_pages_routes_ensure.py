import sys
sys.path.insert(0, "tests")
import test_api_routes  # noqa: F401  installs the app import stubs
sys.path.insert(0, "apps/dbxmetagen-app/app")
import pages_routes as pr

def test_ensure_tables_runs_ddl_once_per_schema(monkeypatch):
    calls = []
    monkeypatch.setattr(pr.us, "ensure_tables", lambda run_sql, fq: calls.append(fq(pr.us.DRAFTS)))
    monkeypatch.setattr(pr, "_TABLES_ENSURED", set())
    monkeypatch.setattr(pr, "_fq", lambda t: f"`c`.`s1`.`{t}`")
    pr._ensure_tables(); pr._ensure_tables()
    monkeypatch.setattr(pr, "_fq", lambda t: f"`c`.`s2`.`{t}`")
    pr._ensure_tables()
    assert calls == ["`c`.`s1`.`page_drafts`", "`c`.`s2`.`page_drafts`"]

def test_failed_ensure_is_retried(monkeypatch):
    n = {"calls": 0}
    def boom(run_sql, fq):
        n["calls"] += 1
        raise RuntimeError("warehouse down")
    monkeypatch.setattr(pr.us, "ensure_tables", boom)
    monkeypatch.setattr(pr, "_TABLES_ENSURED", set())
    for _ in range(2):
        try:
            pr._ensure_tables()
        except Exception:
            pass
    assert n["calls"] == 2
