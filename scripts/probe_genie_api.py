#!/usr/bin/env python
"""READ-ONLY probe of the Databricks Genie API surface.

Purpose: before building the "pull Genie serialization" feature (backlog items
14/15), we must know whether example SQL + benchmarks / curated questions from an
EXISTING Genie space are available in a machine-readable form. The typed SDK
`GenieSpace` only exposes space_id/title/description/warehouse_id — so the useful
content, IF it exists, must come from a lower-level REST endpoint. This script
finds out, without writing anything.

Run against a workspace that HAS at least one real Genie space with example SQL
and/or benchmark questions configured:

    # uses your current Databricks CLI profile / env auth
    uv run python scripts/probe_genie_api.py
    # or target a specific space:
    uv run python scripts/probe_genie_api.py --space-id <id>
    # or a specific CLI profile:
    uv run python scripts/probe_genie_api.py --profile DMVM

It prints: the spaces it can list, and for one space, the raw JSON from several
candidate REST endpoints so we can see where (if anywhere) example_sql /
curated_questions / benchmarks live. NOTHING is created, updated, or deleted.
Paste the output back and we'll design the puller against reality.
"""
import argparse
import json
import sys


def _redact(obj):
    """Best-effort: keep structure/keys, truncate long string values so pasted
    output doesn't leak large SQL bodies or sensitive literals."""
    if isinstance(obj, dict):
        return {k: _redact(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_redact(v) for v in obj[:5]] + (["…(+%d more)" % (len(obj) - 5)] if len(obj) > 5 else [])
    if isinstance(obj, str) and len(obj) > 200:
        return obj[:200] + f"…(+{len(obj) - 200} chars)"
    return obj


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default=None, help="Databricks CLI profile")
    ap.add_argument("--space-id", default=None, help="Probe this space (else the first listed)")
    args = ap.parse_args()

    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    print(f"# Workspace: {ws.config.host}\n")

    # 1. List spaces via the typed SDK.
    print("## 1. genie.list_spaces() (typed SDK)")
    space_id = args.space_id
    try:
        resp = ws.genie.list_spaces()
        spaces = list(getattr(resp, "spaces", None) or [])
        for s in spaces[:20]:
            print(f"  - {getattr(s, 'space_id', '?')}  title={getattr(s, 'title', '?')!r}")
        if not space_id and spaces:
            space_id = spaces[0].space_id
        print(f"  ({len(spaces)} space(s) listed)")
    except Exception as e:
        print(f"  list_spaces failed: {e}")

    if not space_id:
        print("\nNo space to probe (pass --space-id). Stopping.")
        return

    print(f"\n## 2. genie.get_space({space_id!r}) (typed SDK — expected: thin, no example SQL)")
    try:
        sp = ws.genie.get_space(space_id)
        print("  fields:", {k: _redact(v) for k, v in vars(sp).items()} if hasattr(sp, "__dict__") else sp)
    except Exception as e:
        print(f"  get_space failed: {e}")

    # 3. Raw REST probes. CONFIRMED (probed live against DMVM 2026-07): the example
    #    SQL + benchmarks are NOT on /genie/spaces/{id} (thin), but on the legacy
    #    data-rooms endpoints. curated-questions with question_type BENCHMARK /
    #    BENCHMARK_SUGGESTION carry answer_text = the curated example SQL.
    #    ws.api_client.do() does authenticated REST.
    candidate_paths = [
        f"/api/2.0/genie/spaces/{space_id}",                    # thin: id/title/description/warehouse_id
        f"/api/2.0/data-rooms/{space_id}",                      # + table_identifiers, suggestion_description
        f"/api/2.0/data-rooms/{space_id}/curated-questions",    # <- the gold: question_text + answer_text (SQL)
    ]
    print("\n## 3. Raw REST probes (looking for serialized_space / example_sql / benchmarks)")
    for path in candidate_paths:
        print(f"\n### GET {path}")
        try:
            out = ws.api_client.do("GET", path)
            keys = list(out.keys()) if isinstance(out, dict) else type(out).__name__
            print("  top-level keys:", keys)
            # Highlight the fields we care about if present.
            if isinstance(out, dict):
                for interesting in ("serialized_space", "example_sql", "sql_snippets",
                                    "curated_questions", "benchmarks", "instructions",
                                    "sample_questions", "table_identifiers"):
                    if interesting in out:
                        print(f"  >>> FOUND {interesting!r}:")
                        print("     ", json.dumps(_redact(out[interesting]))[:1000])
                # curated-questions: summarize the answer_text (SQL) carriers
                if "curated_questions" in out and isinstance(out["curated_questions"], list):
                    from collections import Counter
                    qs = out["curated_questions"]
                    types = Counter(q.get("question_type") for q in qs)
                    with_sql = sum(1 for q in qs if (q.get("answer_text") or "").strip())
                    print(f"  >>> curated_questions: {len(qs)} total, types={dict(types)}, "
                          f"{with_sql} with answer_text (example SQL)")
        except Exception as e:
            print(f"  -> {type(e).__name__}: {str(e)[:200]}")

    print("\n# Done. Paste this output back so we can design the puller against the real API.")


if __name__ == "__main__":
    sys.exit(main())
