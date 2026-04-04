# Handoff: AgentBench (parallel_workers) alignment and deferred dbxmetagen work

This note bridges **Field Engineering / AgentBench** work in `hackathons/parallel_workers` with **dbxmetagen** so roadmap items stay in the right repo.

## Two product surfaces (do not conflate)

| Stream | Where | What |
|--------|--------|------|
| **Library / jobs** | `examples/integration/`, `src/dbxmetagen/genie/` | Notebooks/DAB: metadata through semantic layer, then `build_genie_space` → Genie REST API + UC Volume export. **No** dbxmetagen FastAPI app required. |
| **Dashboard app** | `apps/dbxmetagen-app/app/agent/` | Agent Chat, Deep Analysis, metadata LangGraph ReAct. **Deep analysis** is **not** LangGraph (deterministic gather + one analysis LLM; see `deep_analysis.py` docstring). |

Genie space generation code: `src/dbxmetagen/genie/__init__.py` (`build_genie_space` → `assemble_genie_context`, `run_genie_agent`, `build_serialized_space`).

## Deferred implementation ideas (dbxmetagen repo)

1. **Genie E2E on test tables** — Follow `examples/integration/README.md` through `05_create_genie_space.py`; DAB snippet in that README. Requires valid `sql_warehouse_id` in the target workspace.
2. **MLflow around `build_genie_space`** — Today inner Genie LLM steps may use `@mlflow.trace` in `genie/agent.py`; add experiment/run + parent trace + log `serialized_space` artifact at `build_genie_space` or notebook 05 if you need full tracking.
3. **Custom “skill” alongside retrieval (dashboard only)** — If extending Deep Analysis, add a **plain callable** in phase-1 gather (`_gather_graphrag` / `_gather_baseline`), not a second LLM planner; `_plan_retrieval` already plans graph edges. Optional LangChain `@tool` on `metadata_agent.py` for the same skill. See `graph_skill.py` vs dynamic guidance.
4. **dbx-unifiedchat** — Optional: enriched Genie docs chunks pipeline for MAS; not required for core `build_genie_space`.

Related internal roadmaps: `docs/GENIE_*`, `docs/CONSOLIDATED_ROADMAP.md`.

## Local-only notes

Use `*.local.md` (gitignored) for scratch; keep this file as the committed handoff.
