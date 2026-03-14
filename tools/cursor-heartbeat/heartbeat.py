#!/usr/bin/env python3
"""
Cursor Heartbeat: Autonomous, scheduled code improvement engine.

Reads a plan file, analyzes the project with skill-augmented prompts and
Databricks MCP tools, then applies improvements on a branch.

Usage:
    python heartbeat.py                          # single run
    python heartbeat.py --daemon                 # loop on schedule
    python heartbeat.py --plan path/to/plan.yaml # custom plan
    python heartbeat.py --dry-run                # suggest but don't apply
"""

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import time
import uuid
from pathlib import Path

import yaml

try:
    import anthropic
except ImportError:
    anthropic = None

try:
    import openai
except ImportError:
    openai = None

try:
    from databricks.sdk import WorkspaceClient
except ImportError:
    WorkspaceClient = None

SCRIPT_DIR = Path(__file__).parent
DEFAULT_PLAN = SCRIPT_DIR / "heartbeat_plan.yaml"

# ---------------------------------------------------------------------------
# Skills loader
# ---------------------------------------------------------------------------

def resolve_skills_dir(plan: dict) -> Path:
    """Find the .cursor/skills directory relative to the project."""
    project_path = plan["project"]["path"]
    skills_dir = Path(project_path) / ".cursor" / "skills"
    if skills_dir.is_dir():
        return skills_dir
    return Path(os.path.expanduser("~")) / ".cursor" / "skills"


def load_skills(plan: dict) -> str:
    """Load SKILL.md files listed in the plan and return as a single context block."""
    skill_names = plan.get("skills", [])
    if not skill_names:
        return ""

    skills_dir = resolve_skills_dir(plan)
    loaded = []
    for name in skill_names:
        skill_path = skills_dir / name / "SKILL.md"
        if not skill_path.exists():
            print(f"  [skills] Skipping {name} (not found at {skill_path})")
            continue
        content = skill_path.read_text(errors="replace")
        if len(content) > 15_000:
            content = content[:15_000] + "\n... (truncated)"
        loaded.append(f"### Skill: {name}\n{content}")
        print(f"  [skills] Loaded {name} ({len(content)} chars)")

    if not loaded:
        return ""
    return (
        "\n\n## Domain Knowledge (from Cursor Skills)\n"
        "Use the following best-practice references when evaluating code.\n\n"
        + "\n\n".join(loaded)
    )


# ---------------------------------------------------------------------------
# Databricks MCP tool definitions (for Anthropic tool_use)
# ---------------------------------------------------------------------------

TOOL_DEFS = {
    "execute_sql": {
        "name": "execute_sql",
        "description": "Execute a SQL query on a Databricks SQL warehouse. Returns rows as list of dicts.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sql_query": {"type": "string", "description": "The SQL query to execute"},
                "catalog": {"type": "string", "description": "Optional catalog context"},
                "schema": {"type": "string", "description": "Optional schema context"},
            },
            "required": ["sql_query"],
        },
    },
    "get_table_schema": {
        "name": "get_table_schema",
        "description": "Get the schema (columns, types) of a Unity Catalog table. Use format: catalog.schema.table",
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Fully qualified table name (catalog.schema.table)"},
            },
            "required": ["table_name"],
        },
    },
    "list_tables": {
        "name": "list_tables",
        "description": "List tables in a Unity Catalog schema.",
        "input_schema": {
            "type": "object",
            "properties": {
                "catalog": {"type": "string", "description": "Catalog name"},
                "schema": {"type": "string", "description": "Schema name"},
            },
            "required": ["catalog", "schema"],
        },
    },
    "list_volumes": {
        "name": "list_volumes",
        "description": "List volumes in a Unity Catalog schema.",
        "input_schema": {
            "type": "object",
            "properties": {
                "catalog": {"type": "string", "description": "Catalog name"},
                "schema": {"type": "string", "description": "Schema name"},
            },
            "required": ["catalog", "schema"],
        },
    },
}


_READONLY_SQL = re.compile(
    r"^\s*(SELECT|DESCRIBE|DESC|SHOW|EXPLAIN|WITH)\b",
    re.IGNORECASE,
)

_cached_warehouse_id: str | None = None


def get_dbx_client() -> "WorkspaceClient | None":
    """Init Databricks client. Supports env vars, ~/.databrickscfg, or Azure/GCP identity."""
    if WorkspaceClient is None:
        return None
    try:
        client = WorkspaceClient()
        client.current_user.me()  # fast auth check
        return client
    except Exception as e:
        print(f"  [mcp] Could not init Databricks client: {e}")
        return None


def _validate_sql_readonly(sql: str):
    if not _READONLY_SQL.match(sql.strip()):
        raise ValueError(f"Only read-only SQL is allowed (SELECT/DESCRIBE/SHOW). Got: {sql[:80]}")


def execute_mcp_tool(client: "WorkspaceClient", tool_name: str, tool_input: dict) -> str:
    """Execute a Databricks MCP tool and return the result as a JSON string."""
    try:
        if tool_name == "execute_sql":
            _validate_sql_readonly(tool_input["sql_query"])
            from databricks.sdk.service.sql import StatementState
            resp = client.statement_execution.execute_statement(
                statement=tool_input["sql_query"],
                warehouse_id=_find_warehouse(client),
                catalog=tool_input.get("catalog"),
                schema=tool_input.get("schema"),
                wait_timeout="30s",
            )
            if resp.status and resp.status.state == StatementState.SUCCEEDED:
                cols = [c.name for c in (resp.manifest.schema.columns or [])]
                rows = []
                for chunk in (resp.result.data_array or []):
                    rows.append(dict(zip(cols, chunk)))
                return json.dumps(rows[:50], default=str)
            return json.dumps({"error": str(resp.status)})

        elif tool_name == "get_table_schema":
            parts = tool_input["table_name"].split(".")
            if len(parts) != 3:
                return json.dumps({"error": "Use catalog.schema.table format"})
            cols = client.tables.get(full_name=tool_input["table_name"]).columns
            schema = [{"name": c.name, "type": str(c.type_name), "comment": c.comment} for c in (cols or [])]
            return json.dumps(schema, default=str)

        elif tool_name == "list_tables":
            tables = client.tables.list(
                catalog_name=tool_input["catalog"],
                schema_name=tool_input["schema"],
            )
            return json.dumps([{"name": t.full_name, "type": str(t.table_type)} for t in tables][:100], default=str)

        elif tool_name == "list_volumes":
            volumes = client.volumes.list(
                catalog_name=tool_input["catalog"],
                schema_name=tool_input["schema"],
            )
            return json.dumps([{"name": v.full_name, "type": str(v.volume_type)} for v in volumes][:50], default=str)

        return json.dumps({"error": f"Unknown tool: {tool_name}"})
    except Exception as e:
        return json.dumps({"error": str(e)[:500]})


def _find_warehouse(client: "WorkspaceClient") -> str:
    """Find a running SQL warehouse, cached after first lookup."""
    global _cached_warehouse_id
    if _cached_warehouse_id:
        return _cached_warehouse_id
    for wh in client.warehouses.list():
        if str(wh.state) in ("RUNNING", "STARTING"):
            _cached_warehouse_id = wh.id
            return wh.id
    raise RuntimeError("No running SQL warehouse found")


# ---------------------------------------------------------------------------
# File collection, journal, and prompt building
# ---------------------------------------------------------------------------

def load_plan(plan_path: str | Path) -> dict:
    with open(plan_path) as f:
        return yaml.safe_load(f)


def in_active_hours(active_hours: str) -> bool:
    if not active_hours:
        return True
    start_s, end_s = active_hours.split("-")
    now = dt.datetime.now().time()
    start = dt.time.fromisoformat(start_s.strip())
    end = dt.time.fromisoformat(end_s.strip())
    return start <= now <= end


def collect_project_files(project_path: str, scope: dict) -> list[dict]:
    """Gather file contents matching scope patterns, respecting exclusions."""
    base = Path(project_path)
    included = set()
    for pattern in scope.get("include_patterns", ["**/*.py"]):
        included.update(base.glob(pattern))

    excluded = set()
    for pattern in scope.get("exclude_patterns", []):
        excluded.update(base.glob(pattern))

    files = sorted(included - excluded)
    result = []
    for fp in files:
        rel = fp.relative_to(base)
        try:
            content = fp.read_text(errors="replace")
            if len(content) > 50_000:
                content = content[:50_000] + "\n... (truncated)"
            result.append({"path": str(rel), "content": content})
        except Exception:
            continue
    return result


def load_journal(journal_path: str) -> list[dict]:
    """Load past runs to provide context on what was already done."""
    p = Path(journal_path)
    if not p.exists():
        return []
    entries = []
    for line in p.read_text().strip().splitlines():
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return entries[-20:]


def build_prompt(plan: dict, files: list[dict], journal: list[dict], skills_context: str) -> list[dict]:
    scope = plan["scope"]
    goals = plan["goals"]

    task = goals["task_prompt"].format(
        max_files=scope.get("max_files_changed", 3),
        max_lines=scope.get("max_lines_changed", 100),
    )

    file_listing = "\n".join(f"- {f['path']} ({len(f['content'].splitlines())} lines)" for f in files)

    file_contents = ""
    for f in files:
        file_contents += f"\n--- {f['path']} ---\n{f['content']}\n"

    recent_changes = ""
    if journal:
        recent_changes = "\n\nRecent improvements already made (DO NOT repeat these):\n"
        for entry in journal[-10:]:
            recent_changes += f"- [{entry.get('timestamp', '?')}] {entry.get('summary', 'unknown')}\n"

    tools_note = ""
    mcp_tools = plan.get("mcp_tools", [])
    if mcp_tools:
        tools_note = (
            "\n\n## Available Databricks Tools\n"
            "You have access to live Databricks workspace tools. Use them to:\n"
            "- Validate SQL patterns you find in the code\n"
            "- Check actual table schemas referenced in the code\n"
            "- Verify that tables/volumes exist\n"
            "- Understand the real data model before suggesting changes\n"
            "Call tools BEFORE proposing changes to ground your suggestions in reality.\n"
        )

    system = goals["system_prompt"] + skills_context

    return [
        {"role": "system", "content": system},
        {"role": "user", "content": (
            f"{task}\n\n"
            f"## Project files ({len(files)} files)\n{file_listing}\n"
            f"{recent_changes}"
            f"{tools_note}\n\n"
            f"## File contents\n{file_contents}\n\n"
            "Respond with:\n"
            "1. **Summary**: One-line description of the improvement\n"
            "2. **Rationale**: Why this matters (1-2 sentences)\n"
            "3. **Changes**: For each file, provide the FULL updated file content "
            "wrapped in a fenced code block labeled with the file path, like:\n"
            "```path/to/file.py\n<full file content>\n```\n"
            "Only include files you are changing.\n"
            "4. **Test verification**: Explain how existing or new tests cover this change."
        )},
    ]


# ---------------------------------------------------------------------------
# LLM call with tool-use loop
# ---------------------------------------------------------------------------

def call_llm(messages: list[dict], plan: dict, dbx_client=None) -> str:
    """Call Anthropic (with tool use) or OpenAI. Runs a tool-use loop if MCP tools are enabled."""
    mcp_tools = plan.get("mcp_tools", [])
    tools_enabled = bool(mcp_tools and dbx_client)

    if os.environ.get("ANTHROPIC_API_KEY") and anthropic:
        return _call_anthropic(messages, plan, dbx_client, tools_enabled, mcp_tools)

    if os.environ.get("OPENAI_API_KEY") and openai:
        return _call_openai(messages, plan)

    raise RuntimeError("No LLM API key found. Set ANTHROPIC_API_KEY or OPENAI_API_KEY.")


def _call_anthropic(messages, plan, dbx_client, tools_enabled, mcp_tools) -> str:
    client = anthropic.Anthropic()
    model = os.environ.get("HEARTBEAT_MODEL", "claude-sonnet-4-20250514")
    system = next((m["content"] for m in messages if m["role"] == "system"), "")
    conv = [m for m in messages if m["role"] != "system"]

    tool_defs = [TOOL_DEFS[t] for t in mcp_tools if t in TOOL_DEFS] if tools_enabled else []

    kwargs = dict(model=model, max_tokens=8192, system=system, messages=conv)
    if tool_defs:
        kwargs["tools"] = tool_defs

    for _ in range(10):  # max tool-use rounds
        resp = client.messages.create(**kwargs)

        text_parts = []
        tool_uses = []
        for block in resp.content:
            if block.type == "text":
                text_parts.append(block.text)
            elif block.type == "tool_use":
                tool_uses.append(block)

        if not tool_uses or not tools_enabled:
            return "\n".join(text_parts)

        # Execute tools and feed results back
        conv.append({"role": "assistant", "content": resp.content})
        tool_results = []
        for tu in tool_uses:
            print(f"  [mcp] Calling {tu.name}({json.dumps(tu.input)[:120]}...)")
            result = execute_mcp_tool(dbx_client, tu.name, tu.input)
            print(f"  [mcp] Result: {result[:200]}...")
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tu.id,
                "content": result,
            })
        conv.append({"role": "user", "content": tool_results})
        kwargs["messages"] = conv

    return "\n".join(text_parts)


def _call_openai(messages, plan) -> str:
    client = openai.OpenAI()
    resp = client.chat.completions.create(
        model=os.environ.get("HEARTBEAT_MODEL", "gpt-4o"),
        max_tokens=8192,
        messages=messages,
    )
    return resp.choices[0].message.content


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def parse_file_changes(response: str) -> dict[str, str]:
    """Extract file path -> content from fenced code blocks in the response."""
    changes = {}
    lines = response.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("```") and not line.strip() == "```":
            file_path = line[3:].strip()
            if "/" in file_path or "." in file_path:
                content_lines = []
                i += 1
                while i < len(lines) and not lines[i].strip().startswith("```"):
                    content_lines.append(lines[i])
                    i += 1
                changes[file_path] = "\n".join(content_lines)
        i += 1
    return changes


def parse_summary(response: str) -> str:
    for line in response.split("\n"):
        line = line.strip()
        if line.startswith("**Summary**:") or line.startswith("## Summary"):
            return line.split(":", 1)[-1].strip().strip("*")
        if line.startswith("1.") and "summary" in line.lower():
            return line.split(":", 1)[-1].strip().strip("*") if ":" in line else line[2:].strip()
    return response[:120]


# ---------------------------------------------------------------------------
# Git and test helpers
# ---------------------------------------------------------------------------

def git(project_path: str, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=project_path, capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0 and "already exists" not in result.stderr:
        raise subprocess.CalledProcessError(result.returncode, f"git {' '.join(args)}", result.stderr)
    return result.stdout.strip()


def run_tests(project_path: str, test_command: str) -> tuple[bool, str]:
    parts = shlex.split(test_command)
    result = subprocess.run(
        parts, cwd=project_path, capture_output=True, text=True, timeout=120,
    )
    output = result.stdout + "\n" + result.stderr
    return result.returncode == 0, output[-2000:]


def validate_file_path(project_path: str, file_path: str) -> Path:
    """Ensure LLM-proposed file paths resolve inside the project directory."""
    base = Path(project_path).resolve()
    target = (base / file_path).resolve()
    if not str(target).startswith(str(base)):
        raise ValueError(f"Path traversal blocked: {file_path} resolves outside project")
    return target


def append_journal(journal_path: str, entry: dict):
    p = Path(journal_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a") as f:
        f.write(json.dumps(entry) + "\n")


# ---------------------------------------------------------------------------
# Main run logic
# ---------------------------------------------------------------------------

def run_once(plan: dict, dry_run: bool = False) -> dict:
    run_id = str(uuid.uuid4())[:8]
    timestamp = dt.datetime.now().isoformat()
    project_path = plan["project"]["path"]
    journal_path = os.path.join(project_path, plan["journal"]["path"])

    print(f"[{timestamp}] Heartbeat run {run_id} starting...")

    # Load skills
    skills_context = load_skills(plan)

    # Init Databricks client if MCP tools are configured
    dbx_client = None
    mcp_tools = plan.get("mcp_tools", [])
    if mcp_tools:
        dbx_client = get_dbx_client()
        if dbx_client:
            print(f"  [mcp] Databricks client ready, tools: {mcp_tools}")
        else:
            print(f"  [mcp] Databricks client unavailable, tools disabled (set DATABRICKS_HOST + DATABRICKS_TOKEN)")

    files = collect_project_files(project_path, plan["scope"])
    if not files:
        return {"run_id": run_id, "status": "skipped", "reason": "no files matched scope"}

    journal = load_journal(journal_path)
    messages = build_prompt(plan, files, journal, skills_context)

    print(f"  Analyzing {len(files)} files (skills: {len(plan.get('skills', []))}, tools: {len(mcp_tools)})...")
    response = call_llm(messages, plan, dbx_client)
    summary = parse_summary(response)
    changes = parse_file_changes(response)
    print(f"  Suggestion: {summary}")
    print(f"  Files to change: {list(changes.keys())}")

    entry = {
        "timestamp": timestamp,
        "run_id": run_id,
        "summary": summary,
        "files_proposed": list(changes.keys()),
        "skills_loaded": plan.get("skills", []),
        "mcp_tools_enabled": bool(dbx_client and mcp_tools),
        "response_hash": hashlib.sha256(response.encode()).hexdigest()[:12],
    }

    if dry_run or not changes:
        entry["status"] = "dry_run" if dry_run else "no_changes_parsed"
        entry["response_preview"] = response[:500]
        append_journal(journal_path, entry)
        print(f"  {'Dry run' if dry_run else 'No changes parsed'} - logged to journal.")
        return entry

    max_files = plan["scope"].get("max_files_changed", 3)
    if len(changes) > max_files:
        entry["status"] = "rejected_too_many_files"
        append_journal(journal_path, entry)
        print(f"  Rejected: {len(changes)} files > limit of {max_files}")
        return entry

    base = plan["project"].get("base_branch", "main")
    branch_name = f"{plan['project']['branch_prefix']}/{run_id}-{summary[:30].lower().replace(' ', '-')}"
    branch_name = "".join(c for c in branch_name if c.isalnum() or c in "-_/")

    # Stash any dirty working tree so we don't lose user's uncommitted work
    stashed = False
    status = git(project_path, "status", "--porcelain")
    if status:
        git(project_path, "stash", "push", "-m", f"heartbeat-autostash-{run_id}")
        stashed = True
        print(f"  Stashed uncommitted changes (will restore after)")

    git(project_path, "checkout", base)
    git(project_path, "checkout", "-b", branch_name)

    try:
        total_lines_changed = 0
        for file_path, content in changes.items():
            full_path = validate_file_path(project_path, file_path)
            full_path.parent.mkdir(parents=True, exist_ok=True)

            old_content = full_path.read_text() if full_path.exists() else ""
            total_lines_changed += abs(
                len(content.splitlines()) - len(old_content.splitlines())
            )
            full_path.write_text(content)

        max_lines = plan["scope"].get("max_lines_changed", 100)
        if total_lines_changed > max_lines:
            git(project_path, "checkout", ".")
            git(project_path, "checkout", base)
            git(project_path, "branch", "-D", branch_name)
            if stashed:
                git(project_path, "stash", "pop")
            entry["status"] = "rejected_too_many_lines"
            entry["lines_changed"] = total_lines_changed
            append_journal(journal_path, entry)
            print(f"  Rejected: {total_lines_changed} lines > limit of {max_lines}")
            return entry

        test_cmd = plan["project"].get("test_command", "python -m pytest -x -q")
        passed, test_output = run_tests(project_path, test_cmd)

        if not passed:
            git(project_path, "checkout", ".")
            git(project_path, "checkout", base)
            git(project_path, "branch", "-D", branch_name)
            if stashed:
                git(project_path, "stash", "pop")
            entry["status"] = "tests_failed"
            entry["test_output"] = test_output
            append_journal(journal_path, entry)
            print(f"  Tests failed - changes rolled back.")
            return entry

        git(project_path, "add", "-A")
        git(project_path, "commit", "-m", f"heartbeat: {summary}")

        entry["status"] = "applied"
        entry["branch"] = branch_name
        entry["lines_changed"] = total_lines_changed
        entry["files_changed"] = list(changes.keys())
        append_journal(journal_path, entry)
        print(f"  Applied on branch: {branch_name}")
        print(f"  Review with: git diff {base}...{branch_name}")

        git(project_path, "checkout", base)
        if stashed:
            git(project_path, "stash", "pop")

    except Exception as e:
        try:
            git(project_path, "checkout", ".")
            git(project_path, "checkout", base)
            git(project_path, "branch", "-D", branch_name)
            if stashed:
                git(project_path, "stash", "pop")
        except Exception:
            pass
        entry["status"] = "error"
        entry["error"] = str(e)
        append_journal(journal_path, entry)
        print(f"  Error: {e}")

    return entry


def daemon_loop(plan: dict, dry_run: bool = False):
    interval = plan["schedule"]["interval_minutes"] * 60
    max_runs = plan["schedule"].get("max_runs_per_day", 50)
    active_hours = plan["schedule"].get("active_hours")

    runs_today = 0
    current_day = dt.date.today()

    print(f"Heartbeat daemon started (every {plan['schedule']['interval_minutes']}m)")
    print(f"  Project: {plan['project']['path']}")
    print(f"  Active hours: {active_hours or 'always'}")
    print(f"  Max runs/day: {max_runs}")
    print(f"  Skills: {plan.get('skills', [])}")
    print(f"  MCP tools: {plan.get('mcp_tools', [])}")
    print()

    while True:
        if dt.date.today() != current_day:
            current_day = dt.date.today()
            runs_today = 0

        if runs_today >= max_runs:
            print(f"  Daily cap reached ({max_runs}). Sleeping until midnight...")
            time.sleep(300)
            continue

        if not in_active_hours(active_hours):
            print(f"  Outside active hours ({active_hours}). Sleeping...")
            time.sleep(300)
            continue

        try:
            run_once(plan, dry_run=dry_run)
            runs_today += 1
        except KeyboardInterrupt:
            print("\nHeartbeat stopped.")
            sys.exit(0)
        except Exception as e:
            print(f"  Run failed: {e}")

        print(f"  Next run in {plan['schedule']['interval_minutes']} minutes...\n")
        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description="Cursor Heartbeat: autonomous code improvement")
    parser.add_argument("--plan", default=str(DEFAULT_PLAN), help="Path to plan YAML")
    parser.add_argument("--daemon", action="store_true", help="Run continuously on schedule")
    parser.add_argument("--dry-run", action="store_true", help="Suggest but don't apply changes")
    args = parser.parse_args()

    plan = load_plan(args.plan)

    if args.daemon:
        daemon_loop(plan, dry_run=args.dry_run)
    else:
        result = run_once(plan, dry_run=args.dry_run)
        print(f"\nResult: {json.dumps(result, indent=2)}")


if __name__ == "__main__":
    main()
