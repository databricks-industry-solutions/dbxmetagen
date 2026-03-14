#!/usr/bin/env bash
# Cursor Heartbeat launcher
# Usage:
#   ./run_heartbeat.sh              # single run
#   ./run_heartbeat.sh --daemon     # continuous
#   ./run_heartbeat.sh --dry-run    # preview only
#
# Cron example (every 15 min, single-shot mode):
#   */15 8-22 * * * /path/to/run_heartbeat.sh >> /tmp/heartbeat.log 2>&1
#
# Launchd: see com.cursor.heartbeat.plist

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"

# Bootstrap venv if needed
if [ ! -d "${VENV_DIR}" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "${VENV_DIR}"
    "${VENV_DIR}/bin/pip" install -q -r "${SCRIPT_DIR}/requirements.txt"
fi

# Source your API key (edit this path or export before running)
if [ -f "${HOME}/.env.heartbeat" ]; then
    set -a
    source "${HOME}/.env.heartbeat"
    set +a
fi

exec "${VENV_DIR}/bin/python" "${SCRIPT_DIR}/heartbeat.py" "$@"
