#!/bin/bash
# Regenerate requirements.txt from local uv.lock, excluding packages that conflict
# with Databricks Runtime (DBR). Called by pre-commit hook when deps change.
set -e

source "$(dirname "$0")/ensure_public_pypi.sh"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOCK_FILE="${REPO_ROOT}/uv.lock"

if [ ! -f "${LOCK_FILE}" ]; then
    echo "No local uv.lock found; skipping requirements.txt export." >&2
    echo "Run 'uv lock' locally, then re-run this script." >&2
    exit 0
fi

uv export -q --frozen --no-dev --no-hashes --no-emit-project \
  --no-emit-package databricks-connect \
  --no-emit-package pyspark \
  --no-emit-package py4j \
  --no-emit-package setuptools \
  --format requirements-txt -o "$REPO_ROOT/requirements.txt"

cat >> "$REPO_ROOT/requirements.txt" <<'EOF'
# --- PI mode: spaCy model (only needed for deterministic PI detection) ---
# The spaCy model is installed conditionally by the generate_metadata notebook
# based on the spacy_model_names widget (default: en_core_web_md).
# For manual installation, use: pip install -r requirements-pi.txt
# Air-gapped alternative:
#   pip install /Volumes/<catalog>/<schema>/<volume>/en_core_web_md-3.8.0-py3-none-any.whl
EOF
