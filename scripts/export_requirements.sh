#!/bin/bash
# Regenerate requirements.txt from uv.lock, excluding packages that conflict
# with Databricks Runtime (DBR). Called by pre-commit hook and deploy.sh.
set -e

# Bridge pip proxy config to uv (see deploy.sh for explanation)
if [ -z "$UV_INDEX_URL" ]; then
    _pip_idx=$(pip3 config get global.index-url 2>/dev/null || true)
    [ -n "$_pip_idx" ] && export UV_INDEX_URL="$_pip_idx"
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

uv export -q --no-dev --no-hashes --no-emit-project \
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

