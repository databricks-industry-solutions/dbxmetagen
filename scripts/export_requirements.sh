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

uv export --no-dev --no-hashes --no-emit-project \
  --no-emit-package databricks-connect \
  --no-emit-package pyspark \
  --no-emit-package py4j \
  --no-emit-package setuptools \
  --format requirements-txt -o "$REPO_ROOT/requirements.txt"

cat >> "$REPO_ROOT/requirements.txt" <<'EOF'
# --- PI mode: spaCy model (only needed for deterministic PI detection) ---
# Comment this line out if you do NOT use PI mode, or if you are in an
# air-gapped environment. For air-gapped PI, install the model from a
# Databricks Volume instead:
#   pip install /Volumes/<catalog>/<schema>/<volume>/en_core_web_lg-3.8.0-py3-none-any.whl
https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-3.8.0/en_core_web_lg-3.8.0-py3-none-any.whl
EOF

git add "$REPO_ROOT/requirements.txt"
