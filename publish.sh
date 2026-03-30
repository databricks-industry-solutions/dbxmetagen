#!/bin/bash
# Publish dbxmetagen artifacts for consumption by other projects.
#
# Usage: ./publish.sh [OPTIONS]
#   --profile PROFILE    Databricks CLI profile (default: DEFAULT)
#   --volume  PATH       UC Volume path (e.g. /Volumes/catalog/schema/dbxmetagen)
#   --workspace PATH     Workspace folder for notebooks (e.g. /Workspace/Shared/dbxmetagen)
#   --help               Show this help

set -e

# Bridge pip proxy config to uv (see deploy.sh for explanation)
if [ -z "$UV_INDEX_URL" ]; then
    _pip_idx=$(pip3 config get global.index-url 2>/dev/null || true)
    [ -n "$_pip_idx" ] && export UV_INDEX_URL="$_pip_idx"
fi

PROFILE="DEFAULT"
VOLUME_PATH=""
WORKSPACE_PATH=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --profile)    PROFILE="$2"; shift 2 ;;
        --volume)     VOLUME_PATH="$2"; shift 2 ;;
        --workspace)  WORKSPACE_PATH="$2"; shift 2 ;;
        --help|-h)    sed -n '2,7p' "$0"; exit 0 ;;
        *)            echo "Unknown option: $1 (use --help)"; exit 1 ;;
    esac
done

if [ -z "$VOLUME_PATH" ]; then
    echo "Error: --volume is required (e.g. /Volumes/my_catalog/my_schema/dbxmetagen)"
    exit 1
fi

if ! command -v databricks &> /dev/null; then
    echo "Error: Databricks CLI not found."
    exit 1
fi

# --- Build wheel ---
echo "=== Building wheel ==="
uv build -q
WHL=$(ls -t dist/*.whl | head -1)
echo "Built: ${WHL}"

# --- Upload wheel to UC Volume ---
echo ""
echo "=== Uploading wheel to ${VOLUME_PATH}/ ==="
databricks fs cp "${WHL}" "dbfs:${VOLUME_PATH}/$(basename ${WHL})" --profile "$PROFILE" --overwrite
echo "Uploaded: ${VOLUME_PATH}/$(basename ${WHL})"

# --- Upload configurations ---
echo ""
echo "=== Uploading configurations ==="
for f in configurations/ontology_bundles/*.yaml configurations/ontology_config.yaml configurations/domain_config*.yaml configurations/geo_config.yaml; do
    [ -f "$f" ] || continue
    REL=${f#configurations/}
    databricks fs cp "$f" "dbfs:${VOLUME_PATH}/configurations/${REL}" --profile "$PROFILE" --overwrite
    echo "  ${REL}"
done

# --- Upload notebooks to workspace (optional) ---
if [ -n "$WORKSPACE_PATH" ]; then
    echo ""
    echo "=== Uploading notebooks to ${WORKSPACE_PATH}/ ==="
    databricks workspace mkdirs "${WORKSPACE_PATH}" --profile "$PROFILE" 2>/dev/null || true
    for nb in notebooks/*.py; do
        databricks workspace import "${WORKSPACE_PATH}/$(basename ${nb} .py)" \
            --file "$nb" --format SOURCE --language PYTHON \
            --profile "$PROFILE" --overwrite
        echo "  $(basename ${nb})"
    done
fi

echo ""
echo "=== Publish complete ==="
echo "Wheel:          ${VOLUME_PATH}/$(basename ${WHL})"
[ -n "$WORKSPACE_PATH" ] && echo "Notebooks:      ${WORKSPACE_PATH}/"
echo "Configurations: ${VOLUME_PATH}/configurations/"
echo ""
echo "Consumers can install with:"
echo "  %pip install ${VOLUME_PATH}/$(basename ${WHL})"
