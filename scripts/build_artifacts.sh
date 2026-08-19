#!/bin/bash
# Build the dbxmetagen wheel for BOTH consumers in one pass.
#
# This script is invoked automatically by `databricks bundle deploy` as the
# bundle's artifacts.default.build command (see databricks.yml). It is the ONLY
# build step -- there is no deploy.sh anymore.
#
# It produces:
#   1. dist/dbxmetagen-<ver>.whl                     -- consumed by JOBS via
#      `libraries: - whl: ../../dist/*.whl` in resources/jobs/*.yml
#   2. apps/dbxmetagen-app/app/dbxmetagen-<ver>.whl  -- consumed by the APP,
#      pinned as the last line of the app's generated requirements.txt
#
# Why the version is stamped with a build timestamp:
#   The Databricks App builder runs pip against the app's requirements.txt.
#   pip is version-keyed: if the wheel version is unchanged it skips reinstall,
#   so app code would go stale. Stamping <base>+<timestamp> forces a fresh
#   install every deploy. This also gives JOBS a unique version each deploy,
#   which is why the bundle sets dynamic_version: false (this script owns
#   versioning; letting DAB also stamp would double the local-version segment).
#
# Run from the repo root (bundle deploy sets cwd to the artifact path, ".").
set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

APP_DIR="apps/dbxmetagen-app/app"

echo "=== build_artifacts: building dbxmetagen wheel ==="

# --- Read base version from pyproject.toml (Python 3.11+ for tomllib) ---
BASE_VERSION=$(python3 -c "
import sys
try:
    import tomllib
except ImportError:
    print('Error: build_artifacts.sh requires Python 3.11+ to read pyproject.toml (tomllib is not in Python 3.10).', file=sys.stderr)
    print(f'Current interpreter: {sys.version}', file=sys.stderr)
    sys.exit(1)
v = tomllib.load(open('pyproject.toml', 'rb'))['project']['version']
print(v.split('+')[0])
") || exit 1

if [ -z "${BASE_VERSION}" ]; then
    echo "Error: could not parse version from pyproject.toml" >&2
    exit 1
fi

DEPLOY_VERSION="${BASE_VERSION}+$(date +%s)"

# --- Stamp the version, build, then always restore pyproject.toml ---
restore_pyproject() {
    [ -f pyproject.toml.build_bak ] && mv pyproject.toml.build_bak pyproject.toml
}
trap restore_pyproject EXIT

sed -i.build_bak "s/^version = \"${BASE_VERSION}[^\"]*\"/version = \"${DEPLOY_VERSION}\"/" pyproject.toml

rm -rf dist/
uv build -q
restore_pyproject
trap - EXIT

WHL_PATH=$(ls dist/dbxmetagen-*.whl)
WHL_NAME=$(basename "${WHL_PATH}")
echo "  Built: ${WHL_NAME}"

# --- Copy the wheel into the app source dir (app installs it via requirements.txt) ---
# Remove any stale wheels from a prior deploy first.
rm -f "${APP_DIR}"/dbxmetagen-*.whl
cp "${WHL_PATH}" "${APP_DIR}/${WHL_NAME}"

# --- Generate the app's requirements.txt from the template ---
# The template ends with `./__WHL_NAME__`; substitute the actual wheel filename.
sed "s|__WHL_NAME__|${WHL_NAME}|" \
    "${APP_DIR}/requirements.txt.template" \
    > "${APP_DIR}/requirements.txt"

# --- Stage the configurations dir into the app source ---
# The app resolves ontology bundles / domain configs from a `configurations/`
# dir next to api_server.py (see _CONFIG_DIR_CANDIDATES). That dir is gitignored
# and exists only as a deploy-time copy; sync.include ships it to the workspace.
rm -rf "${APP_DIR}/configurations"
cp -r configurations "${APP_DIR}/configurations"

echo "  App wheel: ${APP_DIR}/${WHL_NAME}"
echo "  App requirements.txt regenerated"
echo "  App configurations/ staged"
echo "=== build_artifacts: done ==="
