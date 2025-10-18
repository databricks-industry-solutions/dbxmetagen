#!/bin/bash
set -e

# Make sure you are in the project root before running this script

# (1) Install base project (your own wheel, or via pip in editable mode)
pip install .

# (2) Install spaCy and any other Python dependencies from PyPI if network is available.
# Skip this step if you want to be fully offline and have all wheels pre-downloaded!

# (3) Install all wheels from the local external_libs/ directory, including the spaCy model
WHEEL_DIR="external_libs"

# Install everything in the external_libs directory first: 
for whl in $WHEEL_DIR/*.whl; do
    pip install --no-index --find-links=$WHEEL_DIR "$whl"
done

# (4) Optionally validate model installation
python -c "import en_core_web_lg; print('spaCy en_core_web_lg installed:', en_core_web_lg.__file__)" || {
    echo "ERROR: en_core_web_lg model did not install correctly."
    exit 1
}

echo "Deployment completed successfully."