#!/bin/bash
# Force uv to use public PyPI. Internal Databricks pip proxies must not be
# forwarded into uv.lock (customers cannot reach pypi-proxy.dev.databricks.com).
set -euo pipefail

PUBLIC_INDEX="https://pypi.org/simple"

if [ -n "${UV_INDEX_URL:-}" ] && [[ "${UV_INDEX_URL}" == *"pypi-proxy"* ]]; then
    echo "Warning: UV_INDEX_URL points at an internal PyPI proxy; using public PyPI instead." >&2
fi

export UV_INDEX_URL="${PUBLIC_INDEX}"
