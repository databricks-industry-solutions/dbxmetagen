#!/bin/bash
# Force uv to use public PyPI. Internal Databricks pip proxies must not be
# forwarded into uv.lock (customers cannot reach pypi-proxy.dev.databricks.com).
set -euo pipefail

PUBLIC_INDEX="https://pypi.org/simple"

if [ -n "${UV_INDEX_URL:-}" ] && [[ "${UV_INDEX_URL}" == *"pypi-proxy"* ]]; then
    echo "Warning: UV_INDEX_URL points at an internal PyPI proxy; using public PyPI instead." >&2
fi

export UV_INDEX_URL="${PUBLIC_INDEX}"

regenerate_lock_if_poisoned() {
    local lock_file="${1:-uv.lock}"
    if [ ! -f "${lock_file}" ] || ! grep -q 'pypi-proxy' "${lock_file}"; then
        return 0
    fi

    echo "=== Regenerating poisoned uv.lock against public PyPI ==="
    if uv lock; then
        echo "uv.lock regenerated successfully."
        return 0
    fi

    echo "Warning: could not regenerate uv.lock (public PyPI may be unreachable from this machine)." >&2
    echo "Deploy will continue without syncing the full project environment." >&2
    return 0
}
