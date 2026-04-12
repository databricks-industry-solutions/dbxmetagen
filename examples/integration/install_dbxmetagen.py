"""Shared pip install for dbxmetagen notebooks.

Serverless / job compute often has no ``git`` binary; ``pip install git+https://...`` fails.

Resolution order for ``install_source`` (see ``resolve_install_source``):

- ``auto`` (default): newest ``vendor/dbxmetagen-*.whl`` next to this file if present,
  else HTTPS zip of the pinned release tag on ``databricks-industry-solutions/dbxmetagen``.
- Any other pip spec: zip URL, ``.whl`` path, ``git+https://...`` (with zip fallback on git failure).

Override via the ``install_source`` widget or ``METAGEN_INSTALL_SOURCE`` env var.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# Pinned release tag (reproducible). Bump when your project should track a new release.
ZIP_AUTO_FALLBACK = (
    "https://github.com/databricks-industry-solutions/dbxmetagen/archive/refs/tags/v0.8.8.zip"
)

DEFAULT_INSTALL_SOURCE = "auto"


def _vendor_wheel() -> str | None:
    d = Path(__file__).resolve().parent / "vendor"
    if not d.is_dir():
        return None
    wheels = sorted(d.glob("dbxmetagen-*.whl"))
    return str(wheels[-1]) if wheels else None


def resolve_install_source(source: str) -> str:
    """Return concrete pip target: *auto* -> vendor wheel or ``ZIP_AUTO_FALLBACK``."""
    s = (source or "").strip()
    if s.lower() != "auto" and s:
        return s
    w = _vendor_wheel()
    if w:
        print(f"dbxmetagen: install_source auto -> bundled wheel {w}")
        return w
    print(f"dbxmetagen: install_source auto -> {ZIP_AUTO_FALLBACK}")
    return ZIP_AUTO_FALLBACK


def _pip_install(packages: list[str]) -> subprocess.CompletedProcess:
    cmd = [sys.executable, "-m", "pip", "install", "-U", "--no-cache-dir", *packages]
    print(" ".join(cmd))
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def _pip_fail(packages: list[str], r: subprocess.CompletedProcess) -> None:
    detail = ((r.stderr or "") + (r.stdout or "")).strip() or "(no pip output)"
    py = sys.version.split()[0]
    raise RuntimeError(
        f"pip install failed (exit {r.returncode}, Python {py}): {packages!r}\n{detail}"
    )


def install_dbxmetagen(source: str, *extra: str) -> None:
    """Install dbxmetagen from *source* (see ``resolve_install_source``) plus optional extras."""
    print(f"dbxmetagen install: Python {sys.version.split()[0]} ({sys.executable})")
    source = resolve_install_source(source)
    primary = [source, *extra]
    r = _pip_install(primary)
    if r.returncode == 0:
        if r.stdout:
            print(r.stdout)
        return

    combined = (r.stderr or "") + (r.stdout or "")
    print(combined)

    looks_like_git = primary[0].startswith("git+") or primary[0].startswith("git@")
    if looks_like_git:
        print(
            "dbxmetagen: git-based install failed (serverless often has no git). "
            f"Retrying from zip: {ZIP_AUTO_FALLBACK}"
        )
        r2 = _pip_install([ZIP_AUTO_FALLBACK, *extra])
        if r2.returncode == 0:
            if r2.stdout:
                print(r2.stdout)
            return
        print(r2.stderr or r2.stdout or "")
        _pip_fail([ZIP_AUTO_FALLBACK, *extra], r2)

    _pip_fail(primary, r)
