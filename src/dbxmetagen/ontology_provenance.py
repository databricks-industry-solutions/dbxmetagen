"""Bundle / tier index provenance helpers."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional

_TIER_FILES = (
    "entities_tier1.yaml",
    "entities_tier2.yaml",
    "entities_tier3.yaml",
    "edges_tier1.yaml",
    "edges_tier2.yaml",
    "edges_tier3.yaml",
    "equivalent_class_uris.yaml",
)


def tier_indexes_stale(bundle_yaml: Path, tier_subdir: Optional[Path] = None) -> bool:
    """True if bundle YAML is newer than generated tier files, or tier indexes are missing.

    Default ``tier_subdir`` is ``<parent>/<stem>`` (e.g. ``healthcare/`` next to ``healthcare.yaml``).
    """
    if not bundle_yaml.is_file():
        return False
    if tier_subdir is None:
        tier_subdir = bundle_yaml.parent / bundle_yaml.stem
    bundle_mtime = bundle_yaml.stat().st_mtime
    if not tier_subdir.is_dir():
        return True
    mtimes: list[float] = []
    for fname in _TIER_FILES:
        p = tier_subdir / fname
        if p.is_file():
            mtimes.append(p.stat().st_mtime)
    if not mtimes:
        return True
    return bundle_mtime > max(mtimes)


def compute_tier_index_hash(bundle_dir: Path) -> str:
    """Short SHA-256 fingerprint of committed tier YAMLs for audit trails."""
    h = hashlib.sha256()
    for fname in _TIER_FILES:
        p = bundle_dir / fname
        if p.is_file():
            h.update(fname.encode())
            h.update(p.read_bytes())
    return h.hexdigest()[:24]


def read_bundle_metadata_version(bundle_yaml: Path) -> Optional[str]:
    """Return ``metadata.version`` from a bundle YAML if present."""
    try:
        import yaml

        raw = yaml.safe_load(bundle_yaml.read_text(encoding="utf-8"))
        meta = raw.get("metadata") or {}
        return str(meta.get("version") or meta.get("name") or "") or None
    except Exception:
        return None
