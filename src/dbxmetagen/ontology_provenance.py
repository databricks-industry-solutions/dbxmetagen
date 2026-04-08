"""Bundle / tier index provenance helpers."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_TIER_STEMS = (
    "entities_tier1",
    "entities_tier2",
    "entities_tier3",
    "edges_tier1",
    "edges_tier2",
    "edges_tier3",
    "equivalent_class_uris",
)

_TIER_FILES = tuple(f"{s}.yaml" for s in _TIER_STEMS)


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
    for stem in _TIER_STEMS:
        for ext in (".json", ".yaml"):
            p = tier_subdir / f"{stem}{ext}"
            if p.is_file():
                mtimes.append(p.stat().st_mtime)
                break
    if not mtimes:
        return True
    return bundle_mtime > max(mtimes)


def compute_tier_index_hash(bundle_dir: Path) -> str:
    """Short SHA-256 fingerprint of committed tier files for audit trails."""
    h = hashlib.sha256()
    for stem in _TIER_STEMS:
        for ext in (".json", ".yaml"):
            p = bundle_dir / f"{stem}{ext}"
            if p.is_file():
                h.update(f"{stem}{ext}".encode())
                h.update(p.read_bytes())
                break
    return h.hexdigest()[:24]


def read_bundle_metadata_version(bundle_yaml: Path) -> Optional[str]:
    """Return ``metadata.version`` from a bundle YAML if present."""
    try:
        import yaml

        raw = yaml.safe_load(bundle_yaml.read_text(encoding="utf-8"))
        meta = raw.get("metadata") or {}
        return str(meta.get("version") or meta.get("name") or "") or None
    except Exception as e:
        logger.debug("Could not read bundle metadata version from %s: %s", bundle_yaml, e)
        return None
