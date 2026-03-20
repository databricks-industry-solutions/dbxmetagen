"""Tests that deterministic_pi module loads without spacy/presidio installed,
and raises clear errors when PI functions are actually called without deps."""

import importlib
import sys
import types
from unittest.mock import patch

import pytest


class TestLazyImportModuleLoads:
    """Importing deterministic_pi must succeed even when spacy/presidio are absent."""

    def _reload_without(self, blocked):
        mod_name = "dbxmetagen.deterministic_pi"
        saved = {}
        for b in blocked:
            if b in sys.modules:
                saved[b] = sys.modules.pop(b)
        if mod_name in sys.modules:
            saved[mod_name] = sys.modules.pop(mod_name)

        sentinel = types.ModuleType("_sentinel")
        with patch.dict(sys.modules, {b: sentinel for b in blocked}):
            # Make the sentinel raise ImportError on attribute access
            sentinel.__spec__ = None
            sentinel.__path__ = []
            sentinel.__file__ = None

            orig_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__

            def guarded_import(name, *args, **kwargs):
                for b in blocked:
                    if name == b or name.startswith(b + "."):
                        raise ImportError(f"Simulated missing: {name}")
                return orig_import(name, *args, **kwargs)

            with patch("builtins.__import__", side_effect=guarded_import):
                mod = importlib.import_module(mod_name)
        # Restore
        for k, v in saved.items():
            sys.modules[k] = v
        return mod

    def test_module_loads_without_spacy(self):
        mod = self._reload_without(["spacy"])
        assert hasattr(mod, "detect_pi")
        assert hasattr(mod, "ensure_spacy_model")
        assert hasattr(mod, "get_analyzer_engine")

    def test_module_loads_without_presidio(self):
        mod = self._reload_without(["presidio_analyzer"])
        assert hasattr(mod, "detect_pi")
        assert hasattr(mod, "get_analyzer_engine")

    def test_module_loads_without_both(self):
        mod = self._reload_without(["spacy", "presidio_analyzer"])
        assert hasattr(mod, "detect_pi")
        assert hasattr(mod, "luhn_checksum")


class TestLazyImportErrorMessages:
    """Calling PI functions without deps raises ImportError with actionable messages."""

    def test_get_analyzer_engine_missing_presidio(self):
        with patch.dict(sys.modules, {"presidio_analyzer": None}):
            with pytest.raises(ImportError, match="presidio_analyzer"):
                from dbxmetagen.deterministic_pi import get_analyzer_engine
                get_analyzer_engine()

    def test_ensure_spacy_model_missing_spacy(self):
        with patch.dict(sys.modules, {"spacy": None}):
            with pytest.raises(ImportError, match="spacy"):
                from dbxmetagen.deterministic_pi import ensure_spacy_model
                ensure_spacy_model()


class TestLuhnUnaffected:
    """luhn_checksum must work regardless of PI dep availability."""

    def test_luhn_valid(self):
        from dbxmetagen.deterministic_pi import luhn_checksum
        assert luhn_checksum("4532015112830366") is True

    def test_luhn_invalid(self):
        from dbxmetagen.deterministic_pi import luhn_checksum
        assert luhn_checksum("1234567890123456") is False
