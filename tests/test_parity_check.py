"""Tests for scripts/check_parity.py (offline; synthetic parent trees).

When ``SOLO_EMPIRE_ROOT`` points at a real Solo Empire checkout, the live
parity test also runs against the parent ``infra/scripts/data_lake``.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "check_parity.py"

_spec = importlib.util.spec_from_file_location("check_parity", SCRIPT)
check_parity = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(check_parity)

PACKAGED = '''"""Packaged docstring."""
from __future__ import annotations
from pathlib import Path
from .storage import ObjectStore

ROOT = Path(__file__).resolve().parents[2]
LIMIT = 10


def add(a, b):
    """Packaged wording."""
    return a + b
'''

PARENT = '''"""Parent docstring."""
from __future__ import annotations
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = ROOT / "infra" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
try:
    from _env import PROJECT_ROOT
except ImportError:
    PROJECT_ROOT = ROOT
from data_lake.storage import ObjectStore  # noqa: E402

LIMIT = 10


def add(a, b):
    """Parent wording."""
    return a + b
'''


class CompareModuleTests(unittest.TestCase):
    def test_import_style_bootstrap_and_docstrings_are_ignored(self):
        self.assertEqual(check_parity.compare_module("m", PACKAGED, PARENT), [])

    def test_changed_function_body_is_drift(self):
        drifted = PARENT.replace("return a + b", "return a - b")
        self.assertEqual(
            check_parity.compare_module("m", PACKAGED, drifted),
            ["m.add: implementation differs"],
        )

    def test_function_missing_from_package_is_drift(self):
        extended = PARENT + "\n\ndef sub(a, b):\n    return a - b\n"
        self.assertEqual(
            check_parity.compare_module("m", PACKAGED, extended),
            ["m.sub: only in parent (missing from package)"],
        )

    def test_changed_module_constant_is_drift(self):
        drifted = PARENT.replace("LIMIT = 10", "LIMIT = 20")
        self.assertEqual(
            check_parity.compare_module("m", PACKAGED, drifted),
            ["m: module-level statements differ"],
        )

    def test_allowlisted_divergence_is_ignored(self):
        packaged = "def load_query_runtime():\n    return 1\n"
        parent = "def load_query_runtime():\n    return 2\n"
        self.assertEqual(
            check_parity.compare_module("product_adapter", packaged, parent), []
        )
        self.assertEqual(
            check_parity.compare_module("other", packaged, parent),
            ["other.load_query_runtime: implementation differs"],
        )


class CliTests(unittest.TestCase):
    def test_missing_parent_exit_codes(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(check_parity.main(["--parent", tmp]), 2)
            self.assertEqual(check_parity.main(["--parent", tmp, "--allow-missing"]), 0)

    def test_parent_copy_of_package_is_in_parity_and_drift_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent_dir = Path(tmp) / "infra" / "scripts" / "data_lake"
            shutil.copytree(check_parity.PACKAGE_DIR, parent_dir)
            self.assertEqual(check_parity.main(["--parent", tmp]), 0)
            storage = parent_dir / "storage.py"
            storage.write_text(
                storage.read_text(encoding="utf-8") + "\n\ndef extra():\n    return 1\n",
                encoding="utf-8",
            )
            self.assertEqual(check_parity.main(["--parent", tmp]), 1)


@unittest.skipUnless(
    os.environ.get("SOLO_EMPIRE_ROOT"), "set SOLO_EMPIRE_ROOT to check live parity"
)
class LiveParentParityTests(unittest.TestCase):
    def test_packaged_runtime_matches_parent(self):
        parent_dir = check_parity.find_parent(None)
        self.assertIsNotNone(parent_dir, "SOLO_EMPIRE_ROOT has no infra/scripts/data_lake")
        self.assertEqual(check_parity.check(check_parity.PACKAGE_DIR, parent_dir), [])


if __name__ == "__main__":
    unittest.main()
