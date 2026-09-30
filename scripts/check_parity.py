#!/usr/bin/env python3
"""Detect drift between this packaged runtime and the parent Solo Empire copy.

The packaged ``src/data_lake`` modules are a portable copy of the parent
monorepo's ``infra/scripts/data_lake`` modules. They intentionally differ in
import style (relative imports, no ``sys.path`` bootstrap) and in the few
runtime loaders that must work without a parent checkout. Everything else
should stay identical, so this script compares module ASTs after stripping:

* import statements (and module-level ``try``/``if`` blocks that only import
  or bootstrap ``sys.path``),
* assignments to path-bootstrap names (``ROOT``, ``PROJECT_ROOT``, ...),
* docstrings,

and then compares each top-level function/class and the remaining statements.
Known, reviewed divergences are listed in ``ALLOWED_DIVERGENCE`` with a reason.

Usage::

    SOLO_EMPIRE_ROOT=/path/to/solo-empire python scripts/check_parity.py
    python scripts/check_parity.py --parent /path/to/solo-empire

Exit codes: 0 in parity, 1 drift found, 2 parent checkout unavailable
(use ``--allow-missing`` to turn that into 0 for optional CI jobs).
"""

from __future__ import annotations

import argparse
import ast
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_DIR = REPO_ROOT / "src" / "data_lake"
PARENT_SUBDIR = Path("infra") / "scripts" / "data_lake"

BOOTSTRAP_NAMES = frozenset({"ROOT", "PROJECT_ROOT", "SCRIPTS_DIR", "UTILS_DIR"})

# (module, top-level name) -> reason. Keep this list short and reviewed.
ALLOWED_DIVERGENCE: dict[tuple[str, str], str] = {
    ("product_adapter", "_ensure_data_lake_on_path"): (
        "parent-only sys.path bootstrap; the package imports relatively"
    ),
    ("product_adapter", "load_ingest_runtime"): (
        "packaged loader works without a parent checkout"
    ),
    ("product_adapter", "load_query_runtime"): (
        "packaged loader works without a parent checkout"
    ),
    ("product_adapter", "load_iceberg_query_runtime"): (
        "packaged loader works without a parent checkout"
    ),
    ("product_adapter", "landing_object_bytes"): (
        "packaged replay uses ObjectStore directly (pass-1 P0 fix)"
    ),
    ("product_adapter", "_committed_bronze_keys"): (
        "loop variable renamed to avoid shadowing dataclasses.field"
    ),
}


def _is_docstring(node: ast.stmt) -> bool:
    return (
        isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    )


def _is_import_only(stmts: list[ast.stmt]) -> bool:
    for stmt in stmts:
        if isinstance(stmt, (ast.Import, ast.ImportFrom, ast.Pass)):
            continue
        if _is_bootstrap(stmt):
            continue
        return False
    return True


def _is_bootstrap(node: ast.stmt) -> bool:
    if isinstance(node, (ast.Import, ast.ImportFrom)):
        return True
    if isinstance(node, (ast.Assign, ast.AnnAssign)):
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        return all(isinstance(t, ast.Name) and t.id in BOOTSTRAP_NAMES for t in targets)
    if isinstance(node, ast.Expr) and "sys.path" in ast.unparse(node):
        return True
    if isinstance(node, ast.If):
        test = ast.unparse(node.test)
        if "sys.path" in test or "__package__" in test:
            return _is_import_only(node.body) and _is_import_only(node.orelse)
    if isinstance(node, ast.Try):
        handlers = [h.body for h in node.handlers]
        return _is_import_only(node.body) and all(_is_import_only(b) for b in handlers)
    return False


def _strip_docstrings(tree: ast.AST) -> ast.AST:
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module)):
            body = node.body
            if body and _is_docstring(body[0]):
                node.body = body[1:] or [ast.Pass()]
    return tree


def _normalize(source: str) -> tuple[dict[str, str], list[str]]:
    """Return ({top-level def name: dump}, [other statement dumps])."""
    tree = _strip_docstrings(ast.parse(source))
    defs: dict[str, str] = {}
    other: list[str] = []
    for node in tree.body:
        if _is_bootstrap(node):
            continue
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defs[node.name] = ast.dump(node, include_attributes=False)
        else:
            other.append(ast.dump(node, include_attributes=False))
    return defs, other


def compare_module(name: str, packaged: str, parent: str) -> list[str]:
    """Return human-readable drift findings for one module."""
    pkg_defs, pkg_other = _normalize(packaged)
    par_defs, par_other = _normalize(parent)
    findings: list[str] = []
    for def_name in sorted(set(pkg_defs) | set(par_defs)):
        if (name, def_name) in ALLOWED_DIVERGENCE:
            continue
        if def_name not in par_defs:
            findings.append(f"{name}.{def_name}: only in packaged runtime")
        elif def_name not in pkg_defs:
            findings.append(f"{name}.{def_name}: only in parent (missing from package)")
        elif pkg_defs[def_name] != par_defs[def_name]:
            findings.append(f"{name}.{def_name}: implementation differs")
    if pkg_other != par_other:
        findings.append(f"{name}: module-level statements differ")
    return findings


def find_parent(explicit: str | None) -> Path | None:
    candidate = explicit or os.environ.get("SOLO_EMPIRE_ROOT", "")
    if not candidate:
        return None
    parent = Path(candidate).expanduser().resolve() / PARENT_SUBDIR
    return parent if parent.is_dir() else None


def check(package_dir: Path, parent_dir: Path) -> list[str]:
    findings: list[str] = []
    for module_path in sorted(package_dir.glob("*.py")):
        parent_path = parent_dir / module_path.name
        name = module_path.stem
        if not parent_path.is_file():
            findings.append(f"{name}: missing from parent {parent_dir}")
            continue
        findings.extend(
            compare_module(
                name,
                module_path.read_text(encoding="utf-8"),
                parent_path.read_text(encoding="utf-8"),
            )
        )
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--parent", help="Solo Empire checkout (default: $SOLO_EMPIRE_ROOT)")
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="exit 0 when no parent checkout is available",
    )
    args = parser.parse_args(argv)
    parent_dir = find_parent(args.parent)
    if parent_dir is None:
        print(
            f"parity: parent checkout not found (set SOLO_EMPIRE_ROOT or --parent; "
            f"expected <root>/{PARENT_SUBDIR.as_posix()})",
            file=sys.stderr,
        )
        return 0 if args.allow_missing else 2
    findings = check(PACKAGE_DIR, parent_dir)
    if findings:
        print(f"parity: {len(findings)} drift finding(s) vs {parent_dir}")
        for finding in findings:
            print(f"  - {finding}")
        return 1
    print(f"parity: ok ({len(list(PACKAGE_DIR.glob('*.py')))} modules vs {parent_dir})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
