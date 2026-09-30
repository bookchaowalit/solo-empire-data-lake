# Upgrade plan

## Current state

Score: 7/10 (was 5/10) — portable runtime now works without a parent Solo
Empire checkout and matches the parent's `data_lake` surface used by the
book-*-data product repos; Iceberg/S3 paths still lack offline tests.

## Backlog

- P1: Product repos (book-*-data) should add this package as a pinned `[lake]`
  extra and replace `find_solo_empire_root()`-based `skipUnless` guards with
  an import check, so lake tests run standalone.
- P1: Add a parity check (script or CI job) that diffs `src/data_lake/*.py`
  against the parent `infra/scripts/data_lake/*.py` to catch drift early.
- P1: Offline tests for `read_iceberg_rows` using a mocked
  `iceberg_table_location` (assert `metadata_location` is forwarded).
- P2: `ingest.py` still tries `from _env import PROJECT_ROOT`; replace with an
  explicit env/arg so the packaged runtime has no hidden parent dependency.
- P2: Add type checking (mypy/pyright) once public signatures settle.

## Done in this pass

- Fixed P0: `landing_object_bytes` raised `TypeError` when no parent checkout
  existed (`_ensure_data_lake_on_path(None)`); it now uses the packaged
  `ObjectStore` directly. Regression tests added.
- Synced `silver.py` with the parent: `crypto_ohlcv` / `crypto_funding`
  normalizers, `NORMALIZERS` registry, and finite-number validation (NaN/inf
  prices and rates are now rejected).
- Synced `iceberg.py`: catalog-selected `metadata_location` is returned so
  readers do not guess the latest metadata file.
- Added import-surface tests pinning every name the product repos use.
- CI: Python 3.11 + 3.12 matrix, `ruff check`, read-only permissions.
