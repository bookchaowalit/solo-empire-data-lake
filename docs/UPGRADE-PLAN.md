# Upgrade plan

## Current state

Score: 8/10 (was 7/10 after pass 1, 5/10 before) — portable runtime works
without a parent checkout, drift against the parent is now machine-checked,
and the Iceberg read path has offline tests; S3 write paths are still untested.

## Backlog

- P1: Product repos (book-*-data) should add this package as a pinned `[lake]`
  extra and replace `find_solo_empire_root()`-based `skipUnless` guards with
  an import check, so lake tests run standalone.
- P1: Run `scripts/check_parity.py` in CI once the parent repo can be checked
  out from this repo's workflow (needs a read token; today it is local-only).
- P1: Offline tests for the S3 write path in `storage.py` (mock the AWS CLI
  subprocess; assert no credentials reach argv/logs).
- P1: Bump the pinned lake SHA in book-*-data to pick up the pass-3 fixes.
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

## Done in this pass (pass 2)

- Added `scripts/check_parity.py`: AST-level drift check of `src/data_lake`
  against `$SOLO_EMPIRE_ROOT/infra/scripts/data_lake` (ignores import style,
  `sys.path` bootstrap, docstrings; reviewed divergences allow-listed with
  reasons). Parent is in parity today (9 modules).
- `tests/test_parity_check.py`: synthetic-tree tests for the checker plus a
  live parent check when `SOLO_EMPIRE_ROOT` is set.
- `tests/test_iceberg_read.py`: offline tests for `read_iceberg_rows`
  (config guard, `metadata_location` forwarding, error mapping).
- CI/README lint `scripts/` too; README documents the parity check.

## Done in this pass (pass 3: edge cases)

- `ingest._read_payload`: NDJSON was split with `str.splitlines()`, so a JSON
  string holding a raw U+2028/U+2029/NEL (legal, and what
  `json.dumps(ensure_ascii=False)` emits) broke the line into invalid JSON; it
  now splits on CR/LF only. JSON/NDJSON/CSV inputs are decoded as `utf-8-sig`:
  a BOM made JSON unparseable and renamed the first CSV column to `﻿id`,
  silently dropping every `source_record_id`.
- `product_store.load_csv_projection`: same `splitlines()` bug turned one CSV
  cell containing U+2028 (left unquoted by `csv.writer`) into two rows; BOM
  header handled too.
- `product_store.get_record_from_payload`: the product APIs already
  percent-decode the path, and this decoded again, so ids containing `%`
  were unreachable; it now tries the id as given before the decoded form.
- The fixes were ported to the parent `infra/scripts/data_lake` copy (with
  `infra/tests/data_lake/input_edge_cases_test.py`), and the three temporary
  allow-list entries were removed from `scripts/check_parity.py`; live parity
  against the parent passes again. Remaining: bump the pinned lake SHA in
  book-*-data.
- Verified: `tests/test_input_edge_cases.py` (7 of 8 fail on the old code);
  full suite with `PYTHONPATH=src` and live parity; ruff 0.15.8 + 0.16.9.
