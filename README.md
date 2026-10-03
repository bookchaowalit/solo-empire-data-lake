# solo-empire-data-lake

Public shared runtime for Solo Empire's lake-first data products.

The runtime owns the neutral data-plane primitives used by source products:

```text
exact source bytes → landing → Bronze Parquet → manifest/lineage
                                      ↓
                              DuckDB read path
```

It does not own a product API, scraper, credentials, or application database.
Product repositories depend on this package and keep domain normalization in
their own boundary.

## Install

```bash
python -m pip install -e ".[lake]"
```

For a remote checkout, pin a commit or release tag rather than installing an
unpinned branch in production:

```bash
python -m pip install \
  "solo-empire-data-lake[lake] @ https://github.com/bookchaowalit/solo-empire-data-lake/archive/<commit>.tar.gz"
```

`[bronze]` installs PyArrow for landing/Bronze writes. `[query]` installs
DuckDB for Parquet reads. `[iceberg]` is optional and is not required by the
default Bronze API path.

## Boundaries

- Local files use a filesystem lake URI.
- Hosted reads use `s3://` plus DuckDB `httpfs` and runtime-injected R2/S3
  credentials.
- S3 writes use the AWS CLI; credentials are never accepted in object keys,
  manifests, logs, or this repository.
- Parquet is the interchange format; DuckDB is a query engine, not the source
  of truth.
- Iceberg registration is optional and remains behind the explicit catalog
  path.

## Verification

```bash
ruff check src tests scripts
python -m unittest discover -s tests -v
python -m compileall -q src
```

`tests/test_standalone_parity.py` pins the module surface that the
`book-*-data` product repositories import (`product_adapter`,
`product_store`, `silver`, `storage`) and runs it with no parent Solo Empire
checkout on disk.

### Parity with the parent monorepo

The modules in `src/data_lake` are a portable copy of the parent Solo Empire
`infra/scripts/data_lake` modules. Check for drift with:

```bash
SOLO_EMPIRE_ROOT=/path/to/solo-empire python scripts/check_parity.py
```

The check compares module ASTs, ignoring import style, `sys.path` bootstrap,
and docstrings; reviewed divergences are listed in `ALLOWED_DIVERGENCE` in the
script. Exit code 1 means drift, 2 means no parent checkout was found. With
`SOLO_EMPIRE_ROOT` set, `tests/test_parity_check.py` also runs the live check.

### Source ownership and updates

For the nine shared modules currently in `src/data_lake` (including
`__init__.py`), the parent `infra/scripts/data_lake` is the canonical source
for shared algorithms and contracts. This repository owns the installable
runtime, packaging, standalone tests, relative imports and the reviewed
standalone adapters in `ALLOWED_DIVERGENCE`. Parent-only orchestration and
pipeline modules are outside the package and this parity check's coverage.

Use this reviewed update procedure:

1. Change the shared algorithm in the parent and run its focused lake tests.
2. Port that change to the matching package module, preserving the standalone
   imports and adapters. Do not copy the parent directory wholesale. If a fix
   starts in this package, port the shared behavior back to the parent before
   treating the update as complete.
3. From this package checkout, run the existing drift and standalone checks:

   ```bash
   python scripts/check_parity.py --parent /path/to/solo-empire
   SOLO_EMPIRE_ROOT=/path/to/solo-empire PYTHONPATH=src \
     python -m unittest discover -s tests -v
   ruff check src tests scripts
   python -m compileall -q src
   ```

4. Review and commit scoped changes separately in each repository. Record both
   commits, then manually update consumers to the tested package commit or
   release and run their contract tests. Never replace a pinned dependency
   with `main`.

There is no automatic synchronization or consumer-pin updater. The parity
check reports differences; it does not copy code, verify bootstrap imports,
publish a package or prove consumer compatibility. Add an allowed divergence
only for a reviewed package boundary, with its reason and standalone tests.

The package is intentionally small enough for a solo local machine while
remaining installable by public product repositories and hosted containers.
