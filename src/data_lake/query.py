#!/usr/bin/env python3
"""Read Parquet-backed lake datasets with DuckDB.

DuckDB is a compute/query layer here. Bronze/Silver/Gold Parquet files and
optional Iceberg tables stay in Object Storage and can be consumed by other
compatible engines.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Sequence
from urllib.parse import urlparse


class DuckDBUnavailable(RuntimeError):
    """Raised when the optional DuckDB runtime is not installed."""


def _sql_string_literal(value: str) -> str:
    """Quote a path for DuckDB table-function syntax.

    DuckDB does not allow prepared parameters in the relation argument of
    ``iceberg_scan`` / ``read_parquet``.  Doubling single quotes keeps the
    user/provider path a value rather than executable SQL while preserving
    parameter binding for predicates below.
    """
    return "'" + value.replace("'", "''") + "'"


def _duckdb():
    try:
        import duckdb  # type: ignore
    except ImportError as exc:
        raise DuckDBUnavailable(
            "DuckDB query support requires DuckDB; install with "
            "python3 -m pip install -r infra/requirements-data-lake.txt"
        ) from exc
    return duckdb


def _s3_settings() -> dict[str, str | bool]:
    """Return DuckDB httpfs settings for an S3-compatible endpoint.

    DuckDB defaults to AWS virtual-host addressing. R2 and MinIO commonly use
    path-style addressing, so the endpoint must be mapped explicitly rather
    than relying on the AWS hostname convention.
    """
    endpoint = os.environ.get("DATA_LAKE_S3_ENDPOINT") or os.environ.get(
        "AWS_ENDPOINT_URL"
    )
    if not endpoint:
        return {}
    parsed = urlparse(endpoint)
    if not parsed.hostname:
        raise ValueError("DATA_LAKE_S3_ENDPOINT must contain a hostname")
    host = parsed.hostname
    if parsed.port:
        host = f"{host}:{parsed.port}"
    settings: dict[str, str | bool] = {
        "s3_endpoint": host,
        "s3_url_style": "path",
        "s3_use_ssl": parsed.scheme.lower() != "http",
        "s3_region": os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "auto",
    }
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        settings["s3_access_key_id"] = os.environ["AWS_ACCESS_KEY_ID"]
    if os.environ.get("AWS_SECRET_ACCESS_KEY"):
        settings["s3_secret_access_key"] = os.environ["AWS_SECRET_ACCESS_KEY"]
    return settings


def _configure_s3(connection) -> None:
    """Configure DuckDB httpfs for R2/MinIO without logging credentials."""
    settings = _s3_settings()
    if not settings:
        return
    try:
        connection.execute("LOAD httpfs")
    except Exception:
        connection.execute("INSTALL httpfs")
        connection.execute("LOAD httpfs")
    for name, value in settings.items():
        if isinstance(value, bool):
            connection.execute(f"SET {name} = {'true' if value else 'false'}")
        else:
            connection.execute(f"SET {name} = ?", [value])


def _connection(*, enable_iceberg: bool = False):
    duckdb = _duckdb()
    connection = duckdb.connect(":memory:")
    _configure_s3(connection)
    if enable_iceberg:
        connection.execute("INSTALL iceberg")
        connection.execute("LOAD iceberg")
        # Path-based scans do not have a catalog pointer from which DuckDB can
        # resolve the current metadata file. The table location is produced by
        # our Iceberg registration step, so asking DuckDB to resolve the latest
        # version here is safe for this single-reader projection path. Shared
        # multi-writer services should use a catalog-aware connection instead.
        connection.execute("SET unsafe_enable_version_guessing = true")
    return connection


def _fetch_dicts(cursor) -> list[dict[str, Any]]:
    columns = [str(item[0]) for item in cursor.description or []]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def query_parquet(
    paths: str | Sequence[str],
    sql: str = "SELECT * FROM lake_table LIMIT 20",
    parameters: list[Any] | None = None,
) -> list[dict[str, Any]]:
    """Expose one or more Parquet files/globs as ``lake_table`` and query them.

    Prefer this helper for Bronze/Silver/Gold parts that are not registered in
    Iceberg yet. ``paths`` may be a single glob/file or an explicit list of
    files. The SQL string must only reference ``lake_table``.
    """
    if isinstance(paths, str):
        if not paths.strip():
            raise ValueError("Parquet path/glob must not be empty")
        path_sql = _sql_string_literal(paths)
    else:
        cleaned = [str(path).strip() for path in paths if str(path).strip()]
        if not cleaned:
            raise ValueError("Parquet path list must not be empty")
        path_sql = "[" + ", ".join(_sql_string_literal(path) for path in cleaned) + "]"

    connection = _connection(enable_iceberg=False)
    try:
        connection.execute(
            "CREATE VIEW lake_table AS SELECT * FROM read_parquet("
            + path_sql
            + ", union_by_name=true)"
        )
        cursor = connection.execute(sql, parameters or [])
        return _fetch_dicts(cursor)
    finally:
        connection.close()


def query_iceberg(table_location: str, sql: str, parameters: list[Any] | None = None) -> list[dict[str, Any]]:
    """Expose one Iceberg table as ``lake_table`` and execute a read query."""
    connection = _connection(enable_iceberg=True)
    try:
        connection.execute(
            "CREATE VIEW lake_table AS SELECT * FROM iceberg_scan("
            + _sql_string_literal(table_location)
            + ")",
        )
        cursor = connection.execute(sql, parameters or [])
        return _fetch_dicts(cursor)
    finally:
        connection.close()


def read_ingest_run(table_location: str, run_id: str) -> list[dict[str, Any]]:
    """Read only the rows belonging to one idempotent lake ingest run."""
    return query_iceberg(
        table_location,
        "SELECT * FROM lake_table WHERE ingest_run_id = ? ORDER BY event_id",
        [run_id],
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table-location", required=True)
    parser.add_argument("--sql", default="SELECT * FROM lake_table LIMIT 20")
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        rows = query_iceberg(args.table_location, args.sql)
    except (DuckDBUnavailable, RuntimeError) as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    if args.as_json:
        print(json.dumps(rows, ensure_ascii=False, default=str))
    else:
        for row in rows:
            print(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
