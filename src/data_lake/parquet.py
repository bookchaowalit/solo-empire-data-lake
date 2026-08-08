"""Stable Bronze envelope and Parquet writer.

PyArrow is intentionally optional at import time so source adapters can still
run metadata-only checks in a minimal environment.  An actual ingest refuses
to proceed without it rather than silently writing JSON or CSV as "Parquet".
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


BRONZE_COLUMNS = (
    "event_id",
    "source",
    "source_record_id",
    "domain",
    "dataset",
    "schema_version",
    "received_at",
    "event_time",
    "content_type",
    "raw_object_key",
    "raw_sha256",
    "payload_json",
    "metadata_json",
    "ingest_run_id",
    "privacy_class",
    "retention_class",
)


class ParquetUnavailable(RuntimeError):
    """Raised when the Parquet runtime dependency is not installed."""


def _pyarrow():
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except ImportError as exc:
        raise ParquetUnavailable(
            "Parquet ingest requires PyArrow; install with "
            "python3 -m pip install -r infra/requirements-data-lake.txt"
        ) from exc
    return pa, pq


def write_bronze_parquet(records: list[dict[str, Any]], destination: Path) -> None:
    """Write records using the versioned, all-string Bronze envelope schema."""
    pa, pq = _pyarrow()
    schema = pa.schema([(column, pa.string()) for column in BRONZE_COLUMNS])
    arrays = [
        pa.array([record.get(column) for record in records], type=pa.string())
        for column in BRONZE_COLUMNS
    ]
    table = pa.Table.from_arrays(arrays, schema=schema)
    destination.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, destination, compression="zstd", version="2.6")
