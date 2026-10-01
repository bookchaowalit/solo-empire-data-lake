#!/usr/bin/env python3
"""Ingest one source payload into the Solo Empire object-storage data lake.

The command writes three immutable artifacts:

1. ``landing/`` — the exact source bytes;
2. ``bronze/`` — a stable Parquet envelope with one row per logical record;
3. ``control/manifests/`` — a small audit manifest for lineage and replay.

No SQLite or downstream Ops table is written here.  Consumers may project the
Bronze data into Silver/Gold datasets or operational tables after this command
has completed successfully.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from data_lake.parquet import (  # type: ignore
        BRONZE_COLUMNS,
        ParquetUnavailable,
        write_bronze_parquet,
    )
    from data_lake.storage import ObjectNotFound, ObjectStore, StorageError  # type: ignore
    from data_lake.iceberg import IcebergUnavailable  # type: ignore
else:
    from .parquet import BRONZE_COLUMNS, ParquetUnavailable, write_bronze_parquet
    from .storage import ObjectNotFound, ObjectStore, StorageError
    from .iceberg import IcebergUnavailable


try:
    from _env import PROJECT_ROOT  # type: ignore
except ImportError:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]


DEFAULT_LAKE_URI = os.environ.get(
    "SOLO_EMPIRE_DATA_LAKE_URI",
    os.environ.get("DATA_LAKE_URI", str(PROJECT_ROOT / "data" / "lake")),
)


def slug(value: str) -> str:
    """Make user/source values safe for Hive-style object keys."""
    value = re.sub(r"[^A-Za-z0-9._=-]+", "-", str(value).strip().lower())
    value = re.sub(r"-+", "-", value).strip("-.")
    if not value or value in {".", ".."}:
        raise ValueError(f"Cannot use empty path component for {value!r}")
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _batch_id(
    raw_sha256: str,
    *,
    source: str,
    domain: str,
    dataset: str,
    schema_version: str,
    metadata: dict[str, Any],
) -> str:
    """Return an idempotent batch identity without cross-provider collisions.

    Older batches used the raw checksum alone. Keep that identity when no
    provider is declared, and for provider-tagged batches use the complete
    product/provider identity so two source adapters returning identical bytes
    can coexist under one product boundary.
    """
    provider = metadata.get("provider")
    if not provider:
        return raw_sha256[:24]
    identity = canonical_json(
        {
            "raw_sha256": raw_sha256,
            "source": source,
            "domain": domain,
            "dataset": dataset,
            "schema_version": schema_version,
            "provider": str(provider),
        }
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_payload(input_name: str, format_name: str) -> tuple[bytes, list[Any], str]:
    if input_name == "-":
        raw = sys.stdin.buffer.read()
        name = "stdin"
    else:
        path = Path(input_name)
        raw = path.read_bytes()
        name = path.name

    suffix = Path(name).suffix.lower().lstrip(".")
    selected = format_name if format_name != "auto" else suffix
    if selected in {"ndjson", "jsonl"}:
        # Split on CR/LF only: str.splitlines() also breaks on U+2028/U+2029/NEL,
        # which JSON allows unescaped inside strings (raw CR/LF it does not).
        # "utf-8-sig" drops a BOM that would make the first line invalid JSON.
        records = [
            json.loads(line)
            for line in re.split(r"\r\n|\r|\n", raw.decode("utf-8-sig"))
            if line.strip()
        ]
        return raw, records, "jsonl"
    if selected == "json":
        value = json.loads(raw.decode("utf-8-sig"))
        if isinstance(value, list):
            return raw, value, "json"
        return raw, [value], "json"
    if selected == "csv":
        # A BOM (Excel exports) would otherwise rename the first column to
        # "\ufeffid" and silently drop every source_record_id.
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig"), newline=""))
        return raw, list(reader), "csv"
    if selected in {"bin", "binary", ""}:
        return raw, [{"value": None}], "bin"
    raise ValueError(f"Unsupported input format {selected!r}")


def _record_value(record: Any, field: str | None) -> Any:
    if not field or not isinstance(record, dict):
        return None
    value: Any = record
    for part in field.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def _normalize_record(
    record: Any,
    *,
    source: str,
    domain: str,
    dataset: str,
    schema_version: str,
    received_at: str,
    event_time_field: str | None,
    source_record_id_field: str | None,
    content_type: str,
    raw_key: str,
    raw_sha256: str,
    run_id: str,
    metadata: dict[str, Any],
    privacy_class: str,
    retention_class: str,
    raw_input: bool,
) -> dict[str, str | None]:
    source_record_id_value = _record_value(record, source_record_id_field)
    source_record_id = None if source_record_id_value is None else str(source_record_id_value)
    payload_json = None if raw_input else canonical_json(record)
    identity = "\0".join([source, source_record_id or payload_json or raw_sha256])
    event_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    event_time_value = _record_value(record, event_time_field)
    event_time = None if event_time_value is None else str(event_time_value)
    return {
        "event_id": event_id,
        "source": source,
        "source_record_id": source_record_id,
        "domain": domain,
        "dataset": dataset,
        "schema_version": schema_version,
        "received_at": received_at,
        "event_time": event_time,
        "content_type": content_type,
        "raw_object_key": raw_key,
        "raw_sha256": raw_sha256,
        "payload_json": payload_json,
        "metadata_json": canonical_json(metadata),
        "ingest_run_id": run_id,
        "privacy_class": privacy_class,
        "retention_class": retention_class,
    }


def _ingest_payload(
    raw: bytes,
    source_records: list[Any],
    input_format: str,
    *,
    source_name: str,
    domain_name: str,
    dataset_name: str,
    data_lake_uri: str,
    schema_version_name: str = "1",
    source_record_id_field: str | None = "id",
    event_time_field: str | None = None,
    content_type_override: str | None = None,
    metadata: dict[str, Any] | None = None,
    privacy_class: str = "private",
    retention_class: str = "operational",
    iceberg_catalog_uri: str | None = None,
    iceberg_warehouse_uri: str | None = None,
) -> dict[str, Any]:
    raw_sha256 = hashlib.sha256(raw).hexdigest()
    source = slug(source_name)
    domain = slug(domain_name)
    dataset = slug(dataset_name)
    schema_version = slug(schema_version_name)
    metadata = metadata or {}
    legacy_batch_id = raw_sha256[:24]
    batch_id = _batch_id(
        raw_sha256,
        source=source,
        domain=domain,
        dataset=dataset,
        schema_version=schema_version,
        metadata=metadata,
    )
    extension = input_format or "bin"
    content_type = content_type_override or {
        "json": "application/json",
        "jsonl": "application/x-ndjson",
        "csv": "text/csv",
        "html": "text/html",
        "htm": "text/html",
        "bin": "application/octet-stream",
        "binary": "application/octet-stream",
    }.get(extension, "application/octet-stream")
    manifest_key = f"control/manifests/source={source}/batch_id={batch_id}.json"

    store = ObjectStore(data_lake_uri)
    existing_manifest: dict[str, Any] | None = None
    existing_manifest_bytes: bytes | None = None

    def read_manifest(key: str) -> tuple[dict[str, Any] | None, bytes | None]:
        try:
            body = store.get_bytes(key)
        except ObjectNotFound:
            return None, None
        try:
            return json.loads(body.decode("utf-8")), body
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise StorageError(f"Batch manifest is invalid: {key}") from exc

    existing_manifest, existing_manifest_bytes = read_manifest(manifest_key)
    if existing_manifest is None and batch_id != legacy_batch_id:
        # Preserve idempotency for provider-tagged batches written before the
        # provider-aware identity was introduced, but never reuse a legacy
        # manifest belonging to a different provider/source adapter.
        legacy_key = f"control/manifests/source={source}/batch_id={legacy_batch_id}.json"
        legacy_manifest, legacy_bytes = read_manifest(legacy_key)
        if legacy_manifest is not None:
            compatible = all(
                legacy_manifest.get(field) == value
                for field, value in {
                    "source": source,
                    "domain": domain,
                    "dataset": dataset,
                    "schema_version": schema_version,
                    "metadata": metadata,
                }.items()
            )
            compatible = compatible and legacy_manifest.get("raw", {}).get("sha256") == raw_sha256
            if compatible:
                batch_id = legacy_batch_id
                manifest_key = legacy_key
                existing_manifest = legacy_manifest
                existing_manifest_bytes = legacy_bytes

    checkpoint_key = f"control/checkpoints/source={source}/batch_id={batch_id}.json"
    checkpoint: dict[str, Any] | None = None
    try:
        checkpoint_bytes = store.get_bytes(checkpoint_key)
    except ObjectNotFound:
        # A missing manifest/checkpoint is the normal first-ingest path. Partial
        # runs are resumed from the immutable checkpoint written before parts.
        checkpoint = None
    else:
        try:
            checkpoint = json.loads(checkpoint_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise StorageError(f"Batch {batch_id} has an invalid ingest checkpoint") from exc
        if not isinstance(checkpoint, dict):
            raise StorageError(f"Batch {batch_id} has an invalid ingest checkpoint shape")
        if existing_manifest is None:
            expected_checkpoint = {
                "source": source,
                "domain": domain,
                "dataset": dataset,
                "schema_version": schema_version,
                "raw_sha256": raw_sha256,
                "privacy_class": privacy_class,
                "retention_class": retention_class,
                "metadata": metadata,
            }
            for field, value in expected_checkpoint.items():
                if checkpoint.get(field) != value:
                    raise StorageError(f"Batch {batch_id} checkpoint does not match {field}")
            for field in ("received_at", "run_id", "raw_key", "bronze_key"):
                if not isinstance(checkpoint.get(field), str) or not checkpoint[field]:
                    raise StorageError(f"Batch {batch_id} checkpoint is missing {field}")

    if existing_manifest is not None:
        expected = {
            "source": source,
            "domain": domain,
            "dataset": dataset,
            "schema_version": schema_version,
            "privacy_class": privacy_class,
            "retention_class": retention_class,
        }
        for field, value in expected.items():
            if existing_manifest.get(field) != value:
                raise StorageError(
                    f"Batch {batch_id} already exists with a different {field}; "
                    "use a new source batch instead of overwriting it"
                )
        stored_raw = existing_manifest.get("raw", {})
        if stored_raw.get("sha256") != raw_sha256:
            raise StorageError(f"Batch {batch_id} checksum does not match its manifest")
        if existing_manifest.get("metadata", {}) != metadata:
            raise StorageError(f"Batch {batch_id} already exists with different metadata")
        if iceberg_catalog_uri and "iceberg" not in existing_manifest:
            raise StorageError(
                f"Batch {batch_id} has a legacy manifest without Iceberg metadata; "
                "run a dedicated backfill before enabling Iceberg registration"
            )
        received_at = str(existing_manifest["received_at"])
        run_id = str(existing_manifest["run_id"])
        raw_key = str(stored_raw["key"])
        bronze_key = str(existing_manifest["bronze"]["key"])

        # A committed manifest freezes the normalized Bronze representation.
        # Some adapters derive a row event timestamp at capture time when the
        # provider payload has no timestamp of its own.  Re-normalizing the
        # same raw bytes on retry would then produce different Parquet bytes
        # and incorrectly turn an idempotent replay into an immutable conflict.
        # Reuse the committed objects after checking their integrity instead
        # of regenerating Bronze from a potentially time-dependent projection.
        raw_result = store.put_bytes(raw_key, raw, content_type=content_type)
        try:
            stored_bronze = store.get_bytes(bronze_key)
        except ObjectNotFound as exc:
            raise StorageError(
                f"Batch {batch_id} manifest exists but Bronze object is missing: {bronze_key}"
            ) from exc
        expected_bronze_sha = existing_manifest.get("bronze", {}).get("sha256")
        bronze_sha = hashlib.sha256(stored_bronze).hexdigest()
        if not isinstance(expected_bronze_sha, str) or bronze_sha != expected_bronze_sha:
            raise StorageError(f"Batch {batch_id} manifest Bronze checksum does not match its object")
        return {
            "status": "success",
            "data_lake": store.describe(),
            "run_id": run_id,
            "record_count": existing_manifest.get("record_count", 0),
            "raw_key": raw_key,
            "bronze_key": bronze_key,
            "manifest_key": manifest_key,
            "raw_existed": raw_result.existed,
            "bronze_existed": True,
            "manifest_existed": True,
            "checkpoint_key": checkpoint_key,
            "checkpoint_existed": checkpoint is not None,
            "iceberg": None,
        }
    else:
        if checkpoint is not None:
            received_at = str(checkpoint["received_at"])
            run_id = str(checkpoint["run_id"])
            raw_key = str(checkpoint["raw_key"])
            bronze_key = str(checkpoint["bronze_key"])
        else:
            received_at = utc_now()
            received_date = received_at[:10]
            run_id = f"{received_date}-{batch_id}"
            # Optional multi-provider partition keeps raw bytes separated before merge.
            provider_part = ""
            if isinstance(metadata, dict) and metadata.get("provider"):
                try:
                    provider_part = f"/provider={slug(str(metadata['provider']))}"
                except ValueError:
                    provider_part = ""
            raw_key = (
                f"landing/source={source}{provider_part}/batch_id={batch_id}/payload.{extension}"
            )
            bronze_key = (
                f"bronze/domain={domain}/dataset={dataset}/schema_version={schema_version}/"
                f"ingest_date={received_date}/source={source}/part-{batch_id}.parquet"
            )

    if checkpoint is not None:
        if checkpoint.get("input_format") != extension:
            raise StorageError(f"Batch {batch_id} checkpoint does not match input format")
        if checkpoint.get("content_type") != content_type:
            raise StorageError(f"Batch {batch_id} checkpoint does not match content type")
    # ``html`` stores exact browser/source bytes in landing while still writing
    # structured Bronze rows (payload_json) for DuckDB API consumers.
    raw_input = extension in {"bin", "binary"}
    records = [
        _normalize_record(
            record,
            source=source,
            domain=domain,
            dataset=dataset,
            schema_version=schema_version,
            received_at=received_at,
            event_time_field=event_time_field,
            source_record_id_field=source_record_id_field,
            content_type=content_type,
            raw_key=raw_key,
            raw_sha256=raw_sha256,
            run_id=run_id,
            metadata=metadata,
            privacy_class=privacy_class,
            retention_class=retention_class,
            raw_input=raw_input,
        )
        for record in source_records
    ]
    if not records:
        raise ValueError("Input contains no records")

    # Check the writer before putting raw data, so a missing Parquet runtime
    # cannot leave an apparently successful landing object behind.
    with tempfile.TemporaryDirectory(prefix="solo-empire-lake-") as temp_dir:
        parquet_path = Path(temp_dir) / "part.parquet"
        write_bronze_parquet(records, parquet_path)
        parquet_bytes = parquet_path.read_bytes()

    checkpoint_result = None
    if existing_manifest is None and checkpoint is None:
        checkpoint_payload = {
            "checkpoint_version": "1",
            "source": source,
            "domain": domain,
            "dataset": dataset,
            "schema_version": schema_version,
            "raw_sha256": raw_sha256,
            "privacy_class": privacy_class,
            "retention_class": retention_class,
            "metadata": metadata,
            "input_format": extension,
            "content_type": content_type,
            "received_at": received_at,
            "run_id": run_id,
            "raw_key": raw_key,
            "bronze_key": bronze_key,
        }
        # The checkpoint fixes all values that affect the Parquet bytes.  A
        # later retry can therefore resume after a raw/Bronze/manifest outage
        # without producing a conflicting part under the same immutable key.
        checkpoint_result = store.put_bytes(
            checkpoint_key,
            (canonical_json(checkpoint_payload) + "\n").encode("utf-8"),
            content_type="application/json",
        )

    raw_result = store.put_bytes(raw_key, raw, content_type=content_type)
    bronze_result = store.put_bytes(bronze_key, parquet_bytes, content_type="application/vnd.apache.parquet")
    iceberg_result: dict[str, Any] | None = None
    if iceberg_catalog_uri:
        from data_lake.iceberg import register_bronze_file

        iceberg_result = register_bronze_file(
            data_lake_uri=data_lake_uri,
            bronze_key=bronze_key,
            domain=domain,
            dataset=dataset,
            catalog_uri=iceberg_catalog_uri,
            warehouse_uri=iceberg_warehouse_uri,
            run_id=run_id,
        )
    if existing_manifest_bytes is not None:
        # Batch manifests are immutable audit evidence.  A later idempotent
        # retry may observe a newer Iceberg snapshot after another batch was
        # appended, so never regenerate this object's snapshot metadata.
        manifest_result = store.put_bytes(
            manifest_key,
            existing_manifest_bytes,
            content_type="application/json",
        )
    else:
        manifest = {
            "manifest_version": "1",
            "batch_id": batch_id,
            "run_id": run_id,
            "source": source,
            "domain": domain,
            "dataset": dataset,
            "schema_version": schema_version,
            "received_at": received_at,
            "record_count": len(records),
            "raw": {"key": raw_key, "sha256": raw_sha256, "bytes": len(raw)},
            "bronze": {
                "key": bronze_key,
                "sha256": bronze_result.sha256,
                "bytes": len(parquet_bytes),
                "columns": list(BRONZE_COLUMNS),
            },
            "privacy_class": privacy_class,
            "retention_class": retention_class,
            "metadata": metadata,
        }
        if iceberg_result:
            manifest["iceberg"] = {
                # ``metadata_location`` is a catalog pointer, not immutable
                # batch lineage. Keep it in the in-process result for exact
                # reads, but do not persist a mutable catalog pointer.
                key: value
                for key, value in iceberg_result.items()
                if key not in {"status", "metadata_location"}
            }
        manifest["checkpoint_key"] = checkpoint_key
        manifest_result = store.put_bytes(
            manifest_key,
            (canonical_json(manifest) + "\n").encode("utf-8"),
            content_type="application/json",
        )
    return {
        "status": "success",
        "data_lake": store.describe(),
        "run_id": run_id,
        "record_count": len(records),
        "raw_key": raw_result.key,
        "bronze_key": bronze_result.key,
        "manifest_key": manifest_result.key,
        "raw_existed": raw_result.existed,
        "bronze_existed": bronze_result.existed,
        "manifest_existed": manifest_result.existed,
        "checkpoint_key": checkpoint_key,
        "checkpoint_existed": bool(
            checkpoint_result.existed if checkpoint_result is not None else checkpoint is not None
        ),
        "iceberg": iceberg_result,
    }


def ingest_payload(
    raw: bytes,
    records: list[Any],
    *,
    source: str,
    domain: str,
    dataset: str,
    data_lake_uri: str,
    input_format: str = "json",
    schema_version: str = "1",
    source_record_id_field: str | None = "id",
    event_time_field: str | None = None,
    content_type: str | None = None,
    metadata: dict[str, Any] | None = None,
    privacy_class: str = "private",
    retention_class: str = "operational",
    iceberg_catalog_uri: str | None = None,
    iceberg_warehouse_uri: str | None = None,
) -> dict[str, Any]:
    """Ingest exact source bytes plus normalized Bronze records.

    Prefer this helper when a producer still has the original provider payload
    (API body, file bytes). Landing stores ``raw`` immutably; Bronze rows come
    from ``records``. Local CSV/SQLite projections must run only after this
    returns successfully.
    """
    return _ingest_payload(
        raw,
        records,
        input_format,
        source_name=source,
        domain_name=domain,
        dataset_name=dataset,
        data_lake_uri=data_lake_uri,
        schema_version_name=schema_version,
        source_record_id_field=source_record_id_field,
        event_time_field=event_time_field,
        content_type_override=content_type,
        metadata=metadata,
        privacy_class=privacy_class,
        retention_class=retention_class,
        iceberg_catalog_uri=iceberg_catalog_uri,
        iceberg_warehouse_uri=iceberg_warehouse_uri,
    )


def ingest_records(
    records: list[dict[str, Any]],
    *,
    source: str,
    domain: str,
    dataset: str,
    data_lake_uri: str,
    schema_version: str = "1",
    source_record_id_field: str | None = "id",
    event_time_field: str | None = None,
    metadata: dict[str, Any] | None = None,
    privacy_class: str = "private",
    retention_class: str = "operational",
    iceberg_catalog_uri: str | None = None,
    iceberg_warehouse_uri: str | None = None,
) -> dict[str, Any]:
    """Ingest normalized records from another source adapter.

    Landing bytes are canonical JSONL of ``records``. When the exact upstream
    payload is available, call :func:`ingest_payload` instead.
    """
    raw = b"".join(
        (canonical_json(record) + "\n").encode("utf-8") for record in records
    )
    return ingest_payload(
        raw,
        records,
        source=source,
        domain=domain,
        dataset=dataset,
        data_lake_uri=data_lake_uri,
        input_format="jsonl",
        schema_version=schema_version,
        source_record_id_field=source_record_id_field,
        event_time_field=event_time_field,
        content_type="application/x-ndjson",
        metadata=metadata,
        privacy_class=privacy_class,
        retention_class=retention_class,
        iceberg_catalog_uri=iceberg_catalog_uri,
        iceberg_warehouse_uri=iceberg_warehouse_uri,
    )


def ingest(args: argparse.Namespace) -> dict[str, Any]:
    raw, source_records, input_format = _read_payload(args.input, args.format)
    try:
        metadata = json.loads(args.metadata) if args.metadata else {}
    except json.JSONDecodeError as exc:
        raise ValueError("--metadata must be a JSON object") from exc
    if not isinstance(metadata, dict):
        raise ValueError("--metadata must be a JSON object")
    return _ingest_payload(
        raw,
        source_records,
        input_format,
        source_name=args.source,
        domain_name=args.domain,
        dataset_name=args.dataset,
        data_lake_uri=args.data_lake_uri,
        schema_version_name=args.schema_version,
        source_record_id_field=args.source_record_id_field,
        event_time_field=args.event_time_field,
        content_type_override=args.content_type,
        metadata=metadata,
        privacy_class=args.privacy_class,
        retention_class=args.retention_class,
        iceberg_catalog_uri=args.iceberg_catalog_uri,
        iceberg_warehouse_uri=args.iceberg_warehouse_uri,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="JSON/JSONL/CSV/file path, or - for stdin")
    parser.add_argument("--source", required=True, help="Source adapter name, e.g. gmail or github")
    parser.add_argument("--domain", required=True, help="Owning Solo Empire domain")
    parser.add_argument("--dataset", required=True, help="Logical dataset name")
    parser.add_argument("--data-lake-uri", default=DEFAULT_LAKE_URI)
    parser.add_argument(
        "--format",
        choices=["auto", "json", "jsonl", "ndjson", "csv", "html", "binary"],
        default="auto",
    )
    parser.add_argument("--schema-version", default="1")
    parser.add_argument("--source-record-id-field", default="id")
    parser.add_argument("--event-time-field")
    parser.add_argument("--content-type")
    parser.add_argument("--metadata", help="JSON object attached to every Bronze row")
    parser.add_argument("--privacy-class", choices=["public", "internal", "private", "restricted"], default="private")
    parser.add_argument("--retention-class", default="operational")
    parser.add_argument(
        "--iceberg-catalog-uri",
        help="SQL catalog URI, e.g. sqlite:////absolute/path/lakehouse-catalog.db",
    )
    parser.add_argument(
        "--iceberg-warehouse-uri",
        help="Iceberg metadata warehouse URI; defaults to the data-lake URI",
    )
    parser.add_argument("--json", action="store_true", dest="as_json", help="Print a JSON summary")
    return parser


def main() -> int:
    parser = build_parser()
    try:
        args = parser.parse_args()
        result = ingest(args)
    except (ValueError, StorageError, ParquetUnavailable, IcebergUnavailable) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    if args.as_json:
        print(canonical_json(result))
    else:
        print(f"Data lake ingest complete: {result['record_count']} record(s)")
        print(f"  Bronze: {result['bronze_key']}")
        print(f"  Manifest: {result['manifest_key']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
