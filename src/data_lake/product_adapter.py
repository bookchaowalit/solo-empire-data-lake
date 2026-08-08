"""Shared lake-first adapter for Solo Empire data products.

Market data products (crypto, stock, fx, …) must not invent per-repo ingest
paths. They share this adapter:

    exact source bytes → landing → Bronze Parquet → manifest
    → DuckDB API projection
    → optional local CSV (CLI only)

PostgreSQL/ClickHouse stay multi-engine lab projections only.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence
from urllib.parse import unquote, urlparse


class LakeUnavailable(RuntimeError):
    """Raised when the Solo Empire data-lake runtime cannot be loaded."""


class LakeIngestError(RuntimeError):
    """Raised when lake ingest fails after the runtime loaded."""


@dataclass(frozen=True)
class LakeProductContract:
    """Per-product lake registration (not product HTTP envelope schema)."""

    source: str
    domain: str
    product_schema_version: str
    privacy_class: str = "public"
    retention_class: str = "operational"
    bronze_schema_version: str = "1"
    project_root: Optional[Path] = None
    data_lake_uri: str = ""
    solo_empire_root: str = ""
    lineage_filename: str = "lake_lineage.json"
    datasets: tuple[str, ...] = field(default_factory=tuple)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def find_solo_empire_root(
    start: Optional[Path] = None,
    *,
    solo_empire_root: str = "",
) -> Optional[Path]:
    env = (solo_empire_root or os.environ.get("SOLO_EMPIRE_ROOT") or "").strip()
    if env:
        candidate = Path(env).expanduser().resolve()
        if (candidate / "infra" / "scripts" / "data_lake" / "ingest.py").is_file():
            return candidate

    cur = (start or Path.cwd()).resolve()
    for parent in [cur, *cur.parents]:
        if (parent / "infra" / "scripts" / "data_lake" / "ingest.py").is_file():
            return parent
    return None


def default_data_lake_uri(
    *,
    data_lake_uri: str = "",
    project_root: Optional[Path] = None,
    solo_empire_root: str = "",
) -> str:
    explicit = (
        data_lake_uri
        or os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI")
        or os.environ.get("DATA_LAKE_URI")
        or ""
    ).strip()
    if explicit:
        return explicit
    root = find_solo_empire_root(
        project_root, solo_empire_root=solo_empire_root
    )
    if root is not None:
        return str((root / "data" / "lake").resolve())
    base = project_root or Path.cwd()
    return str((base / "data" / "lake").resolve())


def _ensure_data_lake_on_path(solo_root: Path) -> None:
    scripts = str(solo_root / "infra" / "scripts")
    if scripts not in sys.path:
        sys.path.insert(0, scripts)


def load_ingest_runtime(start: Optional[Path] = None, *, solo_empire_root: str = ""):
    try:
        from .ingest import ingest_payload
        from .parquet import ParquetUnavailable
        from .storage import StorageError
    except ImportError as exc:
        raise LakeUnavailable(
            "data_lake package is not importable. Install the Bronze/Parquet "
            "dependencies (at least pyarrow)."
        ) from exc
    runtime_root = find_solo_empire_root(start, solo_empire_root=solo_empire_root)
    return ingest_payload, ParquetUnavailable, StorageError, runtime_root


def load_query_runtime(start: Optional[Path] = None, *, solo_empire_root: str = ""):
    try:
        from .query import DuckDBUnavailable, query_parquet
    except ImportError as exc:
        raise LakeUnavailable(
            "data_lake.query is not importable. Install DuckDB for Parquet reads."
        ) from exc
    runtime_root = find_solo_empire_root(start, solo_empire_root=solo_empire_root)
    return query_parquet, DuckDBUnavailable, runtime_root


def load_iceberg_query_runtime(
    start: Optional[Path] = None, *, solo_empire_root: str = ""
):
    try:
        from .iceberg import IcebergUnavailable, iceberg_table_location
        from .query import DuckDBUnavailable, query_iceberg
        from .storage import StorageError
    except ImportError as exc:
        raise LakeUnavailable(
            "Iceberg query runtime is not importable. Install the optional "
            "PyIceberg dependencies."
        ) from exc
    runtime_root = find_solo_empire_root(start, solo_empire_root=solo_empire_root)
    return (
        iceberg_table_location,
        query_iceberg,
        IcebergUnavailable,
        DuckDBUnavailable,
        StorageError,
        runtime_root,
    )


def resolve_lake_root(data_lake_uri: str) -> Path:
    uri = data_lake_uri.strip()
    if uri.startswith("s3://"):
        raise LakeUnavailable(
            "This operation requires a local lake path; use read_bronze_rows() "
            "for direct s3:// Bronze reads"
        )
    if uri.startswith("file://"):
        parsed = urlparse(uri)
        path = unquote(parsed.path)
        if parsed.netloc and parsed.netloc not in {"", "localhost"}:
            path = f"/{parsed.netloc}{path}"
        return Path(path or ".").expanduser().resolve()
    return Path(uri).expanduser().resolve()


def is_remote_lake_uri(data_lake_uri: str) -> bool:
    """Return whether a lake URI is an S3-compatible object-store location."""
    parsed = urlparse((data_lake_uri or "").strip())
    return parsed.scheme.lower() == "s3" and bool(parsed.netloc)


def remote_bronze_dataset_glob(
    contract: LakeProductContract,
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
) -> str:
    """Build a bounded DuckDB glob for Bronze parts in an S3-compatible lake.

    The ingest contract writes one partition below ``ingest_date`` and
    ``source``. Restricting the glob to those known partition levels avoids
    scanning landing, manifests, or another dataset.
    """
    uri = data_lake_uri or default_data_lake_uri(
        data_lake_uri=contract.data_lake_uri,
        project_root=contract.project_root,
        solo_empire_root=contract.solo_empire_root,
    ).strip()
    if not is_remote_lake_uri(uri):
        raise LakeUnavailable("remote_bronze_dataset_glob requires an s3:// lake URI")
    return (
        f"{uri.rstrip('/')}/bronze/domain={contract.domain}/dataset={dataset}/"
        f"schema_version={contract.bronze_schema_version}/ingest_date=*/"
        "source=*/part-*.parquet"
    )


def bronze_dataset_dir(
    contract: LakeProductContract,
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
) -> Path:
    uri = data_lake_uri or default_data_lake_uri(
        data_lake_uri=contract.data_lake_uri,
        project_root=contract.project_root,
        solo_empire_root=contract.solo_empire_root,
    )
    root = resolve_lake_root(uri)
    return (
        root
        / "bronze"
        / f"domain={contract.domain}"
        / f"dataset={dataset}"
        / f"schema_version={contract.bronze_schema_version}"
    )


def list_bronze_parquet_files(
    contract: LakeProductContract,
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
) -> list[Path]:
    base = bronze_dataset_dir(contract, dataset, data_lake_uri=data_lake_uri)
    if not base.exists():
        return []
    return sorted(path for path in base.rglob("*.parquet") if path.is_file())


def ingest_to_lake(
    contract: LakeProductContract,
    *,
    raw: bytes,
    records: list[dict[str, Any]],
    dataset: str,
    data_lake_uri: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
    content_type: str = "application/json",
    input_format: str = "json",
    provider: str = "",
) -> dict[str, Any]:
    """Persist exact source bytes + Bronze rows. No local projection."""
    if not records:
        raise LakeIngestError("Refusing lake ingest with zero records")

    ingest_payload, ParquetUnavailable, StorageError, solo_root = load_ingest_runtime(
        contract.project_root,
        solo_empire_root=contract.solo_empire_root,
    )
    uri = data_lake_uri or default_data_lake_uri(
        data_lake_uri=contract.data_lake_uri,
        project_root=contract.project_root,
        solo_empire_root=contract.solo_empire_root,
    )
    meta = {
        "adapter": contract.source,
        "product_schema_version": contract.product_schema_version,
    }
    if provider:
        meta["provider"] = provider
    if metadata:
        meta.update(metadata)

    try:
        result = ingest_payload(
            raw,
            records,
            source=contract.source,
            domain=contract.domain,
            dataset=dataset,
            data_lake_uri=uri,
            input_format=input_format,
            schema_version=contract.bronze_schema_version,
            source_record_id_field="id",
            event_time_field="event_time",
            content_type=content_type,
            metadata=meta,
            privacy_class=contract.privacy_class,
            retention_class=contract.retention_class,
        )
    except ParquetUnavailable as exc:
        raise LakeUnavailable(str(exc)) from exc
    except StorageError as exc:
        raise LakeIngestError(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise LakeIngestError(f"Lake ingest failed: {exc}") from exc
    return result


def write_lineage(
    contract: LakeProductContract,
    result: dict[str, Any],
    *,
    dataset: str,
    data_dir: Path,
) -> Path:
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / contract.lineage_filename
    previous: dict[str, Any] = {}
    if path.exists():
        try:
            previous = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            previous = {}

    datasets = previous.get("datasets") if isinstance(previous.get("datasets"), dict) else {}
    datasets[dataset] = {
        "run_id": result.get("run_id"),
        "record_count": result.get("record_count"),
        "raw_key": result.get("raw_key"),
        "bronze_key": result.get("bronze_key"),
        "manifest_key": result.get("manifest_key"),
        "data_lake": result.get("data_lake"),
        "ingested_at": utc_now_iso(),
    }
    body = {
        "source": contract.source,
        "domain": contract.domain,
        "schema_version": contract.bronze_schema_version,
        "product_schema_version": contract.product_schema_version,
        "updated_at": utc_now_iso(),
        "datasets": datasets,
    }
    path.write_text(json.dumps(body, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def load_lineage(data_dir: Path, *, filename: str = "lake_lineage.json") -> Optional[dict[str, Any]]:
    path = Path(data_dir) / filename
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def read_bronze_rows(
    contract: LakeProductContract,
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
    sql: str | None = None,
) -> list[dict[str, Any]]:
    uri = data_lake_uri or default_data_lake_uri(
        data_lake_uri=contract.data_lake_uri,
        project_root=contract.project_root,
        solo_empire_root=contract.solo_empire_root,
    )
    remote = is_remote_lake_uri(uri)
    if remote:
        paths: str | list[str] = remote_bronze_dataset_glob(
            contract, dataset, data_lake_uri=uri
        )
    else:
        files = list_bronze_parquet_files(contract, dataset, data_lake_uri=uri)
        if not files:
            return []
        paths = [str(path) for path in files]

    query_parquet, DuckDBUnavailable, _solo = load_query_runtime(
        contract.project_root,
        solo_empire_root=contract.solo_empire_root,
    )
    statement = sql or (
        "SELECT event_id, source, source_record_id, domain, dataset, "
        "schema_version, received_at, event_time, payload_json, "
        "metadata_json, ingest_run_id, raw_object_key "
        "FROM lake_table "
        "ORDER BY event_time NULLS LAST, received_at NULLS LAST, event_id"
    )
    try:
        return query_parquet(paths, statement)
    except DuckDBUnavailable as exc:
        raise LakeUnavailable(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise LakeIngestError(f"Bronze DuckDB query failed for {dataset}: {exc}") from exc


def read_iceberg_rows(
    contract: LakeProductContract,
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
    sql: str | None = None,
) -> list[dict[str, Any]]:
    """Read a registered Bronze table through the Iceberg catalog + DuckDB.

    ``data_lake_uri`` remains part of the product contract for parity with the
    Parquet path, but the REST catalog owns the actual table location. The
    catalog URI, warehouse, and token are read only from the runtime secret
    provider and are never written to a product response or manifest.
    """
    del data_lake_uri
    catalog_uri = os.environ.get("ICEBERG_CATALOG_URI", "").strip()
    warehouse_uri = os.environ.get("ICEBERG_WAREHOUSE_URI", "").strip()
    if not catalog_uri or not warehouse_uri:
        raise LakeUnavailable(
            "Iceberg read mode requires ICEBERG_CATALOG_URI and "
            "ICEBERG_WAREHOUSE_URI in the runtime secret provider"
        )

    (
        iceberg_table_location,
        query_iceberg,
        IcebergUnavailable,
        DuckDBUnavailable,
        StorageError,
        _solo,
    ) = load_iceberg_query_runtime(
        contract.project_root,
        solo_empire_root=contract.solo_empire_root,
    )
    statement = sql or (
        "SELECT event_id, source, source_record_id, domain, dataset, "
        "schema_version, received_at, event_time, payload_json, "
        "metadata_json, ingest_run_id, raw_object_key "
        "FROM lake_table "
        "ORDER BY event_time NULLS LAST, received_at NULLS LAST, event_id"
    )
    try:
        resolved = iceberg_table_location(
            catalog_uri=catalog_uri,
            warehouse_uri=warehouse_uri,
            domain=contract.domain,
            dataset=dataset,
        )
        return query_iceberg(resolved["table_location"], statement)
    except (IcebergUnavailable, DuckDBUnavailable, StorageError) as exc:
        raise LakeUnavailable(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise LakeIngestError(
            f"Iceberg DuckDB query failed for {dataset}: {exc}"
        ) from exc


def parse_payload_json(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("payload_json")
    if raw is None or raw == "":
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        value = json.loads(str(raw))
    except (json.JSONDecodeError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}


def select_latest_bronze_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("source_record_id") or "").strip()
        if not key:
            payload = parse_payload_json(row)
            key = str(payload.get("id") or "").strip()
        if not key:
            key = str(row.get("event_id") or "")
        previous = best.get(key)
        if previous is None:
            best[key] = row
            continue
        prev_rank = (
            str(previous.get("event_time") or ""),
            str(previous.get("received_at") or ""),
            str(previous.get("event_id") or ""),
        )
        cur_rank = (
            str(row.get("event_time") or ""),
            str(row.get("received_at") or ""),
            str(row.get("event_id") or ""),
        )
        if cur_rank >= prev_rank:
            best[key] = row
    return [best[key] for key in sorted(best)]


def landing_object_bytes(data_lake_uri: str, raw_key: str) -> bytes:
    """Replay exact landing bytes for a raw_object_key (offline proof)."""
    from .storage import ObjectStore, StorageError

    try:
        return ObjectStore(data_lake_uri).get_bytes(raw_key)
    except StorageError as exc:
        raise LakeIngestError(str(exc)) from exc
