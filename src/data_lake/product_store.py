"""Shared Bronze→API store helpers for lake-first data products."""

from __future__ import annotations

import csv
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional, Sequence
from urllib.parse import unquote

from .product_adapter import (
    LakeIngestError,
    LakeProductContract,
    LakeUnavailable,
    bronze_dataset_dir,
    parse_payload_json,
    read_bronze_rows,
    read_iceberg_rows,
    select_latest_bronze_rows,
    default_data_lake_uri,
    is_remote_lake_uri,
)
from .silver import read_silver_rows


TIMESTAMP_KEYS = (
    "updated_at",
    "event_time",
    "scraped_at",
    "date",
    "timestamp",
    "retrieved_at",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_ts(value: str) -> Optional[datetime]:
    if not value:
        return None
    value = value.strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def make_record_id(row: dict[str, Any], *, id_fields: Sequence[str], id_sep: str = ":") -> str:
    parts = []
    for field in id_fields:
        raw = str(row.get(field, "")).strip()
        if not raw and field == "url":
            raw = str(row.get("name", "") or row.get("title", "")).strip()
        parts.append(raw)
    joined = id_sep.join(parts)
    if not joined or joined == id_sep * (len(parts) - 1):
        blob = json.dumps(row, sort_keys=True, default=str)
        return "hash:" + hashlib.sha1(blob.encode("utf-8")).hexdigest()[:16]
    if any(ch in joined for ch in ("://", "?", "#", " ")) or joined.count("/") > 0:
        return "hash:" + hashlib.sha1(joined.encode("utf-8")).hexdigest()[:16]
    return joined.replace("/", "_")


def freshness(
    rows: list[dict[str, Any]],
    *,
    stale_after_hours: float,
) -> tuple[str, Optional[str]]:
    if not rows:
        return "empty", None
    latest: Optional[datetime] = None
    latest_raw = None
    for row in rows:
        for key in TIMESTAMP_KEYS:
            if key in row and row[key]:
                parsed = parse_ts(str(row[key]))
                if parsed and (latest is None or parsed > latest):
                    latest = parsed
                    latest_raw = str(row[key])
                break
    if latest is None:
        return "ok", latest_raw
    age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600.0
    if age_hours > stale_after_hours:
        return "stale", latest_raw
    return "ok", latest_raw


def stringify(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def storage_model_for_source_kind(source_kind: Any, *, product: str = "") -> str:
    """Return a stable API metadata label for the selected lake read path."""
    kind = str(source_kind or "")
    if kind.startswith("silver_s3_"):
        return "lake_first_silver_s3_duckdb"
    if kind.startswith("silver_"):
        return "lake_first_silver_duckdb"
    if kind.startswith("iceberg_"):
        return "lake_first_iceberg_rest_duckdb"
    if kind.startswith("bronze_s3_parquet"):
        prefix = "lake_first" + (f"_{product}" if product else "")
        return f"{prefix}_bronze_s3_duckdb"
    return "lake_first_bronze_duckdb"


def silver_dataset_path(
    contract: LakeProductContract,
    dataset: str,
    *,
    data_lake_uri: str,
) -> str:
    """Return a stable Silver dataset label for local and remote lakes."""
    return (
        f"{data_lake_uri.rstrip('/')}/silver/domain={contract.domain}/"
        f"dataset={dataset}/schema_version=1"
    )


def silver_parity_report(
    bronze_items: list[dict[str, Any]],
    silver_items: list[dict[str, Any]],
    *,
    key_fields: Sequence[str] = ("source_record_id", "event_time"),
    compare_fields: Sequence[str] = (),
) -> dict[str, Any]:
    """Compare consumer-visible values while ignoring physical lineage fields."""
    def key(item: dict[str, Any]) -> str:
        return "|".join(stringify(item.get(field)) for field in key_fields)

    bronze = {key(item): item for item in bronze_items}
    silver = {key(item): item for item in silver_items}
    fields = tuple(compare_fields) or tuple(
        sorted(
            (set().union(*(item.keys() for item in bronze_items + silver_items)))
            - {
                "record_id",
                "ingest_run_id",
                "raw_object_key",
                "source_record_id",
                "event_time",
                "updated_at",
                "scraped_at",
            }
        )
    )
    added = sorted(set(silver) - set(bronze))
    removed = sorted(set(bronze) - set(silver))
    changed: list[dict[str, Any]] = []
    for item_key in sorted(set(bronze) & set(silver)):
        differences = {
            field: {
                "bronze": bronze[item_key].get(field),
                "silver": silver[item_key].get(field),
            }
            for field in fields
            if bronze[item_key].get(field) != silver[item_key].get(field)
        }
        if differences:
            changed.append({"key": item_key, "fields": differences})
    return {
        "status": "passed" if not added and not removed and not changed else "failed",
        "bronze_count": len(bronze_items),
        "silver_count": len(silver_items),
        "added_count": len(added),
        "removed_count": len(removed),
        "changed_count": len(changed),
        "examples": {
            "added": added[:5],
            "removed": removed[:5],
            "changed": changed[:5],
        },
    }


def load_silver_dataset(
    contract: LakeProductContract,
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
    latest_only: bool = False,
    item_builder: Callable[..., dict[str, Any]],
    stale_after_hours: float = 24.0,
) -> dict[str, Any]:
    """Read one shared Silver Parquet dataset and project it for an API."""
    uri = data_lake_uri or default_data_lake_uri(
        data_lake_uri=contract.data_lake_uri,
        project_root=contract.project_root,
        solo_empire_root=contract.solo_empire_root,
    )
    path = silver_dataset_path(contract, dataset, data_lake_uri=uri)
    source_kind = "silver_s3_parquet" if is_remote_lake_uri(uri) else "silver_parquet"
    try:
        rows = read_silver_rows(
            data_lake_uri=uri,
            domain=contract.domain,
            dataset=dataset,
            silver_schema_version="1",
        )
        if latest_only:
            rows = select_latest_bronze_rows(rows)
            items = [item_builder(row, history=False, history_idx=0) for row in rows]
        else:
            items = [
                item_builder(row, history=True, history_idx=idx)
                for idx, row in enumerate(rows)
            ]
        status, _ = freshness(items, stale_after_hours=stale_after_hours)
        return {
            "items": items,
            "data_status": status if items else "empty",
            "error": None,
            "path": path,
            "source_kind": source_kind,
            "read_mode": "parquet",
            "read_fallback": "error",
            "fallback_reason": None,
            "retrieved_at": utc_now_iso(),
        }
    except LakeUnavailable as exc:
        return {
            "items": [],
            "data_status": "error",
            "error": str(exc),
            "path": path,
            "source_kind": source_kind,
            "read_mode": "parquet",
            "read_fallback": "error",
            "fallback_reason": None,
            "retrieved_at": utc_now_iso(),
        }
    except LakeIngestError as exc:
        return {
            "items": [],
            "data_status": "malformed",
            "error": str(exc),
            "path": path,
            "source_kind": source_kind,
            "read_mode": "parquet",
            "read_fallback": "error",
            "fallback_reason": None,
            "retrieved_at": utc_now_iso(),
        }
    except Exception as exc:  # noqa: BLE001 - keep optional Silver reads fail-closed
        return {
            "items": [],
            "data_status": "malformed",
            "error": f"Silver DuckDB query failed: {type(exc).__name__}: {exc}",
            "path": path,
            "source_kind": source_kind,
            "read_mode": "parquet",
            "read_fallback": "error",
            "fallback_reason": None,
            "retrieved_at": utc_now_iso(),
        }


def load_layered_dataset(
    contract: LakeProductContract,
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
    latest_only: bool = False,
    bronze_item_builder: Callable[..., dict[str, Any]],
    silver_item_builder: Callable[..., dict[str, Any]],
    id_fields: Sequence[str],
    id_sep: str = ":",
    stale_after_hours: float = 24.0,
    read_mode: Optional[str] = None,
    read_fallback: Optional[str] = None,
    silver_read_mode: Optional[str] = None,
    compare_fields: Sequence[str] = (),
) -> dict[str, Any]:
    """Read Bronze by default, or run the guarded Bronze/Silver parity pilot."""
    mode = (silver_read_mode or os.environ.get("SILVER_READ_MODE", "bronze")).strip().lower()
    if mode not in {"bronze", "compare", "silver"}:
        return {
            "items": [],
            "data_status": "malformed",
            "error": "SILVER_READ_MODE must be one of: bronze, compare, silver",
            "path": silver_dataset_path(
                contract,
                dataset,
                data_lake_uri=data_lake_uri or default_data_lake_uri(
                    data_lake_uri=contract.data_lake_uri,
                    project_root=contract.project_root,
                    solo_empire_root=contract.solo_empire_root,
                ),
            ),
            "source_kind": "silver_parquet" if mode == "silver" else "bronze_parquet",
            "read_mode": read_mode or os.environ.get("LAKE_READ_MODE", "parquet"),
            "read_fallback": read_fallback or os.environ.get("LAKE_READ_FALLBACK", "error"),
            "fallback_reason": None,
            "silver_read_mode": mode,
            "silver_parity": None,
            "retrieved_at": utc_now_iso(),
        }

    bronze = load_bronze_dataset(
        contract,
        dataset,
        data_lake_uri=data_lake_uri,
        latest_only=latest_only,
        id_fields=id_fields,
        id_sep=id_sep,
        stale_after_hours=stale_after_hours,
        item_builder=bronze_item_builder,
        read_mode=read_mode,
        read_fallback=read_fallback,
    )
    if mode == "bronze":
        bronze.update({"silver_read_mode": mode, "silver_parity": None})
        return bronze

    if not latest_only:
        bronze.update({
            "silver_read_mode": mode,
            "silver_parity": {
                "status": "not_applicable",
                "reason": "current Silver contracts are snapshots; history remains Bronze",
                "bronze_count": len(bronze["items"]),
                "silver_count": 0,
            },
        })
        return bronze

    if bronze["data_status"] in {"error", "malformed"}:
        bronze.update({"silver_read_mode": mode, "silver_parity": None})
        return bronze

    silver = load_silver_dataset(
        contract,
        dataset,
        data_lake_uri=data_lake_uri,
        latest_only=True,
        item_builder=silver_item_builder,
        stale_after_hours=stale_after_hours,
    )
    if silver["data_status"] in {"error", "malformed"}:
        parity = {
            "status": "unavailable",
            "bronze_count": len(bronze["items"]),
            "silver_count": 0,
            "added_count": 0,
            "removed_count": 0,
            "changed_count": 0,
            "error": silver["error"],
        }
        if mode == "compare":
            bronze.update({"silver_read_mode": mode, "silver_parity": parity})
            return bronze
        silver.update({
            "data_status": "error",
            "error": f"Silver serving is unavailable: {silver['error']}",
            "silver_read_mode": mode,
            "silver_parity": parity,
        })
        return silver

    parity = silver_parity_report(
        bronze["items"],
        silver["items"],
        key_fields=("source_record_id", "event_time"),
        compare_fields=compare_fields,
    )
    if mode == "compare":
        bronze.update({"silver_read_mode": mode, "silver_parity": parity})
        return bronze
    if parity["status"] != "passed":
        silver.update({
            "data_status": "malformed",
            "error": "Silver serving is fail-closed until parity passes: "
            + json.dumps(parity, ensure_ascii=False, sort_keys=True),
            "silver_read_mode": mode,
            "silver_parity": parity,
        })
        return silver
    silver.update({"silver_read_mode": mode, "silver_parity": parity})
    return silver


def default_item_from_bronze(
    row: dict[str, Any],
    *,
    id_fields: Sequence[str],
    id_sep: str = ":",
    history: bool = False,
    history_idx: int = 0,
    extra_payload_fields: Sequence[str] = (),
) -> dict[str, Any]:
    """Project Bronze envelope → product API item using payload_json fields."""
    payload = parse_payload_json(row)
    event_time = stringify(row.get("event_time") or payload.get("event_time") or "")
    item: dict[str, Any] = {
        "updated_at": event_time,
        "event_time": event_time,
        "ingest_run_id": stringify(row.get("ingest_run_id", "")),
        "source_record_id": stringify(row.get("source_record_id", "")),
        "raw_object_key": stringify(row.get("raw_object_key", "")),
    }
    # Copy common payload keys; callers can pass extras.
    for key, value in payload.items():
        if key in {"id"}:
            continue
        item[key] = stringify(value) if not isinstance(value, (dict, list)) else value
    for key in extra_payload_fields:
        if key not in item:
            item[key] = stringify(payload.get(key, ""))
    if history:
        item.setdefault("date", event_time)
        item["record_id"] = make_record_id(item, id_fields=id_fields, id_sep=id_sep) + f"#h{history_idx}"
    else:
        item["record_id"] = make_record_id(item, id_fields=id_fields, id_sep=id_sep)
    return item


def load_bronze_dataset(
    contract: LakeProductContract,
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
    latest_only: bool = False,
    id_fields: Sequence[str],
    id_sep: str = ":",
    stale_after_hours: float = 24.0,
    item_builder: Optional[Callable[..., dict[str, Any]]] = None,
    read_mode: Optional[str] = None,
    read_fallback: Optional[str] = None,
) -> dict[str, Any]:
    uri = data_lake_uri or default_data_lake_uri(
        data_lake_uri=contract.data_lake_uri,
        project_root=contract.project_root,
        solo_empire_root=contract.solo_empire_root,
    )
    try:
        bronze_path = str(bronze_dataset_dir(contract, dataset, data_lake_uri=uri))
    except Exception:  # noqa: BLE001 - remote s3:// has no local Path
        bronze_path = (
            f"{uri.rstrip('/')}/bronze/domain={contract.domain}/"
            f"dataset={dataset}/schema_version={contract.bronze_schema_version}"
        )
    mode = (read_mode or os.environ.get("LAKE_READ_MODE", "parquet")).strip().lower()
    fallback = (
        read_fallback or os.environ.get("LAKE_READ_FALLBACK", "error")
    ).strip().lower()
    if mode not in {"parquet", "iceberg"}:
        source_kind = (
            "bronze_s3_parquet" if is_remote_lake_uri(uri) else "bronze_parquet"
        )
        return {
            "items": [],
            "data_status": "malformed",
            "error": "LAKE_READ_MODE must be one of: parquet, iceberg",
            "path": bronze_path,
            "source_kind": "iceberg_rest_duckdb" if mode == "iceberg" else source_kind,
            "read_mode": mode,
            "read_fallback": fallback,
            "fallback_reason": None,
            "retrieved_at": utc_now_iso(),
        }
    if fallback not in {"error", "parquet"}:
        source_kind = (
            "bronze_s3_parquet" if is_remote_lake_uri(uri) else "bronze_parquet"
        )
        return {
            "items": [],
            "data_status": "malformed",
            "error": "LAKE_READ_FALLBACK must be one of: error, parquet",
            "path": bronze_path,
            "source_kind": "iceberg_rest_duckdb" if mode == "iceberg" else source_kind,
            "read_mode": mode,
            "read_fallback": fallback,
            "fallback_reason": None,
            "retrieved_at": utc_now_iso(),
        }
    builder = item_builder or (
        lambda row, history=False, history_idx=0: default_item_from_bronze(
            row,
            id_fields=id_fields,
            id_sep=id_sep,
            history=history,
            history_idx=history_idx,
        )
    )
    try:
        source_kind = (
            "bronze_s3_parquet" if is_remote_lake_uri(uri) else "bronze_parquet"
        )
        fallback_reason = None
        if mode == "iceberg":
            try:
                rows = read_iceberg_rows(contract, dataset, data_lake_uri=uri)
                source_kind = "iceberg_rest_duckdb"
            except (LakeUnavailable, LakeIngestError) as exc:
                if fallback != "parquet":
                    raise
                rows = read_bronze_rows(contract, dataset, data_lake_uri=uri)
                source_kind = (
                    "bronze_s3_parquet_fallback"
                    if is_remote_lake_uri(uri)
                    else "bronze_parquet_fallback"
                )
                fallback_reason = f"{type(exc).__name__}: {exc}"
        else:
            rows = read_bronze_rows(contract, dataset, data_lake_uri=uri)
        if latest_only:
            rows = select_latest_bronze_rows(rows)
            items = [builder(row, history=False, history_idx=0) for row in rows]
        else:
            items = [
                builder(row, history=True, history_idx=idx)
                for idx, row in enumerate(rows)
            ]
        status, _ = freshness(items, stale_after_hours=stale_after_hours)
        return {
            "items": items,
            "data_status": status if items else "empty",
            "error": None,
            "path": bronze_path,
            "source_kind": source_kind,
            "read_mode": mode,
            "read_fallback": fallback,
            "fallback_reason": fallback_reason,
            "retrieved_at": utc_now_iso(),
        }
    except LakeUnavailable as exc:
        source_kind = (
            "bronze_s3_parquet" if is_remote_lake_uri(uri) else "bronze_parquet"
        )
        return {
            "items": [],
            "data_status": "error",
            "error": str(exc),
            "path": bronze_path,
            "source_kind": "iceberg_rest_duckdb" if mode == "iceberg" else source_kind,
            "read_mode": mode,
            "read_fallback": fallback,
            "fallback_reason": None,
            "retrieved_at": utc_now_iso(),
        }
    except LakeIngestError as exc:
        source_kind = (
            "bronze_s3_parquet" if is_remote_lake_uri(uri) else "bronze_parquet"
        )
        return {
            "items": [],
            "data_status": "malformed",
            "error": str(exc),
            "path": bronze_path,
            "source_kind": "iceberg_rest_duckdb" if mode == "iceberg" else source_kind,
            "read_mode": mode,
            "read_fallback": fallback,
            "fallback_reason": None,
            "retrieved_at": utc_now_iso(),
        }


def load_csv_projection(
    *,
    path: Path,
    id_fields: Sequence[str],
    id_sep: str = ":",
    history: bool = False,
    stale_after_hours: float = 24.0,
) -> dict[str, Any]:
    """CLI-only CSV helper; never used by lake-first HTTP APIs."""
    path = Path(path)
    if not path.exists():
        return {
            "items": [],
            "data_status": "empty",
            "error": None,
            "path": str(path),
            "source_kind": "csv_projection",
            "retrieved_at": utc_now_iso(),
        }
    try:
        text = path.read_text(encoding="utf-8")
        if not text.strip():
            rows: list[dict[str, str]] = []
        else:
            rows = [dict(row) for row in csv.DictReader(text.splitlines())]
    except Exception as exc:  # noqa: BLE001
        return {
            "items": [],
            "data_status": "malformed",
            "error": f"malformed: {exc}",
            "path": str(path),
            "source_kind": "csv_projection",
            "retrieved_at": utc_now_iso(),
        }
    status, _ = freshness(rows, stale_after_hours=stale_after_hours)
    items = []
    for idx, row in enumerate(rows):
        item = dict(row)
        rid = make_record_id(row, id_fields=id_fields, id_sep=id_sep)
        item["record_id"] = rid + f"#h{idx}" if history else rid
        items.append(item)
    return {
        "items": items,
        "data_status": status if rows else "empty",
        "error": None,
        "path": str(path),
        "source_kind": "csv_projection",
        "retrieved_at": utc_now_iso(),
    }


def paginate(
    items: list[dict[str, Any]],
    *,
    limit: int = 50,
    cursor: Optional[str] = None,
) -> tuple[list[dict[str, Any]], Optional[str]]:
    limit = max(1, min(int(limit), 500))
    start = 0
    if cursor:
        try:
            start = max(0, int(cursor))
        except ValueError:
            start = 0
    end = start + limit
    page = items[start:end]
    next_cursor = str(end) if end < len(items) else None
    return page, next_cursor


def envelope(
    *,
    schema_version: str,
    source: str,
    items: list[dict[str, Any]],
    data_status: str,
    next_cursor: Optional[str] = None,
    retrieved_at: Optional[str] = None,
    extra: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    body = {
        "schema_version": schema_version,
        "source": source,
        "retrieved_at": retrieved_at or utc_now_iso(),
        "data_status": data_status,
        "items": items,
        "next_cursor": next_cursor,
    }
    if extra:
        body.update(extra)
    return body


def get_record_from_payload(
    record_id: str,
    payload: dict[str, Any],
) -> Optional[dict[str, Any]]:
    record_id = unquote(record_id)
    for item in payload.get("items") or []:
        if item.get("record_id") == record_id:
            return item
    return None
