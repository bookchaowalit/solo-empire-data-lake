#!/usr/bin/env python3
"""Deterministic Bronze → Silver transformation for the lake-first pilots.

This module keeps the physical interchange format as Parquet and makes the
first domain schema explicit for ``book-job-data``. It reads Bronze only,
deduplicates by the stable source record identity, validates required job
fields, writes an immutable Silver part, and records input lineage in a
Silver manifest. No CSV, API, or external consumer is touched here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[2]

from data_lake.product_adapter import (  # noqa: E402
    LakeProductContract,
    default_data_lake_uri,
    is_remote_lake_uri,
    parse_payload_json,
    read_bronze_rows,
    resolve_lake_root,
    select_latest_bronze_rows,
)
from data_lake.query import query_parquet  # noqa: E402
from data_lake.storage import ObjectStore  # noqa: E402


SILVER_SCHEMA_VERSION = "1"
JOB_SILVER_PRODUCT_SCHEMA = "job.silver.v1"
JOB_TRANSFORM_VERSION = "job-postings-silver.v1"
CRYPTO_SILVER_PRODUCT_SCHEMA = "crypto.silver.v1"
CRYPTO_TRANSFORM_VERSION = "crypto-prices-silver.v1"
STOCK_SILVER_PRODUCT_SCHEMA = "stock.silver.v1"
STOCK_TRANSFORM_VERSION = "stock-prices-silver.v1"
FX_SILVER_PRODUCT_SCHEMA = "fx.silver.v1"
FX_TRANSFORM_VERSION = "fx-rates-silver.v1"
CRYPTO_OHLCV_SILVER_PRODUCT_SCHEMA = "crypto-ohlcv.silver.v1"
CRYPTO_OHLCV_TRANSFORM_VERSION = "crypto-ohlcv-silver.v1"
CRYPTO_FUNDING_SILVER_PRODUCT_SCHEMA = "crypto-funding.silver.v1"
CRYPTO_FUNDING_TRANSFORM_VERSION = "crypto-funding-silver.v1"
COMMON_SILVER_COLUMNS = (
    "record_id",
    "source",
    "source_record_id",
    "domain",
    "dataset",
    "silver_schema_version",
    "received_at",
    "event_time",
    "raw_object_key",
    "raw_sha256",
    "bronze_ingest_run_id",
    "silver_run_id",
    "privacy_class",
    "retention_class",
    "payload_json",
    "quality_status",
    "quality_errors",
)
JOB_SILVER_COLUMNS = COMMON_SILVER_COLUMNS + (
    "provider",
    "title",
    "company",
    "canonical_url",
    "location",
    "salary_text",
    "posted_at",
)
CRYPTO_SILVER_COLUMNS = COMMON_SILVER_COLUMNS + (
    "coin_id",
    "currency",
    "price",
    "change_24h_pct",
    "volume_24h",
    "market_cap",
    "last_updated_at",
)
STOCK_SILVER_COLUMNS = COMMON_SILVER_COLUMNS + (
    "symbol",
    "price",
    "prev_close",
    "change",
    "change_pct",
    "currency",
    "exchange",
    "quote_timestamp",
)
FX_SILVER_COLUMNS = COMMON_SILVER_COLUMNS + (
    "base_currency",
    "quote_currency",
    "rate",
    "inverse_rate",
    "rate_date",
    "trend_7d",
    "trend_change_pct",
)
CRYPTO_OHLCV_SILVER_COLUMNS = COMMON_SILVER_COLUMNS + (
    "venue",
    "symbol",
    "interval",
    "open_time_ms",
    "close_time_ms",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trades",
)
CRYPTO_FUNDING_SILVER_COLUMNS = COMMON_SILVER_COLUMNS + (
    "venue",
    "symbol",
    "funding_time_ms",
    "funding_rate",
    "mark_price",
)
SILVER_COLUMNS = JOB_SILVER_COLUMNS


class SilverQualityError(ValueError):
    """Raised when a row cannot satisfy the Silver domain contract."""


class SilverRuntimeUnavailable(RuntimeError):
    """Raised when the optional PyArrow writer is not installed."""


@dataclass(frozen=True)
class SilverProductContract:
    """Domain-owned Silver contract layered on the shared Bronze envelope."""

    source: str = "book-job-data"
    domain: str = "jobs"
    bronze_dataset: str = "job_postings"
    silver_dataset: str = "job_postings"
    product_schema_version: str = JOB_SILVER_PRODUCT_SCHEMA
    bronze_product_schema_version: str = "job.v1"
    bronze_schema_version: str = "1"
    silver_schema_version: str = SILVER_SCHEMA_VERSION
    transform_version: str = JOB_TRANSFORM_VERSION
    normalizer: str = "job_postings"
    required_fields: tuple[str, ...] = ("source_record_id", "title", "canonical_url")
    privacy_class: str = "internal"
    retention_class: str = "operational"


def _pyarrow():
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except ImportError as exc:
        raise SilverRuntimeUnavailable(
            "Silver Parquet requires PyArrow; install infra/requirements-data-lake.txt"
        ) from exc
    return pa, pq


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _first_text(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = _text(payload.get(key))
        if value:
            return value
    return ""


def _numeric_errors(value: str, field: str, *, positive: bool = False) -> list[str]:
    """Validate a numeric field without allowing NaN or infinity through."""
    if not value:
        return [f"missing {field}"]
    try:
        number = float(value)
    except ValueError:
        return [f"{field} is not numeric"]
    if not math.isfinite(number):
        return [f"{field} must be finite"]
    if positive and number <= 0:
        return [f"{field} must be positive"]
    return []


def _canonical_url(value: str) -> str:
    """Normalize a job URL without retaining tracking/query state."""
    value = value.strip()
    if not value:
        return ""
    parsed = urlsplit(value)
    if not parsed.scheme or not parsed.netloc:
        return value.rstrip("/")
    host = parsed.netloc.lower()
    path = parsed.path.rstrip("/") or "/"
    return urlunsplit((parsed.scheme.lower(), host, path, "", ""))


def _safe_component(value: str) -> str:
    result = re.sub(r"[^A-Za-z0-9_-]+", "-", value.strip().lower()).strip("-._")
    return result or "unknown"


def _event_partition(rows: list[dict[str, Any]]) -> str:
    dates = {
        _text(row.get("event_time"))[:10]
        for row in rows
        if _text(row.get("event_time"))[:10]
    }
    if len(dates) == 1 and next(iter(dates)).count("-") == 2:
        return next(iter(dates))
    return "mixed"


def _common_silver_fields(
    row: dict[str, Any],
    *,
    silver_run_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "record_id": _text(row.get("source_record_id")) or "hash:" + hashlib.sha256(
            _canonical_json(payload).encode("utf-8")
        ).hexdigest()[:20],
        "source": _text(row.get("source")),
        "source_record_id": _text(row.get("source_record_id")) or _text(payload.get("id")),
        "domain": _text(row.get("domain")),
        "dataset": _text(row.get("dataset")),
        "silver_schema_version": SILVER_SCHEMA_VERSION,
        "received_at": _text(row.get("received_at")),
        "event_time": _text(row.get("event_time")),
        "raw_object_key": _text(row.get("raw_object_key")),
        "raw_sha256": _text(row.get("raw_sha256")),
        "bronze_ingest_run_id": _text(row.get("ingest_run_id")),
        "silver_run_id": silver_run_id,
        "privacy_class": _text(row.get("privacy_class")),
        "retention_class": _text(row.get("retention_class")),
        "payload_json": _canonical_json(payload),
    }


def normalize_job_row(
    row: dict[str, Any],
    *,
    silver_run_id: str,
) -> tuple[dict[str, Any], list[str]]:
    """Map one generic Bronze envelope into the job Silver schema."""
    payload = parse_payload_json(row)
    source_record_id = _text(row.get("source_record_id")) or _text(payload.get("id"))
    title = _first_text(payload, "title", "job_title", "position", "name")
    company = _first_text(payload, "company", "company_name", "employer", "vendor")
    url = _canonical_url(
        _first_text(payload, "canonical_url", "url", "source_url", "apply_url", "link")
    )
    location = _first_text(payload, "location", "city", "address")
    salary = _first_text(payload, "salary", "salary_text", "compensation")
    posted_at = _first_text(payload, "posted_at", "posted", "created_date", "date")
    provider = _first_text(payload, "provider", "board", "source")
    errors = []
    if not source_record_id:
        errors.append("missing source_record_id")
    if not title:
        errors.append("missing title")
    if not url:
        errors.append("missing canonical_url")
    output = {
        **_common_silver_fields(row, silver_run_id=silver_run_id, payload=payload),
        "source_record_id": source_record_id,
        "provider": provider,
        "title": title,
        "company": company,
        "canonical_url": url,
        "location": location,
        "salary_text": salary,
        "posted_at": posted_at,
        "quality_status": "invalid" if errors else "valid",
        "quality_errors": _canonical_json(errors),
    }
    return output, errors


def normalize_crypto_row(
    row: dict[str, Any],
    *,
    silver_run_id: str,
) -> tuple[dict[str, Any], list[str]]:
    """Map one generic Bronze envelope into the crypto Silver schema."""
    payload = parse_payload_json(row)
    source_record_id = _text(row.get("source_record_id")) or _text(payload.get("id"))
    coin_id = _first_text(payload, "coin_id", "id")
    currency = _first_text(payload, "currency", "vs_currency")
    price = _first_text(payload, "price")
    errors: list[str] = []
    if not source_record_id:
        errors.append("missing source_record_id")
    if not coin_id:
        errors.append("missing coin_id")
    if not currency:
        errors.append("missing currency")
    errors.extend(_numeric_errors(price, "price"))
    output = {
        **_common_silver_fields(row, silver_run_id=silver_run_id, payload=payload),
        "record_id": source_record_id or "hash:" + hashlib.sha256(
            _canonical_json(payload).encode("utf-8")
        ).hexdigest()[:20],
        "source_record_id": source_record_id,
        "coin_id": coin_id,
        "currency": currency.lower(),
        "price": price,
        "change_24h_pct": _first_text(payload, "change_24h_pct", "price_change_24h"),
        "volume_24h": _first_text(payload, "volume_24h", "usd_24h_vol"),
        "market_cap": _first_text(payload, "market_cap", "usd_market_cap"),
        "last_updated_at": _first_text(payload, "last_updated_at"),
        "quality_status": "invalid" if errors else "valid",
        "quality_errors": _canonical_json(errors),
    }
    return output, errors


def normalize_stock_row(
    row: dict[str, Any],
    *,
    silver_run_id: str,
) -> tuple[dict[str, Any], list[str]]:
    """Map one generic Bronze envelope into the stock Silver schema."""
    payload = parse_payload_json(row)
    source_record_id = _text(row.get("source_record_id")) or _text(payload.get("id"))
    symbol = _first_text(payload, "symbol", "ticker").upper()
    price = _first_text(payload, "price", "close")
    currency = _first_text(payload, "currency", "currency_code").upper()
    errors: list[str] = []
    if not source_record_id:
        errors.append("missing source_record_id")
    if not symbol:
        errors.append("missing symbol")
    errors.extend(_numeric_errors(price, "price"))
    if not currency:
        errors.append("missing currency")
    output = {
        **_common_silver_fields(row, silver_run_id=silver_run_id, payload=payload),
        "record_id": source_record_id or "hash:" + hashlib.sha256(
            _canonical_json(payload).encode("utf-8")
        ).hexdigest()[:20],
        "source_record_id": source_record_id,
        "symbol": symbol,
        "price": price,
        "prev_close": _first_text(payload, "prev_close", "previous_close"),
        "change": _first_text(payload, "change"),
        "change_pct": _first_text(payload, "change_pct", "change_percent"),
        "currency": currency,
        "exchange": _first_text(payload, "exchange", "market"),
        "quote_timestamp": _first_text(payload, "timestamp", "quote_timestamp"),
        "quality_status": "invalid" if errors else "valid",
        "quality_errors": _canonical_json(errors),
    }
    return output, errors


def normalize_fx_row(
    row: dict[str, Any],
    *,
    silver_run_id: str,
) -> tuple[dict[str, Any], list[str]]:
    """Map one generic Bronze envelope into the FX Silver schema."""
    payload = parse_payload_json(row)
    source_record_id = _text(row.get("source_record_id")) or _text(payload.get("id"))
    base_currency = _first_text(payload, "base", "base_currency").upper()
    quote_currency = _first_text(payload, "currency", "quote", "quote_currency").upper()
    rate = _first_text(payload, "rate")
    errors: list[str] = []
    if not source_record_id:
        errors.append("missing source_record_id")
    if not base_currency:
        errors.append("missing base_currency")
    if not quote_currency:
        errors.append("missing quote_currency")
    errors.extend(_numeric_errors(rate, "rate", positive=True))
    output = {
        **_common_silver_fields(row, silver_run_id=silver_run_id, payload=payload),
        "record_id": source_record_id or "hash:" + hashlib.sha256(
            _canonical_json(payload).encode("utf-8")
        ).hexdigest()[:20],
        "source_record_id": source_record_id,
        "base_currency": base_currency,
        "quote_currency": quote_currency,
        "rate": rate,
        "inverse_rate": _first_text(payload, "inverse", "inverse_rate"),
        "rate_date": _first_text(payload, "date", "rate_date"),
        "trend_7d": _first_text(payload, "trend_7d", "trend"),
        "trend_change_pct": _first_text(payload, "trend_change_pct"),
        "quality_status": "invalid" if errors else "valid",
        "quality_errors": _canonical_json(errors),
    }
    return output, errors


def _raw_text(value: Any) -> str:
    """Like ``_text`` but keeps numeric zero (e.g. ``trades=0``)."""
    return "" if value is None else str(value).strip()


def _int_errors(value: str, field: str) -> list[str]:
    if not value:
        return [f"missing {field}"]
    try:
        int(value)
    except ValueError:
        return [f"{field} is not an integer"]
    return []


def normalize_crypto_ohlcv_row(
    row: dict[str, Any],
    *,
    silver_run_id: str,
) -> tuple[dict[str, Any], list[str]]:
    """Map one closed-bar Bronze envelope into the OHLCV Silver schema."""
    payload = parse_payload_json(row)
    source_record_id = _text(row.get("source_record_id")) or _text(payload.get("id"))
    fields = {key: _raw_text(payload.get(key)) for key in (
        "venue", "symbol", "interval", "open_time_ms", "close_time_ms",
        "open", "high", "low", "close", "volume", "quote_volume", "trades",
    )}
    errors: list[str] = []
    if not source_record_id:
        errors.append("missing source_record_id")
    for key in ("venue", "symbol", "interval"):
        if not fields[key]:
            errors.append(f"missing {key}")
    errors.extend(_int_errors(fields["open_time_ms"], "open_time_ms"))
    errors.extend(_int_errors(fields["close_time_ms"], "close_time_ms"))
    for key in ("open", "high", "low", "close"):
        errors.extend(_numeric_errors(fields[key], key, positive=True))
    errors.extend(_numeric_errors(fields["volume"], "volume"))
    if not errors:
        o, h, lo, c = (float(fields[k]) for k in ("open", "high", "low", "close"))
        if h < max(o, c, lo) or lo > min(o, c):
            errors.append("high/low do not bound open/close")
        if float(fields["volume"]) < 0:
            errors.append("volume must not be negative")
        if int(fields["close_time_ms"]) <= int(fields["open_time_ms"]):
            errors.append("close_time_ms must be after open_time_ms")
    output = {
        **_common_silver_fields(row, silver_run_id=silver_run_id, payload=payload),
        "record_id": source_record_id,
        "source_record_id": source_record_id,
        **fields,
        "symbol": fields["symbol"].upper(),
        "quality_status": "invalid" if errors else "valid",
        "quality_errors": _canonical_json(errors),
    }
    return output, errors


def normalize_crypto_funding_row(
    row: dict[str, Any],
    *,
    silver_run_id: str,
) -> tuple[dict[str, Any], list[str]]:
    """Map one funding-rate Bronze envelope into the funding Silver schema."""
    payload = parse_payload_json(row)
    source_record_id = _text(row.get("source_record_id")) or _text(payload.get("id"))
    fields = {key: _raw_text(payload.get(key)) for key in (
        "venue", "symbol", "funding_time_ms", "funding_rate", "mark_price",
    )}
    errors: list[str] = []
    if not source_record_id:
        errors.append("missing source_record_id")
    if not fields["symbol"]:
        errors.append("missing symbol")
    errors.extend(_int_errors(fields["funding_time_ms"], "funding_time_ms"))
    errors.extend(_numeric_errors(fields["funding_rate"], "funding_rate"))
    output = {
        **_common_silver_fields(row, silver_run_id=silver_run_id, payload=payload),
        "record_id": source_record_id,
        "source_record_id": source_record_id,
        **fields,
        "symbol": fields["symbol"].upper(),
        "quality_status": "invalid" if errors else "valid",
        "quality_errors": _canonical_json(errors),
    }
    return output, errors


NORMALIZERS = {
    "crypto_ohlcv": (normalize_crypto_ohlcv_row, CRYPTO_OHLCV_SILVER_COLUMNS),
    "crypto_funding": (normalize_crypto_funding_row, CRYPTO_FUNDING_SILVER_COLUMNS),
}


def _write_silver_parquet(
    rows: list[dict[str, Any]],
    destination: Path,
    *,
    columns: tuple[str, ...],
) -> None:
    pa, pq = _pyarrow()
    schema = pa.schema([(column, pa.string()) for column in columns])
    arrays = [
        pa.array([row.get(column) for row in rows], type=pa.string())
        for column in columns
    ]
    table = pa.Table.from_arrays(arrays, schema=schema)
    destination.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, destination, compression="zstd", version="2.6")


def _contract_as_bronze(contract: SilverProductContract) -> LakeProductContract:
    return LakeProductContract(
        source=contract.source,
        domain=contract.domain,
        product_schema_version=contract.bronze_product_schema_version,
        privacy_class=contract.privacy_class,
        retention_class=contract.retention_class,
        bronze_schema_version=contract.bronze_schema_version,
        datasets=(contract.bronze_dataset,),
    )


def read_silver_rows(
    *,
    data_lake_uri: str,
    domain: str,
    dataset: str,
    silver_schema_version: str = SILVER_SCHEMA_VERSION,
    sql: str | None = None,
) -> list[dict[str, Any]]:
    """Read Silver Parquet directly from local storage or an S3-compatible lake.

    This is intentionally path-based, just like the Bronze reader. Silver is
    an immutable Parquet projection and does not require an Iceberg catalog for
    the local/API parity pilot. The caller owns the product-specific API
    mapping and decides whether a read is diagnostic or serving traffic.
    """
    uri = default_data_lake_uri(data_lake_uri=data_lake_uri)
    prefix = (
        f"silver/domain={_safe_component(domain)}/"
        f"dataset={_safe_component(dataset)}/"
        f"schema_version={_safe_component(silver_schema_version)}/"
        "event_date="
    )
    if is_remote_lake_uri(uri):
        paths: str | list[str] = f"{uri.rstrip('/')}/{prefix}*/part-*.parquet"
    else:
        base = resolve_lake_root(uri) / prefix
        files = sorted(base.parent.glob(f"{base.name}*/part-*.parquet"))
        if not files:
            return []
        paths = [str(path) for path in files]
    statement = sql or (
        "SELECT * FROM lake_table "
        "ORDER BY event_time NULLS LAST, received_at NULLS LAST, record_id"
    )
    return query_parquet(paths, statement)


def transform_bronze_to_silver(
    contract: SilverProductContract = SilverProductContract(),
    *,
    data_lake_uri: str = "",
) -> dict[str, Any]:
    """Read one Bronze dataset and publish one deterministic Silver part."""
    uri = default_data_lake_uri(data_lake_uri=data_lake_uri)
    bronze_contract = _contract_as_bronze(contract)
    bronze_rows = read_bronze_rows(
        bronze_contract,
        contract.bronze_dataset,
        data_lake_uri=uri,
    )
    if not bronze_rows:
        raise SilverQualityError(
            f"Bronze dataset is empty: {contract.source}/{contract.bronze_dataset}"
        )
    latest_rows = select_latest_bronze_rows(bronze_rows)
    columns = (
        NORMALIZERS[contract.normalizer][1]
        if contract.normalizer in NORMALIZERS
        else CRYPTO_SILVER_COLUMNS
        if contract.normalizer == "crypto_prices"
        else STOCK_SILVER_COLUMNS
        if contract.normalizer == "stock_prices"
        else FX_SILVER_COLUMNS
        if contract.normalizer == "fx_rates"
        else JOB_SILVER_COLUMNS
    )
    fingerprint_input = [
        {
            "source_record_id": _text(row.get("source_record_id")),
            "event_time": _text(row.get("event_time")),
            "received_at": _text(row.get("received_at")),
            "payload_json": _text(row.get("payload_json")),
            "bronze_ingest_run_id": _text(row.get("ingest_run_id")),
        }
        for row in latest_rows
    ]
    fingerprint = hashlib.sha256(
        _canonical_json({"transform": contract.transform_version, "rows": fingerprint_input}).encode("utf-8")
    ).hexdigest()
    silver_run_id = f"silver-{fingerprint[:24]}"
    normalized: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for row in latest_rows:
        if contract.normalizer in NORMALIZERS:
            item, row_errors = NORMALIZERS[contract.normalizer][0](row, silver_run_id=silver_run_id)
        elif contract.normalizer == "crypto_prices":
            item, row_errors = normalize_crypto_row(row, silver_run_id=silver_run_id)
        elif contract.normalizer == "job_postings":
            item, row_errors = normalize_job_row(row, silver_run_id=silver_run_id)
        elif contract.normalizer == "stock_prices":
            item, row_errors = normalize_stock_row(row, silver_run_id=silver_run_id)
        elif contract.normalizer == "fx_rates":
            item, row_errors = normalize_fx_row(row, silver_run_id=silver_run_id)
        else:
            raise ValueError(f"unsupported Silver normalizer: {contract.normalizer}")
        normalized.append(item)
        if row_errors:
            errors.append({
                "source_record_id": _text(row.get("source_record_id")),
                "errors": row_errors,
            })
    normalized.sort(key=lambda item: (item["record_id"], item["event_time"], item["source_record_id"]))
    if errors:
        raise SilverQualityError(
            "Silver quality gate failed: " + _canonical_json({"invalid_rows": errors})
        )

    store = ObjectStore(uri)
    partition = _event_partition(normalized)
    silver_key = (
        f"silver/domain={_safe_component(contract.domain)}/"
        f"dataset={_safe_component(contract.silver_dataset)}/"
        f"schema_version={_safe_component(contract.silver_schema_version)}/"
        f"event_date={partition}/part-{silver_run_id}.parquet"
    )
    with tempfile.TemporaryDirectory(prefix="solo-empire-silver-") as temp_dir:
        parquet_path = Path(temp_dir) / "part.parquet"
        _write_silver_parquet(normalized, parquet_path, columns=columns)
        parquet_bytes = parquet_path.read_bytes()
    silver_result = store.put_bytes(
        silver_key,
        parquet_bytes,
        content_type="application/vnd.apache.parquet",
    )
    raw_keys = sorted({_text(row.get("raw_object_key")) for row in latest_rows if _text(row.get("raw_object_key"))})
    ingest_runs = sorted({_text(row.get("ingest_run_id")) for row in latest_rows if _text(row.get("ingest_run_id"))})
    manifest = {
        "manifest_version": "1",
        "layer": "silver",
        "source": contract.source,
        "domain": contract.domain,
        "dataset": contract.silver_dataset,
        "schema_version": contract.silver_schema_version,
        "product_schema_version": contract.product_schema_version,
        "transform_version": contract.transform_version,
        "silver_run_id": silver_run_id,
        "input": {
            "layer": "bronze",
            "dataset": contract.bronze_dataset,
            "schema_version": contract.bronze_schema_version,
            "ingest_run_ids": ingest_runs,
            "raw_object_keys": raw_keys,
            "record_count": len(bronze_rows),
            "deduplicated_record_count": len(latest_rows),
        },
        "quality": {
            "status": "passed",
            "invalid_count": 0,
            "duplicate_count": len(bronze_rows) - len(latest_rows),
            "output_record_count": len(normalized),
            "required_fields": list(contract.required_fields),
        },
        "silver": {
            "key": silver_key,
            "sha256": silver_result.sha256,
            "bytes": len(parquet_bytes),
            "columns": list(columns),
        },
        "privacy_class": contract.privacy_class,
        "retention_class": contract.retention_class,
    }
    manifest_key = (
        f"control/manifests/silver/source={_safe_component(contract.source)}/"
        f"dataset={_safe_component(contract.silver_dataset)}/"
        f"transform_id={silver_run_id}.json"
    )
    manifest_result = store.put_bytes(
        manifest_key,
        (_canonical_json(manifest) + "\n").encode("utf-8"),
        content_type="application/json",
    )
    return {
        "status": "success",
        "layer": "silver",
        "data_lake": store.describe(),
        "silver_run_id": silver_run_id,
        "silver_key": silver_result.key,
        "manifest_key": manifest_result.key,
        "record_count": len(normalized),
        "input_record_count": len(bronze_rows),
        "duplicate_count": len(bronze_rows) - len(latest_rows),
        "silver_existed": silver_result.existed,
        "manifest_existed": manifest_result.existed,
        "quality": manifest["quality"],
        "lineage": manifest["input"],
        "csv_projection": "not-written",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-lake-uri", default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", ""))
    parser.add_argument("--source", default="book-job-data")
    parser.add_argument("--domain", default="jobs")
    parser.add_argument("--bronze-dataset", default="job_postings")
    parser.add_argument("--silver-dataset", default="job_postings")
    parser.add_argument("--bronze-product-schema", default="job.v1")
    parser.add_argument("--silver-product-schema", default=JOB_SILVER_PRODUCT_SCHEMA)
    parser.add_argument("--transform-version", default=JOB_TRANSFORM_VERSION)
    parser.add_argument(
        "--normalizer",
        choices=("job_postings", "crypto_prices", "stock_prices", "fx_rates", *NORMALIZERS),
        default="job_postings",
    )
    parser.add_argument("--privacy-class", default="internal")
    parser.add_argument("--retention-class", default="operational")
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    contract = SilverProductContract(
        source=args.source,
        domain=args.domain,
        bronze_dataset=args.bronze_dataset,
        silver_dataset=args.silver_dataset,
        product_schema_version=args.silver_product_schema,
        bronze_product_schema_version=args.bronze_product_schema,
        transform_version=args.transform_version,
        normalizer=args.normalizer,
        privacy_class=args.privacy_class,
        retention_class=args.retention_class,
        required_fields=(
            ("source_record_id", "coin_id", "currency", "price")
            if args.normalizer == "crypto_prices"
            else ("source_record_id", "symbol", "price", "currency")
            if args.normalizer == "stock_prices"
            else ("source_record_id", "base_currency", "quote_currency", "rate")
            if args.normalizer == "fx_rates"
            else ("source_record_id", "symbol", "interval", "open_time_ms", "open", "high", "low", "close")
            if args.normalizer == "crypto_ohlcv"
            else ("source_record_id", "symbol", "funding_time_ms", "funding_rate")
            if args.normalizer == "crypto_funding"
            else ("source_record_id", "title", "canonical_url")
        ),
    )
    try:
        result = transform_bronze_to_silver(contract, data_lake_uri=args.data_lake_uri)
    except Exception as exc:  # noqa: BLE001 - concise fail-closed CLI boundary
        payload = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        else:
            print(f"Silver transform: FAILED — {payload['error']}", file=sys.stderr)
        return 2
    if args.as_json:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    else:
        print("Silver transform: SUCCESS")
        print(f"  dataset: {args.source}/{args.silver_dataset}")
        print(f"  records: {result['record_count']}")
        print(f"  duplicate Bronze rows: {result['duplicate_count']}")
        print(f"  Silver: {result['silver_key']}")
        print(f"  manifest: {result['manifest_key']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
