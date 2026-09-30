"""Standalone-install contracts relied on by sibling product repositories.

Product repos (book-*-data) import ``data_lake.product_adapter``,
``data_lake.product_store``, ``data_lake.silver`` and ``data_lake.storage``.
These tests pin the surface they use and prove it works without a parent
Solo Empire checkout on disk.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from data_lake import product_adapter, product_store, silver, storage
from data_lake.ingest import ingest_payload

PRODUCT_ADAPTER_SURFACE = (
    "LakeProductContract",
    "LakeUnavailable",
    "LakeIngestError",
    "ObjectStore",
    "StorageError",
    "bronze_dataset_dir",
    "default_data_lake_uri",
    "find_solo_empire_root",
    "ingest_to_lake",
    "is_remote_lake_uri",
    "landing_object_bytes",
    "list_bronze_parquet_files",
    "load_lineage",
    "parse_payload_json",
    "read_bronze_rows",
    "read_iceberg_rows",
    "resolve_lake_root",
    "select_latest_bronze_rows",
    "utc_now_iso",
    "write_lineage",
)
PRODUCT_STORE_SURFACE = (
    "envelope",
    "get_record_from_payload",
    "load_bronze_dataset",
    "load_csv_projection",
    "load_layered_dataset",
    "make_record_id",
    "paginate",
    "read_iceberg_rows",
    "storage_model_for_source_kind",
)
SILVER_SURFACE = ("SilverProductContract", "transform_bronze_to_silver", "read_silver_rows")


class NoParentCheckout:
    """Run with no SOLO_EMPIRE_ROOT and a cwd that has no parent checkout."""

    def __enter__(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._cwd = os.getcwd()
        self._env = patch.dict(os.environ, {}, clear=False)
        self._env.start()
        for key in ("SOLO_EMPIRE_ROOT", "SOLO_EMPIRE_DATA_LAKE_URI", "DATA_LAKE_URI"):
            os.environ.pop(key, None)
        os.chdir(self._tmp.name)
        return Path(self._tmp.name)

    def __exit__(self, *exc):
        os.chdir(self._cwd)
        self._env.stop()
        self._tmp.cleanup()
        return False


class ImportSurfaceTests(unittest.TestCase):
    def test_product_repo_import_surface_exists(self):
        for module, names in (
            (product_adapter, PRODUCT_ADAPTER_SURFACE),
            (product_store, PRODUCT_STORE_SURFACE),
            (silver, SILVER_SURFACE),
            (storage, ("ObjectStore", "StorageError", "ObjectNotFound")),
        ):
            for name in names:
                with self.subTest(module=module.__name__, name=name):
                    self.assertTrue(hasattr(module, name))


class LandingReplayTests(unittest.TestCase):
    def test_landing_object_bytes_works_without_parent_checkout(self):
        # Regression: this used to call _ensure_data_lake_on_path(None) and
        # raise TypeError whenever no Solo Empire checkout was discoverable.
        with NoParentCheckout() as lake:
            self.assertIsNone(product_adapter.find_solo_empire_root())
            raw = b'{"id":"btc","price":"1"}'
            result = ingest_payload(
                raw,
                [{"id": "btc", "price": "1", "event_time": "2026-01-01T00:00:00Z"}],
                source="book-crypto-data",
                domain="market",
                dataset="crypto_prices",
                data_lake_uri=str(lake),
                input_format="json",
                event_time_field="event_time",
            )
            self.assertEqual(
                product_adapter.landing_object_bytes(str(lake), result["raw_key"]), raw
            )

    def test_landing_object_bytes_missing_key_is_ingest_error(self):
        with NoParentCheckout() as lake:
            with self.assertRaises(product_adapter.LakeIngestError):
                product_adapter.landing_object_bytes(
                    str(lake), "landing/source=missing/nothing.json"
                )

    def test_default_lake_uri_falls_back_to_project_root(self):
        with NoParentCheckout() as lake:
            self.assertEqual(
                product_adapter.default_data_lake_uri(project_root=lake),
                str((lake / "data" / "lake").resolve()),
            )


def _bronze_row(payload: dict) -> dict:
    return {
        "source_record_id": payload.get("id", ""),
        "source": "book-crypto-data",
        "domain": "market",
        "dataset": "crypto_ohlcv",
        "payload_json": json.dumps(payload),
    }


class SilverNormalizerTests(unittest.TestCase):
    OHLCV = {
        "id": "binance:BTCUSDT:1h:1000",
        "venue": "binance",
        "symbol": "btcusdt",
        "interval": "1h",
        "open_time_ms": 1000,
        "close_time_ms": 3600999,
        "open": "10",
        "high": "12",
        "low": "9",
        "close": "11",
        "volume": "5",
        "quote_volume": "55",
        "trades": 0,
    }

    def test_crypto_ohlcv_valid_bar(self):
        out, errors = silver.normalize_crypto_ohlcv_row(
            _bronze_row(self.OHLCV), silver_run_id="run-1"
        )
        self.assertEqual(errors, [])
        self.assertEqual(out["symbol"], "BTCUSDT")
        self.assertEqual(out["trades"], "0")
        self.assertEqual(out["quality_status"], "valid")
        self.assertEqual(set(silver.CRYPTO_OHLCV_SILVER_COLUMNS) - set(out), set())

    def test_crypto_ohlcv_rejects_non_finite_and_unbounded_bars(self):
        bad = dict(self.OHLCV, high="nan")
        _, errors = silver.normalize_crypto_ohlcv_row(_bronze_row(bad), silver_run_id="r")
        self.assertIn("high must be finite", errors)
        bad = dict(self.OHLCV, high="10.5")
        _, errors = silver.normalize_crypto_ohlcv_row(_bronze_row(bad), silver_run_id="r")
        self.assertIn("high/low do not bound open/close", errors)
        bad = dict(self.OHLCV, close_time_ms=1000)
        _, errors = silver.normalize_crypto_ohlcv_row(_bronze_row(bad), silver_run_id="r")
        self.assertIn("close_time_ms must be after open_time_ms", errors)

    def test_crypto_funding_row(self):
        payload = {
            "id": "binance:BTCUSDT:1000",
            "venue": "binance",
            "symbol": "btcusdt",
            "funding_time_ms": 1000,
            "funding_rate": "-0.0001",
            "mark_price": "100",
        }
        out, errors = silver.normalize_crypto_funding_row(
            _bronze_row(payload), silver_run_id="r"
        )
        self.assertEqual(errors, [])
        self.assertEqual(out["symbol"], "BTCUSDT")
        _, errors = silver.normalize_crypto_funding_row(
            _bronze_row(dict(payload, funding_rate="inf")), silver_run_id="r"
        )
        self.assertIn("funding_rate must be finite", errors)

    def test_price_normalizers_reject_nan(self):
        _, errors = silver.normalize_crypto_row(
            _bronze_row({"id": "btc", "coin_id": "btc", "currency": "usd", "price": "NaN"}),
            silver_run_id="r",
        )
        self.assertIn("price must be finite", errors)

    def test_registry_covers_crypto_datasets(self):
        self.assertIn("crypto_ohlcv", silver.NORMALIZERS)
        self.assertIn("crypto_funding", silver.NORMALIZERS)

    def test_ohlcv_bronze_to_silver_round_trip_without_parent_checkout(self):
        with NoParentCheckout() as lake:
            bar = dict(self.OHLCV, event_time="2026-01-01T00:00:00Z")
            ingest_payload(
                json.dumps(bar).encode("utf-8"),
                [bar],
                source="book-crypto-data",
                domain="market",
                dataset="crypto_ohlcv",
                data_lake_uri=str(lake),
                input_format="json",
                event_time_field="event_time",
            )
            contract = silver.SilverProductContract(
                source="book-crypto-data",
                domain="market",
                bronze_dataset="crypto_ohlcv",
                silver_dataset="crypto_ohlcv",
                product_schema_version=silver.CRYPTO_OHLCV_SILVER_PRODUCT_SCHEMA,
                bronze_product_schema_version="crypto.v1",
                transform_version=silver.CRYPTO_OHLCV_TRANSFORM_VERSION,
                normalizer="crypto_ohlcv",
                required_fields=("source_record_id", "symbol", "open_time_ms"),
                privacy_class="public",
            )
            silver.transform_bronze_to_silver(contract, data_lake_uri=str(lake))
            rows = silver.read_silver_rows(
                data_lake_uri=str(lake),
                domain="market",
                dataset="crypto_ohlcv",
                silver_schema_version="1",
            )
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["symbol"], "BTCUSDT")
            self.assertEqual(rows[0]["quality_status"], "valid")


if __name__ == "__main__":
    unittest.main()
