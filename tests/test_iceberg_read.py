"""Offline tests for product_adapter.read_iceberg_rows (no catalog, no network)."""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from data_lake import product_adapter
from data_lake.product_adapter import LakeIngestError, LakeProductContract, LakeUnavailable

CONTRACT = LakeProductContract(
    source="book-crypto-data", domain="market", product_schema_version="1"
)
ENV = {
    "ICEBERG_CATALOG_URI": "https://catalog.invalid/iceberg",
    "ICEBERG_WAREHOUSE_URI": "s3://warehouse-bucket",
}


class _IcebergUnavailable(RuntimeError):
    pass


class _DuckDBUnavailable(RuntimeError):
    pass


class _StorageError(RuntimeError):
    pass


def _runtime(location=None, query=None):
    calls: dict[str, object] = {}

    def iceberg_table_location(**kwargs):
        calls["location_kwargs"] = kwargs
        if isinstance(location, Exception):
            raise location
        return location

    def query_iceberg(table_location, sql, **kwargs):
        calls["query"] = (table_location, sql, kwargs)
        if isinstance(query, Exception):
            raise query
        return query

    runtime = (
        iceberg_table_location,
        query_iceberg,
        _IcebergUnavailable,
        _DuckDBUnavailable,
        _StorageError,
        None,
    )
    return runtime, calls


class ReadIcebergRowsTests(unittest.TestCase):
    def test_requires_catalog_configuration(self):
        with patch.dict(os.environ, {}, clear=False):
            for key in ENV:
                os.environ.pop(key, None)
            with self.assertRaises(LakeUnavailable):
                product_adapter.read_iceberg_rows(CONTRACT, "crypto_prices")

    def test_forwards_catalog_metadata_location(self):
        runtime, calls = _runtime(
            location={
                "table_location": "s3://warehouse-bucket/uuid/table",
                "metadata_location": "s3://warehouse-bucket/uuid/table/metadata/00007.json",
            },
            query=[{"event_id": "e1"}],
        )
        with patch.dict(os.environ, ENV), patch.object(
            product_adapter, "load_iceberg_query_runtime", return_value=runtime
        ):
            rows = product_adapter.read_iceberg_rows(CONTRACT, "crypto_prices")
        self.assertEqual(rows, [{"event_id": "e1"}])
        self.assertEqual(
            calls["location_kwargs"],
            {
                "catalog_uri": ENV["ICEBERG_CATALOG_URI"],
                "warehouse_uri": ENV["ICEBERG_WAREHOUSE_URI"],
                "domain": "market",
                "dataset": "crypto_prices",
            },
        )
        table_location, sql, kwargs = calls["query"]
        self.assertEqual(table_location, "s3://warehouse-bucket/uuid/table")
        self.assertIn("FROM lake_table", sql)
        self.assertEqual(
            kwargs,
            {"metadata_location": "s3://warehouse-bucket/uuid/table/metadata/00007.json"},
        )

    def test_missing_metadata_location_passes_none_and_custom_sql(self):
        runtime, calls = _runtime(
            location={"table_location": "s3://b/t"}, query=[]
        )
        with patch.dict(os.environ, ENV), patch.object(
            product_adapter, "load_iceberg_query_runtime", return_value=runtime
        ):
            product_adapter.read_iceberg_rows(
                CONTRACT, "crypto_prices", sql="SELECT 1 FROM lake_table"
            )
        self.assertEqual(
            calls["query"], ("s3://b/t", "SELECT 1 FROM lake_table", {"metadata_location": None})
        )

    def test_runtime_errors_map_to_lake_unavailable(self):
        for exc in (_IcebergUnavailable("x"), _DuckDBUnavailable("y"), _StorageError("z")):
            runtime, _ = _runtime(location=exc)
            with self.subTest(exc=type(exc).__name__), patch.dict(os.environ, ENV), patch.object(
                product_adapter, "load_iceberg_query_runtime", return_value=runtime
            ):
                with self.assertRaises(LakeUnavailable):
                    product_adapter.read_iceberg_rows(CONTRACT, "crypto_prices")

    def test_query_failure_maps_to_ingest_error(self):
        runtime, _ = _runtime(
            location={"table_location": "s3://b/t"}, query=ValueError("bad sql")
        )
        with patch.dict(os.environ, ENV), patch.object(
            product_adapter, "load_iceberg_query_runtime", return_value=runtime
        ):
            with self.assertRaises(LakeIngestError) as ctx:
                product_adapter.read_iceberg_rows(CONTRACT, "crypto_prices")
        self.assertIn("crypto_prices", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
