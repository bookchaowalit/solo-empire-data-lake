from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from data_lake.ingest import ingest_payload, slug
from data_lake.product_adapter import LakeProductContract, read_bronze_rows
from data_lake.product_store import load_bronze_dataset
from data_lake.query import _s3_settings, query_parquet
from data_lake.storage import ObjectStore, StorageConflict


class RuntimeTests(unittest.TestCase):
    def test_slug_and_object_store_are_safe_and_idempotent(self):
        self.assertEqual(slug("Gmail / Inbox"), "gmail-inbox")
        with self.assertRaises(ValueError):
            slug("...")
        with tempfile.TemporaryDirectory() as temp_dir:
            store = ObjectStore(temp_dir)
            first = store.put_bytes("landing/source=test/payload.json", b"{}")
            retry = store.put_bytes(first.key, b"{}")
            self.assertFalse(first.existed)
            self.assertTrue(retry.existed)
            self.assertEqual(retry.sha256, hashlib.sha256(b"{}").hexdigest())
            with self.assertRaises(StorageConflict):
                store.put_bytes(first.key, b"changed")

    def test_r2_settings_are_path_style_without_logging_credentials(self):
        with patch.dict(
            "os.environ",
            {
                "DATA_LAKE_S3_ENDPOINT": "https://account.r2.cloudflarestorage.com",
                "AWS_REGION": "auto",
                "AWS_ACCESS_KEY_ID": "test-access",
                "AWS_SECRET_ACCESS_KEY": "test-secret",
            },
            clear=False,
        ):
            settings = _s3_settings()
        self.assertEqual(settings["s3_endpoint"], "account.r2.cloudflarestorage.com")
        self.assertEqual(settings["s3_url_style"], "path")
        self.assertEqual(settings["s3_region"], "auto")

    def test_exact_landing_bronze_and_duckdb_product_read(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            raw = b'{"id":"job-1","title":"Data Engineer"}'
            result = ingest_payload(
                raw,
                [{"id": "job-1", "title": "Data Engineer", "event_time": "2026-08-08T00:00:00Z"}],
                source="book-job-data",
                domain="jobs",
                dataset="job_postings",
                data_lake_uri=temp_dir,
                input_format="json",
                event_time_field="event_time",
                privacy_class="internal",
            )
            self.assertEqual(ObjectStore(temp_dir).get_bytes(result["raw_key"]), raw)
            rows = query_parquet(
                str(Path(temp_dir) / result["bronze_key"]),
                "SELECT source_record_id, payload_json FROM lake_table",
            )
            self.assertEqual(rows[0]["source_record_id"], "job-1")

            contract = LakeProductContract(
                source="book-job-data",
                domain="jobs",
                product_schema_version="job.v1",
                project_root=Path(temp_dir),
            )
            payload = load_bronze_dataset(
                contract,
                "job_postings",
                data_lake_uri=temp_dir,
                latest_only=True,
                id_fields=["id"],
            )
            self.assertEqual(payload["data_status"], "ok")
            self.assertEqual(payload["source_kind"], "bronze_parquet")
            self.assertEqual(len(payload["items"]), 1)

    def test_contract_read_path_uses_bounded_remote_glob(self):
        contract = LakeProductContract(
            source="book-job-data",
            domain="jobs",
            product_schema_version="job.v1",
            project_root=Path("/tmp/product"),
        )
        captured: dict[str, object] = {}

        def fake_query(paths, sql):
            captured["paths"] = paths
            captured["sql"] = sql
            return []

        with patch(
            "data_lake.product_adapter.load_query_runtime",
            return_value=(fake_query, RuntimeError, Path("/tmp/runtime")),
        ):
            read_bronze_rows(contract, "job_postings", data_lake_uri="s3://bucket/prefix")
        self.assertIn("ingest_date=*/source=*/part-*.parquet", str(captured["paths"]))


if __name__ == "__main__":
    unittest.main()
