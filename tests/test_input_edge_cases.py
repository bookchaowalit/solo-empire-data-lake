"""Regression tests for text-encoding edge cases in inputs and projections."""

from __future__ import annotations

import csv
import io
import json
import tempfile
import unittest
from pathlib import Path

from data_lake.ingest import _read_payload
from data_lake.product_store import get_record_from_payload, load_csv_projection


def _csv_text(rows: list[list[str]]) -> str:
    buf = io.StringIO()
    csv.writer(buf).writerows(rows)
    return buf.getvalue()


class ReadPayloadTests(unittest.TestCase):
    def _read(self, raw: bytes, suffix: str):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / f"input.{suffix}"
            path.write_bytes(raw)
            return _read_payload(str(path), "auto")[1]

    def test_jsonl_keeps_line_separator_inside_strings(self) -> None:
        line = json.dumps({"id": "a", "note": "x y\u0085z"}, ensure_ascii=False)
        raw = (line + "\n" + json.dumps({"id": "b"}) + "\n").encode("utf-8")
        records = self._read(raw, "jsonl")
        self.assertEqual([r["id"] for r in records], ["a", "b"])
        self.assertEqual(records[0]["note"], "x y\u0085z")

    def test_jsonl_crlf_and_bom(self) -> None:
        raw = b"\xef\xbb\xbf" + b'{"id": "a"}\r\n{"id": "b"}\r\n'
        self.assertEqual([r["id"] for r in self._read(raw, "jsonl")], ["a", "b"])

    def test_json_with_bom(self) -> None:
        raw = b"\xef\xbb\xbf" + b'[{"id": "a"}]'
        self.assertEqual(self._read(raw, "json"), [{"id": "a"}])

    def test_csv_with_bom_keeps_first_column_name(self) -> None:
        raw = ("﻿" + _csv_text([["id", "name"], ["1", "x"]])).encode("utf-8")
        self.assertEqual(self._read(raw, "csv"), [{"id": "1", "name": "x"}])


class CsvProjectionTests(unittest.TestCase):
    def _load(self, text: str) -> dict:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "p.csv"
            path.write_text(text, encoding="utf-8", newline="")
            return load_csv_projection(path=path, id_fields=["id"])

    def test_unquoted_line_separator_does_not_split_rows(self) -> None:
        text = _csv_text([["id", "desc"], ["a", "x y"], ["b", "z"]])
        items = self._load(text)["items"]
        self.assertEqual([i["record_id"] for i in items], ["a", "b"])
        self.assertEqual(items[0]["desc"], "x y")

    def test_bom_header(self) -> None:
        items = self._load("﻿" + _csv_text([["id"], ["a"]]))["items"]
        self.assertEqual(items[0]["record_id"], "a")


class GetRecordTests(unittest.TestCase):
    def test_already_decoded_id_with_percent(self) -> None:
        payload = {"items": [{"record_id": "100%41"}, {"record_id": "100A"}]}
        self.assertEqual(get_record_from_payload("100%41", payload), {"record_id": "100%41"})

    def test_encoded_id_still_resolves(self) -> None:
        payload = {"items": [{"record_id": "a:b"}]}
        self.assertEqual(get_record_from_payload("a%3Ab", payload), {"record_id": "a:b"})


if __name__ == "__main__":
    unittest.main()
