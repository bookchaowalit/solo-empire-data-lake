"""Register existing Bronze Parquet parts as Apache Iceberg data files.

This module adds Iceberg metadata on top of the Parquet part already written
by ``ingest.py``. It does not rewrite or duplicate the data file. A local SQL
catalog backed by SQLite is suitable for development and a single operator;
production multi-writer deployments should point the same API at a REST or
PostgreSQL-backed catalog instead.
"""

from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import urlparse

from .storage import ObjectStore, StorageError


class IcebergUnavailable(RuntimeError):
    """Raised when the optional Iceberg runtime is not installed."""


def _component(value: str) -> str:
    result = re.sub(r"[^A-Za-z0-9_-]+", "-", str(value).strip().lower()).strip("-.")
    if not result or result in {".", ".."}:
        raise ValueError(f"Invalid Iceberg identifier component: {value!r}")
    return result


def _require_pyiceberg():
    try:
        import pyarrow.parquet as pq  # type: ignore
        from pyiceberg.catalog import load_catalog  # type: ignore
    except ImportError as exc:
        raise IcebergUnavailable(
            "Iceberg registration requires PyIceberg and its SQLite/S3 extras; "
            "install with python3 -m pip install -r infra/requirements-data-lake.txt"
        ) from exc
    return pq, load_catalog


def _catalog_properties(catalog_uri: str, warehouse_uri: str) -> dict[str, str]:
    """Build PyIceberg properties for local SQL or hosted REST catalogs."""
    if catalog_uri.startswith("sqlite://") or catalog_uri.startswith("postgresql"):
        return {
            "type": "sql",
            "uri": catalog_uri,
            "warehouse": warehouse_uri,
            "init_catalog_tables": "true",
        }
    scheme = urlparse(catalog_uri).scheme.lower()
    if scheme in {"http", "https"}:
        token = os.environ.get("ICEBERG_CATALOG_TOKEN") or os.environ.get(
            "R2_DATA_CATALOG_TOKEN"
        )
        if not token:
            raise StorageError(
                "REST Iceberg catalogs require ICEBERG_CATALOG_TOKEN (or "
                "R2_DATA_CATALOG_TOKEN) in the runtime secret provider"
            )
        return {
            "type": "rest",
            "uri": catalog_uri,
            "warehouse": warehouse_uri,
            "token": token,
        }
    raise StorageError(
        "Iceberg registration expects a SQL catalog URI "
        "(sqlite:///... or postgresql+psycopg2://...) or an HTTP(S) REST "
        "catalog URI"
    )


def _configure_remote_s3_environment() -> None:
    """Give PyArrow/PyIceberg the S3-compatible endpoint for remote files."""
    endpoint = os.environ.get("DATA_LAKE_S3_ENDPOINT") or os.environ.get(
        "AWS_ENDPOINT_URL"
    )
    if not endpoint:
        return
    # These are process-local hints consumed by the optional S3 FileIO. They
    # are intentionally not persisted or printed; credentials remain in the
    # runtime secret provider.
    os.environ.setdefault("PYARROW_S3_ENDPOINT_OVERRIDE", endpoint)
    os.environ.setdefault("AWS_ENDPOINT_URL_S3", endpoint)


def iceberg_table_location(
    *,
    catalog_uri: str,
    warehouse_uri: str,
    domain: str,
    dataset: str,
) -> dict[str, str]:
    """Resolve a catalog table to its provider-owned Iceberg location.

    The REST catalog is the authority for the current table location. Readers
    must not reconstruct the R2 Data Catalog UUID path from the warehouse URI;
    doing so would bypass catalog metadata and break after a provider-side
    layout change.
    """
    _configure_remote_s3_environment()
    _pq, load_catalog = _require_pyiceberg()
    domain_name = _component(domain)
    dataset_name = _component(dataset)
    table_identifier = f"bronze.{domain_name}_{dataset_name}"
    catalog = load_catalog(
        "solo_empire",
        **_catalog_properties(catalog_uri, warehouse_uri),
    )
    try:
        if not catalog.table_exists(table_identifier):
            raise StorageError(
                f"Iceberg table is not registered in catalog: {table_identifier}"
            )
        table = catalog.load_table(table_identifier)
        location = getattr(table, "location", None)
        if callable(location):
            location = location()
        if not location:
            raise StorageError(
                f"Iceberg table has no resolved location: {table_identifier}"
            )
        return {
            "table_identifier": table_identifier,
            "table_location": str(location),
            "metadata_location": str(table.metadata_location),
        }
    finally:
        close = getattr(catalog, "close", None)
        if close:
            close()


def register_bronze_file(
    *,
    data_lake_uri: str,
    bronze_key: str,
    domain: str,
    dataset: str,
    catalog_uri: str,
    warehouse_uri: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Register one existing Parquet object in an Iceberg table.

    Re-registering the same file is an idempotent no-op. The table uses the
    generic Bronze schema from the Parquet footer and an Iceberg v2 table.
    """
    _configure_remote_s3_environment()
    pq, load_catalog = _require_pyiceberg()
    store = ObjectStore(data_lake_uri)
    file_uri = store.object_uri(bronze_key)
    catalog_scheme = urlparse(catalog_uri).scheme.lower()
    is_rest_catalog = catalog_scheme in {"http", "https"}
    if is_rest_catalog and not warehouse_uri:
        raise StorageError(
            "REST Iceberg catalogs require the provider-issued warehouse name "
            "through --iceberg-warehouse-uri or ICEBERG_WAREHOUSE_URI"
        )
    warehouse_uri = warehouse_uri or store.describe()
    domain_name = _component(domain)
    dataset_name = _component(dataset)
    table_identifier = f"bronze.{domain_name}_{dataset_name}"
    table_location = "/".join(
        part.rstrip("/")
        for part in (warehouse_uri, "iceberg", "bronze", domain_name, dataset_name)
    )

    if store.scheme == "file":
        assert store.root is not None
        parquet_path = store.root / bronze_key
        schema = pq.read_schema(parquet_path)
        file_location = str(parquet_path.resolve())
    else:
        # PyArrow/PyIceberg use the S3-compatible FileIO for remote objects.
        # The optional s3fs extra is installed by requirements-data-lake.txt.
        schema = pq.read_schema(file_uri)
        file_location = file_uri

    try:
        catalog = load_catalog(
            "solo_empire",
            **_catalog_properties(catalog_uri, warehouse_uri),
        )
    except ImportError as exc:
        raise IcebergUnavailable(
            "The Iceberg catalog needs the PyIceberg SQL or REST runtime"
        ) from exc

    try:
        catalog.create_namespace_if_not_exists("bronze")
        if catalog.table_exists(table_identifier):
            table = catalog.load_table(table_identifier)
        else:
            create_kwargs: dict[str, Any] = {
                "schema": schema,
                "properties": {"format-version": "2"},
            }
            # REST catalogs own the warehouse layout. A local-style location
            # would bypass that ownership and can create invalid cloud paths.
            if not is_rest_catalog:
                create_kwargs["location"] = table_location
            table = catalog.create_table(table_identifier, **create_kwargs)

        resolved_location = getattr(table, "location", None)
        if callable(resolved_location):
            resolved_location = resolved_location()
        if resolved_location:
            table_location = str(resolved_location)

        referenced_files = {
            str(row["file_path"])
            for row in table.inspect.data_files().to_pylist()
        }
        equivalent_locations = {file_location, file_uri}
        if referenced_files.intersection(equivalent_locations):
            status = "already_registered"
        else:
            table.add_files(
                [file_location],
                snapshot_properties={
                    "solo_empire.run_id": run_id or "unknown",
                    "solo_empire.source_file": bronze_key,
                },
            )
            status = "registered"
        snapshot = table.current_snapshot()
        return {
            "status": status,
            "table_identifier": table_identifier,
            "table_location": table_location,
            # Readers must use the catalog-selected metadata file.  Scanning
            # only the table directory makes DuckDB guess the highest numeric
            # metadata version, which is unsafe after a catalog restore or a
            # concurrent writer has left an older branch on disk.
            "metadata_location": str(table.metadata_location),
            "data_file": file_location,
            "snapshot_id": snapshot.snapshot_id if snapshot else None,
        }
    finally:
        close = getattr(catalog, "close", None)
        if close:
            close()
