"""Small object-storage adapter used by the data-lake ingest command.

The adapter deliberately has no cloud SDK dependency.  Local development uses
the filesystem; S3-compatible storage uses the AWS CLI, which works with AWS,
Cloudflare R2, and the repository's MinIO learning lab without putting
credentials in Python configuration.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse


class StorageError(RuntimeError):
    """Raised when an object cannot be read or written safely."""


class ObjectNotFound(StorageError):
    """Raised when an object is absent (as opposed to a provider failure)."""


class StorageConflict(StorageError):
    """Raised when an immutable local object key already contains other data."""


_TRUE_VALUES = {"1", "true", "yes", "on"}
_DEFAULT_CLOUD_MAX_OBJECT_BYTES = 64 * 1024 * 1024


def _is_true(value: str | None) -> bool:
    return str(value or "").strip().lower() in _TRUE_VALUES


def _configured_s3_endpoint() -> str | None:
    return os.environ.get("DATA_LAKE_S3_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")


def _is_local_s3_endpoint(endpoint: str | None) -> bool:
    if not endpoint:
        return False
    hostname = urlparse(endpoint).hostname
    return hostname in {"localhost", "127.0.0.1", "::1", "minio", "se-minio"}


def _cloud_max_object_bytes() -> int:
    raw = os.environ.get(
        "DATA_LAKE_CLOUD_MAX_OBJECT_BYTES",
        str(_DEFAULT_CLOUD_MAX_OBJECT_BYTES),
    )
    try:
        value = int(raw)
    except ValueError as exc:
        raise StorageError(
            "DATA_LAKE_CLOUD_MAX_OBJECT_BYTES must be a non-negative integer"
        ) from exc
    if value < 0:
        raise StorageError("DATA_LAKE_CLOUD_MAX_OBJECT_BYTES must be non-negative")
    return value


@dataclass(frozen=True)
class PutResult:
    key: str
    existed: bool
    sha256: str


def _safe_key(key: str) -> str:
    """Normalize a key and reject traversal outside the object-store prefix."""
    normalized = "/".join(part for part in key.strip("/").split("/") if part)
    if not normalized or normalized.startswith("../") or "/../" in normalized:
        raise StorageError(f"Invalid object key: {key!r}")
    if any(part in {".", ".."} for part in normalized.split("/")):
        raise StorageError(f"Invalid object key: {key!r}")
    return normalized


class ObjectStore:
    """Read/write immutable objects from ``file://`` or ``s3://`` storage."""

    def __init__(self, uri: str):
        self.uri = uri
        if (
            os.name == "nt"
            and len(uri) >= 3
            and uri[0].isalpha()
            and uri[1] == ":"
            and uri[2] in {"\\", "/"}
        ):
            self.root = Path(uri).expanduser().resolve()
            self.bucket = None
            self.prefix = ""
            self.scheme = "file"
            return

        parsed = urlparse(uri)
        scheme = parsed.scheme.lower()
        if not scheme:
            scheme = "file"
            self.root = Path(uri).expanduser().resolve()
            self.bucket = None
            self.prefix = ""
        elif scheme == "file":
            # file:///absolute/path is canonical; also accept file://relative/path
            # for convenient CLI use.
            path = unquote(parsed.path)
            if parsed.netloc and parsed.netloc not in {"", "localhost"}:
                path = f"/{parsed.netloc}{path}"
            elif (
                os.name == "nt"
                and len(path) >= 3
                and path[0] == "/"
                and path[1].isalpha()
                and path[2] == ":"
            ):
                path = path[1:]
            self.root = Path(path or ".").expanduser().resolve()
            self.bucket = None
            self.prefix = ""
        elif scheme == "s3":
            if not parsed.netloc:
                raise StorageError("An s3:// URI must include a bucket")
            self.root = None
            self.bucket = parsed.netloc
            self.prefix = parsed.path.strip("/")
        else:
            raise StorageError(
                f"Unsupported data-lake URI scheme {scheme!r}; use file:// or s3://"
            )
        self.scheme = scheme

    def _full_key(self, key: str) -> str:
        key = _safe_key(key)
        return "/".join(part for part in (self.prefix, key) if part)

    def _aws_args(self) -> list[str]:
        args: list[str] = []
        endpoint = os.environ.get("DATA_LAKE_S3_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")
        region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        if endpoint:
            args.extend(["--endpoint-url", endpoint])
        if region:
            args.extend(["--region", region])
        return args

    def _run_aws(self, args: list[str], *, input_bytes: bytes | None = None) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(
                ["aws", *args],
                input=input_bytes,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
        except FileNotFoundError as exc:
            raise StorageError(
                "S3-compatible data lake needs the AWS CLI (aws) on PATH; "
                "use file:// for local development"
            ) from exc

    def put_bytes(self, key: str, data: bytes, *, content_type: str | None = None) -> PutResult:
        """Write an object once; an identical retry is a no-op."""
        key = _safe_key(key)
        digest = hashlib.sha256(data).hexdigest()
        if self.scheme == "file":
            assert self.root is not None
            destination = self.root / key
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                existing = hashlib.sha256(destination.read_bytes()).hexdigest()
                if existing != digest:
                    raise StorageConflict(f"Immutable object conflict at {key}")
                return PutResult(key=key, existed=True, sha256=digest)
            with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as handle:
                temporary = Path(handle.name)
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                # ``link`` is an atomic create-if-absent operation on the same
                # filesystem.  Unlike replace/rename it cannot clobber a part
                # written concurrently by another worker.
                os.link(temporary, destination)
            except FileExistsError:
                existing = hashlib.sha256(destination.read_bytes()).hexdigest()
                if existing != digest:
                    raise StorageConflict(f"Immutable object conflict at {key}")
                return PutResult(key=key, existed=True, sha256=digest)
            finally:
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass
            return PutResult(key=key, existed=False, sha256=digest)

        assert self.bucket is not None
        # A remote S3 target is opt-in so a fixture or local development run
        # cannot accidentally write to a paid/production bucket. MinIO on the
        # local Docker network remains available without this flag.
        endpoint = _configured_s3_endpoint()
        remote_cloud = not _is_local_s3_endpoint(endpoint)
        if remote_cloud:
            if not _is_true(os.environ.get("DATA_LAKE_CLOUD_WRITE_ENABLED")):
                raise StorageError(
                    "Cloud Object Storage writes are disabled by default; set "
                    "DATA_LAKE_CLOUD_WRITE_ENABLED=true only for an intentional "
                    "R2/S3 run"
                )
            # Reject oversized cloud payloads before any provider call. This
            # keeps the safety budget deterministic and makes the guard truly
            # network-free when a caller hands us an object that is too large.
            max_bytes = _cloud_max_object_bytes()
            if max_bytes and len(data) > max_bytes:
                raise StorageError(
                    f"Cloud object exceeds DATA_LAKE_CLOUD_MAX_OBJECT_BYTES "
                    f"({len(data)} > {max_bytes})"
                )
        full_key = self._full_key(key)
        head = self._run_aws([
            "s3api", "head-object", "--bucket", self.bucket, "--key", full_key,
            *self._aws_args(),
        ])
        if head.returncode == 0:
            # Custom metadata is written with every upload.  Older objects may
            # not have it, so fall back to a byte-for-byte read rather than
            # treating mere existence as proof that an immutable key matches.
            try:
                payload = json.loads(head.stdout.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise StorageError(f"Object metadata returned invalid JSON for {key}") from exc
            metadata = payload.get("Metadata") if isinstance(payload, dict) else None
            stored_digest = None
            if isinstance(metadata, dict):
                for name, value in metadata.items():
                    if str(name).lower() == "sha256":
                        stored_digest = str(value).lower()
                        break
            if stored_digest:
                if stored_digest != digest:
                    raise StorageConflict(f"Immutable object conflict at {key}")
                return PutResult(key=key, existed=True, sha256=digest)
            try:
                existing_digest = hashlib.sha256(self.get_bytes(key)).hexdigest()
            except ObjectNotFound:
                # A provider can report a transiently stale HEAD.  Fail closed
                # instead of overwriting an object whose state is uncertain.
                raise StorageError(f"Object disappeared while checking {key}")
            if existing_digest != digest:
                raise StorageConflict(f"Immutable object conflict at {key}")
            return PutResult(key=key, existed=True, sha256=digest)
        head_error = head.stderr.decode("utf-8", errors="replace").lower()
        if not any(marker in head_error for marker in ("nosuchkey", "not found", "404")):
            # A permission, endpoint, or transient provider failure must not be
            # mistaken for absence and followed by an unsafe overwrite.
            raise StorageError(f"Object metadata lookup failed for {key}")

        # Use the S3 conditional create primitive so two workers racing on a
        # new immutable key cannot overwrite one another.  A temporary file is
        # used because ``put-object --body`` accepts a path consistently across
        # AWS CLI and S3-compatible endpoints.
        with tempfile.NamedTemporaryFile(prefix="solo-empire-upload-", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        command = [
            "s3api", "put-object", "--bucket", self.bucket, "--key", full_key,
            "--body", str(temporary), "--if-none-match", "*", *self._aws_args(),
        ]
        if content_type:
            command.extend(["--content-type", content_type])
        command.extend(["--metadata", f"sha256={digest}"])
        try:
            uploaded = self._run_aws(command)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        if uploaded.returncode != 0:
            error = uploaded.stderr.decode("utf-8", errors="replace").lower()
            if any(marker in error for marker in ("precondition", "if-none-match", "412", "conditional")):
                raise StorageConflict(f"Immutable object conflict at {key}")
            raise StorageError(
                f"Object upload failed for {key}; AWS CLI exit={uploaded.returncode}"
            )
        return PutResult(key=key, existed=False, sha256=digest)

    def get_bytes(self, key: str) -> bytes:
        """Read one object without exposing provider credentials in errors."""
        key = _safe_key(key)
        if self.scheme == "file":
            assert self.root is not None
            try:
                return (self.root / key).read_bytes()
            except FileNotFoundError as exc:
                raise ObjectNotFound(f"Object not found: {key}") from exc

        assert self.bucket is not None
        full_key = self._full_key(key)
        result = self._run_aws([
            "s3", "cp", f"s3://{self.bucket}/{full_key}", "-",
            *self._aws_args(), "--only-show-errors",
        ])
        if result.returncode != 0:
            error = result.stderr.decode("utf-8", errors="replace").lower()
            if any(marker in error for marker in ("nosuchkey", "not found", "404", "nosuchbucket")):
                raise ObjectNotFound(f"Object not found: {key}")
            raise StorageError(f"Object download failed for {key}")
        return result.stdout

    def list_keys(self, prefix: str = "") -> list[str]:
        """List object keys below a prefix for bounded polling jobs."""
        # Keep the local filesystem path bounded to ``self.root`` just like
        # the S3 path is bounded by ``_full_key``.  A maintenance or polling
        # caller may pass a user/configured prefix, so do not let ``..`` turn
        # a read-only listing into a traversal outside the lake root.
        prefix = _safe_key(prefix) if prefix.strip("/") else ""
        if self.scheme == "file":
            assert self.root is not None
            base = self.root / prefix
            if not base.exists():
                return []
            return sorted(
                path.relative_to(self.root).as_posix()
                for path in base.rglob("*")
                if path.is_file()
            )

        assert self.bucket is not None
        full_prefix = self._full_key(prefix) if prefix else self.prefix
        keys: list[str] = []
        continuation_token: str | None = None
        while True:
            args = [
                "s3api",
                "list-objects-v2",
                "--bucket",
                self.bucket,
                "--prefix",
                full_prefix,
                "--output",
                "json",
                *self._aws_args(),
            ]
            if continuation_token:
                args.extend(["--continuation-token", continuation_token])
            result = self._run_aws(args)
            if result.returncode != 0:
                raise StorageError(f"Object listing failed for prefix {prefix}")
            try:
                payload = json.loads(result.stdout or b"{}")
            except json.JSONDecodeError as exc:
                raise StorageError("Object listing returned invalid JSON") from exc
            for item in payload.get("Contents", []):
                full_key = str(item.get("Key", ""))
                if self.prefix and full_key.startswith(self.prefix + "/"):
                    keys.append(full_key[len(self.prefix) + 1 :])
                elif not self.prefix:
                    keys.append(full_key)
            if not payload.get("IsTruncated"):
                break
            continuation_token = payload.get("NextContinuationToken")
            if not continuation_token:
                break
        return sorted(keys)

    def object_size(self, key: str) -> int:
        """Return one object's byte size without downloading its contents."""
        key = _safe_key(key)
        if self.scheme == "file":
            assert self.root is not None
            try:
                return (self.root / key).stat().st_size
            except FileNotFoundError as exc:
                raise StorageError(f"Object not found: {key}") from exc

        assert self.bucket is not None
        result = self._run_aws([
            "s3api", "head-object", "--bucket", self.bucket,
            "--key", self._full_key(key), "--query", "ContentLength",
            "--output", "text", *self._aws_args(),
        ])
        if result.returncode != 0:
            raise StorageError(f"Object metadata lookup failed for {key}")
        try:
            return int(result.stdout.decode("utf-8").strip())
        except (TypeError, ValueError) as exc:
            raise StorageError(f"Object metadata returned invalid size for {key}") from exc

    def describe(self) -> str:
        if self.scheme == "file":
            assert self.root is not None
            return f"file://{self.root}"
        assert self.bucket is not None
        return f"s3://{self.bucket}/{self.prefix}".rstrip("/")

    def object_uri(self, key: str) -> str:
        """Return a provider URI for a key without exposing credentials."""
        key = _safe_key(key)
        if self.scheme == "file":
            assert self.root is not None
            return (self.root / key).resolve().as_uri()
        assert self.bucket is not None
        return f"s3://{self.bucket}/{self._full_key(key)}"
