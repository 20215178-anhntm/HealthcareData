# storage/s3_client.py
from __future__ import annotations
import io
import os
import time
import logging
from typing import Iterable, List, Optional, Dict, Any, Callable, TypeVar

# mypy: ignore-errors
import boto3 
from botocore.client import Config  
from botocore.exceptions import ClientError  

# ---------------------------
# Logging
# ---------------------------
log = logging.getLogger("s3_client")
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | s3_client | %(message)s"))
    log.addHandler(h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    log.propagate = False

# ---------------------------
# Dự phòng SETTINGS nếu import thất bại
# ---------------------------
try:
    from ..common.config import SETTINGS  # type: ignore
except Exception:
    from dataclasses import dataclass
    @dataclass(frozen=True)
    class _Tmp:
        s3_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        s3_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minio")
        s3_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minio123")
    SETTINGS = _Tmp()  # type: ignore

# ---------------------------
# Retry / Backoff decorator
# ---------------------------
T = TypeVar("T")
def retry(
    attempts: int = 3,
    backoff: float = 0.8,   # giây; tăng dần theo cấp số nhân
    exceptions: tuple = (ClientError, Exception),
    op_name: str = "s3op",
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    def deco(fn: Callable[..., T]) -> Callable[..., T]:
        def wrap(*args, **kwargs) -> T:
            last_err = None
            for i in range(1, attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:  # noqa: perf
                    last_err = e
                    wait = backoff * (2 ** (i - 1))
                    log.warning(f"[{op_name}] attempt {i}/{attempts} failed: {e.__class__.__name__}: {e}")
                    if i < attempts:
                        time.sleep(wait)
            log.error(f"[{op_name}] giving up after {attempts} attempts")
            raise last_err  # type: ignore[misc]
        return wrap
    return deco

# ---------------------------
# S3 Client wrapper
# ---------------------------
class S3Client:
    """
    Thin wrapper quanh boto3 cho MinIO/S3-compatible:
      - Path-style (http://endpoint/bucket/key)
      - Signature v4
      - Retry nhẹ & logging
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: str = "us-east-1",
        verify_ssl: bool = False,  # MinIO dev thường là http
    ) -> None:
        self.endpoint = endpoint or SETTINGS.s3_endpoint
        self._access_key = access_key or getattr(SETTINGS, "s3_access_key", None)
        self._secret_key = secret_key or getattr(SETTINGS, "s3_secret_key", None)

        session = boto3.session.Session()
        cfg = Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3, "mode": "standard"},
        )

        self.client = session.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=region,
            verify=verify_ssl,
            config=cfg,
        )
        self.resource = session.resource(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=region,
            verify=verify_ssl,
            config=cfg,
        )
        log.info(f"S3Client init: endpoint={self.endpoint}, path-style=1, region={region}")

    # ---------------------------
    # Bucket ops
    # ---------------------------
    @retry(op_name="list_buckets")
    def list_buckets(self) -> List[str]:
        resp = self.client.list_buckets()
        names = [b["Name"] for b in resp.get("Buckets", [])]
        log.debug(f"[s3] buckets={names}")
        return names

    @retry(op_name="ensure_bucket")
    def ensure_bucket(self, bucket: str) -> None:
        if bucket in self.list_buckets():
            log.info(f"[s3] bucket exists: {bucket}")
            return
        self.client.create_bucket(Bucket=bucket)
        log.info(f"[s3] bucket created: {bucket}")

    # ---------------------------
    # Object ops
    # ---------------------------
    @retry(op_name="list_objects")
    def list_objects(self, bucket: str, prefix: str = "", recursive: bool = True) -> List[str]:
        keys: List[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not recursive and "/" in key[len(prefix):]:
                    continue
                keys.append(key)
        log.info(f"[s3] list_objects bucket={bucket} prefix='{prefix}' -> {len(keys)} keys")
        return keys

    @retry(op_name="list_objects_with_meta")
    def list_objects_with_meta(self, bucket: str, prefix: str = "") -> List[Dict[str, Any]]:
        """
        Trả về [{"key": str, "last_modified": datetime, "size": int}, ...]
        Dùng cho lọc --since theo LastModified.
        """
        out: List[Dict[str, Any]] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                out.append({"key": o["Key"], "last_modified": o["LastModified"], "size": o["Size"]})
        log.info(f"[s3] list_objects_with_meta bucket={bucket} prefix='{prefix}' -> {len(out)} objs")
        return out

    @retry(op_name="upload_file")
    def upload_file(self, bucket: str, key: str, local_path: str) -> None:
        self.client.upload_file(local_path, bucket, key)
        log.info(f"[s3] upload_file {local_path} -> {bucket}/{key}")

    @retry(op_name="download_file")
    def download_file(self, bucket: str, key: str, local_path: str) -> None:
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        self.client.download_file(bucket, key, local_path)
        log.info(f"[s3] download_file {bucket}/{key} -> {local_path}")

    @retry(op_name="put_bytes")
    def put_bytes(self, bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        self.client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
        log.info(f"[s3] put_bytes {bucket}/{key} bytes={len(data)}")

    @retry(op_name="get_bytes")
    def get_bytes(self, bucket: str, key: str) -> bytes:
        obj = self.client.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        log.info(f"[s3] get_bytes {bucket}/{key} bytes={len(data)}")
        return data

    @retry(op_name="copy_object")
    def copy_object(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> None:
        self.client.copy({"Bucket": src_bucket, "Key": src_key}, dst_bucket, dst_key)
        log.info(f"[s3] copy {src_bucket}/{src_key} -> {dst_bucket}/{dst_key}")

    @retry(op_name="delete_object")
    def delete_object(self, bucket: str, key: str) -> None:
        self.client.delete_object(Bucket=bucket, Key=key)
        log.info(f"[s3] delete {bucket}/{key}")

    @retry(op_name="delete_prefix")
    def delete_prefix(self, bucket: str, prefix: str) -> None:
        keys = self.list_objects(bucket, prefix)
        if not keys:
            log.info(f"[s3] delete_prefix: no objects under {bucket}/{prefix}")
            return
        objects = [{"Key": k} for k in keys]
        for i in range(0, len(objects), 1000):
            self.client.delete_objects(Bucket=bucket, Delete={"Objects": objects[i:i+1000]})
        log.info(f"[s3] delete_prefix {bucket}/{prefix} deleted={len(keys)}")

    # ---------------------------
    # Helpers
    # ---------------------------
    @retry(op_name="exists")
    def exists(self, bucket: str, key: str) -> bool:
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            log.debug(f"[s3] exists: {bucket}/{key} -> True")
            return True
        except self.client.exceptions.NoSuchKey:  # type: ignore[attr-defined]
            log.debug(f"[s3] exists: {bucket}/{key} -> False (NoSuchKey)")
            return False
        except ClientError as e:
            if e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
                log.debug(f"[s3] exists: {bucket}/{key} -> False (404)")
                return False
            log.warning(f"[s3] exists head_object error: {e}")
            raise
