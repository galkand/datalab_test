from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from typing import Optional, Dict

from pyspark.sql import SparkSession


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value or not value.strip():
        raise RuntimeError(f"Required environment variable {name} is not set")
    return value


@dataclass(frozen=True)
class Env:
    # S3 (MinIO)
    s3_endpoint: str = field(default_factory=lambda: os.getenv("S3A_ENDPOINT", "http://minio:9000"))
    s3_access_key: str = field(default_factory=lambda: required_env("S3A_ACCESS_KEY"))
    s3_secret_key: str = field(default_factory=lambda: required_env("S3A_SECRET_KEY"))

    # Hive / HDFS
    hive_metastore_uris: str = os.getenv("HIVE_METASTORE_URIS", "thrift://hive-metastore:9083")
    fs_default_fs: str = os.getenv("FS_DEFAULT_FS", "hdfs://namenode:8020")
    warehouse_dir: str = os.getenv("SPARK_WAREHOUSE_DIR", "hdfs://namenode:8020/user/hive/warehouse")

    # ClickHouse (HTTP)
    ch_host: str = os.getenv("CH_HOST", "clickhouse")
    ch_port: int = int(os.getenv("CH_PORT", "8123"))
    ch_db: str = os.getenv("CH_DB", "analytics")
    ch_user: str = os.getenv("CH_USER", "analytics")
    ch_password: str = field(default_factory=lambda: required_env("CH_PASSWORD"))


def build_spark(app_name: str, extra_conf: Optional[Dict[str, str]] = None) -> SparkSession:

    b = (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .config("spark.hadoop.fs.s3a.endpoint", Env().s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", Env().s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", Env().s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.hive.metastore.uris", Env().hive_metastore_uris)
        .config("spark.hadoop.fs.defaultFS", Env().fs_default_fs)
        .config("spark.sql.warehouse.dir", Env().warehouse_dir)
    )
    if extra_conf:
        for k, v in extra_conf.items():
            b = b.config(k, v)
    return b.getOrCreate()


def common_parser(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--app-name", required=False, default=None, help="Override Spark application name (optional).")
    p.add_argument("--dry-run", action="store_true", help="If set, do not write/publish; only print plan.")
    return p


def require_non_empty(s: str, what: str) -> str:
    if not s or not s.strip():
        raise ValueError(f"{what} must be non-empty")
    return s


def log_kv(**kwargs) -> None:
    msg = " ".join([f"{k}={v}" for k, v in kwargs.items()])
    print(msg, flush=True)

