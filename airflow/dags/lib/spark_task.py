from __future__ import annotations

import os
from typing import Mapping, Sequence, Optional, Dict

from airflow.providers.docker.operators.docker import DockerOperator


DEFAULT_IMAGE = "datalab-spark-master:latest"
DEFAULT_NETWORK = "datalabnet"
DEFAULT_MASTER = "spark://spark-master:7077"
SPARK_SUBMIT_BIN = "/opt/spark/bin/spark-submit"


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default

SPARK_PROFILES: dict[str, dict[str, str]] = {
    "mvp": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.shuffle.partitions": _env("SPARK_SQL_SHUFFLE_PARTITIONS", "32"),
    },
    "debug": {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.shuffle.partitions": "8",
    },
}


DEFAULT_CONFS: dict[str, str] = {
    # Driver networking
    "spark.driver.bindAddress": "0.0.0.0",

    # S3A (MinIO)
    "spark.hadoop.fs.s3a.endpoint": _env("S3A_ENDPOINT", "http://minio:9000"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",

    # Hive/HDFS
    "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
    "spark.hadoop.fs.defaultFS": "hdfs://namenode:8020",
    "spark.sql.warehouse.dir": "hdfs://namenode:8020/user/hive/warehouse",
}


def _spark_profile_confs() -> dict[str, str]:
    profile = _env("SPARK_PROFILE", "mvp")
    return dict(SPARK_PROFILES.get(profile, SPARK_PROFILES["mvp"]))


def _s3a_secret_confs() -> dict[str, str]:
    ak = _env("S3A_ACCESS_KEY")
    sk = _env("S3A_SECRET_KEY")
    if ak and sk:
        return {
            "spark.hadoop.fs.s3a.access.key": ak,
            "spark.hadoop.fs.s3a.secret.key": sk,
        }
    return {}


def _default_environment_passthrough() -> Dict[str, str]:
    keys = [
        "S3A_ENDPOINT",
        "S3A_ACCESS_KEY",
        "S3A_SECRET_KEY",
        "CH_HOST",
        "CH_PORT",
        "CH_DB",
        "CH_USER",
        "CH_PASSWORD",
        "SPARK_PROFILE",
        "SPARK_SQL_SHUFFLE_PARTITIONS",
        "SPARK_DRIVER_HOST",
        "RUN_ID",
    ]
    env: Dict[str, str] = {}
    for k in keys:
        v = _env(k)
        if v is not None:
            env[k] = v
    return env


def build_spark_submit_command(
    *,
    app_name: str,
    job_path: str,
    master: str = DEFAULT_MASTER,
    extra_confs: Optional[Mapping[str, str]] = None,
    extra_args: Optional[Sequence[str]] = None,
    debug: bool = False,
) -> str:
    confs = dict(DEFAULT_CONFS)

    confs.update(_spark_profile_confs())

    confs.update(_s3a_secret_confs())

    if extra_confs:
        confs.update(extra_confs)

    args = [a for a in (extra_args or []) if a and a.strip()]

    debug_block = ""
    if debug:
        debug_block = r"""
            echo "[spark] hostname: $(hostname)"
            echo "[spark] hostname -i: $(hostname -i || true)"
            echo "[spark] PATH=$PATH"
            echo "[spark] spark-submit:"
            ls -la /opt/spark/bin/spark-submit || true
        """

    conf_parts = " ".join(f"--conf {k}={v}" for k, v in sorted(confs.items()))
    extra_parts = " ".join(args)

    # IPv4-safe driver host + override via SPARK_DRIVER_HOST
    return f"""bash -lc '
        set -euo pipefail
        {debug_block}

        JOB="{job_path}"
        if [ ! -f "$JOB" ]; then
          echo "[spark] ERROR: job file not found: $JOB"
          echo "[spark] Listing /work/jobs:"
          ls -la /work/jobs || true
          exit 2
        fi

        DRIVER_HOST="${{SPARK_DRIVER_HOST:-}}"
        if [ -z "$DRIVER_HOST" ]; then
          if command -v getent >/dev/null 2>&1; then
            DRIVER_HOST="$(getent ahostsv4 "$(hostname)" | awk "NR==1{{print \\$1}}")"
          fi
        fi
        if [ -z "$DRIVER_HOST" ]; then
          DRIVER_HOST="$(hostname -i | awk "{{print \\$1}}")"
        fi

        echo "[spark] app_name={app_name}"
        echo "[spark] master={master}"
        echo "[spark] driver_host=$DRIVER_HOST"

        {SPARK_SUBMIT_BIN} \
        --master {master} \
        --deploy-mode client \
        --conf spark.app.name={app_name} \
        --conf spark.driver.host=$DRIVER_HOST \
        {conf_parts} \
        "$JOB" \
        {extra_parts}
    '"""


def spark_task(
    *,
    task_id: str,
    app_name: str,
    job_file: str,
    image: str = DEFAULT_IMAGE,
    network_mode: str = DEFAULT_NETWORK,
    environment: Optional[Mapping[str, str]] = None,
    extra_confs: Optional[Mapping[str, str]] = None,
    extra_args: Optional[Sequence[str]] = None,
    debug: bool = False,
) -> DockerOperator:
    env = _default_environment_passthrough()
    if environment:
        env.update(dict(environment))

    return DockerOperator(
        task_id=task_id,
        image=image,
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        auto_remove=True,
        network_mode=network_mode,
        mount_tmp_dir=False,
        do_xcom_push=False,
        tty=True,
        environment=env,
        command=build_spark_submit_command(
            app_name=app_name,
            job_path=f"/work/jobs/{job_file}",
            extra_confs=extra_confs,
            extra_args=extra_args,
            debug=debug,
        ),
    )
