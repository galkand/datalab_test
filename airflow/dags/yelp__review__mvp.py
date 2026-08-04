from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from lib.spark_task import spark_task

DAG_ID = "yelp__review__mvp"
SPARK_POOL = "spark_mvp"

S3_REVIEW_PREFIX = "s3a://raw/yelp/review/full/"
HDFS_REVIEW_LANDING = "/data/landing/review/parquet/full/"


def spark_task_pooled(*, task_id: str, job_file: str, extra_args: list[str] | None = None):
    op = spark_task(
        task_id=task_id,
        app_name=f"{DAG_ID}::{task_id}",
        job_file=job_file,
        extra_args=extra_args or [],
    )
    op.pool = SPARK_POOL
    return op


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["yelp", "mvp", "review"],
) as dag:

    ingest = spark_task_pooled(
        task_id="spark__landing__review__ingest_full",
        job_file="job_review__landing__ingest_full.py",
        extra_args=[
            "--s3-prefix", S3_REVIEW_PREFIX,
            "--hdfs-out", HDFS_REVIEW_LANDING,
        ],
    )

    fact = spark_task_pooled(
        task_id="spark__core__review__write_fact_partitioned",
        job_file="job_review__core__write_fact_partitioned.py",
        extra_args=[
            "--src-table", "yelp_landing.review_raw",
            "--dst-table", "yelp_core.review_fact",
        ],
    )

    with TaskGroup(group_id="marts") as marts:
        quality = spark_task_pooled(
            task_id="spark__mart__review__quality_monthly",
            job_file="job_review__mart__quality_monthly.py",
        )
        enriched = spark_task_pooled(
            task_id="spark__mart__review__fact_enriched",
            job_file="job_review__mart__fact_enriched.py",
        )

        quality >> enriched

    with TaskGroup(group_id="publish") as publish:
        pub_quality = spark_task_pooled(
            task_id="spark__publish__review__ch_quality_monthly",
            job_file="job_review__publish__ch_quality_monthly.py",
        )
        pub_enriched = spark_task_pooled(
            task_id="spark__publish__review__ch_fact_enriched",
            job_file="job_review__publish__ch_fact_enriched.py",
        )

        pub_quality >> pub_enriched

    ingest >> fact >> marts >> publish
