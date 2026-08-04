from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from lib.spark_task import spark_task

DAG_ID = "yelp__business__mvp"
SPARK_POOL = "spark_mvp"

S3_BUSINESS_PREFIX = "s3a://raw/yelp/business/full/"
HDFS_BUSINESS_LANDING = "/data/landing/business/parquet/full/"


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
    tags=["yelp", "mvp", "business"],
) as dag:

    ingest = spark_task_pooled(
        task_id="spark__landing__business__ingest_full",
        job_file="job_business__landing__ingest_full.py",
        extra_args=[
            "--s3-prefix", S3_BUSINESS_PREFIX,
            "--hdfs-out", HDFS_BUSINESS_LANDING,
        ],
    )

    split = spark_task_pooled(
        task_id="spark__core__business__split",
        job_file="job_business__core__split.py",
        extra_args=[
            "--src-table", "yelp_landing.business_raw",
        ],
    )

    with TaskGroup(group_id="marts") as marts:
        mart_city_category = spark_task_pooled(
            task_id="spark__mart__business__city_category_rating",
            job_file="job_business__mart__city_category_rating.py",
        )
        mart_best_city = spark_task_pooled(
            task_id="spark__mart__business__best_city_avg_rate",
            job_file="job_business__mart__best_city_avg_rate.py",
        )

    with TaskGroup(group_id="publish") as publish:
        pub_city_category = spark_task_pooled(
            task_id="spark__publish__business__ch_city_category_rating",
            job_file="job_business__publish__ch_city_category_rating.py",
        )
        pub_best_city = spark_task_pooled(
            task_id="spark__publish__business__ch_best_city_avg_rate",
            job_file="job_business__publish__ch_best_city_avg_rate.py",
        )

    ingest >> split >> marts
    mart_city_category >> pub_city_category
    mart_best_city >> pub_best_city
