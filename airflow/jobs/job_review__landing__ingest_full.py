from __future__ import annotations

import argparse
from pyspark.sql import functions as F

from lib.common import build_spark, common_parser, log_kv, require_non_empty


DEFAULT_S3_PREFIX = "s3a://raw/yelp/review/full/"
DEFAULT_HDFS_OUT = "/data/landing/review/parquet/full/"


def main() -> None:
    p = common_parser("Ingest Yelp review from S3 (raw JSON) to HDFS landing (Parquet).")
    p.add_argument("--s3-prefix", default=DEFAULT_S3_PREFIX, help="S3A prefix with review.json (directory).")
    p.add_argument("--hdfs-out", default=DEFAULT_HDFS_OUT, help="HDFS output directory for Parquet landing.")
    args = p.parse_args()

    s3_prefix = require_non_empty(args.s3_prefix, "--s3-prefix").rstrip("/") + "/"
    hdfs_out = require_non_empty(args.hdfs_out, "--hdfs-out").rstrip("/") + "/"

    app_name = args.app_name or "job_review__landing__ingest_full"
    spark = build_spark(app_name)

    src = s3_prefix + "*.json"
    log_kv(step="read", src=src)
    df = spark.read.json(src)

    expected = [
        "review_id","user_id","business_id","stars","useful","funny","cool","text","date"
    ]
    for c in expected:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    # Normalize date -> review_date TIMESTAMP
    out_df = (df
              .withColumn("review_date", F.to_timestamp("date"))
              .select(
                  "review_id","user_id","business_id",
                  F.col("stars").cast("int").alias("stars"),
                  F.col("useful").cast("int").alias("useful"),
                  F.col("funny").cast("int").alias("funny"),
                  F.col("cool").cast("int").alias("cool"),
                  F.col("text").cast("string").alias("text"),
                  "review_date"
              ))

    out_df = out_df.repartition(200)
    
    log_kv(step="write_parquet", dst=hdfs_out, mode="overwrite")
    if not args.dry_run:
        out_df.write.mode("overwrite").parquet(hdfs_out)

    log_kv(step="done")
    spark.stop()


if __name__ == "__main__":
    main()
