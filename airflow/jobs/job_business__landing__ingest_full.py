from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql import types as T

from lib.common import build_spark, common_parser, log_kv, require_non_empty


DEFAULT_S3_PREFIX = "s3a://raw/yelp/business/full/"
DEFAULT_HDFS_OUT = "/data/landing/business/parquet/full/"


def main() -> None:
    p = common_parser("Ingest Yelp business from S3 (raw JSON) to HDFS landing (Parquet).")
    p.add_argument("--s3-prefix", default=DEFAULT_S3_PREFIX, help="S3A prefix with business.json (directory).")
    p.add_argument("--hdfs-out", default=DEFAULT_HDFS_OUT, help="HDFS output directory for Parquet landing.")
    args = p.parse_args()

    s3_prefix = require_non_empty(args.s3_prefix, "--s3-prefix").rstrip("/") + "/"
    hdfs_out = require_non_empty(args.hdfs_out, "--hdfs-out").rstrip("/") + "/"

    app_name = args.app_name or "job_business__landing__ingest_full"
    spark = build_spark(app_name)

    src = s3_prefix + "*.json"
    log_kv(step="read", src=src)
    df = spark.read.json(src)

    expected = [
        "business_id", "name", "address", "city", "state", "postal_code",
        "latitude", "longitude", "stars", "review_count", "is_open",
        "attributes", "categories", "hours",
    ]

    for c in expected:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    df = (df
          .withColumn("review_count", F.col("review_count").cast("bigint"))
          .withColumn("is_open", F.col("is_open").cast("bigint"))
          .withColumn("latitude", F.col("latitude").cast("double"))
          .withColumn("longitude", F.col("longitude").cast("double"))
          .withColumn("stars", F.col("stars").cast("double"))
          .withColumn("postal_code", F.col("postal_code").cast("string"))
          .withColumn("categories", F.col("categories").cast("string"))
          .withColumn("name", F.col("name").cast("string"))
          .withColumn("address", F.col("address").cast("string"))
          .withColumn("city", F.col("city").cast("string"))
          .withColumn("state", F.col("state").cast("string"))
          .withColumn("business_id", F.col("business_id").cast("string"))
          )

    def json_stringify(col_name: str) -> F.Column:
        col = F.col(col_name)
        return F.when(col.isNull(), F.lit(None).cast("string")) \
                .when(col.cast("string").isNotNull() & (col.dtype == "string"), col.cast("string")) \
                .otherwise(F.to_json(col))

    schema = {f.name: f.dataType for f in df.schema.fields}

    if not isinstance(schema.get("attributes"), T.StringType):
        df = df.withColumn("attributes", F.to_json(F.col("attributes")))
    else:
        df = df.withColumn("attributes", F.col("attributes").cast("string"))

    if not isinstance(schema.get("hours"), T.StringType):
        df = df.withColumn("hours", F.to_json(F.col("hours")))
    else:
        df = df.withColumn("hours", F.col("hours").cast("string"))

    out_df = df.select(*expected)

    log_kv(step="write_parquet", dst=hdfs_out, mode="overwrite")
    if not args.dry_run:
        (out_df
         .write
         .mode("overwrite")
         .parquet(hdfs_out))

    log_kv(step="done")
    spark.stop()


if __name__ == "__main__":
    main()