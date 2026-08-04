from __future__ import annotations

from pyspark.sql import functions as F

from lib.common import build_spark, common_parser, log_kv


SRC_TABLE = "yelp_landing.review_raw"
DST_TABLE = "yelp_core.review_fact"


def main() -> None:
    p = common_parser("Write review fact as managed Hive table partitioned by ym=YYYY-MM.")
    p.add_argument("--src-table", default=SRC_TABLE)
    p.add_argument("--dst-table", default=DST_TABLE)
    args = p.parse_args()

    app_name = args.app_name or "job_review__core__write_fact_partitioned"
    spark = build_spark(app_name)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    log_kv(step="read_table", table=args.src_table)
    df = spark.table(args.src_table)

    fact_df = (df
        .withColumn("ym", F.date_format(F.col("review_date"), "yyyy-MM"))
        .select(
            "review_id",
            "user_id",
            "business_id",
            F.col("stars").cast("int").alias("stars"),
            F.col("useful").cast("int").alias("useful"),
            F.col("funny").cast("int").alias("funny"),
            F.col("cool").cast("int").alias("cool"),
            "review_date",
            "ym",
        ))

    log_kv(step="write_table", table=args.dst_table, mode="overwrite", partition="ym(dynamic)")
    if not args.dry_run:
        fact_df.write.mode("overwrite").insertInto(args.dst_table)

    log_kv(step="done")
    spark.stop()


if __name__ == "__main__":
    main()
