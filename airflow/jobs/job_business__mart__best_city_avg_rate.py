from __future__ import annotations

from pyspark.sql import functions as F

from lib.common import build_spark, common_parser, log_kv


SRC_CORE = "yelp_core.business_core"
DST_MART = "yelp_mart.mart_best_city_avg_rate"


def main() -> None:
    p = common_parser("Build mart: best city avg rate summary.")
    p.add_argument("--dst-table", default=DST_MART)
    args = p.parse_args()

    app_name = args.app_name or "job_business__mart__best_city_avg_rate"
    spark = build_spark(app_name)

    core = spark.table(SRC_CORE).select("business_id", "city", "review_count", F.col("stars").alias("business_stars"))

    df = (core.groupBy("city")
          .agg(
              F.countDistinct("business_id").alias("business_cnt"),
              F.sum(F.col("review_count").cast("bigint")).alias("total_reviews"),
              F.avg("business_stars").alias("avg_business_rating"),
          ))

    log_kv(step="write_table", table=args.dst_table, mode="overwrite")
    if not args.dry_run:
        df.write.mode("overwrite").format("parquet").saveAsTable(args.dst_table)

    log_kv(step="done")
    spark.stop()


if __name__ == "__main__":
    main()
