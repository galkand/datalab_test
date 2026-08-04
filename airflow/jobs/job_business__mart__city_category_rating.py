from __future__ import annotations

from pyspark.sql import functions as F

from lib.common import build_spark, common_parser, log_kv


SRC_CORE = "yelp_core.business_core"
SRC_CAT = "yelp_core.business_category"
DST_MART = "yelp_mart.mart_city_category_rating"


def main() -> None:
    p = common_parser("Build mart: city x category rating rollup.")
    p.add_argument("--dst-table", default=DST_MART)
    args = p.parse_args()

    app_name = args.app_name or "job_business__mart__city_category_rating"
    spark = build_spark(app_name)

    core = spark.table(SRC_CORE).select("business_id", "city", F.col("stars").alias("business_stars"))
    cat = spark.table(SRC_CAT)

    df = (core.join(cat, on="business_id", how="inner")
          .groupBy("city", "category")
          .agg(
              F.countDistinct("business_id").alias("business_count"),
              F.avg("business_stars").alias("avg_rating"),
          ))

    log_kv(step="write_table", table=args.dst_table, mode="overwrite")
    if not args.dry_run:
        df.write.mode("overwrite").format("parquet").saveAsTable(args.dst_table)

    log_kv(step="done")
    spark.stop()


if __name__ == "__main__":
    main()
