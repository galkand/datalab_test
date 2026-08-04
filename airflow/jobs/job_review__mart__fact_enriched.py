from __future__ import annotations

from pyspark.sql import functions as F

from lib.common import build_spark, common_parser, log_kv


SRC_FACT = "yelp_core.review_fact"
SRC_BIZ = "yelp_core.business_core"
DST_MART = "yelp_mart.mart_review_fact_enriched"


def main() -> None:
    p = common_parser("Build mart: review facts enriched with business_core lookup (no text).")
    p.add_argument("--dst-table", default=DST_MART)
    args = p.parse_args()

    app_name = args.app_name or "job_review__mart__fact_enriched"
    spark = build_spark(app_name)

    fact = spark.table(SRC_FACT).select(
        "review_id",
        "ym",
        "review_date",
        "business_id",
        "user_id",
        F.col("stars").cast("int").alias("review_stars"),
        F.col("useful").cast("int").alias("useful"),
        F.col("funny").cast("int").alias("funny"),
        F.col("cool").cast("int").alias("cool"),
    )

    biz = spark.table(SRC_BIZ).select(
        "business_id",
        F.col("name").alias("business_name"),
        "city",
        "state",
        "latitude",
        "longitude",
        "is_open",
        F.col("stars").alias("business_stars"),
        F.col("review_count").alias("business_review_count"),
    )

    df = (fact.join(biz, on="business_id", how="left")
          .select(
              "review_id",
              "ym",
              "review_date",
              "business_id",
              "business_name",
              "city",
              "state",
              "latitude",
              "longitude",
              "is_open",
              "business_stars",
              "business_review_count",
              "user_id",
              "review_stars",
              "useful",
              "funny",
              "cool",
          ))

    log_kv(step="write_table", table=args.dst_table, mode="overwrite")
    if not args.dry_run:
        df.write.mode("overwrite").format("parquet").saveAsTable(args.dst_table)

    log_kv(step="done")
    spark.stop()


if __name__ == "__main__":
    main()
