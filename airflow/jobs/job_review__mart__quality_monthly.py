from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from lib.common import build_spark, common_parser, log_kv


SRC_FACT = "yelp_core.review_fact"
DST_MART = "yelp_mart.mart_business_quality_monthly"


def main() -> None:
    p = common_parser("Build mart: business quality monthly (avg_rating, review_cnt, rating_delta).")
    p.add_argument("--dst-table", default=DST_MART)
    args = p.parse_args()

    app_name = args.app_name or "job_review__mart__quality_monthly"
    spark = build_spark(app_name)

    fact = spark.table(SRC_FACT).select("business_id", "ym", F.col("stars").cast("int").alias("review_stars"))

    base = (fact.groupBy("business_id", "ym")
            .agg(
                F.avg(F.col("review_stars").cast("double")).alias("avg_rating"),
                F.count(F.lit(1)).cast("bigint").alias("review_cnt"),
            ))

    w = Window.partitionBy("business_id").orderBy("ym")
    df = (base
          .withColumn("prev_avg_rating", F.lag("avg_rating").over(w))
          .withColumn("rating_delta", F.col("avg_rating") - F.col("prev_avg_rating"))
          .drop("prev_avg_rating"))

    log_kv(step="write_table", table=args.dst_table, mode="overwrite")
    if not args.dry_run:
        df.write.mode("overwrite").format("parquet").saveAsTable(args.dst_table)

    log_kv(step="done")
    spark.stop()


if __name__ == "__main__":
    main()
