from __future__ import annotations

from pyspark.sql import SparkSession

from lib.common import build_spark, common_parser, log_kv, Env
from lib.ch import publish_df_json_each_row


SRC_TABLE = "yelp_mart.mart_city_category_rating"
DST_TABLE = "mart_city_category_rating"



def main() -> None:
    p = common_parser("Publish Hive mart to ClickHouse (HTTP JSONEachRow, no driver collect).")
    p.add_argument("--src-table", default=SRC_TABLE, help="Hive source table.")
    p.add_argument("--dst-table", default=DST_TABLE, help="ClickHouse destination table (no DB prefix needed if CH_DB set).")
    p.add_argument("--no-truncate", action="store_true", help="Disable TRUNCATE before insert.")
    p.add_argument("--batch-rows", type=int, default=10000)
    args = p.parse_args()

    app_name = args.app_name or "job_business__publish__ch_city_category_rating"
    spark = build_spark(app_name)

    log_kv(step="read_table", table=args.src_table)
    df = spark.table(args.src_table)

    if args.dry_run:
        log_kv(dry_run=True, would_publish_rows=df.count(), dst=args.dst_table)
        spark.stop()
        return

    publish_df_json_each_row(
        df,
        table=args.dst_table,
        truncate=not args.no_truncate,
        batch_rows=args.batch_rows,
        env=Env(),
    )

    spark.stop()


if __name__ == "__main__":
    main()
