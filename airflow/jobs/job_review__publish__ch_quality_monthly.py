from __future__ import annotations


from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType

from lib.common import build_spark, common_parser, log_kv, Env
from lib.ch import publish_df_json_each_row_partitioned


SRC_TABLE = "yelp_mart.mart_business_quality_monthly"
DST_TABLE = "mart_business_quality_monthly" 

def sanitize_nan_inf(df):
    for f in df.schema.fields:
        if isinstance(f.dataType, (DoubleType, FloatType)):
            c = F.col(f.name)
            df = df.withColumn(
                f.name,
                F.when(F.isnan(c) | F.isnull(c), F.lit(None)).otherwise(
                    F.when(c == float("inf"), F.lit(None)).otherwise(
                        F.when(c == float("-inf"), F.lit(None)).otherwise(c)
                    )
                )
            )
    return df



def main() -> None:
    p = common_parser("Publish Hive mart to ClickHouse (HTTP JSONEachRow, no driver collect).")
    p.add_argument("--src-table", default=SRC_TABLE, help="Hive source table.")
    p.add_argument("--dst-table", default=DST_TABLE, help="ClickHouse destination table (no DB prefix needed if CH_DB set).")
    p.add_argument("--no-truncate", action="store_true", help="Disable TRUNCATE before insert.")
    p.add_argument("--batch-rows", type=int, default=10000)
    args = p.parse_args()

    app_name = args.app_name or "job_review__publish__ch_quality_monthly"
    spark = build_spark(app_name)

    log_kv(step="read_table", table=args.src_table)
    df = spark.table(args.src_table)
    df = sanitize_nan_inf(df)

    if args.dry_run:
        log_kv(dry_run=True, would_publish_rows=df.count(), dst=args.dst_table)
        spark.stop()
        return

    publish_df_json_each_row_partitioned(
        df,
        table=args.dst_table,
        partition_col="ym",
        truncate=not args.no_truncate,
        batch_rows=args.batch_rows,
        env=Env(),
    )

    spark.stop()


if __name__ == "__main__":
    main()
