from __future__ import annotations

import re
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lib.common import build_spark, common_parser, log_kv


SRC_TABLE = "yelp_landing.business_raw"

DST_CORE = "yelp_core.business_core"
DST_CAT = "yelp_core.business_category"
DST_ATTR = "yelp_core.business_attributes"
DST_HOURS = "yelp_core.business_hours"


def _clean_attr_key(col):
    # Defensive cleanup for cases like u'Key' / 'Key' / "Key"
    return F.regexp_replace(col, r"^(u')|(u\")|^'|^\"|\"$|'$", "")


def main() -> None:
    p = common_parser("Split business landing table into normalized core tables (per document).")
    p.add_argument("--src-table", default=SRC_TABLE)
    args = p.parse_args()

    app_name = args.app_name or "job_business__core__split"
    spark = build_spark(app_name)

    log_kv(step="read_table", table=args.src_table)
    df = spark.table(args.src_table)

    # -------------------------
    # 1) business_core (per document)
    # -------------------------
    core_cols = [
        "business_id","name","address","city","state","postal_code",
        "latitude","longitude","stars","review_count","is_open"
    ]
    core_df = (df.select(*core_cols)
                 .dropDuplicates(["business_id"]))

    log_kv(step="write_table", table=DST_CORE, mode="overwrite")
    if not args.dry_run:
        (core_df.write
             .mode("overwrite")
             .format("parquet")
             .saveAsTable(DST_CORE))

    # -------------------------
    # 2) business_category (explode categories)
    # -------------------------
    cat_df = (df
        .select("business_id", "categories")
        .where(F.col("categories").isNotNull())
        .withColumn("category", F.explode(F.split(F.col("categories"), r",\s*")))
        .withColumn("category", F.trim(F.col("category")))
        .where(F.length("category") > 0)
        .select("business_id", "category"))

    log_kv(step="write_table", table=DST_CAT, mode="overwrite")
    if not args.dry_run:
        (cat_df.write
              .mode("overwrite")
              .format("parquet")
              .saveAsTable(DST_CAT))

    # -------------------------
    # 3) business_attributes (JSON blob -> key/value)
    # -------------------------
    attr_schema = T.MapType(T.StringType(), T.StringType(), True)
    attr_map = F.from_json(F.col("attributes"), attr_schema, {"primitivesAsString": "true"})
    attr_df = (df
        .select("business_id", attr_map.alias("attr_map"))
        .where(F.col("attr_map").isNotNull())
        .select("business_id", F.explode(F.map_entries("attr_map")).alias("kv"))
        .select(
            "business_id",
            _clean_attr_key(F.col("kv.key")).alias("attribute_key"),
            F.col("kv.value").cast("string").alias("attribute_value"),
        ))

    log_kv(step="write_table", table=DST_ATTR, mode="overwrite")
    if not args.dry_run:
        (attr_df.write
              .mode("overwrite")
              .format("parquet")
              .saveAsTable(DST_ATTR))

    # -------------------------
    # 4) business_hours (JSON blob map day -> "HH:MM-HH:MM")
    # -------------------------
    hours_schema = T.MapType(T.StringType(), T.StringType(), True)
    hours_map = F.from_json(F.col("hours"), hours_schema, {"primitivesAsString": "true"})
    def split_time(prefix: str):
        return (
            F.regexp_extract(F.col("time_str"), r"^(\d{1,2}):(\d{1,2})", 1).cast("int").alias(f"{prefix}_hour"),
            F.regexp_extract(F.col("time_str"), r"^(\d{1,2}):(\d{1,2})", 2).cast("int").alias(f"{prefix}_minute"),
        )

    hours_entries = (df
        .select("business_id", hours_map.alias("hours_map"))
        .where(F.col("hours_map").isNotNull())
        .select("business_id", F.explode(F.map_entries("hours_map")).alias("kv"))
        .select(
            "business_id",
            F.col("kv.key").alias("day_of_week"),
            F.col("kv.value").alias("hours_range"),
        )
        .where(F.col("hours_range").isNotNull())
        .withColumn("open_str", F.split(F.col("hours_range"), "-").getItem(0))
        .withColumn("close_str", F.split(F.col("hours_range"), "-").getItem(1))
    )

    hours_df = (hours_entries
        .select(
            "business_id",
            "day_of_week",
            F.regexp_extract(F.col("open_str"), r"^(\d{1,2})", 1).cast("int").alias("open_hour"),
            F.regexp_extract(F.col("open_str"), r"^\d{1,2}:(\d{1,2})", 1).cast("int").alias("open_minute"),
            F.regexp_extract(F.col("close_str"), r"^(\d{1,2})", 1).cast("int").alias("close_hour"),
            F.regexp_extract(F.col("close_str"), r"^\d{1,2}:(\d{1,2})", 1).cast("int").alias("close_minute"),
        )
        .withColumn("open_minutes", F.col("open_hour") * 60 + F.col("open_minute"))
        .withColumn("close_minutes", F.col("close_hour") * 60 + F.col("close_minute"))
    )

    log_kv(step="write_table", table=DST_HOURS, mode="overwrite")
    if not args.dry_run:
        (hours_df.write
               .mode("overwrite")
               .format("parquet")
               .saveAsTable(DST_HOURS))

    log_kv(step="done")
    spark.stop()


if __name__ == "__main__":
    main()
