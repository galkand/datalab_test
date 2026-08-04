-- ============================================================
-- 01_hive_up.sql
-- DDL for Hive: DBs + tables
--
-- Zones contract:
--   /data/landing/...          -> external tables only (yelp_raw, yelp_landing)
--   /user/hive/warehouse       -> managed tables only (yelp_core, yelp_mart)
--
-- Notes:
--   - LOCATION specified ONLY for external tables.
--   - Core/Mart are managed -> Hive controls storage under warehouse.
-- ============================================================

-- ----------------------------
-- 1) Databases
-- ----------------------------

CREATE DATABASE IF NOT EXISTS yelp_landing;
CREATE DATABASE IF NOT EXISTS yelp_core;
CREATE DATABASE IF NOT EXISTS yelp_mart;

-- ----------------------------
-- 2) Landing zone (external PARQUET)
-- ----------------------------
-- Typed landing in Parquet after Spark parse.
-- attributes/hours kept as JSON blob STRING to avoid fragile MAP typing at landing.

CREATE EXTERNAL TABLE IF NOT EXISTS yelp_landing.business_raw (
  business_id   STRING,
  name          STRING,
  address       STRING,
  city          STRING,
  state         STRING,
  postal_code   STRING,
  latitude      DOUBLE,
  longitude     DOUBLE,
  stars         DOUBLE,
  review_count  INT,
  is_open       INT,
  attributes    STRING,   -- JSON blob
  categories    STRING,   -- comma-separated
  hours         STRING    -- JSON blob (nullable)
)
STORED AS PARQUET
LOCATION '/data/landing/business/parquet/full/';

CREATE EXTERNAL TABLE IF NOT EXISTS yelp_landing.review_raw (
  review_id     STRING,
  user_id       STRING,
  business_id   STRING,
  stars         INT,
  useful        INT,
  funny         INT,
  cool          INT,
  text          STRING,
  review_date   TIMESTAMP
)
STORED AS PARQUET
LOCATION '/data/landing/review/parquet/full/';

-- ----------------------------
-- 3) Core zone (managed PARQUET)
-- ----------------------------
-- Fields in split tables are kept per your document.

CREATE TABLE IF NOT EXISTS yelp_core.business_core (
  business_id   STRING,
  name          STRING,
  address       STRING,
  city          STRING,
  state         STRING,
  postal_code   STRING,
  latitude      DOUBLE,
  longitude     DOUBLE,
  stars         DOUBLE,
  review_count  INT,
  is_open       INT
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS yelp_core.business_category (
  business_id   STRING,
  category      STRING
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS yelp_core.business_hours (
  business_id    STRING,
  day_of_week    STRING,
  open_hour      INT,
  open_minute    INT,
  close_hour     INT,
  close_minute   INT,
  open_minutes   INT,
  close_minutes  INT
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS yelp_core.business_attributes (
  business_id     STRING,
  attribute_key   STRING,
  attribute_value STRING
)
STORED AS PARQUET;

-- Review fact (5GB): managed + partitioned by ym ('YYYY-MM') for MVP sanity.
CREATE TABLE IF NOT EXISTS yelp_core.review_fact (
  review_id     STRING,
  user_id       STRING,
  business_id   STRING,
  stars         INT,
  useful        INT,
  funny         INT,
  cool          INT,
  review_date   TIMESTAMP
)
PARTITIONED BY (ym STRING)
STORED AS PARQUET;

-- ----------------------------
-- 4) Mart zone (managed PARQUET)
-- ----------------------------
-- Vitrines "at my discretion" (BI-friendly, stable, no extra sources).

-- 4.1 City x category rollup
CREATE TABLE IF NOT EXISTS yelp_mart.mart_city_category_rating (
  city           STRING,
  category       STRING,
  business_count BIGINT,
  avg_rating     DOUBLE
)
STORED AS PARQUET;

-- 4.2 Top businesses per category (rn for easy filtering in BI)
CREATE TABLE IF NOT EXISTS yelp_mart.mart_top_businesses_per_category (
  business_id    STRING,
  name           STRING,
  category       STRING,
  city           STRING,
  stars          DOUBLE,
  review_count   INT,
  rn             INT
)
STORED AS PARQUET;

-- 4.3 CreditCard acceptance share per category (based on attributes + category)
CREATE TABLE IF NOT EXISTS yelp_mart.mart_category_creditcard_share (
  category               STRING,
  businesses_with_info   BIGINT,
  accepting_credit_cards BIGINT,
  acceptance_level_pct   DOUBLE
)
STORED AS PARQUET;

-- 4.4 Quality dynamics per business per month (from review_fact)
CREATE TABLE IF NOT EXISTS yelp_mart.mart_business_quality_monthly (
  business_id    STRING,
  ym             STRING,
  avg_rating     DOUBLE,
  review_cnt     BIGINT,
  rating_delta   DOUBLE
)
STORED AS PARQUET;

-- 4.5 Best cities summary
CREATE TABLE IF NOT EXISTS yelp_mart.mart_best_city_avg_rate (
  city                 STRING,
  business_cnt         BIGINT,
  total_reviews        BIGINT,
  avg_business_rating  DOUBLE
)
STORED AS PARQUET;

-- 4.6 NEW: Enriched review facts (review_fact lookup business_core)
-- Purpose: drill-down analytics in Superset, without heavy review text.
-- Grain: 1 row = 1 review
CREATE TABLE IF NOT EXISTS yelp_mart.mart_review_fact_enriched (
  review_id              STRING,
  ym                     STRING,
  review_date            TIMESTAMP,

  business_id            STRING,
  business_name          STRING,
  city                   STRING,
  state                  STRING,
  latitude               DOUBLE,
  longitude              DOUBLE,
  is_open                INT,

  business_stars         DOUBLE,
  business_review_count  INT,

  user_id                STRING,
  review_stars           INT,
  useful                 INT,
  funny                  INT,
  cool                   INT
)
STORED AS PARQUET;