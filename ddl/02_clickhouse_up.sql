-- ============================================================
-- 02_clickhouse_up.sql
-- DDL for ClickHouse serving layer
-- ============================================================

CREATE DATABASE IF NOT EXISTS analytics;

-- 1) mart_city_category_rating
CREATE TABLE IF NOT EXISTS analytics.mart_city_category_rating (
  city           String,
  category       String,
  business_count UInt64,
  avg_rating     Float64
)
ENGINE = MergeTree
ORDER BY (city, category);

-- 2) mart_top_businesses_per_category
CREATE TABLE IF NOT EXISTS analytics.mart_top_businesses_per_category (
  business_id   String,
  name          String,
  category      String,
  city          String,
  stars         Float64,
  review_count  Int32,
  rn            Int32
)
ENGINE = MergeTree
ORDER BY (category, rn, stars, business_id);

-- 3) mart_category_creditcard_share
CREATE TABLE IF NOT EXISTS analytics.mart_category_creditcard_share (
  category               String,
  businesses_with_info   UInt64,
  accepting_credit_cards UInt64,
  acceptance_level_pct   Float64
)
ENGINE = MergeTree
ORDER BY category;

-- 4) mart_business_quality_monthly
CREATE TABLE IF NOT EXISTS analytics.mart_business_quality_monthly (
  business_id   String,
  ym            String,
  avg_rating    Float64,
  review_cnt    UInt64,
  rating_delta  Float64
)
ENGINE = MergeTree
PARTITION BY ym
ORDER BY (business_id, ym);

-- 5) mart_best_city_avg_rate
CREATE TABLE IF NOT EXISTS analytics.mart_best_city_avg_rate (
  city                String,
  business_cnt        UInt64,
  total_reviews       UInt64,
  avg_business_rating Float64
)
ENGINE = MergeTree
ORDER BY city;

-- 6) NEW: mart_review_fact_enriched
CREATE TABLE IF NOT EXISTS analytics.mart_review_fact_enriched (
  review_id             String,
  ym                    String,
  review_date           DateTime,

  business_id           String,
  business_name         String,
  city                  String,
  state                 String,
  latitude              Float64,
  longitude             Float64,
  is_open               UInt8,

  business_stars        Float64,
  business_review_count UInt32,

  user_id               String,
  review_stars          Int32,
  useful                Int32,
  funny                 Int32,
  cool                  Int32
)
ENGINE = MergeTree
PARTITION BY ym
ORDER BY (business_id, review_date, review_id);