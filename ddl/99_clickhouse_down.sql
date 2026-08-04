-- ============================================================
-- 99_clickhouse_down.sql
-- Rollback for ClickHouse objects
-- ============================================================

DROP TABLE IF EXISTS analytics.mart_review_fact_enriched;
DROP TABLE IF EXISTS analytics.mart_best_city_avg_rate;
DROP TABLE IF EXISTS analytics.mart_business_quality_monthly;
DROP TABLE IF EXISTS analytics.mart_category_creditcard_share;
DROP TABLE IF EXISTS analytics.mart_top_businesses_per_category;
DROP TABLE IF EXISTS analytics.mart_city_category_rating;

-- Optional hard reset:
-- DROP DATABASE IF EXISTS analytics;