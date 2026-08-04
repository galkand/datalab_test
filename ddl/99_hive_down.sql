-- ============================================================
-- 99_hive_down.sql
-- Rollback for Hive objects
--
-- IMPORTANT:
--  - Dropping EXTERNAL tables does NOT delete HDFS data in /data/landing/...
--  - Dropping MANAGED tables deletes their data under /user/hive/warehouse/...
-- ============================================================

-- Drop marts first (depend on core)
DROP TABLE IF EXISTS yelp_mart.mart_review_fact_enriched;
DROP TABLE IF EXISTS yelp_mart.mart_best_city_avg_rate;
DROP TABLE IF EXISTS yelp_mart.mart_business_quality_monthly;
DROP TABLE IF EXISTS yelp_mart.mart_category_creditcard_share;
DROP TABLE IF EXISTS yelp_mart.mart_top_businesses_per_category;
DROP TABLE IF EXISTS yelp_mart.mart_city_category_rating;

-- Drop core
DROP TABLE IF EXISTS yelp_core.review_fact;
DROP TABLE IF EXISTS yelp_core.business_attributes;
DROP TABLE IF EXISTS yelp_core.business_hours;
DROP TABLE IF EXISTS yelp_core.business_category;
DROP TABLE IF EXISTS yelp_core.business_core;

-- Drop landing (external)
DROP TABLE IF EXISTS yelp_landing.review_raw;
DROP TABLE IF EXISTS yelp_landing.business_raw;

-- Option A (safe): keep databases
-- Option B (hard reset): uncomment CASCADE drops below.

-- DROP DATABASE IF EXISTS yelp_mart CASCADE;
-- DROP DATABASE IF EXISTS yelp_core CASCADE;
-- DROP DATABASE IF EXISTS yelp_landing CASCADE;
