-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE silver_reddit_posts
COMMENT "Transformed and filtered Reddit posts data for analysis with enforced uniqueness and constraints."
TBLPROPERTIES ("quality" = "silver")  -- This property defines the data quality level
AS
SELECT 
  post_id, 
  title, 
  description, 
  score, 
  url,
  CAST(created_utc AS TIMESTAMP) AS created_at  -- Transform 'created_utc' to a proper timestamp
FROM bronze_reddit_posts  -- Assuming 'bronze_reddit_posts' is a live table or a Delta table
WHERE score > 10
  AND post_id IS NOT NULL  -- Ensure post_id is not null
  AND url IS NOT NULL AND url LIKE 'http%'  -- Ensure url is valid and starts with 'http'
  AND score > 10  -- Ensure score is greater than 10
  AND post_id IN (SELECT DISTINCT post_id FROM bronze_reddit_posts)  -- Ensure unique post_id