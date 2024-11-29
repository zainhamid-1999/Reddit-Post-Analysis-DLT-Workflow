# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create processed Reddit data Delta table
# MAGIC CREATE TABLE IF NOT EXISTS processed_reddit_posts (
# MAGIC     id INT,
# MAGIC     title STRING,
# MAGIC     score STRING,
# MAGIC     num_comments STRING,
# MAGIC     author STRING,
# MAGIC     created_utc STRING,
# MAGIC     url STRING,
# MAGIC     over_18 INT,
# MAGIC     edited INT,
# MAGIC     spoiler STRING,
# MAGIC     stickied INT
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- id,	title,	score,	num_comments,	author,	created_utc,	url,	over_18,	edited,	spoiler,	stickied.
# MAGIC