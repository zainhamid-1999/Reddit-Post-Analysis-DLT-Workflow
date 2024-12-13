# Databricks notebook source
# MAGIC %pip install praw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Libraries

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from datetime import datetime
import praw
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


# COMMAND ----------

# MAGIC %md
# MAGIC ### Reddit API Setup

# COMMAND ----------

# Reddit API credentials and setup
REDDIT_CLIENT_ID = "Cb9XMUcLMOnhnSO7JyHOSw"
REDDIT_CLIENT_SECRET = "XZlHL-BNgvaVJYasA6lG0DjFbqXbmA"
REDDIT_USER_AGENT = "Delta Live pipeline"

# Initialize Reddit API client
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch Subreddit Data

# COMMAND ----------

# Function to fetch subreddit data
def fetch_subreddit_data(subreddit_name="pakistan", limit=100):
    """Fetch hot posts from a subreddit."""
    subreddit = reddit.subreddit(subreddit_name)
    posts = [
        {
            "post_id": post.id,
            "title": post.title,
            "description": post.selftext or None,
            "subreddit": subreddit_name,
            "author": str(post.author),
            "score": post.score,
            "created_utc": datetime.utcfromtimestamp(post.created_utc),
            "url": post.url
        }
        for post in subreddit.hot(limit=limit)
    ]
    return posts

# Fetch raw data from the subreddit
subreddit_name = "pakistan"
raw_data = fetch_subreddit_data(subreddit_name=subreddit_name, limit=100)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Spark Session & Define Schema

# COMMAND ----------

# Create Spark session with Delta Lake extensions
spark = SparkSession.builder \
    .appName("Reddit Bronze Layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define schema for the DataFrame
schema = StructType([
    StructField("post_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("author", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("created_utc", TimestampType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Data to Delta Format

# COMMAND ----------

# Create DataFrame from raw data
df = spark.createDataFrame(raw_data, schema=schema)

# Create or replace the temporary view with the DataFrame
df.createOrReplaceTempView("bronze_reddit_posts_temp")

# Write the raw data to Delta format as the Bronze layer
df.write.format("delta").mode("overwrite").saveAsTable("big_data_analytics_v.big_data_analytics_sesssion_v.bronze_reddit_posts")