# Databricks notebook source
import praw
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

# Reddit API credentials
reddit = praw.Reddit(
    client_id="Cb9XMUcLMOnhnSO7JyHOSw",
    client_secret="XZlHL-BNgvaVJYasA6lG0DjFbqXbmA",
    user_agent="Delta Live pipeline"
)

# Fetch subreddit data synchronously using simple PRAW
def fetch_subreddit_data(subreddit_name="pakistan", limit=100):
    posts = []
    subreddit = reddit.subreddit(subreddit_name)
    
    # Fetching posts in a synchronous way
    for post in subreddit.hot(limit=limit):
        posts.append({
            "post_id": post.id,
            "title": post.title,
            "description": post.selftext if post.selftext else None,
            "subreddit": subreddit_name,
            "author": str(post.author),
            "score": post.score,
            "created_utc": datetime.utcfromtimestamp(post.created_utc),
            "url": post.url
        })
    
    print(f"Fetched {len(posts)} posts.")
    return posts

# Initialize Spark session with Delta support
def init_spark_session():
    spark = SparkSession.builder \
        .appName("Reddit Bronze Layer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    print("Spark session initialized.")
    return spark

# Function to save data to the Bronze layer (Delta format)
def save_to_bronze_layer():
    try:
        # Fetch raw data
        raw_data = fetch_subreddit_data(subreddit_name="pakistan", limit=100)

        if not raw_data:
            print("No data fetched, skipping save.")
            return

        # Initialize Spark session
        spark = init_spark_session()

        # Define schema for the data
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

        # Create DataFrame from raw data
        df = spark.createDataFrame(raw_data, schema=schema)
        print(f"DataFrame created with {df.count()} rows.")

        # Path to save the data to Delta table
        volume_path = "/mnt/Volumes/big_data_analytics_v/big_data_analytics_sesssion_v/volume_reddit/"

        # Write data to Delta table
        df.write.format("delta").mode("append").save(volume_path)
        print(f"Data saved to {volume_path}.")
    
    except Exception as e:
        print(f"Error in save_to_bronze_layer: {e}")

# Execute the pipeline
if __name__ == "__main__":
    save_to_bronze_layer()