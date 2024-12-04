# Databricks notebook source
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Reddit Silver Layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the path for the Bronze layer folder (where you saved the raw data in Notebook 1)
bronze_layer_path = "dbfs:/mnt/big_data_analytics_v/big_data_analytics_sesssion_v/volume_reddit/bronze_layer"

# Load the data from the Bronze Layer into a DataFrame
df_bronze = spark.read.format("delta").load(bronze_layer_path)

# Step 1: Clean the data
# Drop rows with null values in any column
df_cleaned = df_bronze.dropna()

# Step 2: Ensure uniqueness by dropping duplicate rows based on the 'post_id' column
df_unique = df_cleaned.dropDuplicates(["post_id"])

# Step 3: Define the path for the Silver layer folder (where we will save the cleaned data)
silver_layer_path = "dbfs:/mnt/big_data_analytics_v/big_data_analytics_sesssion_v/volume_reddit/silver_layer"

# Step 4: Save the cleaned, unique data to the Silver Layer in Delta format
df_unique.write.format("delta").mode("overwrite").save(silver_layer_path)

# COMMAND ----------

# Step 5: Insert the cleaned and unique data into the Silver table in Unity Catalog
df_unique.createOrReplaceTempView("silver_reddit_posts_temp")

# Insert data into the Silver table from the temporary view
spark.sql("""
    INSERT INTO big_data_analytics_v.big_data_analytics_sesssion_v.silver_reddit_posts
    SELECT * FROM silver_reddit_posts_temp
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 6: Display data from the Silver table
# MAGIC SELECT * FROM big_data_analytics_v.big_data_analytics_sesssion_v.silver_reddit_posts