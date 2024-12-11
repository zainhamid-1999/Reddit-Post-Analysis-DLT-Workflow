# Databricks notebook source

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql.functions import count, avg
from pyspark.sql.functions import *



# Create Spark session
spark = SparkSession.builder \
    .appName("Reddit Silver Layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


# COMMAND ----------

# Define the path for the Bronze layer folder (where you saved the raw data in Notebook 1)
bronze_layer_path = "dbfs:/mnt/big_data_analytics_v/big_data_analytics_sesssion_v/volume_reddit/bronze_layer"

# Load the data from the Bronze Layer into a DataFrame
df_bronze = spark.read.format("delta").load(bronze_layer_path)

# COMMAND ----------

# Step 1: Clean the data
# Drop rows with null values in any column
df_cleaned = df_bronze.dropna()

# Step 2: Ensure uniqueness by dropping duplicate rows based on the 'post_id' column
df_unique = df_cleaned.dropDuplicates(["post_id"])


# COMMAND ----------

# Log the number of unique rows for validation
print(f"Number of unique rows: {df_unique.count()}")

# Step 3: Perform additional transformations
df_transformed = df_unique.withColumn(
    "created_at", 
    unix_timestamp("created_utc").cast("timestamp")  # Make sure 'created_utc' is a valid timestamp
).filter(col("score") > 20)

# Step 4: Aggregate data by author to see the number of posts per author
df_author_count = df_transformed.groupBy("author", "subreddit").agg(count("post_id").alias("post_count"))


# COMMAND ----------

#from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, desc, row_number, col, lower, hour, weekofyear
# Step 5: Calculate Average Score per Author and Subreddit
df_avg_score = df_transformed.groupBy("author", "subreddit").agg(avg("score").alias("avg_score"))

# Step 6: Identify Top Performing Posts by Subreddit (Add Rank Column)
window_spec = Window.partitionBy("subreddit").orderBy(desc("score"))
df_top_posts = df_transformed.withColumn("rank", row_number().over(window_spec)).filter(col("rank") <= 10)

# Step 8: Extract Active Hours of Posting
df_active_hours = df_transformed.withColumn("hour", hour(col("created_at")))

# Step 9: Count Posts with URLs
df_with_urls = df_active_hours.withColumn("has_url", col("url").isNotNull())

# Step 10: Aggregate by Subreddit and Time Period
df_subreddit_trend = df_with_urls.withColumn("week", weekofyear(col("created_at")))



# COMMAND ----------

# Convert Spark DataFrame to Pandas DataFrame
df_combined_pd = df_subreddit_trend.toPandas()

# Display the Pandas DataFrame with better formatting
#from IPython.display import display
display(df_combined_pd)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG big_data_analytics_v;
# MAGIC
# MAGIC USE SCHEMA big_data_analytics_sesssion_v;
# MAGIC
# MAGIC CREATE TABLE silver_reddit_layer (
# MAGIC     post_id STRING,
# MAGIC     title STRING,
# MAGIC     description STRING,
# MAGIC     subreddit STRING,
# MAGIC     author STRING,
# MAGIC     score INT,
# MAGIC     created_at TIMESTAMP,
# MAGIC     url STRING,
# MAGIC     has_url BOOLEAN,
# MAGIC     created_utc TIMESTAMP,
# MAGIC     hour INT,
# MAGIC     week INT
# MAGIC )
# MAGIC USING delta;

# COMMAND ----------

# Step 3: Define the path for the Silver layer folder (where we will save the cleaned data)
silver_layer_path = "dbfs:/mnt/big_data_analytics_v/big_data_analytics_sesssion_v/tables/silver_reddit_layer"




# COMMAND ----------

# Step 4: Save the cleaned, unique data to the Silver Layer in Delta format
df_subreddit_trend.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(silver_layer_path)


# COMMAND ----------

df_subreddit_trend.createOrReplaceTempView("silver_reddit_layer_temp")

# Insert data into the newly created Delta table
spark.sql("""
    INSERT INTO big_data_analytics_v.big_data_analytics_sesssion_v.silver_reddit_layer
    SELECT 
        post_id,
        title,
        description,
        subreddit,
        author,
        score,
        created_at,
        url,
        has_url,
        created_utc,
        hour,
        week
    FROM silver_reddit_layer_temp
""")

# Log the completion of the insert process
print("Data inserted into Silver table successfully!")
