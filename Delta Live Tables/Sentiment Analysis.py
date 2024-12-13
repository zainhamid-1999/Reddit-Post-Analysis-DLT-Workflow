# Databricks notebook source
# MAGIC %pip install TextBlob
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.ml.feature import HashingTF, IDF, Tokenizer
# MAGIC from pyspark.ml import Pipeline
# MAGIC from textblob import TextBlob
# MAGIC
# MAGIC # Define UDFs for Sentiment and Emotion Analysis
# MAGIC def get_sentiment(text):
# MAGIC     if text:
# MAGIC         blob = TextBlob(text)
# MAGIC         return blob.sentiment.polarity
# MAGIC     return 0.0  # Return 0.0 for empty or null texts
# MAGIC
# MAGIC def get_emotion(text):
# MAGIC     sentiment = get_sentiment(text)
# MAGIC     if sentiment > 0.1:
# MAGIC         return "Positive"
# MAGIC     elif sentiment < -0.1:
# MAGIC         return "Negative"
# MAGIC     else:
# MAGIC         return "Neutral"
# MAGIC
# MAGIC # Register UDFs for sentiment and emotion analysis
# MAGIC spark.udf.register("get_sentiment", get_sentiment)
# MAGIC spark.udf.register("get_emotion", get_emotion)
# MAGIC
# MAGIC # Define the transformed Reddit posts (named 'gold_reddit_posts')
# MAGIC def gold_reddit_posts():
# MAGIC     # Load data from the Silver layer (LIVE keyword for continuous data)
# MAGIC     silver_df = spark.read.table("LIVE.silver_reddit_posts")  # DLT automatically reads from the live source
# MAGIC     
# MAGIC     # Perform sentiment and emotion analysis on 'title' and 'description'
# MAGIC     transformed_df = silver_df.withColumn("title_polarity", F.expr("get_sentiment(title)")) \
# MAGIC                               .withColumn("title_emotion", F.expr("get_emotion(title)")) \
# MAGIC                               .withColumn("description_polarity", F.expr("get_sentiment(description)")) \
# MAGIC                               .withColumn("description_emotion", F.expr("get_emotion(description)"))
# MAGIC     
# MAGIC     # Replace null values in 'title' and 'description' columns with empty strings
# MAGIC     transformed_df = transformed_df.fillna({'title': '', 'description': ''})
# MAGIC
# MAGIC     # Perform TF-IDF Feature Extraction for 'title' and 'description'
# MAGIC     tokenizer_title = Tokenizer(inputCol="title", outputCol="title_words")
# MAGIC     tokenizer_description = Tokenizer(inputCol="description", outputCol="description_words")
# MAGIC
# MAGIC     hashing_tf_title = HashingTF(inputCol="title_words", outputCol="title_tfidf")
# MAGIC     hashing_tf_description = HashingTF(inputCol="description_words", outputCol="description_tfidf")
# MAGIC
# MAGIC     idf_title = IDF(inputCol="title_tfidf", outputCol="title_tfidf_features")
# MAGIC     idf_description = IDF(inputCol="description_tfidf", outputCol="description_tfidf_features")
# MAGIC
# MAGIC     # Create a pipeline for TF-IDF feature extraction
# MAGIC     pipeline = Pipeline(stages=[tokenizer_title, tokenizer_description, hashing_tf_title, 
# MAGIC                                 hashing_tf_description, idf_title, idf_description])
# MAGIC
# MAGIC     # Fit and transform the data to extract features
# MAGIC     pipeline_model = pipeline.fit(transformed_df)
# MAGIC     final_df = pipeline_model.transform(transformed_df)
# MAGIC
# MAGIC     # Selecting necessary columns and transforming the data
# MAGIC     final_df_gold = final_df.select(
# MAGIC         "post_id",
# MAGIC         "title",
# MAGIC         "description",
# MAGIC         "score",
# MAGIC         "url",
# MAGIC         "created_at",
# MAGIC         "title_polarity",
# MAGIC         "title_emotion",
# MAGIC         "description_polarity",
# MAGIC         "description_emotion",
# MAGIC         "title_tfidf_features",
# MAGIC         "description_tfidf_features"
# MAGIC     )
# MAGIC     
# MAGIC     # Insert the transformed data into the Delta table (direct insert)
# MAGIC     final_df_gold.write.format("delta").mode("append").saveAsTable("big_data_analytics_v.big_data_analytics_sesssion_v.gold_reddit_posts")
# MAGIC

# COMMAND ----------

