# Reddit Data Pipeline

This project implements a Delta Live Tables (DLT) pipeline to extract, transform, and load (ETL) Reddit data into Bronze, Silver, and Gold layers for further analysis and reporting.

# Features

# Data Extraction

Fetches data from Reddit's API using the praw Python library.

Extracts hot posts from the pakistan subreddit.

# Bronze Layer

Raw Reddit data is stored in Delta format in the Bronze layer.

# Silver Layer

Performs transformations, filtering, and derives additional metrics such as:

post_count and avg_score using window functions.

Unique constraints and quality checks.

# Gold Layer

Enhances the data with sentiment analysis using TextBlob.

Adds columns such as title_sentiment, description_sentiment, title_length, and description_length.

# Project Structure

## Bronze Layer

### Raw Reddit posts are stored with the following schema:

post_id (String)

title (String)

description (String)

subreddit (String)

author (String)

score (Integer)

created_utc (Timestamp)

url (String)

## Silver Layer

### Transformed Reddit posts include additional derived metrics:

has_url: Boolean to indicate if a post has a URL.

hour: Extracted hour of creation.

week: Week of the year the post was created.

post_count: Number of posts by the same author in the subreddit.

avg_score: Average score of posts by the same author in the subreddit.

rank: Rank of the post based on score within the subreddit.

## Gold Layer

### Final layer with additional enhancements:

Sentiment analysis for title and description using TextBlob.

Added metrics like title_length and description_length.

# Technologies Used

Languages: Python, SQL

Big Data Frameworks: Apache Spark, Delta Lake

Sentiment Analysis: TextBlob

Pipeline Orchestration: Delta Live Tables (DLT)

Cloud Storage: Databricks File System (DBFS)

# Setup Instructions

## Install the required Python packages:

pip install pyspark praw textblob delta-spark

Configure Databricks File System (DBFS) paths.

Update the Reddit API credentials in the praw.Reddit section of the script.

# Run the pipeline scripts sequentially:

Bronze Layer: Fetch and save raw Reddit data.

Silver Layer: Execute the SQL query in a Databricks notebook.

Gold Layer: Use Delta Live Tables for sentiment analysis and metrics.

Folder Structure

project/
|-- bronze_layer.py         # Script to fetch and store raw Reddit data
|-- silver_layer.sql        # SQL script for Silver layer transformations
|-- gold_layer.py           # Gold layer with sentiment analysis
|-- README.md               # Project documentation

# How It Works

Fetch data from Reddit's API using praw.

Store the raw data in the Bronze layer using Delta format.

Apply transformations, filtering, and derive metrics in the Silver layer.

Enhance the data with sentiment analysis and finalize the schema in the Gold layer.

# Data Flow

Bronze Layer: Raw data ingestion.

Silver Layer: Clean and derive metrics.

Gold Layer: Enhance with sentiment analysis.

# Future Enhancements

Incorporate more subreddits dynamically.

Add advanced sentiment analysis models.

Enable scheduling for regular data updates.

Integrate dashboards using Power BI or Tableau.

Contributing

Feel free to fork this repository and submit pull requests. For major changes, please open an issue first to discuss your ideas.

