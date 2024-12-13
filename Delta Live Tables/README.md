# Reddit Post Analysis Workflow

## Overview

This workflow processes Reddit posts using a multi-layered Delta Lake pipeline. The goal is to fetch, clean, and analyze Reddit posts from a specified subreddit, with sentiment and emotion analysis, followed by feature extraction using TF-IDF. The data is stored in Delta tables across three layers:

1. **Bronze Layer**: Raw data fetched from Reddit.
2. **Silver Layer**: Transformed and filtered data, enforcing data quality rules.
3. **Gold Layer**: Further transformation, including sentiment and emotion analysis and feature extraction.

The process leverages **PySpark** for data processing and **Delta Lake** for storage. Sentiment analysis is performed using **TextBlob**, and textual features are extracted using **TF-IDF**.

---

## Workflow Overview

### 1. **Reddit Data Fetching (Bronze Layer)**
- The Reddit API is used to fetch hot posts from a specified subreddit.
- Each post's key attributes, such as `post_id`, `title`, `description`, `score`, and `url`, are retrieved.
- The raw data is stored in a Delta table in the **Bronze Layer** for further processing.

### 2. **Data Transformation and Filtering (Silver Layer)**
- The data from the **Bronze Layer** is loaded and transformed:
  - Data is filtered to exclude posts with invalid or missing fields (e.g., `post_id`, `url`).
  - Posts with a score greater than 10 are selected.
  - The timestamp (`created_utc`) is cast to a proper timestamp format.
- The transformed data is stored in the **Silver Layer**.

### 3. **Sentiment and Emotion Analysis (Gold Layer)**
- **TextBlob** is used to perform sentiment analysis on the `title` and `description` of each post.
  - Sentiment polarity is calculated, which provides a score ranging from -1 (negative) to 1 (positive).
  - Posts are classified as "Positive," "Negative," or "Neutral" based on the sentiment polarity.
- The data is further processed using **TF-IDF** to extract features from the `title` and `description` text.
  - **Tokenizer**: Splits the text into words.
  - **HashingTF**: Converts words into numeric term frequencies.
  - **IDF**: Transforms the term frequencies into TF-IDF scores, emphasizing the importance of rare terms.
- The final data, which includes sentiment scores and TF-IDF features, is stored in the **Gold Layer**.

---

## Key Concepts

### Delta Lake
Delta Lake is a storage layer that brings reliability, consistency, and performance to data lakes. It supports ACID transactions, schema enforcement, and time travel. Delta tables are used throughout the workflow to store data at each stage (Bronze, Silver, and Gold).

### Sentiment Analysis
Sentiment analysis uses natural language processing (NLP) to determine the sentiment of a given text. In this workflow, **TextBlob** is used to calculate the sentiment polarity of each post's `title` and `description`. Sentiment scores range from -1 to 1, where negative values indicate negative sentiment, positive values indicate positive sentiment, and values around 0 indicate neutral sentiment.

### TF-IDF (Term Frequency-Inverse Document Frequency)
TF-IDF is a statistical measure used to evaluate the importance of a word in a document relative to a collection of documents (corpus). It is commonly used in text mining and natural language processing (NLP) to extract features from text.

- **Term Frequency (TF)**: Measures how frequently a term appears in a document.
- **Inverse Document Frequency (IDF)**: Measures how important a term is in the entire corpus (rarer terms get higher importance).
- The product of these values gives the **TF-IDF score**, which is used to represent the importance of words in the text.

---

## Layers in the Data Pipeline

### Bronze Layer
- Raw data fetched from Reddit posts is stored here.
- The focus is on collecting unfiltered and unprocessed data.

### Silver Layer
- Data is cleaned, filtered, and enriched.
- Data quality constraints (e.g., non-null `post_id`, valid URLs) are applied.
- Ensures only posts that meet specific criteria (e.g., score > 10) are included.

### Gold Layer
- Advanced transformations are applied, including sentiment and emotion analysis.
- Features are extracted using TF-IDF to create useful representations of text data for analysis or machine learning.


