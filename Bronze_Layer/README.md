# Bronze Layer

## Overview
The **Bronze Layer** serves as the raw data ingestion layer in the Delta Lake pipeline. It is designed to store unprocessed, raw data from external sources, such as APIs or logs, in its native form. This layer is primarily used to capture and store data before any transformations or cleaning are applied, ensuring that the original data remains intact for historical analysis or reprocessing.

## Data Ingestion
Data is ingested from external sources (e.g., Reddit posts) using APIs, and the raw records are stored as Delta tables in the **Bronze Layer**. This layer contains the most granular level of data and includes fields like post ID, title, author, score, creation timestamp, etc.

## Structure
- **Data Format**: Delta format (optimized for ACID transactions and scalability)
- **Location**: `/mnt/big_data_analytics_v/big_data_analytics_sesssion_v/volume_reddit/bronze_layer`
- **Schema**: 
  - `post_id` (String)
  - `title` (String)
  - `description` (String)
  - `subreddit` (String)
  - `author` (String)
  - `score` (Integer)
  - `created_utc` (Timestamp)
  - `url` (String)

## Usage
- The **Bronze Layer** stores raw, untransformed data, which can be accessed and processed in downstream layers (Silver, Gold) for further refinement and analysis.
- Data in this layer is typically not queried directly but serves as the foundational input for data transformations in the pipeline.
s.


