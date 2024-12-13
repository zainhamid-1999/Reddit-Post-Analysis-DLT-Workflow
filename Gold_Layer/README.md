## Gold Layer
This project processes Reddit posts, analyzing their sentiment and emotion using Apache Spark. It involves the following steps:

1. **Sentiment & Emotion Analysis**: The sentiment (polarity) of the title and description of Reddit posts is analyzed using the `TextBlob` library. Based on the sentiment score, emotions (Positive, Negative, or Neutral) are assigned to each post.
  
2. **Feature Extraction with TF-IDF**: The title and description are tokenized, and Term Frequency-Inverse Document Frequency (TF-IDF) features are extracted to represent the text data numerically.

3. **Data Storage in Delta Lake**: The processed data is stored in a Delta Lake format, allowing efficient querying and storage in a structured, scalable manner.

4. **Gold Layer**: The final cleaned and enriched data is stored in a "Gold" layer table, ready for analysis.

## Process Flow
- **Input**: Reddit posts (including title, description, etc.) stored in the Silver layer.
- **Processing**: 
  - Sentiment and emotion analysis using `TextBlob`.
  - TF-IDF feature extraction for text representation.
- **Output**: Processed data with sentiment, emotion, and TF-IDF features is stored in the Gold layer for further use.


