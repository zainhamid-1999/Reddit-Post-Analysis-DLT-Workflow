### Silver Layer 

The **Silver Layer** is a critical step in our data pipeline that refines raw data from the Bronze layer to ensure it is clean, consistent, and structured for further analysis. Key operations performed in this layer include:

1. **Data Cleaning**: We remove rows with missing or invalid values in critical columns (e.g., `post_id`, `title`, `score`), ensuring data integrity.
2. **Deduplication**: Duplicate entries are removed based on the `post_id`, ensuring that each post is unique in the dataset.
3. **Data Transformation**: 
   - Timestamps are converted to human-readable formats for better usability.
   - Only posts with a score greater than 10 are retained, focusing on high-quality content.
4. **Aggregation**: Data is aggregated by `author` to provide insights into user activity and content contributions.
5. **Schema Enforcement**: Data is written to a Delta table with a well-defined schema, guaranteeing consistency and enabling optimized reads and writes.

The **Silver Layer** acts as a refined, cleansed dataset, preparing the data for more advanced analysis, aggregation, and reporting in the Gold layer, providing a solid foundation for generating business insights.

![image](https://github.com/user-attachments/assets/c0207410-cb53-4b83-994e-b63ff0e9143c)
![image](https://github.com/user-attachments/assets/2aadea9a-06b0-4bb1-b56c-9a1bf6e6af6f)

