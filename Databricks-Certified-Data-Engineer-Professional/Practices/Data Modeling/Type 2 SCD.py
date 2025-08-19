# Run notebook to copy/setup datasets
%run ../Includes/Copy-Datasets

# -----------------------------------
# SQL MERGE example (for reference)
# This query demonstrates the Type 2 SCD upsert logic for books_silver.
# It's used inside the `type2_upsert()` function with foreachBatch.
"""
MERGE INTO books_silver
USING (
    SELECT updates.book_id as merge_key, updates.*
    FROM updates

    UNION ALL

    SELECT NULL as merge_key, updates.*
    FROM updates
    JOIN books_silver ON updates.book_id = books_silver.book_id
    WHERE books_silver.current = true AND updates.price <> books_silver.price
  ) staged_updates
ON books_silver.book_id = merge_key 
WHEN MATCHED AND books_silver.current = true AND books_silver.price <> staged_updates.price THEN
  UPDATE SET current = false, end_date = staged_updates.updated
WHEN NOT MATCHED THEN
  INSERT (book_id, title, author, price, current, effective_date, end_date)
  VALUES (staged_updates.book_id, staged_updates.title, staged_updates.author, staged_updates.price, true, staged_updates.updated, NULL)
"""

# -----------------------------------
# Python function to perform Type 2 SCD upsert for each micro-batch
def type2_upsert(microBatchDF, batch):
    # Create temporary view for SQL MERGE
    microBatchDF.createOrReplaceTempView("updates")
    
    sql_query = """
        MERGE INTO books_silver
        USING (
            SELECT updates.book_id as merge_key, updates.*
            FROM updates

            UNION ALL

            SELECT NULL as merge_key, updates.*
            FROM updates
            JOIN books_silver ON updates.book_id = books_silver.book_id
            WHERE books_silver.current = true AND updates.price <> books_silver.price
          ) staged_updates
        ON books_silver.book_id = merge_key 
        WHEN MATCHED AND books_silver.current = true AND books_silver.price <> staged_updates.price THEN
          UPDATE SET current = false, end_date = staged_updates.updated
        WHEN NOT MATCHED THEN
          INSERT (book_id, title, author, price, current, effective_date, end_date)
          VALUES (staged_updates.book_id, staged_updates.title, staged_updates.author, staged_updates.price, true, staged_updates.updated, NULL)
    """
    
    # Execute the MERGE SQL for this micro-batch
    microBatchDF.sparkSession.sql(sql_query)

# -----------------------------------
# Create the Silver table for books if it doesn't exist
"""
CREATE TABLE IF NOT EXISTS books_silver
(book_id STRING, title STRING, author STRING, price DOUBLE, current BOOLEAN, effective_date TIMESTAMP, end_date TIMESTAMP)
"""

# -----------------------------------
# Function to process books topic from Bronze and apply Type 2 upsert
def process_books():
    schema = "book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP"
 
    query = (
        spark.readStream
             .table("bronze")  # Streaming read from Bronze table
             .filter("topic = 'books'")  # Only books topic
             .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))  # Parse JSON
             .select("v.*")  # Flatten struct
             .writeStream
             .foreachBatch(type2_upsert)  # Upsert each micro-batch into books_silver
             .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/books_silver")  # Checkpoint for exactly-once
             .trigger(availableNow=True)  # Process all available data immediately
             .start()
    )
    
    query.awaitTermination()

# Run the streaming process
process_books()

# -----------------------------------
# Display all books in Silver ordered by book_id and effective_date
books_df = spark.read.table("books_silver").orderBy("book_id", "effective_date")
display(books_df)

# -----------------------------------
# Simulate updates: load new book updates and process Bronze
bookstore.load_books_updates()
bookstore.process_bronze()
process_books()

# Display updated Silver table
books_df = spark.read.table("books_silver").orderBy("book_id", "effective_date")
display(books_df)

# -----------------------------------
# Create current_books table containing only the latest records
"""
CREATE OR REPLACE TABLE current_books
AS SELECT book_id, title, author, price
   FROM books_silver
   WHERE current IS TRUE
"""

# Select current books ordered by book_id
"""
SELECT *
FROM current_books
ORDER BY book_id
"""
