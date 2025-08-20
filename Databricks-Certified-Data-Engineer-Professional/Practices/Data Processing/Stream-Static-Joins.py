from pyspark.sql import functions as F

def process_books_sales():
    # Read streaming data from the "orders_silver" Delta table.
    # Each order may contain an array of books, so we use `explode` to flatten the array.
    orders_df = (
        spark.readStream.table("orders_silver")
             .withColumn("book", F.explode("books"))
    )

    # Read the static reference table containing current book information.
    # This table is not streaming — it acts as the static side of the join.
    books_df = spark.read.table("current_books")

    # Join streaming orders with static book reference data on `book_id`.
    # This enriches each order with details from the `current_books` table.
    query = (
        orders_df
            .join(books_df, orders_df.book.book_id == books_df.book_id, "inner")
            .writeStream
               # Append mode: new records are continuously appended to the sink.
               .outputMode("append")
               # Define checkpoint location for state management and fault tolerance.
               .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/books_sales")
               # Trigger once and process all available data (batch-like execution).
               .trigger(availableNow=True)
               # Write the results into a Delta table called "books_sales".
               .table("books_sales")
    )

    # Keep the query running until termination is requested.
    query.awaitTermination()
    
# Run the streaming job
process_books_sales()
