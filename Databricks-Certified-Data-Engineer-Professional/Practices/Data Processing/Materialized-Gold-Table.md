from pyspark.sql import functions as F

# Read streaming data from the "books_sales" Delta table
query = (
    spark.readStream
         .table("books_sales")
         # Set watermark on the "order_timestamp" column to handle late data.
         # Here, we allow data that is up to 10 minutes late to be considered.
         .withWatermark("order_timestamp", "10 minutes")
         # Group by a time window of 5 minutes and the "author" column
         # This allows aggregation per author within each 5-minute window.
         .groupBy(
             F.window("order_timestamp", "5 minutes").alias("time"),
             "author"
         )
         # Compute aggregations:
         # - Count the number of orders per author per window
         # - Compute the average quantity sold per author per window
         .agg(
             F.count("order_id").alias("orders_count"),
             F.avg("quantity").alias("avg_quantity")
         )
         # Write the aggregated results to a Delta table
         .writeStream
             # Checkpoint location to allow fault-tolerance and state recovery
             .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/authors_stats")
             # Trigger once to process all available data immediately
             .trigger(availableNow=True)
             # Output to a Delta table called "authors_stats"
             .table("authors_stats")
)

# Keep the streaming query running until termination is requested
query.awaitTermination()


### withWatermark: Allows handling late events while limiting state memory usage.
### window: Time-based aggregation, here 5-minute tumbling windows.
### Aggregations: Count of orders and average quantity per author per window.
### availableNow=True trigger: Processes all currently available data once, similar to batch processing, then stops.
### Checkpointing: Ensures fault-tolerance and exactly-once processing.