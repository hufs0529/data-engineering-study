# Count total rows in Bronze table for topic 'orders' (batch read)
(spark.read
      .table("bronze")  # Batch read from Bronze table
      .filter("topic = 'orders'")  # Filter only orders topic
      .count()
)

# COMMAND ----------
from pyspark.sql import functions as F

# Define JSON schema for orders
json_schema = """
order_id STRING, 
order_timestamp Timestamp, 
customer_id STRING, 
quantity BIGINT, 
total BIGINT, 
books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>
"""

# Read Bronze table as batch, parse JSON, remove duplicates, and count total
batch_total = (
    spark.read
         .table("bronze")  # Batch read
         .filter("topic = 'orders'")
         .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))  # Parse JSON
         .select("v.*")  # Flatten JSON
         .dropDuplicates(["order_id", "order_timestamp"])  # Remove duplicate orders
         .count()
)

print(batch_total)

# COMMAND ----------
# Stream processing: Bronze → Silver with deduplication and watermarking
deduped_df = (
    spark.readStream
         .table("bronze")  # Streaming read
         .filter("topic = 'orders'")  # Only orders topic
         .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))  # Parse JSON
         .select("v.*")  # Flatten JSON
         .withWatermark("order_timestamp", "30 seconds")  # Watermark for late data handling
         .dropDuplicates(["order_id", "order_timestamp"])  # Deduplicate based on order_id and timestamp
)

# COMMAND ----------
# Define foreachBatch function to upsert micro-batches into Silver table
def upsert_data(microBatchDF, batch):
    # Create temporary view for SQL MERGE
    microBatchDF.createOrReplaceTempView("orders_microbatch")
    
    sql_query = """
      MERGE INTO orders_silver a
      USING orders_microbatch b
      ON a.order_id=b.order_id AND a.order_timestamp=b.order_timestamp
      WHEN NOT MATCHED THEN INSERT *
    """
    
    # Execute MERGE SQL query
    microBatchDF.sparkSession.sql(sql_query)

# Create Silver table if not exists
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS orders_silver
# MAGIC (order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>)

# Write the streaming deduped DF to Silver using foreachBatch (MERGE for upsert)
query = (
    deduped_df.writeStream
              .foreachBatch(upsert_data)  # Upsert micro-batches into Silver table
              .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")  # Checkpoint location
              .trigger(availableNow=True)  # Process all available data immediately
              .start()
)

query.awaitTermination()

# Count total rows in Silver table after streaming processing
streaming_total = spark.read.table("orders_silver").count()

print(f"batch total: {batch_total}")
print(f"streaming total: {streaming_total}")
