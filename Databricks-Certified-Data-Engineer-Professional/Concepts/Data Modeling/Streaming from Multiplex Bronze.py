


from pyspark.sql import functions as F

# Define the schema for the JSON orders data
json_schema = """
order_id STRING,
order_timestamp Timestamp,
customer_id STRING,
quantity BIGINT,
total BIGINT,
books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>
"""

# Read Bronze Delta table as a streaming source and process Orders data
query = (
    spark.readStream.table("bronze")  # Read streaming data from Bronze layer
        .filter("topic = 'orders'")  # Filter only 'orders' topic
        # Parse the JSON string in 'value' column using the defined schema
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")  # Flatten the JSON structure into individual columns
     .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")  # Set checkpoint location
        .trigger(availableNow=True)  # Process all available files immediately as a batch
        .table("orders_silver")  # Write output to Silver Delta table
)

# Wait until the streaming query finishes
query.awaitTermination()
