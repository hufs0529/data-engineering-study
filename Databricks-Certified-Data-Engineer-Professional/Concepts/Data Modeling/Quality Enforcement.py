# COMMAND ----------
# Run a notebook that copies/sets up datasets for the demo
# This prepares the data needed for the Bronze/Silver pipeline
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------
# Add a CHECK constraint on Silver table to ensure order_timestamp >= '2020-01-01'
# Prevents inserting rows with timestamps before 2020
# MAGIC %sql
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT timestamp_within_range CHECK (order_timestamp >= '2020-01-01');

# COMMAND ----------
# Describe the Silver table schema, properties, and constraints
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------
# Insert sample rows into Silver table for testing
# Row 2 violates the timestamp constraint
# MAGIC %sql
# MAGIC INSERT INTO orders_silver
# MAGIC VALUES ('1', '2022-02-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC        ('2', '2019-05-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC        ('3', '2023-01-01 00:00:00.000', 'C00001', 0, 0, NULL)

# COMMAND ----------
# Query inserted rows to check constraint enforcement
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver
# MAGIC WHERE order_id IN ('1', '2', '3')

# COMMAND ----------
# Add a CHECK constraint to enforce positive quantity
# Prevents rows with quantity <= 0
# MAGIC %sql
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT valid_quantity CHECK (quantity > 0);

# COMMAND ----------
# Describe table to confirm new constraints
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------
# Find rows that violate the quantity constraint
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver
# MAGIC WHERE quantity <= 0

# COMMAND ----------
from pyspark.sql import functions as F

# Define schema for orders JSON data
json_schema = """
order_id STRING, 
order_timestamp Timestamp, 
customer_id STRING, 
quantity BIGINT, 
total BIGINT, 
books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>
"""

# Stream processing: Bronze → Silver
query = (
    spark.readStream.table("bronze")  # Read Bronze table as streaming source
        .filter("topic = 'orders'")  # Filter only 'orders' topic
        # Parse JSON string in 'value' column into structured fields
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")  # Flatten JSON
        .filter("quantity > 0")  # Enforce quantity constraint
     .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")  # Checkpoint path
        .trigger(availableNow=True)  # Process all available data immediately
        .table("orders_silver")  # Write to Silver Delta table
)

# Wait for streaming query to finish
query.awaitTermination()

# COMMAND ----------
# Drop the timestamp constraint previously added
# MAGIC %sql
# MAGIC ALTER TABLE orders_silver DROP CONSTRAINT timestamp_within_range;

# COMMAND ----------
# Describe Silver table to confirm constraint removal
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------
# Drop the Silver table completely
# MAGIC %sql
# MAGIC DROP TABLE orders_silver

# COMMAND ----------
# Delete the checkpoint folder for cleanup
dbutils.fs.rm("dbfs:/mnt/demo_pro/checkpoints/orders_silver", True)
