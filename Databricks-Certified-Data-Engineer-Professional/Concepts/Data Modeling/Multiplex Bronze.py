# List files in Kafka raw dataset folder
files = dbutils.fs.ls(f"{dataset_bookstore}/kafka-raw")
display(files)  # Display files in Databricks

# Read Kafka raw JSON files in batch mode
df_raw = spark.read.json(f"{dataset_bookstore}/kafka-raw")
display(df_raw)  # Display the raw data

from pyspark.sql import functions as F

# Define a function to process Bronze layer
def process_bronze():
    # Define the schema for Kafka JSON data
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

    # Stream processing using Autoloader
    query = (
        spark.readStream
             .format("cloudFiles")  # Use Databricks Autoloader
             .option("cloudFiles.format", "json")  # Specify file format
             .schema(schema)  # Apply schema
             .load(f"{dataset_bookstore}/kafka-raw")  # Kafka raw data path
             .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))  # Convert ms -> timestamp
             .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))  # Add year-month column
             .writeStream
                 .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/bronze")  # Checkpoint path
                 .option("mergeSchema", True)  # Allow schema merging
                 .partitionBy("topic", "year_month")  # Partition data
                 .trigger(availableNow=True)  # Batch trigger for immediate execution
                 .table("bronze")  # Save as Bronze Delta table
    )
    
    query.awaitTermination()  # Wait for streaming query to finish

# Run the Bronze processing function
process_bronze()

# Read Bronze Delta table as a batch DataFrame
batch_df = spark.table("bronze")
display(batch_df)  # Display Bronze layer data

# SQL: Select all records from Bronze table
# %sql
# SELECT * FROM bronze

# SQL: List distinct Kafka topics in Bronze table
# %sql
# SELECT DISTINCT(topic)
# FROM bronze

#  ----------
# Load new data into Kafka raw folder (simulate streaming data)
bookstore.load_new_data()

# Re-run Bronze processing to include newly added data
process_bronze()

# SQL: Count total records in Bronze table
# %sql
# SELECT COUNT(*) FROM bronze
