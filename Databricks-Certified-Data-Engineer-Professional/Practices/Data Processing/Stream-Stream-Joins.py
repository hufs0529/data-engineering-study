# Run helper notebook to load dataset paths and functions
%run ../Includes/Copy-Datasets

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Function to perform upsert for each micro-batch
def batch_upsert(microBatchDF, batchId):
    # Define window to rank changes by commit timestamp for each order/customer
    window = Window.partitionBy("order_id", "customer_id").orderBy(F.col("_commit_timestamp").desc())
    
    # Filter only inserts or post-update rows, pick latest change per key, clean columns, and rename timestamp
    (microBatchDF.filter(F.col("_change_type").isin(["insert", "update_postimage"]))
                 .withColumn("rank", F.rank().over(window))
                 .filter("rank = 1")
                 .drop("rank", "_change_type", "_commit_version")
                 .withColumnRenamed("_commit_timestamp", "processed_timestamp")
                 .createOrReplaceTempView("ranked_updates"))
    
    # MERGE changes into the target Delta table
    query = """
        MERGE INTO customers_orders c
        USING ranked_updates r
        ON c.order_id=r.order_id AND c.customer_id=r.customer_id
            WHEN MATCHED AND c.processed_timestamp < r.processed_timestamp
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """
    
    # Execute the MERGE statement
    microBatchDF.sparkSession.sql(query)

# Read orders_silver as streaming DataFrame
orders_df = spark.readStream.table("orders_silver")

# Read customers_silver with Change Data Feed enabled
cdf_customers_df = (spark.readStream
                             .option("readChangeData", True)
                             .option("startingVersion", 2)
                             .table("customers_silver")
                       )

# Function to join customers CDF with orders and upsert into final table
def process_customers_orders():
    query = (orders_df
                .join(cdf_customers_df, ["customer_id"], "inner")
                .writeStream
                    .foreachBatch(batch_upsert)  # Use foreachBatch to handle MERGE
                    .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/customers_orders")
                    .trigger(availableNow=True)
                    .start()
            )
    
    query.awaitTermination()

# Run the pipeline
process_customers_orders()

# Load new data and reprocess to simulate incremental updates
bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_orders_silver()
bookstore.process_customers_silver()
process_customers_orders()


## batch_upsert: Handles row-level changes from CDF and merges them into the target table, ensuring only the latest change per order/customer is applied.
## Window + rank: Ensures we take the most recent _commit_timestamp for each key.
## MERGE INTO: Performs upsert (update or insert) for CDC.
## foreachBatch: Required for streaming CDF to apply MERGE in each micro-batch.
## processed_timestamp: Tracks the applied change version to avoid overwriting newer records.