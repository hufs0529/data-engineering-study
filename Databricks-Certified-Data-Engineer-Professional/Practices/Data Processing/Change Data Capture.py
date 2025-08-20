# -----------------------------------
# Define schema and extract customers data from Bronze
from pyspark.sql import functions as F

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"

customers_df = (
    spark.table("bronze")  # Read Bronze table
         .filter("topic = 'customers'")  # Only customers topic
         .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))  # Parse JSON
         .select("v.*")  # Flatten struct
         .filter(F.col("row_status").isin(["insert", "update"]))  # Only insert/update rows
)

display(customers_df)

# -----------------------------------
# Apply window function to get the latest record per customer
from pyspark.sql.window import Window

window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())

ranked_df = (
    customers_df
    .withColumn("rank", F.rank().over(window))  # Rank rows by row_time descending per customer
    .filter("rank == 1")  # Keep only the latest record per customer
    .drop("rank")
)

display(ranked_df)

# -----------------------------------
# Example: This will fail because window functions on streaming DF are not supported directly
ranked_df_stream = (
    spark.readStream
         .table("bronze")
         .filter("topic = 'customers'")
         .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
         .select("v.*")
         .filter(F.col("row_status").isin(["insert", "update"]))
         .withColumn("rank", F.rank().over(window))  # Not supported on streaming DF
         .filter("rank == 1")
         .drop("rank")
)

display(ranked_df_stream)

# -----------------------------------
# Define foreachBatch function for streaming upsert into Silver table
from pyspark.sql.window import Window

def batch_upsert(microBatchDF, batchId):
    window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())
    
    # Keep latest record per customer for the micro-batch
    (microBatchDF
     .filter(F.col("row_status").isin(["insert", "update"]))
     .withColumn("rank", F.rank().over(window))
     .filter("rank == 1")
     .drop("rank")
     .createOrReplaceTempView("ranked_updates")
    )
    
    # MERGE into Silver table
    query = """
        MERGE INTO customers_silver c
        USING ranked_updates r
        ON c.customer_id = r.customer_id
            WHEN MATCHED AND c.row_time < r.row_time
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(query)

# -----------------------------------
# Create Silver table if it does not exist
"""
CREATE TABLE IF NOT EXISTS customers_silver
(customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP)
"""

# -----------------------------------
# Load country lookup table for enrichment
df_country_lookup = spark.read.json(f"{dataset_bookstore}/country_lookup")
display(df_country_lookup)

# -----------------------------------
# Streaming pipeline: Bronze -> join country lookup -> foreachBatch upsert
query = (
    spark.readStream
         .table("bronze")
         .filter("topic = 'customers'")
         .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
         .select("v.*")
         .join(F.broadcast(df_country_lookup), F.col("country_code") == F.col("code"), "inner")  # Enrich with country name
         .writeStream
         .foreachBatch(batch_upsert)  # Use foreachBatch to handle window function and MERGE
         .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/customers_silver")
         .trigger(availableNow=True)
         .start()
)

query.awaitTermination()

# -----------------------------------
# Unit test: Ensure Silver table has one row per customer
count = spark.table("customers_silver").count()
expected_count = spark.table("customers_silver").select("customer_id").distinct().count()

assert count == expected_count
print("Unit test passed.")
