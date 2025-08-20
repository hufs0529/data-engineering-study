# Run dataset preparation notebook
%run ../Includes/Copy-Datasets

# Load new data into Bronze table and process Silver tables
bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_orders_silver()
bookstore.process_customers_silver()

# Read CDF from customers_silver starting at version 2
cdf_df = (spark.readStream
               .format("delta")
               .option("readChangeData", True)  # Enable reading only changed rows
               .option("startingVersion", 2)    # Start reading from version 2
               .table("customers_silver"))

display(cdf_df)

# List the physical files in the Delta table directory
files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/customers_silver")
display(files)

# List the change data files specifically
files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/customers_silver/_change_data")
display(files)


## spark.readStream.format("delta").option("readChangeData", True) → Reads only the changed rows (CDF) in streaming mode starting from a specific version.