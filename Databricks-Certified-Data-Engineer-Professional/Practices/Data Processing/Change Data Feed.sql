-- Enable Change Data Feed (CDF) on the Delta table
ALTER TABLE customers_silver 
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Show detailed metadata of the table
DESCRIBE TABLE EXTENDED customers_silver;

-- Show the version history of the table
DESCRIBE HISTORY customers_silver;

-- Query changes from version 2 using the CDF function
SELECT * 
FROM table_changes("customers_silver", 2);

-- ALTER TABLE … SET TBLPROPERTIES → Enables CDF so that all row-level changes are tracked.
-- DESCRIBE TABLE EXTENDED → Displays schema, properties, and storage details.
-- DESCRIBE HISTORY → Shows commit history including inserts, updates, and deletes.
-- table_changes() → Retrieves all row-level changes between the specified table version (here, version 2) and the latest.