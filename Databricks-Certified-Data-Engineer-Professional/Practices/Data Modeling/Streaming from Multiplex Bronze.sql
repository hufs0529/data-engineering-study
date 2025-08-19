-- Select key and value columns from Bronze table, cast to STRING for readability
SELECT cast(key AS STRING), cast(value AS STRING)
FROM bronze
LIMIT 20;

-- Parse JSON orders data from Bronze table
-- 'from_json' converts the string in 'value' column into structured columns
SELECT v.*
FROM (
  SELECT from_json(
           cast(value AS STRING), 
           "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"
         ) AS v
  FROM bronze
  WHERE topic = "orders"  -- Filter only 'orders' topic
);

-- Parse JSON orders data from temporary Bronze view 'bronze_tmp'
-- Useful when using streaming DataFrame as SQL view
SELECT v.*
FROM (
  SELECT from_json(
           cast(value AS STRING), 
           "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"
         ) AS v
  FROM bronze_tmp
  WHERE topic = "orders"
);

-- Create or replace a temporary view for Silver processing
-- Converts JSON 'value' column into structured columns and stores as temp view
CREATE OR REPLACE TEMPORARY VIEW orders_silver_tmp AS
  SELECT v.*
  FROM (
    SELECT from_json(
             cast(value AS STRING), 
             "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"
           ) AS v
    FROM bronze_tmp
    WHERE topic = "orders"
  );
