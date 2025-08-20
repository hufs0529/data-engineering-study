# 📦 Partitioning Delta Lake Tables

## 📌 Concept
**Partitioning** in Delta Lake is a technique to **physically organize data files** in a table based on the values of one or more columns.  

- Partitioning divides a large dataset into **smaller, manageable chunks** called partitions.  
- Each partition corresponds to a specific value (or range of values) in a column.  
- Common use case: time-based partitioning (e.g., by `year`, `month`, `day`) for large event or transaction tables.  


---

## ✅ Benefits
1. **Query Performance**
   - Spark reads only the **relevant partitions** instead of scanning the entire table (predicate pushdown).  
   - Dramatically reduces I/O and query time for selective queries.  

2. **Efficient File Management**
   - Helps with **small file problem** by grouping related rows together.  
   - Improves compaction and maintenance in large tables.  

3. **Scalability**
   - Partitioning enables Delta Lake to handle **massive datasets** efficiently in cloud storage.  

---

## ⚠️ Limitations / Considerations
1. **Too Many Partitions**
   - Having many small partitions can hurt performance (too much metadata, scheduling overhead).  

2. **Skewed Data**
   - If one partition contains much more data than others, it can lead to **straggler tasks** in Spark jobs.  

3. **Partition Pruning Only Works for Certain Queries**
   - Queries must filter on the **partition column** to benefit from pruning.  

4. **Partition Column Choice**
   - Choose columns with **moderate cardinality**: not too high (millions of unique values) or too low (few values).  

---

## 🔧 Example (PySpark)
```python
# Write a Delta table partitioned by year and month
sales_df.write.format("delta") \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .save("/mnt/delta/sales_delta")
