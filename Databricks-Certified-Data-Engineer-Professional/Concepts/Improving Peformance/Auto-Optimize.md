# ⚡ Delta Lake Auto Optimize

## 📌 Concept
- **Auto Optimize** is a Delta Lake feature that **automatically optimizes data layout** during writes.  
- It helps improve **query performance** by reducing small files and improving file sizes for efficient scanning.  
- There are two main optimizations:
  1. **Auto Compaction** – combines small files into larger ones during write operations.  
  2. **Auto Z-Ordering** – sorts data within files based on one or more columns to improve query pruning.

---

## ✅ Benefits
1. **Improved Query Performance**
   - Reduces the number of small files to scan.
   - Z-Ordering accelerates queries on frequently filtered columns.

2. **Reduced Maintenance**
   - No need for manual OPTIMIZE or compaction jobs after every write.

3. **Seamless Integration**
   - Works automatically with Delta table writes; transparent to users.

---

## ⚠️ Limitations / Considerations
1. **Write Overhead**
   - Auto compaction may slightly increase write latency due to file merging.

2. **Resource Usage**
   - Requires cluster resources (CPU, memory, disk) for compaction and Z-ordering.

3. **Not Always Necessary**
   - Small tables or tables with infrequent reads may not benefit significantly.

---

## 🔧 How to Enable
### At Table Level
```sql
ALTER TABLE sales_delta SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = true,
  'delta.autoOptimize.autoCompact' = true
);


### At Spark Session Level
``` spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true") ```
