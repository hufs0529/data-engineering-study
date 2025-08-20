# 📝 Delta Lake Transaction Log

## 📌 Concept
- The **Delta Lake Transaction Log** (`_delta_log`) is the **core of Delta Lake's ACID functionality**.  
- It keeps a **chronological record of all changes** to a Delta table, including inserts, updates, deletes, merges, and schema changes.  
- Each transaction is stored as a **JSON file**, and periodically **checkpointed as a Parquet file** for efficiency.  
- Enables **atomicity, consistency, isolation, and durability (ACID)** for both **batch and streaming workloads**.

---

## ✅ Features

### 1. Streaming + Batch
- Supports **exactly-once processing** in Structured Streaming pipelines.  
- Allows **batch and streaming workloads** to safely read and write the same Delta table.

### 2. Schema Enforcement & Evolution
- **Schema enforcement** ensures all new writes comply with the table schema.  
- **Schema evolution** allows controlled updates, such as adding new columns without breaking existing queries.  

### 3. Auditability
- Every change is **logged** in the `_delta_log`.  
- Enables **debugging, data lineage tracking, and compliance auditing**.

---

## ⚠️ Limitations / Considerations

### 1. Storage Overhead
- `_delta_log` grows with each transaction.  
- Old JSON transaction files are compacted into Parquet **checkpoints** to reduce storage impact.

### 2. Performance
- Frequent small writes may create many tiny log files, which can degrade performance.  
- Recommended to **batch writes** or perform **compaction** for better efficiency.

### 3. Manual Cleanup
- To manage storage and remove outdated table versions, use **VACUUM**:  
```sql
VACUUM sales_delta RETAIN 168 HOURS; -- Retain 7 days

## 🔧 How It Works

### 1. Write Operation
- Each write to a Delta table creates a new **JSON transaction file** in the `_delta_log` directory.
- Logs the changes such as inserts, updates, deletes, and schema modifications.

### 2. Checkpointing
- After multiple transactions, Delta Lake creates a **Parquet checkpoint**.
- Checkpoints consolidate JSON logs for faster reads and lower metadata overhead.

### 3. Read Operation
- When reading a Delta table, Delta Lake **reconstructs the current table state** by applying the JSON logs on top of the latest checkpoint.
- Ensures **ACID consistency** and accurate query results even with concurrent writes.
