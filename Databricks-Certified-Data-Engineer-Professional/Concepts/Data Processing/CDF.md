# Change Data Feed (CDF) in Delta Lake

## 1️⃣ What is CDF?

**CDF (Change Data Feed)** is a feature in **Delta Lake** that tracks **row-level changes** (inserts, updates, deletes) in a Delta table over time.  

- **Purpose:** Allows downstream systems or pipelines to efficiently consume only the changed data instead of scanning the entire table.
- **Key Idea:** Every Delta table can expose a change feed, which records the type of change and the data before/after the change.

---

## 2️⃣ How CDF Works

- When **enabled** on a Delta table, Delta automatically logs all row-level changes.
- Users can query the **changes between two versions** of the table.
- Supports streaming and batch consumption.

**Example:**  
```python
# Read changes from version 100 to 101
df_changes = (
    spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 100)
        .table("customers_silver")
)
df_changes.show()

3️⃣ CDF vs Non-CDF

| Feature               | Using CDF                                          | Without CDF                                     |
| --------------------- | -------------------------------------------------- | ----------------------------------------------- |
| **Change tracking**   | Automatic, row-level `_change_type`                | Manual, need full table scan or custom logic    |
| **ETL efficiency**    | Incremental read possible, less compute            | Must read entire table, more I/O and cost       |
| **Streaming support** | Stream only changed rows with `readStream`         | Stream entire table or implement custom CDC     |
| **Historical audit**  | Built-in `_commit_version` and `_commit_timestamp` | Requires snapshots or external logging          |
| **SCD / Upserts**     | Works seamlessly with MERGE for Type 1/2           | Need additional logic to deduplicate and upsert |
