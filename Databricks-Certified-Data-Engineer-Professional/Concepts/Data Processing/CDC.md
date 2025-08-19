# CDC, MERGE Limitations, and Rank/Window Functions in Delta Lake

## 1️⃣ Change Data Capture (CDC)

**CDC**: A technique to capture only the changes (inserts, updates, deletes) in source systems and propagate them to target tables efficiently.

- **Use Case:** Synchronize source DB with Delta Lake table in near real-time.
- **Implementation:** Streaming ingestion from Kafka, databases, or CDC tools like Debezium.
- **Benefits:** Avoid full table reloads, reduce processing cost, maintain historical consistency.

---

## 2️⃣ Delta Lake MERGE Limitations

The `MERGE INTO` statement in Delta Lake allows upserts (insert + update) for CDC-like patterns.  
However, there are **some important limitations**:

| Limitation | Explanation |
|------------|-------------|
| **Single target table only** | `MERGE` can update only one Delta table at a time. |
| **No nested MERGE** | You cannot have another `MERGE` inside the source subquery. |
| **Complex joins** | Large or complex joins may lead to performance issues; use staging tables if needed. |
| **Streaming + MERGE** | Only supported with `foreachBatch` in streaming mode, not directly in `writeStream.table()`. |

**Implication:**  
When implementing CDC with `MERGE`, we often need **staging tables** and **batch/stream orchestration** to avoid hitting these limitations.

---

## 3️⃣ Using RANK / Window Functions for CDC

When source changes arrive with multiple updates per key, we need to identify the **latest record** for each primary key before MERGE:

```sql
WITH ranked_updates AS (
    SELECT *,
           RANK() OVER (PARTITION BY book_id ORDER BY updated DESC) AS rk
    FROM updates
)
SELECT *
FROM ranked_updates
WHERE rk = 1  -- pick only the latest change per key
