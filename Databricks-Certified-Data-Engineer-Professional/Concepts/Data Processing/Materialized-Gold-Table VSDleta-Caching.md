# Delta Caching & Materialized Gold Tables

## ⚡ Delta Caching

### 📌 Concept
- **Delta Caching** is a **Databricks feature** that caches data **locally on cluster storage (SSD or memory)**.  
- Unlike Spark’s in-memory caching, Delta caching stores **uncompressed and optimized data blocks** on the cluster’s local disks.  
- It significantly speeds up queries on **frequently accessed Delta tables**, especially when data resides in cloud object storage (S3, ADLS, GCS).  

---

### ✅ Benefits
- Faster read performance (often **2–3× speedup**).  
- No need to load entire data into memory — works from local disk.  
- Transparent to the user: once enabled, Spark queries automatically benefit.  
- Cost optimization by reducing cloud storage I/O.  

---

### ⚠️ Limitations
1. **Cluster-scoped**  
   - Cache exists only on the cluster that created it.  
   - If cluster terminates, cache is lost.  

2. **Not for Streaming Writes**  
   - Delta cache is read-optimized; streaming jobs writing continuously won’t benefit.  

3. **Limited Size**  
   - Bound by local disk/SSD size of worker nodes.  

---

### 🔧 Example
```sql
-- Enable delta caching on the cluster
SET spark.databricks.io.cache.enabled = true;

-- Run queries (cache is automatic when table is repeatedly accessed)
SELECT * FROM sales_silver WHERE country = 'AU';

## 🪙 Materialized Gold Table

### 📌 Concept
A **Materialized Gold Table** is a **curated Delta table** in the **Gold layer** of the Lakehouse architecture.  
It is **precomputed and stored physically**, meaning aggregations or transformations from upstream (Bronze/Silver) are persisted for **fast downstream consumption**.  

- **Bronze → Silver → Gold** is the typical medallion architecture.  
- Gold tables are **ready for analytics, dashboards, and ML models**.  
- "Materialized" means the results are **physically written and stored**, not just computed at query time.  

---

### ✅ Benefits
- **Performance**: Queries run much faster since heavy transformations are pre-aggregated.  
- **Consistency**: All downstream teams (BI, ML, reporting) use the same curated dataset.  
- **Cost Efficiency**: Avoids repeated computation; reduces cluster usage for common queries.  
- **Governance**: Gold tables act as a **single source of truth** for business metrics.  

---

### ⚠️ Limitations
1. **Staleness**  
   - Gold tables need **scheduled refresh** (batch or streaming).  
   - If refresh is infrequent, data may become outdated.  

2. **Storage Cost**  
   - Requires **extra storage** since results are duplicated from Silver tables.  

3. **Maintenance**  
   - Pipelines must be orchestrated (e.g., Delta Live Tables, Airflow, or scheduled jobs).  

---

### 🔧 Example (SQL)
```sql
-- Create a materialized gold table for daily sales
CREATE OR REPLACE TABLE sales_gold AS
SELECT
    book_id,
    CAST(order_date AS DATE) AS order_day,
    SUM(quantity) AS total_sold,
    SUM(price * quantity) AS revenue
FROM books_sales
GROUP BY book_id, CAST(order_date AS DATE); ```


| Feature            | **Delta Caching**                                                                                       | **Materialized Gold Table**                                                               |
| ------------------ | ------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| **What it is**     | A **read optimization technique** that stores frequently read data blocks on **local cluster SSD/disk** | A **precomputed, physical Delta table** built from upstream data (Bronze/Silver)          |
| **Scope**          | **Cluster-level optimization** (temporary, exists only while cluster is running)                        | **Data-level optimization** (persistent, stored as a new Delta table in storage)          |
| **Purpose**        | Speed up repeated **reads** from the same dataset                                                       | Speed up repeated **analytics/aggregations** by materializing results                     |
| **Persistence**    | ❌ Lost when the cluster shuts down                                                                      | ✅ Stored permanently until explicitly dropped/overwritten                                 |
| **Best for**       | Interactive queries, repeated scans of large datasets (e.g., ad-hoc analytics, notebooks)               | BI dashboards, ML feature stores, business reports needing consistent pre-aggregated data |
| **Data Freshness** | Always fresh (reads directly from source, cached locally)                                               | May become **stale** unless refreshed by a scheduled job                                  |
| **Cost/Storage**   | Saves **compute + cloud I/O cost** (no extra storage needed)                                            | Requires **extra storage** (duplicate aggregated results saved)                           |
