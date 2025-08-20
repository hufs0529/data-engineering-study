# Streaming Static Join

## 📌 Concept
A **Streaming Static Join** refers to joining a **streaming dataset** (continuous flow of data, e.g., Kafka, socket, files) with a **static dataset** (non-changing reference data, e.g., dimension table, lookup data, CSV/Parquet file, database snapshot).  

- The **streaming side** is unbounded and continuously arrives.  
- The **static side** is bounded, usually small enough to be broadcasted in memory.  
- Typical use case: Enriching streaming events with static reference information.  
  - Example: Joining a stream of transaction events with a static customer table to add demographic info.  

In Spark Structured Streaming, this is often implemented by broadcasting the static dataset to all worker nodes so that every incoming streaming record can be joined efficiently.

---

## ✅ Benefits
- Efficient for **lookup/enrichment** use cases.  
- Easy to implement since the static dataset is preloaded and doesn’t change frequently.  
- Lower computational cost compared to streaming-streaming joins.  

---

## ⚠️ Limitations
1. **Static Data is Not Updated**  
   - The static dataset is fixed at the time the query starts.  
   - If the reference data changes (e.g., customer moves city), it won’t be reflected unless the query restarts.  

2. **Memory Constraints**  
   - The static dataset is usually **broadcasted** to worker nodes.  
   - Large static datasets may cause memory pressure and poor performance.  

3. **Not Suitable for Dynamic Dimension Tables**  
   - If the "static" table is frequently updated (like real-time product catalog or pricing), this approach fails to capture updates.  

4. **Limited Scalability**  
   - Works best when static data is small to medium. For large static tables, consider other approaches (e.g., Delta Lake merge, lookup services, or streaming-streaming joins).  

---