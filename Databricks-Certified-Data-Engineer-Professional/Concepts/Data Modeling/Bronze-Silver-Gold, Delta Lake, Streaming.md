# Databricks Lakehouse, Delta Lake & Streaming Concepts

Databricks Lakehouse organizes data in **multi-layer architecture**, with **Delta Lake reliability** and **streaming support**.

---

## 🏗️ Lakehouse Layers

### 1️⃣ Bronze (Raw)
- Raw data ingested from source systems
- Minimal processing, may contain duplicates or errors
- **Ingestion types:**
  - **Singleplex:** One data source per pipeline - for Batch
  - **Multiplex:** Multiple data sources ingested together - for Streaming
- Examples: Kafka logs, ERP/CRM CSV dumps, IoT sensor events

### 2️⃣ Silver (Cleaned / Conformed)
- Bronze data **cleaned, standardized, and integrated**
- Ready for analytics or ML
- Examples: Deduplicated customer table, standardized date/region codes

### 3️⃣ Gold (Business / Analytics)
- Silver data **aggregated for business use**
- Used in BI dashboards, reports, ML training
- Examples: Daily/monthly sales aggregates, customer segment patterns

---

## 🗃️ Delta Lake Features

- **ACID Transactions:** Ensures consistency during concurrent reads/writes
- **Schema Enforcement & Evolution:** Prevents corrupt/incompatible data
- **Time Travel:** Query historical versions for audits or recovery
- **Upserts & Deletes:** Supports `MERGE`, `UPDATE`, `DELETE`
- **Performance:** Delta caching, Z-Ordering, file compaction

---

## 💨 Streaming Concepts

- **Streaming Sources:** Kafka, Event Hubs, IoT devices, files  
- **Structured Streaming:** Continuous processing of live data in Databricks  
- **Checkpointing:** Tracks progress to ensure exactly-once processing  
- **Watermarking:** Handles late-arriving data in event-time processing  
- **Triggers:** Control micro-batch or continuous streaming execution  
- **Output Modes:** `Append`, `Update`, `Complete` for sink operations  

**Example Flow:**

---

## 🔄 Lakehouse Flow with Delta & Streaming

```mermaid
flowchart LR
    A[Bronze (Raw)] --> B[Silver (Cleaned / Conformed)]
    B --> C[Gold (Business / Analytics)]
    subgraph DeltaLake [Delta Lake Features]
        direction TB
        DA[ACID Transactions]
        DB[Schema Enforcement & Evolution]
        DC[Time Travel]
        DD[Upserts & Deletes]
        DE[Performance Optimization]
    end
    A --> DeltaLake
    B --> DeltaLake
    C --> DeltaLake

    subgraph Streaming [Streaming Concepts]
        direction TB
        SA[Streaming Sources]
        SB[Structured Streaming]
        SC[Checkpointing & Watermarks]
        SD[Triggers & Output Modes]
    end
    SA --> A
    SB --> B
    SC --> B
    SD --> C
