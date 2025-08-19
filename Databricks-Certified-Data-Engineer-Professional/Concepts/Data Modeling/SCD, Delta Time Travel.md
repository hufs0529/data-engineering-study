# Slowly Changing Dimensions (SCD) & Delta Lake Time Travel

## 1️⃣ SCD Overview

**Slowly Changing Dimensions (SCD)**: Dimension tables in a data warehouse that track changes over time.  
Common types:

| Type | Description | Behavior on Change | Example Use Case |
|------|-------------|-----------------|----------------|
| **SCD Type 0** | **Fixed / Static** | No changes allowed; original value never updates | Product codes that never change |
| **SCD Type 1** | **Overwrite** | Updates old values with new ones; no history preserved | Customer address update, always latest |
| **SCD Type 2** | **Historical Tracking** | Preserves history; creates a new row for each change with start/end dates or version number | Employee department changes, retain all historical records |

**Key Notes:**
- Type 0 = immutable  
- Type 1 = overwrite → no history  
- Type 2 = history preserved → multiple versions of same entity  

---

## 2️⃣ Delta Lake Time Travel

**Delta Lake Time Travel** allows querying a table **as it existed at a specific point in time**.  
- Useful for auditing, recovering data, or reproducing reports.  

**Example Syntax:**

```sql
-- Query table as of a timestamp
SELECT * FROM orders_silver TIMESTAMP AS OF '2025-08-19 10:00:00';

-- Query table as of a version
SELECT * FROM orders_silver VERSION AS OF 3;


| Feature  | SCD Type 2                                  | Delta Time Travel                                          |
| -------- | ------------------------------------------- | ---------------------------------------------------------- |
| Purpose  | Track changes to specific records over time | Query full table state at past point/version               |
| Storage  | Adds new rows for each change               | Maintains Delta log to reconstruct state                   |
| Access   | Row-level history                           | Table-level historical snapshot                            |
| Use Case | Historical analytics on specific dimension  | Recover deleted/overwritten data or reproduce past results |

---

💡 **Tip:**  
- SCD Type 2 is row-based history tracking.  
- Delta Time Travel is table-based snapshot tracking.  
- **Delta Lake + SCD Type 2** → powerful combo for data warehousing and audit compliance.

---