# 🧠 Samba Enterprises Ltd – Architecture Overview

## 🔹 Overview
This document summarizes the architecture implemented for **Samba Enterprises Ltd** to support scalable, automated data pipelines and analytics using Snowflake and AWS S3.

---

## 🗄️ Storage Layer – AWS S3
**Bucket Name:** `samba-data-eng`  
**Region:** `eu-north-1`  
**Integration:** `s3_int_samba`

### Folder Structure
```
samba-data-eng/
 ├── cities/
 ├── campaigns/
 ├── employees/
 ├── products/
 ├── branches/
 ├── staff/
 ├── staff_branch_assignments/
 └── sales/
```

Each folder corresponds to a source table and has a dedicated Snowflake **external stage** for data ingestion.

---

## ❄️ Compute Layer – Snowflake
- **Warehouse:** `SAMBA_WH`
- **Database:** `SAMBA_DB`
- **Schemas:**
  - `RAW_STAGE` – Initial ingestion (transient tables)
  - `TRANSFORMED` – Cleansed and joined datasets
  - `PRODUCTION` – Analytics-ready views and facts

SQL DDL for setup is stored in `/sql_scripts/Samba_Virtual_Warehouse_DB_Schemas.sql`.

---

## 🔁 ELT Process

**Extract:** Data exported to AWS S3 (JSON, CSV, Parquet)  
**Load:** Ingested to `RAW_STAGE` via Snowpipe and COPY INTO  
**Transform:** Processed into `TRANSFORMED` and `PRODUCTION` schemas  

Automation via Snowflake **Tasks** and **Streams** ensures continuous updates.

---

## 🔐 Governance & Access
Roles and privileges managed using RBAC:
- `SAMBA_ADMIN` – Full control  
- `SAMBA_DATA_ENGINEER` – Pipeline operations  
- `SAMBA_DATA_ANALYST` – Read-only on production data  

RBAC configuration script: `/sql_scripts/Samba_RBAC.sql`

---

## 🧩 Automation Tools
- **Streams** – Track data changes for incremental loading.  
- **Tasks** – Automate and schedule transformations.  
- **Zero Copy Clone** – Simplifies dev/test environment creation.

---

## 📦 File Formats
Snowflake file formats used:
- JSON → `RAW_STAGE.samba_json_format`
- CSV → `RAW_STAGE.samba_csv_format`
- Parquet → `RAW_STAGE.samba_parquet_format`

Defined in `/sql_scripts/Samba_File_Format.sql`.

---

## 📈 Outcome & Impact
- Centralized and automated data management.  
- Reduced manual ETL effort and data latency.  
- Improved governance, scalability, and analytics readiness.  

---

📅 **Last Updated:** 2025-10-05  
👤 **Author:** Frank Ouna
