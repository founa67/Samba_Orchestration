# 🍸 Samba Enterprises Ltd – Data Engineering Portfolio Project

## 📊 Project Overview
This project demonstrates a full-scale **data engineering architecture** for **Samba Enterprises Ltd**, a bar and restaurant chain operating across five cities in Kenya with fifteen branches.

The purpose of this portfolio project is to showcase an **end-to-end ELT pipeline** on **Snowflake** integrated with **AWS S3**, focusing on scalability, governance, and analytics-readiness.

---

## 🏗️ Architecture Summary
**Cloud Warehouse:** Snowflake  
**Storage Layer:** AWS S3 (`samba-data-eng`)  
**Pipeline Type:** ELT (Extract → Load → Transform)  
**Integration Tools:** Snowpipe, DBT, Fivetran  
**Automation Tools:** Snowflake Tasks & Streams  
**Version Control:** GitHub  
**Documentation:** SQL scripts + docx + markdown files

---

## 🧩 Schema Design

**Database:** `SAMBA_DB`  
**Warehouse:** `SAMBA_WH`

**Schemas:**
- `RAW_STAGE` – Transient tables for raw ingestion.  
- `TRANSFORMED` – Intermediate transformations.  
- `PRODUCTION` – Final analytics-ready tables.

---

## ⚙️ Pipeline Components

### 1. Data Ingestion
- Source data stored in AWS S3 bucket: `samba-data-eng`
- Subfolders for each source (e.g., cities, campaigns, products, staff, sales)
- Snowflake **stages** mapped to each folder
- Ingestion handled via **Snowpipe** and **COPY INTO** commands

### 2. Transformation (ELT)
- SQL transformations run inside Snowflake
- Data flows from `RAW_STAGE` → `TRANSFORMED` → `PRODUCTION`
- DBT models define repeatable transformations

### 3. Automation & Monitoring
- **Streams & Tasks** manage incremental loads and scheduled jobs  
- **ETL logging tables** monitor pipeline activity and data quality  
- **Zero Copy Clone** enables sandbox testing without storage overhead

---

## 📂 Repository Structure
```
Samba_Data_Engineering_Project/
│
├── README.md
├── docs/
│   ├── Architecture_Overview.md
│   ├── Samba_Enterprises_Ltd_Project_Overview.docx
│   ├── S3_Bucket_Structure.docx
│
├── sql_scripts/
│   ├── Samba_Virtual_Warehouse_DB_Schemas.sql
│   ├── Samba_RBAC.sql
│   ├── s3_int_samba.sql
│   ├── Samba_File_Format.sql
│   ├── Internal_Stages.sql
│   ├── Samba_Data_Pipelines_COPY_Command.sql
│   ├── Samba_Tasks_Streams.sql
│   ├── Samba_Transformations.sql
│   ├── Samba_ETL_Log.sql
│   ├── Samba_Zero_Copy_Clone.sql
│   ├── FIVETRAN/
│   │   └── Samba_Fivetran.sql
│   └── DBT/
│       └── Samba_DBT.sql
└── data/
    └── sample_files/
```

---

## 💡 Key Skills Demonstrated
✅ Cloud data warehousing (Snowflake)  
✅ AWS S3 integration & storage design  
✅ ELT pipeline automation (Snowpipe, Streams, Tasks)  
✅ Role-Based Access Control (RBAC)  
✅ Data modeling & transformation (DBT)  
✅ Fivetran orchestration  
✅ Documentation & version control via GitHub  

---

## 🚀 Next Steps
- Load sample data into RAW_STAGE schema  
- Validate ingestion and transformations  
- Connect Snowflake to BI tools (Power BI / Tableau)  

---

📅 **Last Updated:** 2025-10-05  
👤 **Author:** Frank Ouna  
🔗 **GitHub Repo:** [Samba Data Engineering Project](https://github.com/founa67/Samba_Data_Engineering_Project)
