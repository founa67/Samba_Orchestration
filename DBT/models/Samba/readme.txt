README - DBT + Snowflake + GitHub Integration (Samba_DBT Project)
==============================================================

This document explains the technical process of setting up DBT Cloud with Snowflake, 
building models, running DBT, validating the pipeline, and linking the project with GitHub.

--------------------------------------------------------------
1. Connecting DBT Cloud to Snowflake
--------------------------------------------------------------
- A dedicated Snowflake warehouse, database, schema, role, and user were created.
- Example setup:
    CREATE WAREHOUSE DBT_WH WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 600 AUTO_RESUME = TRUE;
    CREATE DATABASE DBT_DB;
    CREATE ROLE DBT_ROLE;
    CREATE USER DBT_USER PASSWORD='********' DEFAULT_ROLE=DBT_ROLE DEFAULT_WAREHOUSE=DBT_WH;
    GRANT ROLE DBT_ROLE TO USER DBT_USER;
- In DBT Cloud, Snowflake was selected as the data platform and DBT_USER credentials 
  were used to connect.

--------------------------------------------------------------
2. Schema.yml
--------------------------------------------------------------
- The `schema.yml` file documents and tests the DBT models.
- It defines:
  * Sources → Raw tables from Fivetran (KENYA_HOLIDAYS).
  * Models → The transformed table (holidays).
  * Tests → Ensures data quality (e.g., unique, not_null, accepted_values).
- Purpose: Acts as both documentation and a test framework for DBT models.

--------------------------------------------------------------
3. Model: holidays.sql
--------------------------------------------------------------
- The `holidays.sql` model transforms raw holiday data into a clean, canonical table.
- Steps inside the model:
  * Normalize → Clean column names and values.
  * Parse → Convert raw dates into strict DATE format.
  * Canonicalize → Standardize holiday names, classify holiday types, and derive attributes (year, month, weekday, etc.).
  * Finalize → Remove duplicates, generate unique keys, and prepare for incremental load.
- Materialization: `incremental` → Ensures efficient updates via Snowflake MERGE.

--------------------------------------------------------------
4. Compiling and Running DBT
--------------------------------------------------------------
- Commands:
    dbt run        → Executes transformations and builds models in Snowflake.
    dbt test       → Runs tests defined in schema.yml.
    dbt compile    → Prepares SQL for execution in the target database.
- Output: A clean `holidays` table in Snowflake (DBT_DB.DBT_SCHEMA).

--------------------------------------------------------------
5. Verification in Snowflake
--------------------------------------------------------------
- Logged into Snowflake and verified:
    SELECT * FROM DBT_DB.DBT_SCHEMA.HOLIDAYS;
- Confirmed data was cleaned, dates parsed, and uniqueness enforced.

--------------------------------------------------------------
6. Linking DBT with GitHub
--------------------------------------------------------------
- Repository: https://github.com/founa67/SAMBA_DBT
- Steps:
  * Connected GitHub repository from DBT Cloud project settings.
  * Configured DBT to push and pull code from GitHub.

--------------------------------------------------------------
7. Branching in GitHub
--------------------------------------------------------------
- Created a feature branch for development:
    Branch: founa67-patch-1

--------------------------------------------------------------
8. Pull Request & Merge
--------------------------------------------------------------
- Opened a Pull Request (PR) to merge `founa67-patch-1` into `main`.
- Code was reviewed, approved, and merged successfully.

--------------------------------------------------------------
End Result
--------------------------------------------------------------
- Automated pipeline from Fivetran → Snowflake → DBT Cloud → GitHub.
- Clean, tested, and version-controlled holiday dataset available in Snowflake.
