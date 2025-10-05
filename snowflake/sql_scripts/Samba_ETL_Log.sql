-- This part of the Samba_Transformations.sql
-- Separrated it here for technical reasons
-- AFTER initial loading into Schema RAW_STAGE
-- We transform the data and keep the logs. We keep the data in VIEWS in TRANSFORMED SCHEMA

-- Optional: a lightweight ETL log table
CREATE TABLE IF NOT EXISTS SAMBA_DB.TRANSFORMED.ETL_LOG (
  step_name STRING, started_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  rows_affected NUMBER, notes STRING
);
--- Create one “clean” view per source. Use TRY_TO_*, NULLIF, TRIM, and QUALIFY to type, normalize and de-duplicate.
____________________________________________________________________________________________________________________
CREATE TABLE IF NOT EXISTS SAMBA_DB.TRANSFORMED.ETL_LOG_DETAILS (
  step_name      STRING,
  started_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  rows_inserted  NUMBER,
  rows_updated   NUMBER,
  rows_deleted   NUMBER,
  notes          STRING
);
___________________________________________________________________________________________________________________

-- Log rows loaded/updated
-- run this immediately after your MERGE in the same session
INSERT INTO TRANSFORMED.ETL_LOG_DETAILS (step_name, rows_inserted, rows_updated, rows_deleted, notes)
SELECT
  'LOAD_FACT_SALES',
  q.rows_inserted, q.rows_updated, q.rows_deleted,
  'MERGE from v_sales_clean'
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
QUALIFY ROW_NUMBER() OVER (ORDER BY start_time DESC) = 1;  -- last statement (the MERGE)

___________________________________________________________________________________________________________________