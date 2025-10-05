USE ROLE SAMBA_DATA_ENGINEER;
____________________________________________________________________________________________________________________
// Define pipe
CREATE OR REPLACE PIPE branches_pipe
auto_ingest = TRUE
AS
COPY INTO RAW_STAGE.branches (branch_name, city_name)
FROM (
    SELECT 
        $1:branch_name::STRING,
        $1:city_name::STRING
    FROM @RAW_STAGE.external_samba_stage/branches/branches.json
);
-- FROM @RAW_STAGE.external_samba_stage/branches_new/
-- FROM @RAW_STAGE.external_samba_stage/branches/branches.json
-- @RAW_STAGE.external_samba_stage/branches/branches_new/branches.json
-- @RAW_STAGE.external_samba_stage/branches/branches_archive/branches.json
// Describe pipe

DESC pipe branches_pipe;
SELECT get_ddl('pipe','branches_pipe');
--take to AWS. notification_channel
-- arn:aws:sqs:eu-north-1:565393042555:sf-snowpipe-AIDAYHJANFR5Y4KIMKZUB-EsM5ex47r93-WeSH8CqaEA
SELECT * FROM branches ;
SELECT count(*) FROM branches ;


--- created Event Notification: Samba_Snowpipe_Branches_Upload
-- Prefix branches/
-- Suffix .json
-- Event Type: All object create events
-- Destination: SQS Queue
-- Enter SQS queue ARN: arn:aws:sqs:eu-north-1:565393042555:sf-snowpipe-AIDAYHJANFR5Y4KIMKZUB-EsM5ex47r93-WeSH8CqaEA (from notification_channel )

-- Upload into the bucket or push new branches file

--PIPE ERROR HANDLING
SELECT SYSTEM$PIPE_STATUS('branches_pipe');
ALTER PIPE branches_pipe REFRESH;

// Snowpipe error message
SELECT * FROM TABLE(VALIDATE_PIPE_LOAD(
    PIPE_NAME => 'branches_pipe',
    START_TIME => DATEADD(HOUR,-2,CURRENT_TIMESTAMP())));

// COPY command history from table to see error massage

SELECT * FROM TABLE (INFORMATION_SCHEMA.COPY_HISTORY(
   table_name  =>  'branches',
   START_TIME =>DATEADD(HOUR,-2,CURRENT_TIMESTAMP())));

-- Manage pipes -- 

DESC pipe branches_pipe;

SHOW PIPES;

SHOW PIPES like '%branches%';

SHOW PIPES in database SAMBA_DB;

SHOW PIPES in schema SAMBA_DB.RAW_STAGE;

SHOW PIPES like '%branches%' in Database SAMBA_DB;
// Pause pipe
ALTER PIPE SAMBA_DB.RAW_STAGE.branches_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
SELECT SYSTEM$PIPE_STATUS('branches_pipe');
ALTER PIPE SAMBA_DB.RAW_STAGE.branches_pipe SET PIPE_EXECUTION_PAUSED = FALSE;
-------------------------------------------------------------------------------------------------------------------------
-- Create Campaigns PIPE Object
CREATE OR REPLACE PIPE SAMBA_DB.RAW_STAGE.campaigns_pipe
  AUTO_INGEST = TRUE
AS
COPY INTO SAMBA_DB.RAW_STAGE.campaigns
  (campaign_id, city_id, channel_id, impressions, clicks, conversions, spend, start_date, end_date)
FROM @SAMBA_DB.RAW_STAGE.external_samba_stage/campaigns/campaigns_with_dates.csv
  FILE_FORMAT = (FORMAT_NAME = SAMBA_DB.RAW_STAGE.samba_csv_format);

__________________________________________________________________________________________________________________________
CREATE OR REPLACE PIPE RAW_STAGE.campaign_channels_pipe
  AUTO_INGEST = TRUE
AS
COPY INTO RAW_STAGE.campaign_channels (channel_id, channel_name)
FROM (
  SELECT
    $1:channel_id::INT,
    $1:channel_name::STRING
  FROM @RAW_STAGE.external_samba_stage/campaign_channels/
       (FILE_FORMAT => 'RAW_STAGE.samba_json_format')
)
ON_ERROR = CONTINUE; 
_________________________________________________________________________________________________________________________
CREATE OR REPLACE PIPE RAW_STAGE.employees_pipe
  AUTO_INGEST = TRUE
AS
COPY INTO RAW_STAGE.employees (employee_id, first_name, last_name, email, phone, hire_date, role)
FROM (
  SELECT
    $1:employee_id::STRING,
    $1:first_name::STRING,
    $1:last_name::STRING,
    $1:email::STRING,
    $1:phone::STRING,
    TO_TIMESTAMP_NTZ($1:hire_date::STRING),
    $1:role::STRING
  FROM @RAW_STAGE.external_samba_stage/employees/employees.json
       (FILE_FORMAT => 'RAW_STAGE.samba_json_format')
)
PATTERN = '.*\\.json(\\.gz)?'
ON_ERROR = CONTINUE;
____________________________________________________________________________________________________________________________
CREATE OR REPLACE PIPE RAW_STAGE.products_pipe
  AUTO_INGEST = TRUE
AS
COPY INTO RAW_STAGE.products
  (product_id, product_name, category, price)
FROM @RAW_STAGE.external_samba_stage/products/products_with_price.csv
  --FILES = ('products_with_price.csv')
  FILE_FORMAT = (FORMAT_NAME = RAW_STAGE.samba_csv_format)
  --MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  ON_ERROR = CONTINUE;
  --TRUNCATECOLUMNS = TRUE;
____________________________________________________________________________________________________________________________
-- Pipe: auto-load any *.json (or .json.gz) under the staff/ prefix
CREATE OR REPLACE PIPE RAW_STAGE.staff_pipe
  AUTO_INGEST = TRUE
AS
COPY INTO RAW_STAGE.staff (staff_id, employee_id)
FROM (
  SELECT
    $1:staff_id::STRING,
    $1:employee_id::STRING
  FROM @RAW_STAGE.external_samba_stage/staff/
       (FILE_FORMAT => 'RAW_STAGE.samba_json_format')
)
PATTERN = '.*\\.json(\\.gz)?'
ON_ERROR = CONTINUE;   -- skip bad records, keep loading
____________________________________________________________________________________________________________________________
-- Snowpipe (auto-ingest any *.json/.json.gz under the prefix)
CREATE OR REPLACE PIPE RAW_STAGE.staff_branch_assignments_pipe
  AUTO_INGEST = TRUE
AS
COPY INTO RAW_STAGE.staff_branch_assignments (staff_id, branch_id, start_date, end_date)
FROM (
  SELECT
    $1:staff_id::STRING,
    $1:branch_id::INT,
    TO_TIMESTAMP_NTZ($1:start_date::STRING),
    TRY_TO_TIMESTAMP_NTZ(NULLIF($1:end_date::STRING, 'NaT'))  -- turn "NaT" into NULL
  FROM @RAW_STAGE.external_samba_stage/staff_branch_assignments/
       (FILE_FORMAT => 'RAW_STAGE.samba_json_format')         -- STRIP_OUTER_ARRAY=TRUE
)
PATTERN = '.*\\.json(\\.gz)?'
ON_ERROR = CONTINUE;   -- skip bad rows and keep loading
__________________________________________________________________________________________________________________________
-- 2) Snowpipe (CSV) — drop files under s3://.../sales/
-- Uses your existing CSV file format: RAW_STAGE.samba_csv_format
CREATE OR REPLACE PIPE RAW_STAGE.sales_pipe
  AUTO_INGEST = TRUE
AS
COPY INTO RAW_STAGE.sales
  (sale_id, branch_id, staff_id, product_id, sale_date, volume_sold, price, cost, revenue)
FROM (
  SELECT
    $1::NUMBER(38,0)          AS sale_id,
    $2::INT                   AS branch_id,
    $3::STRING                AS staff_id,
    $4::INT                   AS product_id,
    TO_TIMESTAMP_NTZ($5)      AS sale_date,
    $6::NUMBER(12,2)          AS volume_sold,
    $7::NUMBER(12,2)          AS price,
    $8::NUMBER(12,2)          AS cost,
    $9::NUMBER(12,2)          AS revenue
  FROM @RAW_STAGE.external_samba_stage/sales/
       (FILE_FORMAT => 'RAW_STAGE.samba_csv_format')
)
ON_ERROR = CONTINUE;
--TRUNCATECOLUMNS = TRUE;