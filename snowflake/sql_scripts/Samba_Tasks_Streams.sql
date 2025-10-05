// Create task
CREATE OR REPLACE TASK BRANCHES_INSERT
    WAREHOUSE = SAMBA_WH
    SCHEDULE = '1 MINUTE'
    AS
COPY INTO RAW_STAGE.branches (branch_name, city_name)
FROM (
    SELECT 
        $1:branch_name::STRING,
        $1:city_name::STRING
    FROM @RAW_STAGE.external_samba_stage/branches/branches.json
);

SHOW TASKS;
// Task starting and suspending
ALTER TASK BRANCHES_INSERT RESUME;
ALTER TASK BRANCHES_INSERT SUSPEND;

// Create task
CREATE OR REPLACE TASK RAW_STAGE.CITIES_INSERT
    WAREHOUSE = SAMBA_WH
    SCHEDULE = '1 MINUTE'
    AS 
    INSERT INTO CITIES(city_name) VALUES(CURRENT_TIMESTAMP);

    // Create task WHEN STREAM HAS DATA
CREATE OR REPLACE TASK RAW_STAGE.CITIES_INSERT_HAS_DATA
    WAREHOUSE = SAMBA_WH
    SCHEDULE = '1 MINUTE'
    WHEN
    SYSTEM$STREAM_HAS_DATA('raw_cities_stream')
    AS 
    INSERT INTO CITIES(CREATE_DATE) VALUES(CURRENT_TIMESTAMP);
    

SHOW TASKS;

// Task starting and suspending
ALTER TASK RAW_STAGE.CITIES_INSERT RESUME;
ALTER TASK RAW_STAGE.CITIES_INSERT SUSPEND;
_________________________________________________________________________________________________________

--Incremental loads with Streams & Tasks
_________________________________________________________________________________________________________________________
-- RAW tables are still receiving new files, capture changes and MERGE only deltas.
-- Stream changes on RAW sales
CREATE OR REPLACE STREAM SAMBA_DB.RAW_STAGE.sales_stream ON TABLE SAMBA_DB.RAW_STAGE.sales;


_____________________________________________________________________________________________________________________