USE ROLE SAMBA_DATA_ENGINEER;
Use SAMBA_DB;

 -- Internal named stage
 CREATE OR REPLACE STAGE RAW_STAGE.samba_stage
  FILE_FORMAT = RAW_STAGE.samba_json_format;
  
  -- External S3 Stage
  CREATE OR REPLACE STAGE RAW_STAGE.external_samba_stage
  FILE_FORMAT = RAW_STAGE.samba_json_format;


 // Create stage object with integration object & file format object
CREATE OR REPLACE STAGE RAW_STAGE.external_samba_stage
    URL = 's3://samba-data-eng/'
    STORAGE_INTEGRATION = s3_int_samba
    FILE_FORMAT = RAW_STAGE.samba_json_format;

LIST @RAW_STAGE.external_samba_stage;

  CREATE OR REPLACE TRANSIENT TABLE RAW_STAGE.cities (
    city_id INT AUTOINCREMENT PRIMARY KEY,
    city_name VARCHAR(50) UNIQUE NOT NULL
);

INSERT INTO RAW_STAGE.cities (city_name)
SELECT 
    $1:city_name::STRING
FROM @RAW_STAGE.samba_stage/cities.json;

SELECT * FROM RAW_STAGE.cities;

SELECT get_ddl('table','cities');
SHOW tables like '%cit%';
show tables;
