-- 1. Create a Warehouse for dbt to use (for transforming data)
CREATE WAREHOUSE DBT_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 600 -- suspends after 10 mins of inactivity
  AUTO_RESUME = TRUE;

-- 2. Create a Database for dbt to build models into
CREATE DATABASE DBT_DB;

-- 3. Create a Role specifically for dbt
USE ROLE SECURITYADMIN;
CREATE ROLE DBT_ROLE;

-- 4. Grant necessary privileges to the dbt role
GRANT USAGE ON WAREHOUSE DBT_WH TO ROLE DBT_ROLE;
GRANT ALL ON DATABASE DBT_DB TO ROLE DBT_ROLE;
GRANT ROLE DBT_ROLE TO ROLE ACCOUNTADMIN;

-- Future grants: Automatically grant permissions on all future schemas/tables in the DBT_DB
GRANT ALL ON ALL SCHEMAS IN DATABASE DBT_DB TO ROLE DBT_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE DBT_DB TO ROLE DBT_ROLE;
GRANT ALL ON ALL TABLES IN DATABASE DBT_DB TO ROLE DBT_ROLE;
GRANT ALL ON FUTURE TABLES IN DATABASE DBT_DB TO ROLE DBT_ROLE;

-- 5. Create a User for dbt to connect with
DROP USER IF EXISTS DBT_USER;

CREATE USER DBT_USER
  PASSWORD = '/&&hgßaiji7!'
  DEFAULT_ROLE = DBT_ROLE
  DEFAULT_WAREHOUSE = DBT_WH;
  -- Note: DISABLE_MFA parameter is not valid in Snowflake

-- 6. Grant the role to the user
GRANT ROLE DBT_ROLE TO USER DBT_USER;

-- 7. Grant the role to your user so you can see the data (optional but helpful)
GRANT ROLE DBT_ROLE TO USER FOUNA96;

-- 8. Test the setup
USE ROLE DBT_ROLE;
USE WAREHOUSE DBT_WH;
SELECT CURRENT_TIME();

-- 9. Create initial schema for dbt models
CREATE SCHEMA DBT_DB.DBT_SCHEMA;



-- DBT TESTS
select * 
from "HOLIDAYS" 
limit 5;