--Snowflake: You prepare the destination - create user, role, warehouse, and database for Fivetran.
-- Phase 1: Setup in Snowflake (The Destination)
-- 1. Create a Warehouse for Fivetran to use (for loading data)
CREATE WAREHOUSE FIVETRAN_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 600 -- suspends after 10 mins of inactivity
  AUTO_RESUME = TRUE;

-- 2. Create a Database for Fivetran to load data into
CREATE DATABASE FIVETRAN_DB;

-- 3. Create a Role specifically for Fivetran
USE ROLE SECURITYADMIN;
CREATE ROLE FIVETRAN_ROLE;

-- 4. Grant necessary privileges to the Fivetran role
GRANT USAGE ON WAREHOUSE FIVETRAN_WH TO ROLE FIVETRAN_ROLE;
GRANT ALL ON DATABASE FIVETRAN_DB TO ROLE FIVETRAN_ROLE;
GRANT ROLE FIVETRAN_ROLE TO ROLE ACCOUNTADMIN;
GRANT ROLE FIVETRAN_ROLE TO ROLE DBT_ROLE;

-- Future grants: Automatically grant permissions on all future schemas/tables in the FIVETRAN_DB
GRANT ALL ON ALL SCHEMAS IN DATABASE FIVETRAN_DB TO ROLE FIVETRAN_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE FIVETRAN_DB TO ROLE FIVETRAN_ROLE;
GRANT ALL ON ALL TABLES IN DATABASE FIVETRAN_DB TO ROLE FIVETRAN_ROLE;
GRANT ALL ON FUTURE TABLES IN DATABASE FIVETRAN_DB TO ROLE FIVETRAN_ROLE;

-- 5. Create a User for Fivetran to connect with
--CREATE USER FIVETRAN_USER
 -- PASSWORD = 'pwdpwpd45(/&%' -- Use a very strong password
--  DEFAULT_ROLE = FIVETRAN_ROLE
--  DEFAULT_WAREHOUSE = FIVETRAN_WH;

DROP USER FIVETRAN_USER;

-- . Recreate the user with MFA explicitly disabled
CREATE USER FIVETRAN_USER
  PASSWORD = 'F&%(/b$$§$V!'
  DEFAULT_ROLE = FIVETRAN_ROLE
  DEFAULT_WAREHOUSE = FIVETRAN_WH
  --DISABLE_MFA = TRUE;  -- This is the key parameter

 
-- 6. Grant the role to the user
GRANT ROLE FIVETRAN_ROLE TO USER FIVETRAN_USER;

-- 7. Grant the role to your user so you can see the data (optional but helpful)
GRANT ROLE FIVETRAN_ROLE TO USER FOUNA96;

USE ROLE FIVETRAN_ROLE;
select current_time();
CREATE SCHEMA FIVETRAN_DB.FIVETRAN_SCHEMA;


-- DBT TESTS
select * 
from "FIVETRAN_DB"."FIVETRAN_SCHEMA"."KENYA_HOLIDAYS" 
limit 5;
