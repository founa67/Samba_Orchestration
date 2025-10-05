USE ROLE SAMBA_DATA_ENGINEER;
// Create storage integration object
-- s3 bucket: samba-data-en--with folders branches, cities, staff, employees, sales, products, 
-- s3://samba-data-eng/
-- Role secure way to grant permissions between entities that you trust. eg snowflake account and aws account
-- Require external ID (Best practice when a third party will assume this role) -- 00000 -- Add permissions (Permissions policies )
-- AmazonS3FullAccess -- Provides full access to all buckets via the...
-- create IAM role: samba-access-role
-- Bring from AWS: arn:aws:iam::093524450293:role/samba-access-role nb: edited to add role/samba-access-role

create or replace storage integration s3_int_samba
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::093524450293:role/samba-access-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://samba-data-eng/')
   COMMENT = 'The s3://samba-data-eng/ integration object' ;
   
   
// See storage integration properties to fetch external_id so we can update it in S3
DESC integration s3_int_samba;
-- take to AWS 
-- 1. STORAGE_AWS_ROLE_ARN: arn:aws:iam::565393042555:user/heiy0000-s and 
-- 2. STORAGE_AWS_EXTERNAL_ID: RL60857_SFCRole=5_GM8eohDw3hsDMHEIY3RjxUqqJGQ=