-- AFTER initial loading into Schema RAW_STAGE
-- We transform the data and keep the logs. We keep the data in VIEWS in TRANSFORMED SCHEMA then load into DIM Tables and Sales FACT table

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
--Cities
CREATE OR REPLACE VIEW SAMBA_DB.TRANSFORMED.v_cities_clean AS
SELECT
  TRY_TO_NUMBER(city_id)        AS city_id,
  INITCAP(TRIM(city_name))      AS city_name
FROM SAMBA_DB.RAW_STAGE.cities
WHERE city_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY city_id ORDER BY city_name) = 1;
____________________________________________________________________________________________________________________
-- Branches (assumes RAW_STAGE.branches has branch_id, branch_name, city_id or city_name)
CREATE OR REPLACE VIEW SAMBA_DB.TRANSFORMED.v_branches_clean AS
SELECT
  TRY_TO_NUMBER(b.branch_id)            AS branch_id,
  INITCAP(TRIM(b.branch_name))          AS branch_name,
  INITCAP(TRIM(b.city_name))          AS city_name
  -- Prefer city_id if present; otherwise look up by city_name
  --COALESCE(TRY_TO_NUMBER(b.city_name), c.city_name) AS city_id
FROM SAMBA_DB.RAW_STAGE.branches b
LEFT JOIN SAMBA_DB.RAW_STAGE.cities c
  ON INITCAP(TRIM(b.city_name)) = INITCAP(TRIM(c.city_name))
WHERE branch_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY branch_id ORDER BY branch_name) = 1;
____________________________________________________________________________________________________________________
-- Products
CREATE OR REPLACE VIEW SAMBA_DB.TRANSFORMED.v_products_clean AS
SELECT
  TRY_TO_NUMBER(product_id)     AS product_id,
  INITCAP(TRIM(product_name))   AS product_name,
  INITCAP(TRIM(category))       AS category
FROM SAMBA_DB.RAW_STAGE.products
WHERE product_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_name) = 1;
____________________________________________________________________________________________________________________
-- Staff & Employees (join)
-- staff: staff_id ↔ employee_id; employees has names/role/phone/email/hire_date
CREATE OR REPLACE VIEW SAMBA_DB.TRANSFORMED.v_staff_clean AS
SELECT
  TRIM(s.staff_id)                            AS staff_id,
  TRIM(s.employee_id)                         AS employee_id,
  INITCAP(TRIM(e.first_name))                 AS first_name,
  INITCAP(TRIM(e.last_name))                  AS last_name,
  INITCAP(TRIM(e.role))                       AS role,
  e.hire_date::TIMESTAMP_NTZ                  AS hire_date
FROM SAMBA_DB.RAW_STAGE.staff s
LEFT JOIN SAMBA_DB.RAW_STAGE.employees e
  ON e.employee_id::STRING = s.employee_id::STRING
WHERE s.staff_id IS NOT NULL
QUALIFY ROW_NUMBER()
  OVER (PARTITION BY s.staff_id ORDER BY e.hire_date DESC NULLS LAST) = 1;
____________________________________________________________________________________________________________________
-- Staff–Branch assignment
CREATE OR REPLACE VIEW SAMBA_DB.TRANSFORMED.v_staff_latest_assignment AS
SELECT
  TRIM(staff_id) AS staff_id,
  TRY_TO_NUMBER(branch_id) AS branch_id,
  start_date AS start_date,
  --e.hire_date::TIMESTAMP_NTZ                  AS hire_date
  end_date    AS end_date
FROM SAMBA_DB.RAW_STAGE.staff_branch_assignments
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY staff_id ORDER BY COALESCE(end_date, '9999-12-31') DESC, start_date DESC
) = 1;
___________________________________________________________________________________________________________________
-- Sales (type casts + derived revenue)
CREATE OR REPLACE VIEW SAMBA_DB.TRANSFORMED.v_sales_clean AS
WITH s AS (
  SELECT
    TRY_TO_NUMBER(sale_id)            AS sale_id,
    TRY_TO_NUMBER(branch_id)          AS branch_id,
    TRIM(staff_id)                    AS staff_id,
    TRY_TO_NUMBER(product_id)         AS product_id,
    sale_date   AS sale_ts,
    TRY_TO_DECIMAL(volume_sold,12,2)  AS volume_sold,
    TRY_TO_DECIMAL(price,12,2)        AS price_raw,
    TRY_TO_DECIMAL(cost,12,2)         AS cost_raw,
    TRY_TO_DECIMAL(revenue,12,2)      AS revenue_raw
  FROM SAMBA_DB.RAW_STAGE.sales
  WHERE sale_id IS NOT NULL
)
SELECT
  s.sale_id,
  s.branch_id,
  s.staff_id,
  s.product_id,
  s.sale_ts,
  s.volume_sold,
  /* Use sales.price if present, otherwise product.price */
  COALESCE(s.price_raw, TRY_TO_DECIMAL(p.price,12,2)) AS price,
  s.cost_raw                                          AS cost,
  /* Use given revenue; else effective price * volume */
  COALESCE(
    s.revenue_raw,
    COALESCE(s.price_raw, TRY_TO_DECIMAL(p.price,12,2)) * s.volume_sold
  ) AS revenue
FROM s
LEFT JOIN SAMBA_DB.RAW_STAGE.products p
  ON p.product_id = s.product_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.sale_id ORDER BY s.sale_ts DESC NULLS LAST) = 1;

____________________________________________________________________________________________________________________
-- Build dimension tables in TRANSFORMED (idempotent MERGEs)
-- Create surrogate keys (sequences) and upsert distinct business keys from the clean views.
USE SCHEMA SAMBA_DB.TRANSFORMED;

-- Sequences for surrogate keys
CREATE OR REPLACE SEQUENCE seq_dim_product START WITH = 1 INCREMENT BY = 1;
CREATE OR REPLACE SEQUENCE seq_dim_branch  START WITH = 1 INCREMENT BY = 1;
CREATE OR REPLACE SEQUENCE seq_dim_staff   START WITH = 1 INCREMENT BY = 1;
CREATE OR REPLACE SEQUENCE seq_dim_date    START WITH = 1 INCREMENT BY = 1;

-- Date dimension from sales date range
CREATE OR REPLACE TABLE DIM_DATE (
  date_key   INT DEFAULT seq_dim_date.NEXTVAL PRIMARY KEY,
  date       DATE,
  day        TINYINT,
  month      TINYINT,
  year       SMALLINT,
  quarter    TINYINT,
  day_name   STRING,
  is_weekend BOOLEAN
);

-- Populate DIM_DATE once (or as needed)
CREATE OR REPLACE TEMP TABLE _date_bounds AS
SELECT MIN::DATE AS min_d, MAX::DATE AS max_d
FROM (SELECT MIN(sale_ts) MIN, MAX(sale_ts) MAX FROM TRANSFORMED.v_sales_clean);

INSERT INTO DIM_DATE (date, day, month, year, quarter, day_name, is_weekend)
SELECT
  d::DATE,
  EXTRACT(DAY     FROM d),
  EXTRACT(MONTH   FROM d),
  EXTRACT(YEAR    FROM d),
  EXTRACT(QUARTER FROM d),
  TO_CHAR(d, 'Dy'),
  DAYOFWEEKISO(d) IN (6,7)
FROM (SELECT min_d, max_d FROM _date_bounds) AS b
JOIN LATERAL (
  SELECT DATEADD('day', SEQ4(), b.min_d) AS d
  FROM TABLE(GENERATOR(ROWCOUNT => 100000))   -- must be a constant; pick safely large
) AS g
WHERE d <= b.max_d;



-- City / Branch
CREATE OR REPLACE TABLE DIM_CITY (
  city_id INT PRIMARY KEY,
  city_name STRING
);

MERGE INTO DIM_CITY t
USING TRANSFORMED.v_cities_clean s
ON t.city_id = s.city_id
WHEN MATCHED THEN UPDATE SET city_name = s.city_name
WHEN NOT MATCHED THEN INSERT (city_id, city_name) VALUES (s.city_id, s.city_name);

CREATE OR REPLACE TABLE DIM_BRANCH (
  branch_key INT DEFAULT seq_dim_branch.NEXTVAL PRIMARY KEY,
  branch_id  INT,
  city_name    STRING,
  branch_name STRING
);

MERGE INTO DIM_BRANCH t
USING TRANSFORMED.v_branches_clean s
ON t.branch_id = s.branch_id
WHEN MATCHED THEN UPDATE SET t.branch_id = s.branch_id, t.branch_name = s.branch_name
WHEN NOT MATCHED THEN INSERT (branch_id, city_name, branch_name)
VALUES (s.branch_id, s.city_name, s.branch_name);

-- Product
CREATE OR REPLACE TABLE DIM_PRODUCT (
  product_key   INT DEFAULT seq_dim_product.NEXTVAL PRIMARY KEY,
  product_id    INT,
  product_name  STRING,
  category      STRING
);

MERGE INTO DIM_PRODUCT t
USING TRANSFORMED.v_products_clean s
ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET product_name = s.product_name, category = s.category
WHEN NOT MATCHED THEN INSERT (product_id, product_name, category)
VALUES (s.product_id, s.product_name, s.category);

-- Staff
CREATE OR REPLACE TABLE DIM_STAFF (
  staff_key   INT DEFAULT seq_dim_staff.NEXTVAL PRIMARY KEY,
  staff_id    STRING,
  employee_id STRING,
  first_name  STRING,
  last_name   STRING,
  role        STRING
);

MERGE INTO DIM_STAFF t
USING TRANSFORMED.v_staff_clean s
ON t.staff_id = s.staff_id
WHEN MATCHED THEN UPDATE SET employee_id = s.employee_id, first_name = s.first_name,
                            last_name = s.last_name, role = s.role
WHEN NOT MATCHED THEN INSERT (staff_id, employee_id, first_name, last_name, role)
VALUES (s.staff_id, s.employee_id, s.first_name, s.last_name, s.role);

____________________________________________________________________________________________________________________
-- Build the fact table in TRANSFORMED
-- Create a typed staging view (already done: v_sales_clean) → join to dims by business keys → insert with surrogate keys.
-- Fact table
CREATE OR REPLACE TABLE FACT_SALES (
  sale_id      NUMBER(38,0) PRIMARY KEY,
  date_key     INT,
  product_key  INT,
  branch_key   INT,
  staff_key    INT,
  sale_ts      TIMESTAMP_NTZ,
  volume_sold  NUMBER(12,2),
  price        NUMBER(12,2),
  cost         NUMBER(12,2),
  revenue      NUMBER(12,2)
);

-- Insert (idempotent MERGE) from clean sales + dimension lookups
MERGE INTO FACT_SALES f
USING (
  SELECT
    s.sale_id,
    d.date_key,
    p.product_key,
    b.branch_key,
    st.staff_key,
    s.sale_ts,
    s.volume_sold,
    s.price,
    s.cost,
    s.revenue
  FROM TRANSFORMED.v_sales_clean s
  JOIN TRANSFORMED.DIM_DATE    d ON d.date = CAST(s.sale_ts AS DATE)
  LEFT JOIN TRANSFORMED.DIM_PRODUCT p ON p.product_id = s.product_id
  LEFT JOIN TRANSFORMED.DIM_BRANCH  b ON b.branch_id  = s.branch_id
  LEFT JOIN TRANSFORMED.DIM_STAFF   st ON st.staff_id = s.staff_id
) src
ON f.sale_id = src.sale_id
WHEN MATCHED THEN UPDATE SET
  f.date_key = src.date_key, f.product_key = src.product_key,
  f.branch_key = src.branch_key, f.staff_key = src.staff_key,
  f.sale_ts = src.sale_ts, f.volume_sold = src.volume_sold,
  f.price = src.price, f.cost = src.cost, f.revenue = src.revenue
WHEN NOT MATCHED THEN INSERT (
  sale_id, date_key, product_key, branch_key, staff_key,
  sale_ts, volume_sold, price, cost, revenue
) VALUES (
  src.sale_id, src.date_key, src.product_key, src.branch_key, src.staff_key,
  src.sale_ts, src.volume_sold, src.price, src.cost, src.revenue
);

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
