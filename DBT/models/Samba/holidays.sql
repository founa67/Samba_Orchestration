-- dbt model: holidays.sql
-- Purpose: clean and standardize Kenya public holidays loaded via Fivetran into Snowflake
-- Source table: FIVETRAN_DB.FIVETRAN_SCHEMA.KENYA_HOLIDAYS
-- Materialization: incremental (Snowflake MERGE using holiday_key as unique_key)

{{
  config(
    materialized = 'incremental',
    unique_key = 'holiday_key'
  )
}}

with raw as (
  select *
  from "FIVETRAN_DB"."FIVETRAN_SCHEMA"."KENYA_HOLIDAYS"
),

-- SAFE mapping: map only existing columns from the sample to canonical raw_* names
normalized as (
  select
    "DATE"         as raw_holiday_date,
    null           as raw_observed_date,
    "HOLIDAY_NAME" as raw_holiday_name,
    null           as raw_local_name,
    "TYPE"         as raw_holiday_type,
    "YEAR"         as raw_year,
    *
  from raw
),

-- Parse the raw date/timestamp into strict DATE columns - COMPLETELY REMOVED TRY_CAST
parsed as (
  select
    *,
    -- Use direct date conversion without any CAST operations
    case
      when raw_holiday_date is not null then
        coalesce(
          try_to_date(raw_holiday_date::string),  -- First try direct date conversion
          try_to_date(raw_holiday_date::string, 'MM/DD/YYYY'),  -- Then try with format
          try_to_date(raw_holiday_date::string, 'YYYY-MM-DD')   -- Then try ISO format
        )
      else null
    end as holiday_date,

    case
      when raw_observed_date is not null then
        coalesce(
          try_to_date(raw_observed_date::string),
          try_to_date(raw_observed_date::string, 'MM/DD/YYYY'),
          try_to_date(raw_observed_date::string, 'YYYY-MM-DD')
        )
      else null
    end as observed_date

  from normalized
),

-- canonicalize and derive columns from the parsed dates
canon as (
  select
    upper(trim(raw_holiday_name))                                   as holiday_name,
    nullif(trim(raw_local_name), '')                                as local_name,
    holiday_date,
    observed_date,
    coalesce(nullif(trim(raw_holiday_type), ''), 'Public')          as holiday_type,

    -- derived date parts (null-safe)
    case when holiday_date is not null then year(holiday_date) end  as year,
    case when holiday_date is not null then month(holiday_date) end as month,
    case when holiday_date is not null then day(holiday_date) end   as day,
    case when holiday_date is not null then trim(dayname(holiday_date)) end as day_name,

    -- is_weekend: Snowflake dayofweek returns 0=Sunday .. 6=Saturday
    case when holiday_date is not null and dayofweek(holiday_date) in (0,6) then true else false end as is_weekend,

    case when observed_date is not null and holiday_date is not null and observed_date != holiday_date then true else false end as is_observed,

    -- deterministic unique key for upsert (date + normalized name)
    md5(coalesce(holiday_date::string, '') || '||' || coalesce(upper(trim(raw_holiday_name)), '')) as holiday_key

  from parsed
),

-- pick canonical rows, de-duplicate by holiday_key preferring the most recently observed or non-null observed_date
final as (
  select
    holiday_key,
    holiday_date,
    observed_date,
    holiday_name,
    local_name,
    holiday_type,
    year        as year,
    month       as month,
    day         as day,
    day_name,
    is_weekend,
    is_observed,
    current_timestamp() as dbt_processed_at
  from (
    select
      *,
      row_number() over (
        partition by holiday_key
        order by
          case when observed_date is not null then 1 else 0 end desc,
          observed_date desc nulls last
      ) as rn
    from canon
    where holiday_date is not null
  ) t
  where rn = 1
)

-- incremental merge into target table for Snowflake
{% if is_incremental() %}

merge into {{ this }} as target
using ( select * from final ) as source
  on target.holiday_key = source.holiday_key
when matched and (
     target.holiday_date is distinct from source.holiday_date
  or target.observed_date is distinct from source.observed_date
  or target.holiday_name is distinct from source.holiday_name
  or target.holiday_type is distinct from source.holiday_type
) then update set
  holiday_date      = source.holiday_date,
  observed_date     = source.observed_date,
  holiday_name      = source.holiday_name,
  local_name        = source.local_name,
  holiday_type      = source.holiday_type,
  year              = source.year,
  month             = source.month,
  day               = source.day,
  day_name          = source.day_name,
  is_weekend        = source.is_weekend,
  is_observed       = source.is_observed,
  dbt_processed_at  = current_timestamp()
when not matched then insert (
  holiday_key, holiday_date, observed_date, holiday_name, local_name, holiday_type,
  year, month, day, day_name, is_weekend, is_observed, dbt_processed_at
) values (
  source.holiday_key, source.holiday_date, source.observed_date, source.holiday_name, source.local_name, source.holiday_type,
  source.year, source.month, source.day, source.day_name, source.is_weekend, source.is_observed, current_timestamp()
)

{% else %}

select * from final

{% endif %}