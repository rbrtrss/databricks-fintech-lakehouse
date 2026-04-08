{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

with ranked as (
    select
        customer_id,
        full_name,
        lower(email) as email,
        upper(country_code) as country_code,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        upper(customer_status) as customer_status,
        batch_id as ingestion_batch_id,
        source_file,
        cast(ingested_at as timestamp) as ingested_at,
        cast(record_loaded_date as date) as record_loaded_date,
        raw_payload,
        row_number() over (
            partition by customer_id
            order by cast(updated_at as timestamp) desc, cast(ingested_at as timestamp) desc
        ) as row_num
    from {{ source('bronze', 'bronze_customers_raw') }}
)

select
    customer_id,
    full_name,
    email,
    country_code,
    created_at,
    updated_at,
    customer_status,
    ingestion_batch_id,
    source_file,
    ingested_at,
    record_loaded_date,
    raw_payload
from ranked
where row_num = 1

{% if is_incremental() %}
  and updated_at >= (
      select coalesce(max(updated_at), cast('1900-01-01' as timestamp))
      from {{ this }}
  )
{% endif %}
