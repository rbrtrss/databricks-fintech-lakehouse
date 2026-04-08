{{ config(
    materialized='incremental',
    unique_key='account_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

with ranked as (
    select
        account_id,
        customer_id,
        upper(account_type) as account_type,
        upper(account_currency) as account_currency,
        cast(opened_at as timestamp) as opened_at,
        cast(updated_at as timestamp) as updated_at,
        upper(account_status) as account_status,
        batch_id as ingestion_batch_id,
        source_file,
        cast(ingested_at as timestamp) as ingested_at,
        cast(record_loaded_date as date) as record_loaded_date,
        raw_payload,
        row_number() over (
            partition by account_id
            order by cast(updated_at as timestamp) desc, cast(ingested_at as timestamp) desc
        ) as row_num
    from {{ source('bronze', 'bronze_accounts_raw') }}
)

select
    account_id,
    customer_id,
    account_type,
    account_currency,
    opened_at,
    updated_at,
    account_status,
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
