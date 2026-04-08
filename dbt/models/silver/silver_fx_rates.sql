{{ config(
    materialized='incremental',
    unique_key=['rate_date', 'base_currency', 'quote_currency'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

with ranked as (
    select
        cast(rate_date as date) as rate_date,
        upper(base_currency) as base_currency,
        upper(quote_currency) as quote_currency,
        cast(exchange_rate as decimal(18, 8)) as exchange_rate,
        batch_id as ingestion_batch_id,
        source_file,
        cast(ingested_at as timestamp) as ingested_at,
        cast(record_loaded_date as date) as record_loaded_date,
        raw_payload,
        row_number() over (
            partition by cast(rate_date as date), upper(base_currency), upper(quote_currency)
            order by cast(ingested_at as timestamp) desc
        ) as row_num
    from {{ source('bronze', 'bronze_fx_rates_raw') }}
)

select
    rate_date,
    base_currency,
    quote_currency,
    exchange_rate,
    ingestion_batch_id,
    source_file,
    ingested_at,
    record_loaded_date,
    raw_payload
from ranked
where row_num = 1

{% if is_incremental() %}
  and rate_date >= (
      select coalesce(max(rate_date), cast('1900-01-01' as date))
      from {{ this }}
  )
{% endif %}
