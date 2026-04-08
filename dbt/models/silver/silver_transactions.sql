{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

with ranked as (
    select
        transaction_id,
        account_id,
        cast(transaction_timestamp as timestamp) as transaction_timestamp,
        cast(updated_at as timestamp) as updated_at,
        upper(transaction_type) as transaction_type,
        upper(channel) as channel,
        cast(amount as decimal(18, 2)) as amount,
        upper(currency) as currency,
        upper(status) as status,
        batch_id as ingestion_batch_id,
        source_file,
        cast(ingested_at as timestamp) as ingested_at,
        cast(record_loaded_date as date) as record_loaded_date,
        raw_payload,
        row_number() over (
            partition by transaction_id
            order by cast(updated_at as timestamp) desc, cast(ingested_at as timestamp) desc
        ) as row_num
    from {{ source('bronze', 'bronze_transactions_raw') }}
),
deduplicated as (
    select *
    from ranked
    where row_num = 1
),
enriched as (
    select
        tx.transaction_id,
        acct.customer_id,
        tx.account_id,
        cast(tx.transaction_timestamp as date) as transaction_date,
        tx.transaction_timestamp,
        tx.updated_at as record_updated_at,
        tx.transaction_type,
        tx.channel,
        tx.amount,
        tx.currency,
        fx.exchange_rate,
        case
            when tx.currency = 'USD' then tx.amount
            when fx.exchange_rate is not null then cast(tx.amount * fx.exchange_rate as decimal(18, 2))
            else null
        end as amount_usd,
        tx.status,
        acct.account_currency,
        case
            when acct.account_currency is null then null
            when tx.currency <> acct.account_currency then true
            else false
        end as is_international,
        tx.ingestion_batch_id,
        tx.source_file,
        tx.ingested_at,
        tx.record_loaded_date,
        tx.raw_payload
    from deduplicated as tx
    left join {{ ref('silver_accounts') }} as acct
        on tx.account_id = acct.account_id
    left join {{ ref('silver_fx_rates') }} as fx
        on cast(tx.transaction_timestamp as date) = fx.rate_date
       and tx.currency = fx.base_currency
       and fx.quote_currency = 'USD'
),
valid_rows as (
    select *
    from enriched
    where {{
        transaction_rejection_reason(
            customer_id_col='customer_id',
            status_col='status',
            transaction_timestamp_col='transaction_timestamp',
            updated_at_col='record_updated_at',
            ingested_at_col='ingested_at',
            transaction_type_col='transaction_type',
            amount_col='amount',
            currency_col='currency',
            exchange_rate_col='exchange_rate'
        )
    }} is null
      and amount_usd is not null
)

select
    transaction_id,
    customer_id,
    account_id,
    transaction_date,
    transaction_timestamp,
    record_updated_at,
    transaction_type,
    channel,
    amount,
    currency,
    exchange_rate,
    amount_usd,
    status,
    is_international,
    ingestion_batch_id,
    source_file,
    ingested_at,
    record_loaded_date,
    raw_payload
from valid_rows

{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} as existing
    where existing.transaction_id = valid_rows.transaction_id
      and (
          existing.record_updated_at > valid_rows.record_updated_at
          or (
              existing.record_updated_at = valid_rows.record_updated_at
              and coalesce(existing.ingested_at, cast('1900-01-01' as timestamp)) >= coalesce(valid_rows.ingested_at, cast('1900-01-01' as timestamp))
          )
      )
)
{% endif %}
