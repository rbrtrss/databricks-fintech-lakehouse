{{ config(materialized='table') }}

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
        tx.*,
        acct.customer_id,
        acct.account_currency,
        fx.exchange_rate
    from deduplicated as tx
    left join {{ ref('silver_accounts') }} as acct
        on tx.account_id = acct.account_id
    left join {{ ref('silver_fx_rates') }} as fx
        on cast(tx.transaction_timestamp as date) = fx.rate_date
       and tx.currency = fx.base_currency
       and fx.quote_currency = 'USD'
),
classified as (
    select
        *,
        case
            when customer_id is null then 'INVALID_ACCOUNT'
            when status not in ('PENDING', 'SETTLED', 'FAILED', 'REVERSED') then 'INVALID_STATUS'
            when transaction_timestamp > coalesce(updated_at, ingested_at) then 'FUTURE_TIMESTAMP'
            when transaction_type = 'PURCHASE' and amount <= 0 then 'NON_POSITIVE_PURCHASE_AMOUNT'
            when currency <> 'USD' and exchange_rate is null then 'MISSING_FX_RATE'
            else null
        end as rejection_reason
    from enriched
)

select
    transaction_id,
    account_id,
    transaction_timestamp,
    updated_at as record_updated_at,
    transaction_type,
    channel,
    amount,
    currency,
    status,
    ingestion_batch_id,
    source_file,
    ingested_at,
    record_loaded_date,
    rejection_reason,
    raw_payload
from classified
where rejection_reason is not null
