{{ config(materialized='table') }}

select
    transaction_id,
    customer_id,
    account_id,
    transaction_date,
    transaction_timestamp,
    transaction_type,
    channel,
    currency,
    amount,
    amount_usd,
    cast(
        case
            when transaction_type = 'PURCHASE' and status = 'SETTLED' then amount_usd * 0.01
            else 0
        end as decimal(18, 2)
    ) as fee_amount_usd,
    is_international,
    status,
    ingestion_batch_id,
    record_updated_at
from {{ ref('silver_transactions') }}
