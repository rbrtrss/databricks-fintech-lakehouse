{{ config(materialized='table') }}

select
    date_trunc('month', transaction_date) as revenue_month,
    count(*) as settled_transaction_count,
    cast(sum(amount_usd) as decimal(18, 2)) as settled_volume_usd,
    cast(sum(fee_amount_usd) as decimal(18, 2)) as revenue_usd
from {{ ref('gold_fact_transactions') }}
where status = 'SETTLED'
group by 1
