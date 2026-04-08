{{ config(materialized='table') }}

select
    transaction_date,
    customer_id,
    count(*) as transaction_count,
    cast(sum(amount_usd) as decimal(18, 2)) as total_spend_usd,
    cast(avg(amount_usd) as decimal(18, 2)) as average_transaction_usd,
    cast(sum(case when is_international then amount_usd else 0 end) as decimal(18, 2)) as international_spend_usd
from {{ ref('gold_fact_transactions') }}
where status = 'SETTLED'
group by 1, 2
