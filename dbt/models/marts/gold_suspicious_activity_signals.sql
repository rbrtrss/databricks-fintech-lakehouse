{{ config(materialized='table') }}

with settled_purchases as (
    select
        transaction_id,
        customer_id,
        transaction_date,
        transaction_timestamp,
        channel,
        amount_usd,
        is_international
    from {{ ref('gold_fact_transactions') }}
    where status = 'SETTLED'
      and transaction_type = 'PURCHASE'
),
large_settled_purchase as (
    select
        transaction_id,
        customer_id,
        transaction_date as signal_date,
        'LARGE_SETTLED_PURCHASE' as signal_type,
        'HIGH' as risk_level,
        amount_usd,
        channel
    from settled_purchases
    where amount_usd >= 100
),
international_purchase as (
    select
        transaction_id,
        customer_id,
        transaction_date as signal_date,
        'INTERNATIONAL_PURCHASE' as signal_type,
        'MEDIUM' as risk_level,
        amount_usd,
        channel
    from settled_purchases
    where is_international
),
multi_channel_24h as (
    select distinct
        base.transaction_id,
        base.customer_id,
        base.transaction_date as signal_date,
        'MULTI_CHANNEL_24H' as signal_type,
        'MEDIUM' as risk_level,
        base.amount_usd,
        base.channel
    from settled_purchases as base
    inner join settled_purchases as comparison
        on base.customer_id = comparison.customer_id
       and base.transaction_id <> comparison.transaction_id
       and base.channel <> comparison.channel
       and abs(unix_timestamp(base.transaction_timestamp) - unix_timestamp(comparison.transaction_timestamp)) <= 24 * 60 * 60
)

select * from large_settled_purchase
union all
select * from international_purchase
union all
select * from multi_channel_24h
