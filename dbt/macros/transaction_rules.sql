{% macro transaction_statuses() -%}
(
{%- for status in var('allowed_transaction_statuses', ['PENDING', 'SETTLED', 'FAILED', 'REVERSED']) -%}
    '{{ status }}'{% if not loop.last %}, {% endif %}
{%- endfor -%}
)
{%- endmacro %}


{% macro transaction_rejection_reason(
    customer_id_col='customer_id',
    status_col='status',
    transaction_timestamp_col='transaction_timestamp',
    updated_at_col='updated_at',
    ingested_at_col='ingested_at',
    transaction_type_col='transaction_type',
    amount_col='amount',
    currency_col='currency',
    exchange_rate_col='exchange_rate'
) -%}
case
    when {{ customer_id_col }} is null then 'INVALID_ACCOUNT'
    when {{ status_col }} not in {{ transaction_statuses() }} then 'INVALID_STATUS'
    when {{ transaction_timestamp_col }} > coalesce({{ updated_at_col }}, {{ ingested_at_col }}) then 'FUTURE_TIMESTAMP'
    when {{ transaction_type_col }} = 'PURCHASE' and {{ amount_col }} <= 0 then 'NON_POSITIVE_PURCHASE_AMOUNT'
    when {{ currency_col }} <> 'USD' and {{ exchange_rate_col }} is null then 'MISSING_FX_RATE'
    else null
end
{%- endmacro %}
