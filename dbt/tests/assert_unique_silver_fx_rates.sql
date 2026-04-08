select
    rate_date,
    base_currency,
    quote_currency,
    count(*) as row_count
from {{ ref('silver_fx_rates') }}
group by 1, 2, 3
having count(*) > 1
