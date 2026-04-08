select *
from main.gold.gold_daily_customer_spend
order by transaction_date desc, total_spend_usd desc;
