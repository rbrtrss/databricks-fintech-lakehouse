select *
from main.gold.gold_suspicious_activity_signals
order by signal_date desc, customer_id, signal_type;
