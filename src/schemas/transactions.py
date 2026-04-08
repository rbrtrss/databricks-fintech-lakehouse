"""Column definitions used by Bronze ingestion and Silver models."""

BRONZE_METADATA_COLUMNS = [
    "batch_id",
    "source_file",
    "ingested_at",
    "record_loaded_date",
    "raw_payload",
]

CUSTOMER_COLUMNS = [
    "customer_id",
    "full_name",
    "email",
    "country_code",
    "created_at",
    "updated_at",
    "customer_status",
]

ACCOUNT_COLUMNS = [
    "account_id",
    "customer_id",
    "account_type",
    "account_currency",
    "opened_at",
    "updated_at",
    "account_status",
]

TRANSACTION_COLUMNS = [
    "transaction_id",
    "account_id",
    "transaction_timestamp",
    "updated_at",
    "transaction_type",
    "channel",
    "amount",
    "currency",
    "status",
]

FX_RATE_COLUMNS = [
    "rate_date",
    "base_currency",
    "quote_currency",
    "exchange_rate",
]
