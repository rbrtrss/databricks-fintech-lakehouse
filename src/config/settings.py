"""Project configuration for the first Databricks lakehouse slice."""

from pathlib import Path

CATALOG = "main"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

BASE_DIR = Path(__file__).resolve().parents[2]
SAMPLE_DATA_DIR = BASE_DIR / "sample_data"

ALLOWED_CUSTOMER_STATUSES = {"ACTIVE", "DORMANT", "SUSPENDED"}
ALLOWED_ACCOUNT_STATUSES = {"ACTIVE", "FROZEN", "CLOSED"}
ALLOWED_TRANSACTION_STATUSES = {"PENDING", "SETTLED", "FAILED", "REVERSED"}
ALLOWED_TRANSACTION_TYPES = {"PURCHASE", "ATM_WITHDRAWAL", "TRANSFER"}
ALLOWED_CHANNELS = {"CARD", "APP", "ATM"}
SUPPORTED_CURRENCIES = {"USD", "EUR", "GBP", "ARS", "BRL"}
