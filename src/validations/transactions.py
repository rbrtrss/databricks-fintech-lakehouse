"""Reusable validation helpers for the first transaction slice."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from src.config.settings import ALLOWED_TRANSACTION_STATUSES, SUPPORTED_CURRENCIES
from src.transformations.transactions import normalize_currency_code


def is_valid_status(status: str) -> bool:
    return status in ALLOWED_TRANSACTION_STATUSES


def is_valid_currency(currency: str | None) -> bool:
    return normalize_currency_code(currency) in SUPPORTED_CURRENCIES


def has_positive_amount(amount: Decimal | float | str) -> bool:
    return Decimal(str(amount)) > Decimal("0")


def is_future_timestamp(timestamp_value: str, now: datetime | None = None) -> bool:
    comparison_time = now or datetime.now(UTC)
    event_time = datetime.fromisoformat(timestamp_value.replace("Z", "+00:00"))
    return event_time > comparison_time
