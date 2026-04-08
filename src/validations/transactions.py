"""Reusable validation helpers for the first transaction slice."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

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


def classify_transaction_rejection(
    transaction: Mapping[str, Any],
    *,
    customer_id: str | None,
    exchange_rate: Decimal | None,
) -> str | None:
    if customer_id is None:
        return "INVALID_ACCOUNT"
    if not is_valid_status(str(transaction["status"])):
        return "INVALID_STATUS"
    if _is_after_reference(
        str(transaction["transaction_timestamp"]),
        str(transaction.get("updated_at") or transaction["ingested_at"]),
    ):
        return "FUTURE_TIMESTAMP"
    if str(transaction["transaction_type"]) == "PURCHASE" and not has_positive_amount(transaction["amount"]):
        return "NON_POSITIVE_PURCHASE_AMOUNT"
    if normalize_currency_code(str(transaction["currency"])) != "USD" and exchange_rate is None:
        return "MISSING_FX_RATE"
    return None


def _is_after_reference(timestamp_value: str, reference_value: str) -> bool:
    event_time = datetime.fromisoformat(timestamp_value.replace("Z", "+00:00"))
    reference_time = datetime.fromisoformat(reference_value.replace("Z", "+00:00"))
    return event_time > reference_time
