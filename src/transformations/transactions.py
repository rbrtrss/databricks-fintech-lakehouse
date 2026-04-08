"""Reusable transformation helpers for transaction-domain logic."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any


def normalize_currency_code(code: str | None) -> str:
    if code is None:
        return ""
    return code.strip().upper()


def find_applicable_fx_rate(
    rates: Iterable[dict[str, Any]],
    currency: str,
    rate_date: str,
    quote_currency: str = "USD",
) -> Decimal | None:
    normalized_currency = normalize_currency_code(currency)
    normalized_quote = normalize_currency_code(quote_currency)
    if normalized_currency == normalized_quote:
        return Decimal("1.0")

    for rate in rates:
        if (
            rate.get("rate_date") == rate_date
            and normalize_currency_code(rate.get("base_currency")) == normalized_currency
            and normalize_currency_code(rate.get("quote_currency")) == normalized_quote
        ):
            return Decimal(str(rate["exchange_rate"]))
    return None


def apply_fx_rate(amount: Decimal | float | str, fx_rate: Decimal | None) -> Decimal | None:
    if fx_rate is None:
        return None
    normalized_amount = Decimal(str(amount))
    return (normalized_amount * fx_rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def derive_fee_amount_usd(
    transaction_type: str,
    status: str,
    amount_usd: Decimal | float | str | None,
) -> Decimal:
    if amount_usd is None:
        return Decimal("0.00")
    if transaction_type != "PURCHASE" or status != "SETTLED":
        return Decimal("0.00")
    normalized_amount = Decimal(str(amount_usd))
    return (normalized_amount * Decimal("0.01")).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )


def select_latest_records(
    records: Iterable[dict[str, Any]],
    key_field: str,
    timestamp_fields: tuple[str, ...],
) -> list[dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for record in records:
        key = str(record[key_field])
        current = latest.get(key)
        if current is None or _sort_key(record, timestamp_fields) > _sort_key(current, timestamp_fields):
            latest[key] = record
    return list(latest.values())


def _sort_key(record: dict[str, Any], timestamp_fields: tuple[str, ...]) -> tuple[datetime, ...]:
    return tuple(_parse_timestamp(record.get(field)) for field in timestamp_fields)


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if value is None:
        return datetime.min
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
