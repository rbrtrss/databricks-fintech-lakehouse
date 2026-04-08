"""Reusable transformation helpers for transaction-domain logic."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta
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


def derive_suspicious_activity_signals(
    transactions: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    settled_purchases = [
        transaction
        for transaction in transactions
        if transaction.get("status") == "SETTLED" and transaction.get("transaction_type") == "PURCHASE"
    ]

    signals: list[dict[str, Any]] = []

    for transaction in settled_purchases:
        amount_usd = Decimal(str(transaction["amount_usd"]))

        if amount_usd >= Decimal("100.00"):
            signals.append(
                _build_signal(
                    transaction,
                    signal_type="LARGE_SETTLED_PURCHASE",
                    risk_level="HIGH",
                )
            )

        if bool(transaction.get("is_international")):
            signals.append(
                _build_signal(
                    transaction,
                    signal_type="INTERNATIONAL_PURCHASE",
                    risk_level="MEDIUM",
                )
            )

    settled_by_customer: dict[str, list[dict[str, Any]]] = {}
    for transaction in settled_purchases:
        settled_by_customer.setdefault(str(transaction["customer_id"]), []).append(transaction)

    for customer_transactions in settled_by_customer.values():
        ordered_transactions = sorted(
            customer_transactions,
            key=lambda transaction: _parse_timestamp(transaction.get("transaction_timestamp")),
        )
        for transaction in ordered_transactions:
            transaction_time = _parse_timestamp(transaction.get("transaction_timestamp"))
            has_mixed_channel_neighbor = any(
                comparison.get("transaction_id") != transaction.get("transaction_id")
                and abs(_parse_timestamp(comparison.get("transaction_timestamp")) - transaction_time)
                <= timedelta(hours=24)
                and comparison.get("channel") != transaction.get("channel")
                for comparison in ordered_transactions
            )
            if has_mixed_channel_neighbor:
                signals.append(
                    _build_signal(
                        transaction,
                        signal_type="MULTI_CHANNEL_24H",
                        risk_level="MEDIUM",
                    )
                )

    return signals


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


def filter_incremental_records(
    incoming_records: Iterable[dict[str, Any]],
    existing_records: Iterable[dict[str, Any]],
    key_field: str,
    source_timestamp_fields: tuple[str, ...],
    existing_timestamp_fields: tuple[str, ...],
) -> list[dict[str, Any]]:
    existing_by_key = {str(record[key_field]): record for record in existing_records}
    incremental: list[dict[str, Any]] = []

    for record in incoming_records:
        existing_record = existing_by_key.get(str(record[key_field]))
        if existing_record is None:
            incremental.append(record)
            continue

        source_sort_key = _sort_key(record, source_timestamp_fields)
        existing_sort_key = _sort_key(existing_record, existing_timestamp_fields)
        if source_sort_key > existing_sort_key:
            incremental.append(record)

    return incremental


def _sort_key(record: dict[str, Any], timestamp_fields: tuple[str, ...]) -> tuple[datetime, ...]:
    return tuple(_parse_timestamp(record.get(field)) for field in timestamp_fields)


def _build_signal(
    transaction: dict[str, Any],
    *,
    signal_type: str,
    risk_level: str,
) -> dict[str, Any]:
    return {
        "transaction_id": transaction["transaction_id"],
        "customer_id": transaction["customer_id"],
        "signal_type": signal_type,
        "risk_level": risk_level,
    }


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if value is None:
        return datetime.min
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
