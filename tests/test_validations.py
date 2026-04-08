from datetime import UTC, datetime
from decimal import Decimal

from src.validations.transactions import (
    classify_transaction_rejection,
    has_positive_amount,
    is_future_timestamp,
    is_valid_currency,
    is_valid_status,
)


def test_valid_status() -> None:
    assert is_valid_status("SETTLED") is True


def test_invalid_status() -> None:
    assert is_valid_status("UNKNOWN") is False


def test_valid_currency() -> None:
    assert is_valid_currency(" ars ") is True


def test_invalid_currency() -> None:
    assert is_valid_currency("btc") is False


def test_positive_amount() -> None:
    assert has_positive_amount("10.00") is True


def test_future_timestamp_detection() -> None:
    reference_now = datetime(2026, 2, 1, tzinfo=UTC)
    assert is_future_timestamp("2026-02-02T00:00:00Z", now=reference_now) is True


def test_classify_transaction_rejection_for_invalid_account() -> None:
    transaction = {
        "transaction_timestamp": "2026-02-01T10:15:00Z",
        "updated_at": "2026-02-01T10:20:00Z",
        "ingested_at": "2026-02-01T10:21:00Z",
        "transaction_type": "PURCHASE",
        "amount": "10.00",
        "currency": "USD",
        "status": "SETTLED",
    }

    assert classify_transaction_rejection(transaction, customer_id=None, exchange_rate=Decimal("1.0")) == "INVALID_ACCOUNT"


def test_classify_transaction_rejection_for_future_timestamp_uses_record_time_not_clock_time() -> None:
    transaction = {
        "transaction_timestamp": "2026-02-03T00:00:00Z",
        "updated_at": "2026-02-02T09:00:00Z",
        "ingested_at": "2026-02-02T09:05:00Z",
        "transaction_type": "PURCHASE",
        "amount": "10.00",
        "currency": "USD",
        "status": "SETTLED",
    }

    assert classify_transaction_rejection(
        transaction,
        customer_id="C001",
        exchange_rate=Decimal("1.0"),
    ) == "FUTURE_TIMESTAMP"


def test_classify_transaction_rejection_for_missing_fx_rate() -> None:
    transaction = {
        "transaction_timestamp": "2026-02-01T10:15:00Z",
        "updated_at": "2026-02-01T10:20:00Z",
        "ingested_at": "2026-02-01T10:21:00Z",
        "transaction_type": "PURCHASE",
        "amount": "10.00",
        "currency": "BRL",
        "status": "SETTLED",
    }

    assert classify_transaction_rejection(transaction, customer_id="C001", exchange_rate=None) == "MISSING_FX_RATE"
