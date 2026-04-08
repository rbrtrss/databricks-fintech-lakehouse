from datetime import UTC, datetime

from src.validations.transactions import (
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
