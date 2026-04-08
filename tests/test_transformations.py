from decimal import Decimal

from src.transformations.transactions import (
    apply_fx_rate,
    derive_fee_amount_usd,
    find_applicable_fx_rate,
    normalize_currency_code,
    select_latest_records,
)


def test_normalize_currency_code() -> None:
    assert normalize_currency_code(" usd ") == "USD"


def test_find_applicable_fx_rate_for_currency() -> None:
    rates = [
        {
            "rate_date": "2026-02-01",
            "base_currency": "BRL",
            "quote_currency": "USD",
            "exchange_rate": "0.18",
        }
    ]
    assert find_applicable_fx_rate(rates, "brl", "2026-02-01") == Decimal("0.18")


def test_apply_fx_rate_quantizes_to_cents() -> None:
    assert apply_fx_rate("240.00", Decimal("0.18")) == Decimal("43.20")


def test_derive_fee_for_settled_purchase() -> None:
    assert derive_fee_amount_usd("PURCHASE", "SETTLED", Decimal("120.50")) == Decimal("1.21")


def test_select_latest_records_prefers_newer_update() -> None:
    records = [
        {"transaction_id": "T002", "updated_at": "2026-02-01T11:05:00Z", "ingested_at": "2026-02-01T11:10:00Z"},
        {"transaction_id": "T002", "updated_at": "2026-02-02T09:00:00Z", "ingested_at": "2026-02-02T09:05:00Z"},
    ]
    latest = select_latest_records(records, "transaction_id", ("updated_at", "ingested_at"))
    assert latest == [records[1]]
