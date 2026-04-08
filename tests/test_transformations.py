from decimal import Decimal

from src.transformations.transactions import (
    apply_fx_rate,
    derive_suspicious_activity_signals,
    derive_fee_amount_usd,
    filter_incremental_records,
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


def test_filter_incremental_records_keeps_new_key_even_if_older_than_global_watermark() -> None:
    incoming_records = [
        {"transaction_id": "T008", "updated_at": "2026-02-01T09:00:00Z", "ingested_at": "2026-02-05T00:00:00Z"},
    ]
    existing_records = [
        {"transaction_id": "T001", "record_updated_at": "2026-02-04T12:00:00Z", "ingested_at": "2026-02-04T12:05:00Z"},
    ]

    incremental = filter_incremental_records(
        incoming_records,
        existing_records,
        key_field="transaction_id",
        source_timestamp_fields=("updated_at", "ingested_at"),
        existing_timestamp_fields=("record_updated_at", "ingested_at"),
    )

    assert incremental == incoming_records


def test_filter_incremental_records_keeps_newer_version_for_existing_key() -> None:
    incoming_records = [
        {"transaction_id": "T002", "updated_at": "2026-02-02T09:00:00Z", "ingested_at": "2026-02-02T09:05:00Z"},
    ]
    existing_records = [
        {"transaction_id": "T002", "record_updated_at": "2026-02-01T11:05:00Z", "ingested_at": "2026-02-01T11:10:00Z"},
    ]

    incremental = filter_incremental_records(
        incoming_records,
        existing_records,
        key_field="transaction_id",
        source_timestamp_fields=("updated_at", "ingested_at"),
        existing_timestamp_fields=("record_updated_at", "ingested_at"),
    )

    assert incremental == incoming_records


def test_filter_incremental_records_drops_stale_replay_for_existing_key() -> None:
    incoming_records = [
        {"transaction_id": "T002", "updated_at": "2026-02-01T11:05:00Z", "ingested_at": "2026-02-01T11:10:00Z"},
    ]
    existing_records = [
        {"transaction_id": "T002", "record_updated_at": "2026-02-02T09:00:00Z", "ingested_at": "2026-02-02T09:05:00Z"},
    ]

    incremental = filter_incremental_records(
        incoming_records,
        existing_records,
        key_field="transaction_id",
        source_timestamp_fields=("updated_at", "ingested_at"),
        existing_timestamp_fields=("record_updated_at", "ingested_at"),
    )

    assert incremental == []


def test_filter_incremental_records_uses_ingestion_time_as_tiebreaker() -> None:
    incoming_records = [
        {"transaction_id": "T002", "updated_at": "2026-02-02T09:00:00Z", "ingested_at": "2026-02-02T09:10:00Z"},
    ]
    existing_records = [
        {"transaction_id": "T002", "record_updated_at": "2026-02-02T09:00:00Z", "ingested_at": "2026-02-02T09:05:00Z"},
    ]

    incremental = filter_incremental_records(
        incoming_records,
        existing_records,
        key_field="transaction_id",
        source_timestamp_fields=("updated_at", "ingested_at"),
        existing_timestamp_fields=("record_updated_at", "ingested_at"),
    )

    assert incremental == incoming_records


def test_derive_suspicious_activity_signals_from_sample_patterns() -> None:
    transactions = [
        {
            "transaction_id": "T001",
            "customer_id": "C001",
            "transaction_timestamp": "2026-02-01T10:15:00Z",
            "transaction_type": "PURCHASE",
            "channel": "CARD",
            "amount_usd": Decimal("120.50"),
            "status": "SETTLED",
            "is_international": False,
        },
        {
            "transaction_id": "T005",
            "customer_id": "C001",
            "transaction_timestamp": "2026-01-31T23:55:00Z",
            "transaction_type": "PURCHASE",
            "channel": "APP",
            "amount_usd": Decimal("97.34"),
            "status": "SETTLED",
            "is_international": True,
        },
        {
            "transaction_id": "T003",
            "customer_id": "C003",
            "transaction_timestamp": "2026-02-02T12:00:00Z",
            "transaction_type": "ATM_WITHDRAWAL",
            "channel": "ATM",
            "amount_usd": Decimal("5.00"),
            "status": "SETTLED",
            "is_international": False,
        },
    ]

    signals = derive_suspicious_activity_signals(transactions)

    assert {(signal["transaction_id"], signal["signal_type"]) for signal in signals} == {
        ("T001", "LARGE_SETTLED_PURCHASE"),
        ("T001", "MULTI_CHANNEL_24H"),
        ("T005", "INTERNATIONAL_PURCHASE"),
        ("T005", "MULTI_CHANNEL_24H"),
    }
