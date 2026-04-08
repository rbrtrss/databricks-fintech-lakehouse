from __future__ import annotations

from decimal import Decimal

from data_generator.generate_accounts import build_batches as build_account_batches
from data_generator.generate_fx_rates import build_batches as build_fx_rate_batches
from data_generator.generate_transactions import build_batches as build_transaction_batches
from src.transformations.transactions import (
    apply_fx_rate,
    derive_suspicious_activity_signals,
    find_applicable_fx_rate,
    select_latest_records,
)
from src.validations.transactions import classify_transaction_rejection


def _flatten_batches(batches: dict[str, list[dict[str, object]]]) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for batch_index, (_, batch_records) in enumerate(sorted(batches.items()), start=1):
        for record in batch_records:
            records.append(
                {
                    **record,
                    "ingested_at": f"2026-02-{batch_index:02d}T00:00:00Z",
                }
            )
    return records


def _classify_transactions() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    transactions = _flatten_batches(build_transaction_batches())
    accounts = {
        record["account_id"]: record
        for record in select_latest_records(
            _flatten_batches(build_account_batches()),
            "account_id",
            ("updated_at", "ingested_at"),
        )
    }
    fx_rates = _flatten_batches(build_fx_rate_batches())

    accepted: list[dict[str, object]] = []
    rejected: list[dict[str, object]] = []

    latest_transactions = select_latest_records(
        transactions,
        "transaction_id",
        ("updated_at", "ingested_at"),
    )

    for transaction in latest_transactions:
        account = accounts.get(str(transaction["account_id"]))
        exchange_rate = find_applicable_fx_rate(
            fx_rates,
            str(transaction["currency"]),
            str(transaction["transaction_timestamp"])[:10],
        )
        rejection_reason = classify_transaction_rejection(
            transaction,
            customer_id=None if account is None else str(account["customer_id"]),
            exchange_rate=exchange_rate,
        )
        amount_usd: Decimal | None = apply_fx_rate(transaction["amount"], exchange_rate)

        if rejection_reason is not None:
            rejected.append(
                {
                    "transaction_id": transaction["transaction_id"],
                    "rejection_reason": rejection_reason,
                }
            )
            continue

        accepted.append(
            {
                "transaction_id": transaction["transaction_id"],
                "transaction_timestamp": transaction["transaction_timestamp"],
                "transaction_type": transaction["transaction_type"],
                "channel": transaction["channel"],
                "status": transaction["status"],
                "amount_usd": amount_usd,
                "is_international": str(transaction["currency"]) != str(account["account_currency"]),
                "customer_id": account["customer_id"],
                "exchange_rate": exchange_rate,
            }
        )

    return accepted, rejected


def test_sample_transactions_are_split_between_accepted_and_rejected_outputs() -> None:
    accepted, rejected = _classify_transactions()

    assert {row["transaction_id"] for row in accepted} == {"T001", "T002", "T003", "T005"}
    assert {row["transaction_id"] for row in rejected} == {"T004", "T006", "T007"}


def test_latest_transaction_version_is_kept_for_status_and_fx_amount() -> None:
    accepted, _ = _classify_transactions()
    transaction_t002 = next(row for row in accepted if row["transaction_id"] == "T002")

    assert transaction_t002["status"] == "SETTLED"
    assert transaction_t002["amount_usd"] == Decimal("43.20")
    assert transaction_t002["exchange_rate"] == Decimal("0.18")


def test_rejected_transactions_have_expected_reasons() -> None:
    _, rejected = _classify_transactions()
    rejection_reasons = {row["transaction_id"]: row["rejection_reason"] for row in rejected}

    assert rejection_reasons == {
        "T004": "INVALID_ACCOUNT",
        "T006": "FUTURE_TIMESTAMP",
        "T007": "NON_POSITIVE_PURCHASE_AMOUNT",
    }


def test_cross_currency_transaction_is_marked_international_with_usd_normalization() -> None:
    accepted, _ = _classify_transactions()
    transaction_t005 = next(row for row in accepted if row["transaction_id"] == "T005")

    assert transaction_t005["customer_id"] == "C001"
    assert transaction_t005["is_international"] is True
    assert transaction_t005["amount_usd"] == Decimal("97.34")


def test_sample_accepted_transactions_produce_expected_suspicious_signals() -> None:
    accepted, _ = _classify_transactions()
    signals = derive_suspicious_activity_signals(accepted)

    assert {(signal["transaction_id"], signal["signal_type"]) for signal in signals} == {
        ("T001", "LARGE_SETTLED_PURCHASE"),
        ("T001", "MULTI_CHANNEL_24H"),
        ("T005", "INTERNATIONAL_PURCHASE"),
        ("T005", "MULTI_CHANNEL_24H"),
    }
