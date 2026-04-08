"""Generate transaction JSONL batches for the first lakehouse slice."""

from __future__ import annotations

import json
from pathlib import Path

SAMPLE_DATA_DIR = Path(__file__).resolve().parents[1] / "sample_data"


def build_batches() -> dict[str, list[dict[str, str | int | float]]]:
    return {
        "transactions_batch_001.jsonl": [
            {
                "transaction_id": "T001",
                "account_id": "A001",
                "transaction_timestamp": "2026-02-01T10:15:00Z",
                "updated_at": "2026-02-01T10:20:00Z",
                "transaction_type": "PURCHASE",
                "channel": "CARD",
                "amount": 120.50,
                "currency": "USD",
                "status": "SETTLED",
            },
            {
                "transaction_id": "T002",
                "account_id": "A002",
                "transaction_timestamp": "2026-02-01T11:00:00Z",
                "updated_at": "2026-02-01T11:05:00Z",
                "transaction_type": "PURCHASE",
                "channel": "CARD",
                "amount": 240.00,
                "currency": "BRL",
                "status": "PENDING",
            },
            {
                "transaction_id": "T003",
                "account_id": "A003",
                "transaction_timestamp": "2026-02-02T12:00:00Z",
                "updated_at": "2026-02-02T12:01:00Z",
                "transaction_type": "ATM_WITHDRAWAL",
                "channel": "ATM",
                "amount": 5000.00,
                "currency": "ARS",
                "status": "SETTLED",
            },
            {
                "transaction_id": "T004",
                "account_id": "A999",
                "transaction_timestamp": "2026-02-02T14:00:00Z",
                "updated_at": "2026-02-02T14:01:00Z",
                "transaction_type": "PURCHASE",
                "channel": "CARD",
                "amount": 50.00,
                "currency": "USD",
                "status": "SETTLED",
            },
        ],
        "transactions_batch_002.jsonl": [
            {
                "transaction_id": "T002",
                "account_id": "A002",
                "transaction_timestamp": "2026-02-01T11:00:00Z",
                "updated_at": "2026-02-02T09:00:00Z",
                "transaction_type": "PURCHASE",
                "channel": "CARD",
                "amount": 240.00,
                "currency": "BRL",
                "status": "SETTLED",
            },
            {
                "transaction_id": "T005",
                "account_id": "A001",
                "transaction_timestamp": "2026-01-31T23:55:00Z",
                "updated_at": "2026-02-03T08:00:00Z",
                "transaction_type": "PURCHASE",
                "channel": "APP",
                "amount": 89.30,
                "currency": "EUR",
                "status": "SETTLED",
            },
            {
                "transaction_id": "T006",
                "account_id": "A003",
                "transaction_timestamp": "2027-01-01T00:00:00Z",
                "updated_at": "2026-02-03T08:10:00Z",
                "transaction_type": "PURCHASE",
                "channel": "CARD",
                "amount": 42.00,
                "currency": "ARS",
                "status": "SETTLED",
            },
            {
                "transaction_id": "T007",
                "account_id": "A001",
                "transaction_timestamp": "2026-02-03T13:10:00Z",
                "updated_at": "2026-02-03T13:20:00Z",
                "transaction_type": "PURCHASE",
                "channel": "CARD",
                "amount": -10.00,
                "currency": "USD",
                "status": "FAILED",
            },
        ],
    }


def write_batches(output_dir: Path | None = None) -> None:
    target_dir = output_dir or (SAMPLE_DATA_DIR / "transactions")
    target_dir.mkdir(parents=True, exist_ok=True)
    for file_name, records in build_batches().items():
        file_path = target_dir / file_name
        with file_path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record) + "\n")


def main() -> None:
    write_batches()
    print(f"Wrote transaction batches to {SAMPLE_DATA_DIR / 'transactions'}")


if __name__ == "__main__":
    main()
