"""Generate account JSONL batches for the first lakehouse slice."""

from __future__ import annotations

import json
from pathlib import Path

SAMPLE_DATA_DIR = Path(__file__).resolve().parents[1] / "sample_data"


def build_batches() -> dict[str, list[dict[str, str]]]:
    return {
        "accounts_batch_001.jsonl": [
            {
                "account_id": "A001",
                "customer_id": "C001",
                "account_type": "CHECKING",
                "account_currency": "USD",
                "opened_at": "2026-01-05T10:05:00Z",
                "updated_at": "2026-01-05T10:05:00Z",
                "account_status": "ACTIVE",
            },
            {
                "account_id": "A002",
                "customer_id": "C002",
                "account_type": "WALLET",
                "account_currency": "BRL",
                "opened_at": "2026-01-11T13:45:00Z",
                "updated_at": "2026-01-11T13:45:00Z",
                "account_status": "ACTIVE",
            },
        ],
        "accounts_batch_002.jsonl": [
            {
                "account_id": "A002",
                "customer_id": "C002",
                "account_type": "WALLET",
                "account_currency": "BRL",
                "opened_at": "2026-01-11T13:45:00Z",
                "updated_at": "2026-02-03T08:00:00Z",
                "account_status": "FROZEN",
            },
            {
                "account_id": "A003",
                "customer_id": "C003",
                "account_type": "SAVINGS",
                "account_currency": "ARS",
                "opened_at": "2026-02-01T09:10:00Z",
                "updated_at": "2026-02-01T09:10:00Z",
                "account_status": "ACTIVE",
            },
        ],
    }


def write_batches(output_dir: Path | None = None) -> None:
    target_dir = output_dir or (SAMPLE_DATA_DIR / "accounts")
    target_dir.mkdir(parents=True, exist_ok=True)
    for file_name, records in build_batches().items():
        file_path = target_dir / file_name
        with file_path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record) + "\n")


def main() -> None:
    write_batches()
    print(f"Wrote account batches to {SAMPLE_DATA_DIR / 'accounts'}")


if __name__ == "__main__":
    main()
