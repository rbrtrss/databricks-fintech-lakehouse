"""Generate customer JSONL batches for the first lakehouse slice."""

from __future__ import annotations

import json
from pathlib import Path

SAMPLE_DATA_DIR = Path(__file__).resolve().parents[1] / "sample_data"


def build_batches() -> dict[str, list[dict[str, str]]]:
    return {
        "customers_batch_001.jsonl": [
            {
                "customer_id": "C001",
                "full_name": "Ava Carter",
                "email": "ava.carter@example.com",
                "country_code": "US",
                "created_at": "2026-01-05T10:00:00Z",
                "updated_at": "2026-01-05T10:00:00Z",
                "customer_status": "ACTIVE",
            },
            {
                "customer_id": "C002",
                "full_name": "Mateo Silva",
                "email": "mateo.silva@example.com",
                "country_code": "BR",
                "created_at": "2026-01-11T13:30:00Z",
                "updated_at": "2026-01-11T13:30:00Z",
                "customer_status": "ACTIVE",
            },
        ],
        "customers_batch_002.jsonl": [
            {
                "customer_id": "C002",
                "full_name": "Mateo Silva",
                "email": "mateo.silva+vip@example.com",
                "country_code": "BR",
                "created_at": "2026-01-11T13:30:00Z",
                "updated_at": "2026-02-02T08:15:00Z",
                "customer_status": "ACTIVE",
            },
            {
                "customer_id": "C003",
                "full_name": "Lucia Romero",
                "email": "lucia.romero@example.com",
                "country_code": "AR",
                "created_at": "2026-02-01T09:00:00Z",
                "updated_at": "2026-02-01T09:00:00Z",
                "customer_status": "DORMANT",
            },
        ],
    }


def write_batches(output_dir: Path | None = None) -> None:
    target_dir = output_dir or (SAMPLE_DATA_DIR / "customers")
    target_dir.mkdir(parents=True, exist_ok=True)
    for file_name, records in build_batches().items():
        file_path = target_dir / file_name
        with file_path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record) + "\n")


def main() -> None:
    write_batches()
    print(f"Wrote customer batches to {SAMPLE_DATA_DIR / 'customers'}")


if __name__ == "__main__":
    main()
