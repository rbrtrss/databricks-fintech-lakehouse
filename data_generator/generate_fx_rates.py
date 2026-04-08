"""Generate FX rate JSONL batches for the first lakehouse slice."""

from __future__ import annotations

import json
from pathlib import Path

SAMPLE_DATA_DIR = Path(__file__).resolve().parents[1] / "sample_data"


def build_batches() -> dict[str, list[dict[str, str | float]]]:
    return {
        "fx_rates_batch_001.jsonl": [
            {
                "rate_date": "2026-01-31",
                "base_currency": "EUR",
                "quote_currency": "USD",
                "exchange_rate": 1.09,
            },
            {
                "rate_date": "2026-02-01",
                "base_currency": "BRL",
                "quote_currency": "USD",
                "exchange_rate": 0.18,
            },
            {
                "rate_date": "2026-02-01",
                "base_currency": "ARS",
                "quote_currency": "USD",
                "exchange_rate": 0.0011,
            },
        ],
        "fx_rates_batch_002.jsonl": [
            {
                "rate_date": "2026-02-02",
                "base_currency": "ARS",
                "quote_currency": "USD",
                "exchange_rate": 0.0010,
            },
            {
                "rate_date": "2026-02-03",
                "base_currency": "ARS",
                "quote_currency": "USD",
                "exchange_rate": 0.0010,
            },
            {
                "rate_date": "2026-02-03",
                "base_currency": "BRL",
                "quote_currency": "USD",
                "exchange_rate": 0.19,
            },
        ],
    }


def write_batches(output_dir: Path | None = None) -> None:
    target_dir = output_dir or (SAMPLE_DATA_DIR / "fx_rates")
    target_dir.mkdir(parents=True, exist_ok=True)
    for file_name, records in build_batches().items():
        file_path = target_dir / file_name
        with file_path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record) + "\n")


def main() -> None:
    write_batches()
    print(f"Wrote FX rate batches to {SAMPLE_DATA_DIR / 'fx_rates'}")


if __name__ == "__main__":
    main()
