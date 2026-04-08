# Fintech Lakehouse on Databricks: Simulated OLTP to Analytics Pipeline

Built a Databricks-based lakehouse that ingests raw fintech transaction data, applies incremental and quality-controlled transformations with Delta Lake, and serves analytics-ready marts for finance, customer, and risk analysis. The repository is structured so local Python development, Databricks execution, and `dbt` analytics modeling can live side by side.

The first implemented slice covers `customers`, `accounts`, `transactions`, and `fx_rates` from generated JSONL source batches through Bronze ingestion, Silver conformance, and Gold reporting outputs.

## Business Context
This project models the analytics platform for a digital wallet / neobank. The operational system produces OLTP-style data for customers, accounts, cards, merchants, transactions, transfers, fees, and FX rates. The goal is to land raw source data in Databricks, preserve history, clean and standardize it, and publish business-ready outputs for downstream reporting and decision-making.

## What This Project Demonstrates
- Databricks lakehouse design with Bronze, Silver, and Gold layers
- Delta Lake tables with append-only raw ingestion and `MERGE`-based upserts
- Incremental processing patterns for mutable entities and transactional facts
- PySpark transformations, data quality validation, and rejected-row handling
- `dbt` model organization and SQL testing on Databricks
- Databricks SQL marts for KPI reporting and suspicious activity monitoring
- Workflow-style orchestration across ingestion, transformation, mart build, and audit steps

## Architecture Overview
Source data is simulated locally as JSONL batches representing application extracts and daily FX feeds. Bronze preserves raw history with ingestion metadata and raw payloads for traceability and replay. Silver enforces schema, deduplicates records, standardizes types and codes, applies business rules, and enriches transactions with FX to compute USD-normalized amounts. Gold publishes fact tables and analytics marts for business use cases.

Suggested diagrams:
- `docs/architecture.png`
- `docs/medallion_model.png`
- `docs/workflow.png`

## Intended Repository Structure
```text
fintech-lakehouse-databricks/
├── README.md
├── AGENTS.md
├── docs/
├── data_generator/
├── notebooks/
├── src/
├── dbt/
├── sql/
├── tests/
└── sample_data/
```

Key folders:
- `data_generator/`: simulated source data creation
- `notebooks/`: Databricks notebook flow from bronze ingestion to quality checks
- `src/`: reusable code for schemas, transformations, validations, config, and utilities
- `dbt/`: SQL models, tests, and Databricks profile scaffolding for Silver and Gold layers
- `sql/`: gold-layer reporting queries
- `sample_data/`: generated source files by domain

## First Implemented Slice
The repository now includes an end-to-end slice for:
- `customers`
- `accounts`
- `transactions`
- `fx_rates`

Implemented outputs:
- Bronze tables: `bronze_customers_raw`, `bronze_accounts_raw`, `bronze_transactions_raw`, `bronze_fx_rates_raw`
- Silver tables: `silver_customers`, `silver_accounts`, `silver_transactions`, `silver_fx_rates`, `silver_transactions_rejected`
- Gold outputs: `gold_fact_transactions`, `gold_daily_customer_spend`, `gold_monthly_revenue`

The generated sample data includes realistic behaviors for incremental processing and validation: updated customer and account records, duplicate transaction IDs with later status changes, a late-arriving transaction, an invalid account reference, a future-dated transaction, and FX-driven USD normalization.

## Development Model
Use this repository as the source of truth and treat Databricks as the execution platform.

- Build reusable Python logic locally in `src/`
- Keep notebooks in `notebooks/` thin and orchestration-focused
- Use `dbt/` for SQL-first Silver and Gold modeling plus warehouse-side tests
- Run local checks with `uv`
- Sync the repository into Databricks Repos or a deployment workflow when ready to execute on cluster

This split keeps transformation logic testable in Git while still showing Databricks-native execution patterns.

## Medallion Design
### Bronze
Append raw batches into tables such as `bronze_customers_raw`, `bronze_accounts_raw`, and `bronze_transactions_raw`. Keep parsed columns alongside metadata like `batch_id`, `source_file`, `ingested_at`, `record_loaded_date`, and a raw JSON payload.

### Silver
Build conformed entities such as `silver_customers`, `silver_accounts`, `silver_transactions`, and `silver_fx_rates`. Typical steps include schema enforcement, deduplication by business key, type casting, null handling, referential checks, and FX enrichment to compute normalized USD amounts. Invalid transaction rows are isolated into `silver_transactions_rejected`.

### Gold
Expose the first analytical outputs:
- Fact: `gold_fact_transactions`
- Marts: `gold_daily_customer_spend`, `gold_monthly_revenue`

## Core Business Questions
- How much transaction volume did the fintech process per day?
- Which customers are most active by value and transaction count?
- What share of settled transactions are international?
- How do failed transaction rates and fee revenue change over time?

## Incremental Processing Strategy
Bronze is append-only and loads only new source batches. Silver uses Delta `MERGE INTO` patterns keyed by `customer_id`, `account_id`, `transaction_id`, and `(rate_date, base_currency, quote_currency)` to insert new records and update changed ones. Gold currently rebuilds table outputs from the conformed Silver layer.

## Data Quality Rules
Representative checks include:
- unique `customer_id`, `account_id`, and `transaction_id`
- valid ISO currency codes and non-negative FX rates
- `created_at <= updated_at` for customers
- valid account references on transactions
- `amount > 0` for purchase events
- transaction timestamps not in the future
- allowed statuses such as `PENDING`, `SETTLED`, `FAILED`, and `REVERSED`

Recommended audit outputs:
- `silver_transactions_rejected`
- `gold_data_quality_results`

## Workflow Design
A typical Databricks Workflow can be organized into six tasks:
1. Ingest bronze raw data and log row counts
2. Build silver reference entities such as customers, accounts, and FX
3. Build silver transactional tables and rejected-row outputs
4. Run `dbt` Silver and Gold models on Databricks SQL or a job cluster
5. Refresh gold marts for reporting
6. Run validation and audit checks

## Example Local Development Commands
Use `uv` for local dependency management and execution.

```bash
uv sync
uv run python data_generator/generate_customers.py
uv run python data_generator/generate_accounts.py
uv run python data_generator/generate_transactions.py
uv run python data_generator/generate_fx_rates.py
uv run pytest tests/
uv run dbt parse --project-dir dbt
```

`dbt` connection settings should come from environment variables and a local copy of `dbt/profiles.yml.example`.

## Current Modeling Notes
- `silver_transactions` deduplicates by `transaction_id` and keeps the latest `updated_at` plus `ingested_at`
- `silver_transactions` joins accounts for `customer_id` and FX rates for `amount_usd`
- `silver_transactions_rejected` captures invalid account references, invalid statuses, future timestamps, non-positive purchase amounts, and missing FX rates
- `gold_fact_transactions` derives a simple placeholder revenue rule of `1%` of USD amount for settled purchase transactions

## Future Improvements
- Add transfers, fees, cards, and merchants end to end
- Include late-arriving data, status changes, and schema evolution scenarios
- Publish dashboard screenshots from Databricks SQL
- Add clearer deployment instructions for Databricks Workflows or Asset Bundles
- Add suspicious activity marts and dimension tables
