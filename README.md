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
- Gold outputs: `gold_fact_transactions`, `gold_daily_customer_spend`, `gold_monthly_revenue`, `gold_suspicious_activity_signals`

The generated sample data includes realistic behaviors for incremental processing and validation: updated customer and account records, duplicate transaction IDs with later status changes, a late-arriving transaction, an invalid account reference, a future-dated transaction, and FX-driven USD normalization.

Local test coverage now includes transaction-flow assertions that verify the sample batches split into accepted and rejected transaction outputs with the expected latest-record behavior, rejection reasons, FX normalization, and international transaction flags.
It also includes incremental-selection tests that protect against skipping late-arriving transaction IDs simply because their `updated_at` is older than the current global maximum in the target table.
Transaction acceptance and rejection rules are now centralized in two places by execution layer: Python tests and local helpers use `src.validations.transactions.classify_transaction_rejection`, while `dbt` models use the shared macro in `dbt/macros/transaction_rules.sql`.
Suspicious-activity coverage now verifies sample-data signals for large settled purchases, international settled purchases, and mixed-channel settled purchase activity within 24 hours for the same customer.
The orchestration notebooks after Bronze are now executable Python wrappers that build or run the intended `dbt` commands instead of existing only as comments.

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
- Risk mart: `gold_suspicious_activity_signals`

## Core Business Questions
- How much transaction volume did the fintech process per day?
- Which customers are most active by value and transaction count?
- What share of settled transactions are international?
- How do failed transaction rates and fee revenue change over time?
- Which settled purchases should be flagged for simple suspicious-activity review?

## Incremental Processing Strategy
Bronze is append-only and loads only new source batches. Silver uses Delta `MERGE INTO` patterns keyed by `customer_id`, `account_id`, `transaction_id`, and `(rate_date, base_currency, quote_currency)` to insert new records and update changed ones. For transactions, incremental selection should compare candidate rows against the current target by `transaction_id` plus `(record_updated_at, ingested_at)` ordering rather than relying on a single global watermark, so new late-arriving keys are not skipped. Gold currently rebuilds table outputs from the conformed Silver layer.

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
1. Run setup to create the catalog and Bronze/Silver/Gold schemas
2. Ingest bronze raw data and log row counts
3. Build silver reference entities such as customers, accounts, and FX
4. Build silver transactional tables and rejected-row outputs
5. Run `dbt` Silver and Gold models on Databricks SQL or a job cluster
6. Run validation and audit checks

The repository notebooks `02` through `06` now expose executable `main()` functions that either print the exact `dbt` command they will run or execute it when called with `execute=True`. These wrappers are intended for Databricks job or notebook environments where `dbt` is available directly on the execution image. Local development should continue to use `uv run dbt ...`.

The repo also includes Databricks job runner notebooks for each stage:
- [00_setup_catalogs_runner.py](/home/roberto/databricks-fintech-lakehouse/notebooks/00_setup_catalogs_runner.py)
- [01_bronze_ingestion_runner.py](/home/roberto/databricks-fintech-lakehouse/notebooks/01_bronze_ingestion_runner.py)
- [02_silver_entities_runner.py](/home/roberto/databricks-fintech-lakehouse/notebooks/02_silver_entities_runner.py)
- [03_silver_transactions_runner.py](/home/roberto/databricks-fintech-lakehouse/notebooks/03_silver_transactions_runner.py)
- [04_gold_fact_runner.py](/home/roberto/databricks-fintech-lakehouse/notebooks/04_gold_fact_runner.py)
- [05_gold_marts_runner.py](/home/roberto/databricks-fintech-lakehouse/notebooks/05_gold_marts_runner.py)
- [06_data_quality_checks_runner.py](/home/roberto/databricks-fintech-lakehouse/notebooks/06_data_quality_checks_runner.py)

Use [databricks_workflow_job.json](/home/roberto/databricks-fintech-lakehouse/docs/databricks_workflow_job.json) as a Databricks Jobs API or UI template. Before creating the job, replace:
- `{{job.parameters.repo_workspace_path}}` with your actual Databricks Repos path if your import flow does not support job parameters directly
- `<existing-cluster-id-or-replace-with-job-cluster>` with your compute choice

The job template expects:
- the repo to be synced into Databricks Repos
- `dbt` to be available on the execution environment for stages `02` through `06`
- `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, and `DATABRICKS_TOKEN` to be available for the `dbt` steps

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

`dbt` connection settings should come from environment variables and a local, untracked copy of `dbt/profiles.yml.example` saved as `dbt/profiles.yml`. For local runs, prefer `uv run dbt ...`; the orchestration notebook wrappers intentionally emit bare `dbt ...` commands for Databricks execution contexts.

## Databricks Bootstrap
Before running Bronze ingestion in Databricks, create the catalog and schemas. The repository now includes [00_setup_catalogs.py](/home/roberto/databricks-fintech-lakehouse/notebooks/00_setup_catalogs.py), which creates:
- `main.bronze`
- `main.silver`
- `main.gold`

Typical usage in Databricks:
```python
main(spark)
```

Then run [01_bronze_ingestion.py](/home/roberto/databricks-fintech-lakehouse/notebooks/01_bronze_ingestion.py) with a valid `source_root` that points to your synced repo in Databricks Repos.

If you want a one-off executable wrapper in Databricks without modifying the source module, use [00_setup_catalogs_runner.py](/home/roberto/databricks-fintech-lakehouse/notebooks/00_setup_catalogs_runner.py). It loads [00_setup_catalogs.py](/home/roberto/databricks-fintech-lakehouse/notebooks/00_setup_catalogs.py) and calls `main(spark)` explicitly.

## Current Modeling Notes
- `silver_transactions` deduplicates by `transaction_id` and keeps the latest `updated_at` plus `ingested_at`
- `silver_transactions` joins accounts for `customer_id` and FX rates for `amount_usd`
- `silver_transactions_rejected` captures invalid account references, invalid statuses, future timestamps, non-positive purchase amounts, and missing FX rates
- `gold_fact_transactions` derives a simple placeholder revenue rule of `1%` of USD amount for settled purchase transactions
- `gold_suspicious_activity_signals` currently flags large settled purchases, international settled purchases, and mixed-channel settled purchase activity within a 24-hour window

## Future Improvements
- Add transfers, fees, cards, and merchants end to end
- Include late-arriving data, status changes, and schema evolution scenarios
- Publish dashboard screenshots from Databricks SQL
- Add clearer deployment instructions for Databricks Workflows or Asset Bundles
- Add suspicious activity marts and dimension tables
