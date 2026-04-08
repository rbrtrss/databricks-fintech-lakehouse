# Repository Guidelines

## Project Structure & Module Organization
This repository is organized as a Databricks fintech lakehouse project with shared Python code and a dedicated `dbt` project. Keep assets in their intended layers:

- `data_generator/` creates input datasets such as customers, accounts, transactions, and FX rates.
- `notebooks/` contains the medallion pipeline from bronze ingestion through gold marts and data quality checks.
- `src/` holds reusable Python code in `config/`, `schemas/`, `transformations/`, `validations/`, and `utils/`.
- `dbt/` contains SQL models, tests, macros, and Databricks connection configuration examples.
- `sql/` stores gold-layer analytical queries such as `gold_monthly_revenue.sql`.
- `tests/` contains unit tests for transformations and validations.
- `sample_data/` stores generated example inputs by entity domain.
- `docs/` contains architecture and workflow diagrams.

Prefer business logic in `src/`, use notebooks for orchestration and Databricks execution, and keep SQL transformations that belong in the warehouse model inside `dbt/`.

## Build, Test, and Development Commands
Use `uv` for installing libraries and running local commands.

- `uv sync`: install project dependencies from `pyproject.toml`.
- `uv add <package>`: add a new library.
- `uv run python data_generator/generate_transactions.py`: generate sample source data locally.
- `uv run pytest tests/`: run the automated test suite.
- `uv run python -m py_compile src tests`: perform a quick syntax validation pass.
- `uv run dbt parse --project-dir dbt`: validate the `dbt` project structure locally.

If Databricks Asset Bundles or CLI deployment commands are added, document them here and keep local validation runnable with `uv`.

## Coding Style & Naming Conventions
Use 4-space indentation and standard Python `snake_case` for modules, functions, and variables. Keep schema, transformation, and validation code small and composable. Name notebooks with ordered numeric prefixes such as `01_bronze_ingestion`, keep SQL files descriptive, and organize `dbt` models by layer (`bronze`, `silver`, `gold`, `marts`).

## Testing Guidelines
Write tests in `tests/` using `test_<feature>.py` names. Cover transformation correctness, schema enforcement, and validation rules. Favor unit tests against reusable `src/` code instead of notebook-only logic. Add test data that maps clearly to medallion stages and expected outputs. For `dbt`, prefer model tests for uniqueness, null checks, relationships, and accepted values.

## Commit & Pull Request Guidelines
Use imperative commit messages like `Add silver transaction validation`. Keep each commit scoped to one logical change. Pull requests should include a short summary, affected layers or domains, test evidence, and screenshots only when notebooks or diagrams change.

## Configuration & Security
Do not commit secrets, Databricks tokens, workspace URLs, or environment-specific catalog settings. Keep configuration centralized under `src/config/`, use an example profile such as `dbt/profiles.yml.example` for `dbt`, and document required environment variables in `README.md`. Avoid checking in large generated datasets unless they are intentional fixtures under `sample_data/`.

## Documentation Maintenance
After any codebase change, review `README.md` and update it when the repository structure, setup steps, workflow, architecture, commands, outputs, or project scope have changed. Keep the README aligned with the actual implementation rather than leaving documentation updates for a later cleanup pass.
