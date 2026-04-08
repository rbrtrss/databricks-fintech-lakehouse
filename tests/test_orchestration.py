from __future__ import annotations

import importlib.util
from pathlib import Path
from subprocess import CompletedProcess

from src.utils.orchestration import build_dbt_command, format_command


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load_notebook_module(relative_path: str):
    module_path = PROJECT_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(relative_path.replace("/", "_"), module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_build_dbt_command_creates_runnable_cli_args() -> None:
    command = build_dbt_command(
        "run",
        ("silver_transactions", "silver_transactions_rejected"),
        project_dir="dbt",
        profiles_dir="dbt",
    )

    assert command == [
        "dbt",
        "run",
        "--project-dir",
        "dbt",
        "--profiles-dir",
        "dbt",
        "--select",
        "silver_transactions",
        "silver_transactions_rejected",
    ]


def test_build_dbt_command_uses_databricks_style_direct_dbt_invocation() -> None:
    command = build_dbt_command("run", ("silver_customers",))

    assert command[0] == "dbt"


def test_format_command_keeps_select_pattern_shell_safe() -> None:
    command = build_dbt_command("test", ("silver_*", "gold_*"))

    assert format_command(command) == "dbt test --project-dir dbt --profiles-dir dbt --select 'silver_*' 'gold_*'"


def test_silver_entities_notebook_returns_expected_command() -> None:
    notebook = _load_notebook_module("notebooks/02_silver_entities.py")

    assert notebook.main() == (
        "dbt run --project-dir dbt --profiles-dir dbt --select "
        "silver_customers silver_accounts silver_fx_rates"
    )


def test_silver_entities_notebook_docstring_marks_command_as_databricks_oriented() -> None:
    notebook = _load_notebook_module("notebooks/02_silver_entities.py")

    assert "Databricks job or notebook environments" in notebook.__doc__


def test_gold_marts_notebook_executes_through_runner() -> None:
    notebook = _load_notebook_module("notebooks/05_gold_marts.py")
    captured: list[list[str]] = []

    def runner(command: list[str]) -> CompletedProcess[str]:
        captured.append(command)
        return CompletedProcess(args=command, returncode=0, stdout="ok", stderr="")

    output = notebook.main(execute=True, runner=runner)

    assert output == "ok"
    assert captured == [[
        "dbt",
        "run",
        "--project-dir",
        "dbt",
        "--profiles-dir",
        "dbt",
        "--select",
        "gold_daily_customer_spend",
        "gold_monthly_revenue",
        "gold_suspicious_activity_signals",
    ]]


def test_data_quality_notebook_targets_silver_and_gold_tests() -> None:
    notebook = _load_notebook_module("notebooks/06_data_quality_checks.py")

    assert notebook.build_command() == [
        "dbt",
        "test",
        "--project-dir",
        "dbt",
        "--profiles-dir",
        "dbt",
        "--select",
        "silver_*",
        "gold_*",
    ]
