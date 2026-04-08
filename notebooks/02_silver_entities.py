"""Databricks notebook code for Silver entity orchestration.

The generated command is intended for Databricks job or notebook environments
where `dbt` is available directly on the execution image.
"""

from __future__ import annotations

from collections.abc import Callable
from subprocess import CompletedProcess

from src.utils.orchestration import build_dbt_command, format_command, run_command


def build_command(*, project_dir: str = "dbt", profiles_dir: str = "dbt") -> list[str]:
    return build_dbt_command(
        "run",
        ("silver_customers", "silver_accounts", "silver_fx_rates"),
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )


def main(
    *,
    execute: bool = False,
    project_dir: str = "dbt",
    profiles_dir: str = "dbt",
    runner: Callable[[list[str]], CompletedProcess[str]] = run_command,
) -> str:
    command = build_command(project_dir=project_dir, profiles_dir=profiles_dir)
    if execute:
        result = runner(command)
        return result.stdout
    formatted_command = format_command(command)
    print(formatted_command)
    return formatted_command
