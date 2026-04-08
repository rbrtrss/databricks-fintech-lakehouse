"""Helpers for notebook-style orchestration commands."""

from __future__ import annotations

import shlex
import subprocess
from collections.abc import Iterable


def build_dbt_command(
    operation: str,
    select_models: Iterable[str],
    *,
    project_dir: str = "dbt",
    profiles_dir: str = "dbt",
) -> list[str]:
    return [
        "dbt",
        operation,
        "--project-dir",
        project_dir,
        "--profiles-dir",
        profiles_dir,
        "--select",
        *select_models,
    ]


def format_command(command: Iterable[str]) -> str:
    return " ".join(shlex.quote(part) for part in command)


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, text=True, capture_output=True)
