"""Databricks job runner for Silver transaction models."""

from __future__ import annotations

from _job_runner_utils import get_widget_value, load_notebook_module


def main() -> str:
    silver_module = load_notebook_module(__file__, "03_silver_transactions.py", "silver_transactions")
    project_dir = get_widget_value("project_dir", "dbt")
    profiles_dir = get_widget_value("profiles_dir", "dbt")
    output = silver_module.main(
        execute=True,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )
    print(output)
    return output


if __name__ == "__main__":
    main()
