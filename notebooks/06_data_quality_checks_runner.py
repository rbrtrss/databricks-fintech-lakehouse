"""Databricks job runner for dbt tests and audit checks."""

from __future__ import annotations

from _job_runner_utils import get_widget_value, load_notebook_module


def main() -> str:
    quality_module = load_notebook_module(__file__, "06_data_quality_checks.py", "data_quality_checks")
    project_dir = get_widget_value("project_dir", "dbt")
    profiles_dir = get_widget_value("profiles_dir", "dbt")
    output = quality_module.main(
        execute=True,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )
    print(output)
    return output


if __name__ == "__main__":
    main()
