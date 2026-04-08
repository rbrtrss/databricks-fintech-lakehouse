from __future__ import annotations

import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_databricks_workflow_template_has_expected_task_sequence() -> None:
    workflow_path = PROJECT_ROOT / "docs" / "databricks_workflow_job.json"
    workflow = json.loads(workflow_path.read_text(encoding="utf-8"))

    task_keys = [task["task_key"] for task in workflow["tasks"]]
    assert task_keys == [
        "setup_catalogs",
        "bronze_ingestion",
        "silver_entities",
        "silver_transactions",
        "gold_fact",
        "gold_marts",
        "data_quality_checks",
    ]


def test_databricks_workflow_template_uses_runner_notebooks() -> None:
    workflow_path = PROJECT_ROOT / "docs" / "databricks_workflow_job.json"
    workflow = json.loads(workflow_path.read_text(encoding="utf-8"))

    notebook_paths = [task["notebook_task"]["notebook_path"] for task in workflow["tasks"]]
    assert notebook_paths == [
        "{{job.parameters.repo_workspace_path}}/notebooks/00_setup_catalogs_runner",
        "{{job.parameters.repo_workspace_path}}/notebooks/01_bronze_ingestion_runner",
        "{{job.parameters.repo_workspace_path}}/notebooks/02_silver_entities_runner",
        "{{job.parameters.repo_workspace_path}}/notebooks/03_silver_transactions_runner",
        "{{job.parameters.repo_workspace_path}}/notebooks/04_gold_fact_runner",
        "{{job.parameters.repo_workspace_path}}/notebooks/05_gold_marts_runner",
        "{{job.parameters.repo_workspace_path}}/notebooks/06_data_quality_checks_runner",
    ]
