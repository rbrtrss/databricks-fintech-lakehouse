"""Databricks job runner for Bronze ingestion."""

from __future__ import annotations

from _job_runner_utils import get_widget_value, load_notebook_module


def main() -> dict[str, int]:
    bronze_module = load_notebook_module(__file__, "01_bronze_ingestion.py", "bronze_ingestion")
    source_root = get_widget_value(
        "source_root",
        "/Workspace/Repos/<user-or-service-principal>/databricks-fintech-lakehouse",
    )
    batch_id = get_widget_value("batch_id", "workflow_batch")
    row_counts = bronze_module.run_bronze_ingestion(
        spark,
        source_root=source_root,
        batch_id=batch_id,
    )
    print(row_counts)
    return row_counts


if __name__ == "__main__":
    main()
