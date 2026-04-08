"""Databricks notebook code to bootstrap catalog and schema structure."""

from __future__ import annotations

from collections.abc import Iterable


def ensure_catalog_and_schemas(
    spark,
    catalog: str = "main",
    schemas: Iterable[str] = ("bronze", "silver", "gold"),
) -> None:
    spark.sql(f"create catalog if not exists {catalog}")
    for schema_name in schemas:
        spark.sql(f"create schema if not exists {catalog}.{schema_name}")


def main(spark) -> None:
    ensure_catalog_and_schemas(spark)
    print("Ensured catalog main and schemas bronze, silver, gold exist.")
