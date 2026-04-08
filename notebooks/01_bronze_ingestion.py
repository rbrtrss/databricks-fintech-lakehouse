"""Databricks notebook code for Bronze ingestion of the first slice."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.schemas.transactions import (
    ACCOUNT_COLUMNS,
    CUSTOMER_COLUMNS,
    FX_RATE_COLUMNS,
    TRANSACTION_COLUMNS,
)


@dataclass(frozen=True)
class BronzeDomain:
    input_glob: str
    table_name: str
    business_columns: list[str]


BRONZE_DOMAINS = {
    "customers": BronzeDomain(
        input_glob="sample_data/customers/*.jsonl",
        table_name="main.bronze.bronze_customers_raw",
        business_columns=CUSTOMER_COLUMNS,
    ),
    "accounts": BronzeDomain(
        input_glob="sample_data/accounts/*.jsonl",
        table_name="main.bronze.bronze_accounts_raw",
        business_columns=ACCOUNT_COLUMNS,
    ),
    "transactions": BronzeDomain(
        input_glob="sample_data/transactions/*.jsonl",
        table_name="main.bronze.bronze_transactions_raw",
        business_columns=TRANSACTION_COLUMNS,
    ),
    "fx_rates": BronzeDomain(
        input_glob="sample_data/fx_rates/*.jsonl",
        table_name="main.bronze.bronze_fx_rates_raw",
        business_columns=FX_RATE_COLUMNS,
    ),
}


def ingest_bronze_domain(
    spark: SparkSession,
    source_root: str,
    batch_id: str,
    domain: BronzeDomain,
) -> DataFrame:
    raw_df = spark.read.json(f"{source_root.rstrip('/')}/{domain.input_glob}")
    payload_columns = [F.col(column_name) for column_name in domain.business_columns]
    bronze_df = raw_df.select(*domain.business_columns).withColumn(
        "batch_id",
        F.lit(batch_id),
    ).withColumn(
        "source_file",
        F.input_file_name(),
    ).withColumn(
        "ingested_at",
        F.current_timestamp(),
    ).withColumn(
        "record_loaded_date",
        F.current_date(),
    ).withColumn(
        "raw_payload",
        F.to_json(F.struct(*payload_columns)),
    )
    bronze_df.write.format("delta").mode("append").saveAsTable(domain.table_name)
    return bronze_df


def run_bronze_ingestion(
    spark: SparkSession,
    source_root: str = "/Workspace/Repos/fintech-lakehouse-databricks",
    batch_id: str = "local_demo_batch",
) -> dict[str, int]:
    row_counts: dict[str, int] = {}
    for domain_name, domain in BRONZE_DOMAINS.items():
        bronze_df = ingest_bronze_domain(spark, source_root, batch_id, domain)
        row_counts[domain_name] = bronze_df.count()
    return row_counts
