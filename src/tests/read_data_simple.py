#!/usr/bin/env python3
"""
Usage:
    python src/tests/read_data_simple.py bronze
    python src/tests/read_data_simple.py silver
    python src/tests/read_data_simple.py gold
    python src/tests/read_data_simple.py quarantine
"""
import sys
from pathlib import Path

root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(root))

from src.common import SparkFactory
from src.common.config import SETTINGS


BRONZE_DATASETS = ["patients", "insurance", "diabetes", "noshows"]
GOLD_KPIS = ["kpi_cost_by_gender", "kpi_cost_by_condition",
             "kpi_test_results_by_condition", "kpi_diabetes_by_age_group"]


def read_bronze(spark):
    for ds in BRONZE_DATASETS:
        path = f"s3a://{SETTINGS.bucket_bronze}/{ds}/"
        try:
            df = spark.read.format("delta").load(path)
            print(f"\n[Bronze/{ds}] {df.count():,} rows | {len(df.columns)} cols")
            df.show(5, truncate=True)
        except Exception as e:
            print(f"[Bronze/{ds}] ERROR: {e}")


def read_layer(spark, path, label):
    try:
        df = spark.read.format("delta").load(path)
        print(f"\n[{label}] {df.count():,} rows | {len(df.columns)} cols")
        df.printSchema()
        df.show(10, truncate=True)
    except Exception as e:
        print(f"[{label}] ERROR: {e}")


def main():
    layer = sys.argv[1] if len(sys.argv) > 1 else "silver"
    spark = SparkFactory.get("read_data_simple")

    try:
        if layer == "bronze":
            read_bronze(spark)
        elif layer == "silver":
            read_layer(spark, f"s3a://{SETTINGS.bucket_silver}/data/", "Silver")
        elif layer == "gold":
            for kpi in GOLD_KPIS:
                read_layer(spark, f"s3a://{SETTINGS.bucket_gold}/{kpi}/", f"Gold/{kpi}")
        elif layer == "quarantine":
            for ds in BRONZE_DATASETS:
                path = f"s3a://{SETTINGS.bucket_bronze}/quarantine/{ds}/"
                read_layer(spark, path, f"Quarantine/{ds}")
        else:
            print(f"Unknown layer: {layer}. Use bronze / silver / gold / quarantine")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
