"""
Data Quality checker — chạy tự động tại Bronze ingestion stage.
Row vi phạm rule sẽ được ghi vào quarantine (Delta table) thay vì bị xóa âm thầm.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when, current_timestamp
from ..common.logging import setup_logger

log = setup_logger("dq_checker")


class DQChecker:
    def __init__(self, spark: SparkSession, quarantine_base: str):
        self.spark = spark
        self.quarantine_base = quarantine_base  # ví dụ: s3a://bronze/quarantine

    def check(self, df: DataFrame, dataset: str, rules: list) -> DataFrame:
        """
        Kiểm tra DQ rules và tách dữ liệu thành good / bad.

        rules: list of (rule_name: str, fail_condition: Column)
               fail_condition = điều kiện trả về True khi row LỖI

        Trả về DataFrame chỉ chứa rows hợp lệ.
        Rows lỗi được ghi vào quarantine/{dataset}/ kèm cột dq_failed_rule.
        """
        total = df.count()
        if total == 0:
            log.info(f"[DQ] {dataset}: empty dataset, skip")
            return df

        # Gán lý do lỗi cho mỗi row (single-pass, rule đầu tiên vi phạm được ghi)
        reason_expr = lit(None).cast("string")
        for rule_name, condition in reversed(rules):
            reason_expr = when(condition, lit(rule_name)).otherwise(reason_expr)

        tagged = df.withColumn("_dq_reason", reason_expr)
        good = tagged.filter(col("_dq_reason").isNull()).drop("_dq_reason")
        bad  = tagged.filter(col("_dq_reason").isNotNull())

        quarantined = bad.count()
        passed = total - quarantined

        if quarantined > 0:
            pct = quarantined / total * 100
            log.warning(f"[DQ] {dataset}: {total} total | {passed} passed | {quarantined} quarantined ({pct:.1f}%)")

            # Chi tiết từng rule
            breakdown = (bad.groupBy("_dq_reason")
                            .count()
                            .withColumnRenamed("_dq_reason", "rule")
                            .collect())
            for row in breakdown:
                log.warning(f"  ↳ {row['rule']}: {row['count']} rows")

            # Ghi quarantine
            quarantine_path = f"{self.quarantine_base}/{dataset}/"
            try:
                (bad.withColumnRenamed("_dq_reason", "dq_failed_rule")
                    .withColumn("dq_timestamp", current_timestamp())
                    .write.format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .save(quarantine_path))
                log.info(f"  → quarantine: {quarantine_path}")
            except Exception as e:
                log.warning(f"  → quarantine write failed (non-fatal): {e}")
        else:
            log.info(f"[DQ] {dataset}: {total} rows | all passed ✓")

        return good
