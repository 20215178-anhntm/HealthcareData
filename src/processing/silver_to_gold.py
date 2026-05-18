from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, current_date, lit, when, round as F_round
from ..common import SparkFactory
from ..common.config import SETTINGS
from ..common.logging import setup_logger

log = setup_logger("silver_to_gold")


class SilverToGold:
    def __init__(self):
        try:
            self.spark = SparkFactory.get("silver_to_gold")
        except Exception as e:
            log.exception(f"init failed: {e}")
            raise

    def run(self):
        try:
            silver_root = f"s3a://{SETTINGS.bucket_silver}/data/"

            try:
                test_df = self.spark.read.format("delta").load(silver_root)
                if test_df.count() == 0:
                    log.warning("Silver layer is empty, skipping silver_to_gold")
                    return
            except Exception as e:
                log.warning(f"Silver layer has no data: {e}")
                log.info("Run bronze_to_silver first (mode=silver) to get data in silver layer")
                return

            df = self.spark.read.format("delta").load(silver_root).where("dataset='healthcare_unified'")
            log.info(f"silver count={df.count()} cols={len(df.columns)}")

            self._write_gold(self._build_cost_by_gender(df), "kpi_cost_by_gender")
            self._write_gold(self._build_cost_by_condition(df), "kpi_cost_by_condition")
            self._write_gold(self._build_test_results_by_condition(df), "kpi_test_results_by_condition")
            self._write_gold(self._build_diabetes_by_age_group(df), "kpi_diabetes_by_age_group")

        except Exception as e:
            log.exception(f"silver_to_gold failed: {e}")
            raise

    def _build_cost_by_gender(self, df: DataFrame) -> DataFrame:
        return (df.groupBy("gender")
                  .agg(F_round(avg(col("avg_insurance_charge")), 2).alias("avg_insurance_charges"),
                       F_round(avg(col("billing_amount")), 2).alias("avg_treatment_cost"))
                  .withColumn("load_date", current_date()))

    def _build_cost_by_condition(self, df: DataFrame) -> DataFrame:
        return (df.groupBy("medical_condition")
                  .agg(F_round(avg(col("billing_amount")), 2).alias("avg_treatment_cost"),
                       count("*").alias("patient_count"))
                  .withColumn("load_date", current_date()))

    def _build_test_results_by_condition(self, df: DataFrame) -> DataFrame:
        return (df.groupBy("medical_condition", "test_results")
                  .agg(count("*").alias("count"))
                  .withColumn("load_date", current_date()))

    def _build_diabetes_by_age_group(self, df: DataFrame) -> DataFrame:
        age_group = (when(col("age") < 30, "13-29")
                     .when(col("age") < 45, "30-44")
                     .when(col("age") < 60, "45-59")
                     .when(col("age") < 75, "60-74")
                     .otherwise("75-89"))

        return (df.withColumn("age_group", age_group)
                  .groupBy("age_group")
                  .agg(F_round(avg(col("diabetes_positive_rate")), 4).alias("diabetes_positive_rate"),
                       F_round(avg(col("avg_glucose")), 2).alias("avg_glucose"),
                       count("*").alias("patient_count"))
                  .withColumn("load_date", current_date()))

    def _write_gold(self, kpi: DataFrame, name: str):
        path = f"s3a://{SETTINGS.bucket_gold}/{name}/"
        (kpi.repartition(1)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("load_date")
            .save(path))
        log.info(f"gold written -> {path}")
