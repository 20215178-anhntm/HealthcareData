from pyspark.sql.functions import avg, col, current_date, lit
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
            
            # Check if silver has data
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

            kpi = (df.groupBy("gender")
                    .agg(avg(col("avg_insurance_charge")).alias("avg_insurance_charges"),
                         avg(col("billing_amount")).alias("avg_treatment_cost"))
                    .withColumn("dataset", lit("kpi_cost_by_gender"))
                    .withColumn("load_date", current_date()))

            gold_root = f"s3a://{SETTINGS.bucket_gold}/data/"
            (kpi.repartition(1)
                .write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy("dataset", "load_date")
                .save(gold_root))
            log.info(f"gold written -> {gold_root}")
        except Exception as e:
            log.exception(f"silver_to_gold failed: {e}")
            raise
