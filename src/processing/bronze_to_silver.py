from pyspark.sql.functions import col, trim, lower, regexp_replace, current_date, lit, avg, count, when
from ..common import SparkFactory
from ..common.config import SETTINGS
from ..common.logging import setup_logger
from ..validation.dq_checker import DQChecker

log = setup_logger("bronze_to_silver")

class BronzeToSilver:
    def __init__(self):
        try:
            self.spark = SparkFactory.get("bronze_to_silver")
            self.dq = DQChecker(self.spark, f"s3a://{SETTINGS.bucket_bronze}/quarantine")
        except Exception as e:
            log.exception(f"Spark init failed: {e}")
            raise

    def _safe_rename(self, df, old_name: str, new_name: str):
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
        return df

    def _safe_add_name_norm(self, df):
        if "name" in df.columns:
            df = df.withColumn(
                "name_norm",
                lower(regexp_replace(col("name"), r"[^a-z0-9 ]", "")),
            )
        return df

    def _safe_read(self, path: str, dataset: str):
        try:
            df = self.spark.read.format("delta").load(path)
            count = df.count()
            log.info(f"{dataset} loaded: {count} rows")
            return df
        except Exception as e:
            log.warning(f"Dataset '{dataset}' missing or unreadable at {path}: {e}")
            return None

    def _drop_metadata(self, df):
        for c in ("dataset", "load_date", "_source_file", "source_file", "channel"):
            if c in df.columns:
                df = df.drop(c)
        return df

    def run(self):
        try:
            bronze_root = f"s3a://{SETTINGS.bucket_bronze}"

            p   = self._safe_read(f"{bronze_root}/patients/",  "patients")
            ins = self._safe_read(f"{bronze_root}/insurance/", "insurance")
            dia = self._safe_read(f"{bronze_root}/diabetes/",  "diabetes")
            nos = self._safe_read(f"{bronze_root}/noshows/",   "noshows")

            if p is None:
                log.error("No primary dataset (patients) found → cannot build Silver.")
                return

            # ── DQ checks (Bronze → Silver gate) ─────────────────────────────
            # Bronze giữ toàn bộ data thô. DQ chạy tại đây trước khi promote
            # lên Silver — rows vi phạm vào quarantine, không vào unified profile.
            p = self.dq.check(p, "patients", [
                ("null_age",        col("age").isNull()),
                ("invalid_age",     col("age").isNotNull() & ((col("age") < 0) | (col("age") > 115))),
                ("null_name",       col("name").isNull() | (trim(col("name")) == "")),
                ("invalid_billing", col("billing_amount").isNotNull() & (col("billing_amount") <= 0)),
                ("invalid_dates",   col("discharge_date").isNotNull() & col("date_of_admission").isNotNull()
                                    & (col("discharge_date") < col("date_of_admission"))),
            ])
            if ins is not None:
                ins = self.dq.check(ins, "insurance", [
                    ("null_age",        col("age").isNull()),
                    ("invalid_age",     col("age").isNotNull() & ((col("age") < 0) | (col("age") > 115))),
                    ("invalid_bmi",     col("bmi").isNotNull() & (col("bmi") <= 0)),
                    ("invalid_charges", col("charges").isNotNull() & (col("charges") <= 0)),
                ])
            if dia is not None:
                dia = self.dq.check(dia, "diabetes", [
                    ("null_age",     col("age").isNull()),
                    ("invalid_age",  col("age").isNotNull() & ((col("age") < 0) | (col("age") > 115))),
                    ("zero_glucose", col("glucose").isNotNull() & (col("glucose") == 0)),
                    ("zero_bmi",     col("bmi").isNotNull() & (col("bmi") == 0)),
                ])
            if nos is not None:
                nos = self.dq.check(nos, "noshows", [
                    ("null_patientid", col("patientid").isNull()),
                    ("null_age",       col("age").isNull()),
                    ("negative_age",   col("age").isNotNull() & (col("age") < 0)),
                ])

            # ── Chuẩn bị patients (primary / "hospital channel") ──────────────
            p = self._safe_add_name_norm(p)
            p = self._drop_metadata(p)
            if "bmi" in p.columns:
                p = p.withColumnRenamed("bmi", "bmi_patients")
            # Chuẩn hóa gender về lowercase để join khớp với insurance/noshows
            if "gender" in p.columns:
                p = p.withColumn("gender", lower(col("gender")))

            # ── Insurance channel: aggregate theo age+gender trước khi join ───
            # Tránh cartesian explosion: 55K patients × 1.3K insurance = hàng triệu rows
            # Thay vào đó: mỗi bệnh nhân nhận thông tin TRUNG BÌNH của nhóm tuổi/giới
            if ins is not None:
                ins = self._safe_rename(ins, "sex", "gender")
                ins = self._drop_metadata(ins)
                # insurance dùng lowercase (male/female) — đảm bảo nhất quán
                if "gender" in ins.columns:
                    ins = ins.withColumn("gender", lower(col("gender")))
                ins_agg = ins.groupBy("age", "gender").agg(
                    avg("charges").alias("avg_insurance_charge"),
                    avg("bmi").alias("avg_bmi_insurance"),
                    count("*").alias("insurance_channel_size"),
                )
                log.info(f"insurance aggregated: {ins_agg.count()} age/gender groups")
                p = p.join(ins_agg, ["age", "gender"], "left")

            # ── Lab Results channel: aggregate theo age ───────────────────────
            if dia is not None:
                dia = self._drop_metadata(dia)
                dia_agg = dia.groupBy("age").agg(
                    avg("glucose").alias("avg_glucose"),
                    avg("bmi").alias("avg_bmi_lab"),
                    avg("insulin").alias("avg_insulin"),
                    avg(when(col("outcome") == 1, 1.0).otherwise(0.0))
                        .alias("diabetes_positive_rate"),
                    count("*").alias("lab_channel_size"),
                )
                log.info(f"diabetes aggregated: {dia_agg.count()} age groups")
                p = p.join(dia_agg, ["age"], "left")

            # ── Appointment channel: aggregate theo age+gender ────────────────
            if nos is not None:
                nos = self._drop_metadata(nos)
                if "gender" in nos.columns:
                    # noshows dùng "F"/"M", chuẩn hóa về "female"/"male" để join khớp với patients
                    nos = nos.withColumn("gender",
                        when(lower(col("gender")) == lit("f"), lit("female"))
                        .when(lower(col("gender")) == lit("m"), lit("male"))
                        .otherwise(lower(col("gender")))
                    )
                nos_agg = nos.groupBy("age", "gender").agg(
                    avg(when(col("showed_up") == True, 1.0).otherwise(0.0))
                        .alias("appointment_show_rate"),
                    count("*").alias("appointment_channel_size"),
                )
                log.info(f"noshows aggregated: {nos_agg.count()} age/gender groups")
                p = p.join(nos_agg, ["age", "gender"], "left")

            # ── Gắn metadata Silver ───────────────────────────────────────────
            unified = (p
                .withColumn("dataset", lit("healthcare_unified"))
                .withColumn("load_date", current_date()))

            row_count = unified.count()
            log.info(f"Silver unified profile: {row_count} rows (1 row per patient)")

            silver_root = f"s3a://{SETTINGS.bucket_silver}/data/"
            (unified.repartition(1)
                    .write.format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .partitionBy("dataset", "load_date")
                    .save(silver_root))
            log.info(f"Silver written successfully → {silver_root}")

        except Exception as e:
            log.exception(f"bronze_to_silver failed: {e}")
            raise
