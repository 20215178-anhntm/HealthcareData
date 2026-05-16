import re
from typing import List, Iterable, Union
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, current_date, input_file_name, lit
from ..common import SparkFactory
from ..common.config import SETTINGS
from ..common.logging import setup_logger
from ..storage.s3_client import S3Client
from ..validation.schema import HealthcareSchemas

log = setup_logger("csv_ingestor")

class CsvIngestor:
    def __init__(self, since: Union[str, None] = None):
        try:
            self.spark = SparkFactory.get("csv_ingestor")
            self.s3 = S3Client()
            self.bucket = SETTINGS.bucket_raw
            self.manifest_key = "_manifests/ingested.csv"
            self.since_dt = datetime.strptime(since, "%Y-%m-%d").date() if since else None
            log.info(f"CsvIngestor init: bucket={self.bucket}, since={self.since_dt}")
            
            # Ensure necessary buckets exist
            for bucket_name in [SETTINGS.bucket_raw, SETTINGS.bucket_bronze, 
                               SETTINGS.bucket_silver, SETTINGS.bucket_gold]:
                self.s3.ensure_bucket(bucket_name)
        except Exception as e:
            log.exception(f"CsvIngestor init failed: {e}")
            raise

    def _load_manifest(self) -> set:
        """Load manifest file. Returns empty set if not exists (first run)."""
        try:
            # Check if manifest exists first to avoid noisy error logs
            if not self.s3.exists(self.bucket, self.manifest_key):
                log.info("No manifest found -> starting fresh (first run or after clean)")
                return set()
            
            b = self.s3.get_bytes(self.bucket, self.manifest_key).decode("utf-8")
            st = set([x.strip() for x in b.splitlines() if x.strip()])
            log.info(f"Manifest loaded: {len(st)} processed files")
            return st
        except Exception as e:
            log.warning(f"Failed to load manifest: {e}. Starting fresh.")
            return set()

    def _save_manifest(self, processed: Iterable[str]) -> None:
        try:
            content = "\n".join(sorted(set(processed))) + "\n"
            self.s3.put_bytes(self.bucket, self.manifest_key, content.encode("utf-8"), "text/plain")
            log.info(f"manifest saved: {len(set(processed))} keys")
        except Exception as e:
            log.exception(f"save manifest failed: {e}")
            # don't raise to avoid blocking ETL, but log error

    def _discover(self, prefix: str, patterns: List[str]) -> List[str]:
        try:
            objs = self.s3.list_objects_with_meta(self.bucket, prefix=prefix)
            paths = []
            for o in objs:
                key = o["key"]
                if not key.lower().endswith(".csv"):
                    continue
                if patterns and not any(p in key.lower() for p in patterns):
                    continue
                if self.since_dt and o["last_modified"].date() < self.since_dt:
                    continue
                paths.append(f"s3a://{self.bucket}/{key}")
            log.info(f"discover {patterns}: found={len(paths)}")
            return paths
        except Exception as e:
            log.exception(f"discover failed (prefix={prefix}, patterns={patterns}): {e}")
            raise

    def _normalize_column_names(self, df: DataFrame) -> DataFrame:
        """Lowercase + replace spaces/special chars with underscore.
        Delta Lake rejects column names containing spaces or ' ,;{}()\n\t='."""
        for old_name in df.columns:
            new_name = re.sub(r'[ ,;{}()\n\t=]+', '_', old_name).strip('_').lower()
            if old_name != new_name:
                df = df.withColumnRenamed(old_name, new_name)
        return df

    def _read_many_csv(self, paths: List[str], schema=None) -> DataFrame:
        try:
            if not paths:
                raise ValueError("no input CSV paths")
            reader = (self.spark.read
                      .option("header", True)
                      .option("multiLine", True)
                      .option("escape", '"'))
            if schema: reader = reader.schema(schema)
            df = reader.csv(paths).withColumn("_source_file", input_file_name())
            df = self._normalize_column_names(df)
            log.info(f"read_many_csv: files={len(paths)} rows={df.count()}")
            return df
        except Exception as e:
            log.exception(f"read_many_csv failed: {e}")
            raise

    def _write_delta(self, df: DataFrame, dst: str, partitions: int = 1):
        """Write DataFrame as Delta table. Each dataset has its own path."""
        try:
            (df.repartition(partitions)
               .write.format("delta")
               .mode("append")
               .option("mergeSchema", "true")
               .partitionBy("load_date")
               .save(dst))
            log.info(f"write delta ok -> {dst}")
        except Exception as e:
            log.exception(f"write delta failed (dst={dst}): {e}")
            raise

    def ingest_patients(self):
        try:
            processed = self._load_manifest()
            candidates = self._discover(prefix="", patterns=["patients", "patient"])
            todo = [p for p in candidates if p.replace(f"s3a://{self.bucket}/","") not in processed]
            if not todo:
                log.info("patients: nothing new"); return
            df = (self._read_many_csv(todo, HealthcareSchemas.patients_schema())
                    .dropDuplicates().na.drop("all")
                    .withColumn("name", trim(col("name")))
                    .withColumn("channel", lit("hospital"))
                    .withColumn("load_date", current_date()))
            out = f"s3a://{SETTINGS.bucket_bronze}/patients/"
            log.info(f"patients -> {out}")
            self._write_delta(df, out, partitions=1)
            processed.update([p.replace(f"s3a://{self.bucket}/","") for p in todo])
            self._save_manifest(processed)
        except Exception as e:
            log.exception(f"ingest_patients failed: {e}")
            raise

    def ingest_insurance(self):
        try:
            processed = self._load_manifest()
            candidates = self._discover(prefix="", patterns=["insurance"])
            todo = [p for p in candidates if p.replace(f"s3a://{self.bucket}/","") not in processed]
            if not todo:
                log.info("insurance: nothing new"); return
            df = (self._read_many_csv(todo, HealthcareSchemas.insurance_schema())
                    .dropDuplicates()
                    .withColumn("channel", lit("insurance"))
                    .withColumn("load_date", current_date()))
            out = f"s3a://{SETTINGS.bucket_bronze}/insurance/"
            log.info(f"insurance -> {out}")
            self._write_delta(df, out, partitions=1)
            processed.update([p.replace(f"s3a://{self.bucket}/","") for p in todo])
            self._save_manifest(processed)
        except Exception as e:
            log.exception(f"ingest_insurance failed: {e}")
            raise

    def ingest_diabetes(self):
        try:
            processed = self._load_manifest()
            candidates = self._discover(prefix="", patterns=["diabetes"])
            todo = [p for p in candidates if p.replace(f"s3a://{self.bucket}/","") not in processed]
            if not todo:
                log.info("diabetes: nothing new"); return
            df = (self._read_many_csv(todo, HealthcareSchemas.diabetes_schema())
                    .dropDuplicates()
                    .withColumn("channel", lit("lab_results"))
                    .withColumn("load_date", current_date()))
            out = f"s3a://{SETTINGS.bucket_bronze}/diabetes/"
            log.info(f"diabetes -> {out}")
            self._write_delta(df, out, partitions=1)
            processed.update([p.replace(f"s3a://{self.bucket}/","") for p in todo])
            self._save_manifest(processed)
        except Exception as e:
            log.exception(f"ingest_diabetes failed: {e}")
            raise

    def ingest_noshows(self):
        try:
            processed = self._load_manifest()
            candidates = self._discover(prefix="", patterns=["noshows"])
            todo = [p for p in candidates if p.replace(f"s3a://{self.bucket}/","") not in processed]
            if not todo:
                log.info("noshows: nothing new"); return
            df = (self._read_many_csv(todo, HealthcareSchemas.noshows_schema())
                    .dropDuplicates()
                    .withColumn("channel", lit("appointment"))
                    .withColumn("load_date", current_date()))
            out = f"s3a://{SETTINGS.bucket_bronze}/noshows/"
            log.info(f"noshows -> {out}")
            self._write_delta(df, out, partitions=1)
            processed.update([p.replace(f"s3a://{self.bucket}/","") for p in todo])
            self._save_manifest(processed)
        except Exception as e:
            log.exception(f"ingest_noshows failed: {e}")
            raise