import os
import sys
# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from dotenv import load_dotenv

# Thêm root path để Python nhận module utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.streaming.writer_db import WriterDB
from src.common.logging import setup_logger


class StreamProcessor:
    def __init__(self):
        load_dotenv()

        # Load ENV
        self.MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
        self.MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
        self.MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
        self.S3_BUCKET_CHECKPOINT = os.getenv("S3_BUCKET_CHECKPOINT")
        self.S3_PREFIX_CHECKPOINT = os.getenv("S3_PREFIX_CHECKPOINT")

        self.KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
        self.KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")

        # Logger setup
        self.logger = setup_logger("stream_processor")
        self.logger.info("Starting Spark Structured Streaming job")

        # SparkSession placeholder
        self.spark = None
        self.query = None

    def create_spark_session(self):
        self.spark = (
            SparkSession.builder
            .appName("HealthcareStreaming")
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
            # S3A configs for MinIO
            .config("spark.hadoop.fs.s3a.endpoint", self.MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", self.MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", self.MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.connection.timeout", "30000")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
            .config("spark.sql.adaptive.enabled", "false")
            # Kafka and MinIO connector packages
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")
        self.logger.info("Spark session started")

        # Verify Hadoop IPC configuration
        conf_value = self.spark.sparkContext._jsc.hadoopConfiguration().get("ipc.maximum.data.length")
        self.logger.info(f"Hadoop IPC max data length: {conf_value}")

    def read_kafka_stream(self):
        self.logger.info(f"Subscribing to Kafka topic: {self.KAFKA_TOPIC}")

        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.KAFKA_BOOTSTRAP)
            .option("subscribe", self.KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", "500")  # Tăng số lượng records mỗi batch
            .option("failOnDataLoss", "false")  # Bỏ qua nếu mất dữ liệu nhỏ
            .load()
        )

    def define_schema(self):
        return StructType([
            StructField("patient_id", IntegerType()),
            StructField("name", StringType()),
            StructField("age", IntegerType()),
            StructField("gender", StringType()),
            StructField("pregnancies", IntegerType()),
            StructField("glucose", IntegerType()),
            StructField("blood_pressure", IntegerType()),
            StructField("skin_thickness", IntegerType()),
            StructField("insulin", IntegerType()),
            StructField("bmi", DoubleType()),
            StructField("diabetes_pedigree", DoubleType()),
            StructField("outcome", IntegerType()),
            StructField("insurance_type", StringType()),
            StructField("visit_cost", DoubleType()),
            StructField("timestamp", StringType()),
        ])

    def run(self):
        try:
            # Create Spark session
            self.create_spark_session()

            # Read Kafka stream
            raw_stream = self.read_kafka_stream()

            # Parse stream
            schema = self.define_schema()
            parsed = (
                raw_stream
                .select(from_json(col("value").cast("string"), schema).alias("data"))
                .select("data.*")
                .filter(col("patient_id").isNotNull())
            )

            self.logger.info("Kafka stream schema:")
            parsed.printSchema()

            # Checkpoint (Sử dụng local thay vì MinIO để tránh lỗi Hadoop S3A NoSuchBucket trên Windows)
            checkpoint_dir = f"./checkpoints/{self.S3_PREFIX_CHECKPOINT}"
            self.logger.info(f"Using Local checkpoint: {checkpoint_dir}")

            # Start stream
            writer_db = WriterDB()
            self.query = (
                parsed.writeStream
                .outputMode("append")
                .foreachBatch(writer_db.write_to_mongodb)
                .option("checkpointLocation", checkpoint_dir)
                .trigger(processingTime="5 seconds")  # Giảm từ 20s xuống 5s để xử lý nhanh hơn
                .start()
            )

            self.logger.info("Streaming query started (Ctrl+C to stop)")
            self.query.awaitTermination()

        except Exception as e:
            self.logger.error(f"Streaming job failed: {str(e)}", exc_info=True)

        finally:
            try:
                if self.query and self.query.isActive:
                    self.query.stop()
                if self.spark:
                    self.spark.stop()
                self.logger.info("Spark job stopped cleanly.")
            except Exception as e:
                self.logger.warning(f"Error during shutdown: {e}")


if __name__ == "__main__":
    processor = StreamProcessor()
    processor.run()
