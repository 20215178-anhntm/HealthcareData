from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    # Spark
    spark_master: str = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    shuffle_partitions: int = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))

    # MinIO / S3
    s3_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    s3_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minio")
    s3_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minio123")
    bucket_raw: str = os.getenv("S3_BUCKET_RAW", "raw")
    bucket_bronze: str = os.getenv("S3_BUCKET_BRONZE", "bronze")
    bucket_silver: str = os.getenv("S3_BUCKET_SILVER", "silver")
    bucket_gold: str = os.getenv("S3_BUCKET_GOLD", "gold")

    # Kafka
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    topic_orders: str = os.getenv("KAFKA_TOPIC_ORDERS", "orders")

    # Mongo
    mongo_uri: str = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
    mongo_db: str = os.getenv("MONGO_DB", "datalake")
    mongo_coll_rt: str = os.getenv("MONGO_COLL_RT", "orders_rt")

SETTINGS = Settings()
