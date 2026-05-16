from pyspark.sql import SparkSession
from .config import SETTINGS

class SparkFactory:
    @staticmethod
    def get(app_name: str) -> SparkSession:
        spark = (SparkSession.builder
                 .appName(app_name)
                 .master(SETTINGS.spark_master)
                 .config("spark.sql.shuffle.partitions", str(SETTINGS.shuffle_partitions))
                 # Delta Lake extensions
                 .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                 .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                 # S3/MinIO
                 .config("spark.hadoop.fs.s3a.endpoint", SETTINGS.s3_endpoint)
                 .config("spark.hadoop.fs.s3a.path.style.access", "true")
                 .config("spark.hadoop.fs.s3a.access.key", SETTINGS.s3_access_key)
                 .config("spark.hadoop.fs.s3a.secret.key", SETTINGS.s3_secret_key)
                 .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                 .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                 .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
                 .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
                 # JAR packages: Delta Lake + S3A/MinIO connectors
                 # delta-spark 3.1.0 is compatible with pyspark 3.5.1
                 .config("spark.jars.packages",
                         "io.delta:delta-spark_2.12:3.1.0,"
                         "org.apache.hadoop:hadoop-aws:3.3.4,"
                         "com.amazonaws:aws-java-sdk-bundle:1.12.262")
                 # Fix Windows networking: force driver to use loopback instead of WiFi IP
                 .config("spark.driver.host", "127.0.0.1")
                 .config("spark.driver.bindAddress", "127.0.0.1")
                 .getOrCreate())
        return spark
