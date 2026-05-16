import os
from dotenv import load_dotenv
# mypy: ignore-errors
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col 

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY     = os.getenv("MINIO_ACCESS_KEY") 
MINIO_SECRET_KEY     = os.getenv("MINIO_SECRET_KEY")
S3_BUCKET_RAW         = os.getenv("S3_BUCKET_RAW")   # dùng bucket nào cũng được
SPARK_MASTER_URL   = os.getenv("SPARK_MASTER_URL")


def build_spark(app_name="spark_minio_connectivity_check") -> SparkSession:
    """
    Tạo SparkSession đã cấu hình S3A cho MinIO.
    Nếu cluster của bạn chưa có hadoop-aws, hãy thêm --packages khi spark-submit (xem phần chạy lệnh).
    """
    spark = (SparkSession.builder
             .appName(app_name)
             .master(SPARK_MASTER_URL)
             .config("spark.sql.shuffle.partitions", "2")
             # S3A (MinIO)
             .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
             .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
             .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
             # timeouts hữu ích khi debug mạng
             .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
             .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
             .getOrCreate())
    return spark

def main():
    spark = build_spark()
    
    # --- 1) đọc tất cả CSV trong bucket ---
    path_all = f"s3a://{S3_BUCKET_RAW}/healthcare_diabetes.csv"
    df_all = (spark.read
            .option("header", True)
            .option("inferSchema", True)     # hoặc tự khai báo schema nếu muốn chuẩn
            .option("multiLine", True)       # nếu có dòng xuống hàng trong field
            .option("escape", '"')           # xử lý dấu phẩy trong chuỗi
            .csv(path_all))

    print("Số dòng:", df_all.count())
    print("Số cột:", len(df_all.columns))
    df_all.printSchema()
    df_all.show(10, truncate=False)

    # --- 2) nếu muốn đọc từng file cụ thể ---
    # df_patients = spark.read.option("header", True).csv(f"s3a://{S3_BUCKET_RAW}/healthcare_dataset.csv")
    # df_diabetes = spark.read.option("header", True).csv(f"s3a://{S3_BUCKET_RAW}/healthcare_diabetes.csv")
    # df_insurance = spark.read.option("header", True).csv(f"s3a://{S3_BUCKET_RAW}/healthcare_insurance.csv")
    # df_noshows = spark.read.option("header", True).csv(f"s3a://{S3_BUCKET_RAW}/healthcare_noshows.csv")

    # ví dụ làm sạch nhẹ & chuẩn tên cột

    # df_patients = df_patients.withColumnRenamed("Treatment Cost","treatment_cost").withColumn("Age", col("Age").cast("int"))

    # --- 3) (tuỳ chọn) ghi ra lớp bronze dạng Parquet ---
    # df_patients.write.mode("overwrite").parquet("s3a://bronze/healthcare_dataset/")
    # df_diabetes.write.mode("overwrite").parquet("s3a://bronze/healthcare_diabetes/")
    # df_insurance.write.mode("overwrite").parquet("s3a://bronze/healthcare_insurance/")

if __name__ == "__main__":
    main()
