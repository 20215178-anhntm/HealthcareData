import duckdb
import os
from urllib.parse import urlparse

print("[INFO] Building DuckDB file from MinIO/S3 ...")

con = duckdb.connect("/data/silver.duckdb")

# Configure S3 to read from MinIO
s3_endpoint_full = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
s3_access_key = os.getenv("MINIO_ACCESS_KEY")
s3_secret_key = os.getenv("MINIO_SECRET_KEY")
s3_bucket = os.getenv("S3_BUCKET_SILVER")

# Extract host:port từ endpoint URL
# DuckDB cần format host:port, không phải full URL
parsed = urlparse(s3_endpoint_full)
s3_endpoint = f"{parsed.hostname}:{parsed.port}" if parsed.port else parsed.hostname

print(f"[INFO] Configuring S3 endpoint: {s3_endpoint}")
print(f"[INFO] S3 bucket: {s3_bucket}")

# Install and load httpfs extension
print("[INFO] Installing httpfs extension...")
try:
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    print("[INFO] httpfs extension loaded successfully")
except Exception as e:
    print(f"[ERROR] Failed to install/load httpfs: {e}")
    raise

# Configure S3/MinIO connection
print("[INFO] Configuring S3 connection settings...")
con.execute(f"""
SET s3_endpoint='{s3_endpoint}';
SET s3_access_key_id='{s3_access_key}';
SET s3_secret_access_key='{s3_secret_key}';
SET s3_use_ssl=false;
SET s3_url_style='path';
""")

# Read parquet from S3/MinIO
parquet_path = f's3://{s3_bucket}/data/dataset=healthcare_unified/**/*.parquet'
print(f"[INFO] Reading parquet files from: {parquet_path}")

try:
    con.execute(f"""
    CREATE OR REPLACE TABLE healthcare_unified AS
    SELECT * FROM read_parquet('{parquet_path}')
    """)
    
    # Check number of records
    result = con.execute("SELECT COUNT(*) FROM healthcare_unified").fetchone()
    if result:
        print(f"[INFO] Successfully loaded {result[0]} records into healthcare_unified table")
    else:
        print("[WARNING] No records found in healthcare_unified table")
    
except Exception as e:
    print(f"[ERROR] Failed to read parquet files: {e}")
    raise

print("[INFO] DuckDB build completed!")
con.close()
