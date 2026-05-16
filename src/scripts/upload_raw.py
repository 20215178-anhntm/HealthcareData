"""
Upload CSV files từ thư mục dataset/ lên MinIO bucket raw-healthcare.
Chạy một lần trước khi chạy batch pipeline.

    python -m src.scripts.upload_raw
"""
import os
from pathlib import Path
from src.storage.s3_client import S3Client
from src.common.config import SETTINGS
from src.common.logging import setup_logger

log = setup_logger("upload_raw")

DATASET_DIR = Path(__file__).parent.parent.parent / "dataset"

def main():
    s3 = S3Client()
    bucket = SETTINGS.bucket_raw

    s3.ensure_bucket(bucket)
    log.info(f"Uploading CSVs to bucket: {bucket}")

    csv_files = list(DATASET_DIR.rglob("*.csv"))
    if not csv_files:
        log.error(f"Không tìm thấy file CSV nào trong {DATASET_DIR}")
        return

    for csv_path in csv_files:
        key = csv_path.name  # ví dụ: healthcare_patients.csv
        log.info(f"Uploading {csv_path.name} ...")
        s3.upload_file(bucket, key, str(csv_path))
        log.info(f"  ✓ {key} uploaded")

    log.info(f"\nHoàn tất: {len(csv_files)} files uploaded to s3://{bucket}/")
    log.info("Bây giờ chạy pipeline: python -m src.jobs.batch_etl --mode all --since 2025-10-26")

if __name__ == "__main__":
    main()
