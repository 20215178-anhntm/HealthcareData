"""
Demo luồng Medallion đúng chuẩn:

  BRONZE  — nhận TOÀN BỘ data kể cả xấu (faithful copy)
  SILVER  — DQ chạy tại đây: rows xấu → quarantine, rows sạch → Silver

Chạy:
    python -m src.scripts.inject_bad_data
"""

from __future__ import annotations
from datetime import datetime
from pyspark.sql import functions as F
from src.storage.s3_client import S3Client
from src.common.config import SETTINGS
from src.ingestion.csv_ingestor import CsvIngestor
from src.processing.bronze_to_silver import BronzeToSilver

DEMO_TAG = datetime.now().strftime("%Y%m%d_%H%M%S")
CSV_KEY  = f"demo_patients_bad_{DEMO_TAG}.csv"

BAD_CSV = (
    "Name,Age,Gender,Blood Type,Medical Condition,Date of Admission,"
    "Doctor,Hospital,Insurance Provider,Billing Amount,Room Number,"
    "Admission Type,Discharge Date,Medication,Test Results\n"
    # Schema enforcement: room_number="INVALID_ROOM" → null, nhưng vẫn vào Bronze
    "Alice TypeEnforce,45,Female,A+,Hypertension,2024-01-15,"
    "Dr. Smith,City Hospital,Aetna,5000.50,INVALID_ROOM,"
    "Elective,2024-01-20,Aspirin,Normal\n"
    # DQ: age=-999 → invalid_age
    "Bob NegativeAge,-999,Male,B+,Diabetes,2024-01-10,"
    "Dr. Jones,Metro Hospital,Blue Cross,5000.00,303,"
    "Emergency,2024-01-15,Metformin,Normal\n"
    # DQ: name rỗng → null_name
    ",35,Female,O-,Asthma,2024-01-05,"
    "Dr. Brown,General Hospital,Cigna,3000.00,404,"
    "Urgent,2024-01-10,Albuterol,Normal\n"
    # DQ: billing âm → invalid_billing
    "Dave NegBilling,30,Male,AB+,Cancer,2024-02-01,"
    "Dr. White,Specialist Clinic,United,-100.00,505,"
    "Elective,2024-02-10,Chemotherapy,Normal\n"
)

SEP = "=" * 65

def section(title: str) -> None:
    print(f"\n{SEP}\n  {title}\n{SEP}")


def main() -> None:
    section("DEMO  —  Medallion Architecture: Bronze → Silver + DQ")

    # ── 1. Upload CSV xấu ─────────────────────────────────────────
    section("STEP 1  Upload CSV (4 rows xấu lẫn tốt) lên MinIO")
    s3 = S3Client()
    s3.put_bytes(SETTINGS.bucket_raw, CSV_KEY, BAD_CSV.encode("utf-8"), "text/csv")
    print(f"\n  ✓ s3://{SETTINGS.bucket_raw}/{CSV_KEY}")
    print("\n  Nội dung CSV:")
    for line in BAD_CSV.strip().splitlines():
        print(f"    {line}")

    # ── 2. Bronze ingestion — nhận TOÀN BỘ, không filter ─────────
    section("STEP 2  Bronze ingestion  (schema + metadata, không DQ)")
    print("\n  Khởi động Spark...\n")
    ingestor = CsvIngestor()
    ingestor.ingest_patients()
    spark = ingestor.spark
    spark.sparkContext.setLogLevel("ERROR")

    bronze_df   = spark.read.format("delta").load(f"s3a://{SETTINGS.bucket_bronze}/patients/")
    demo_bronze = bronze_df.filter(F.col("source_file").contains(f"demo_patients_bad_{DEMO_TAG}"))
    n_bronze    = demo_bronze.count()

    print(f"\n  Rows từ file demo vào Bronze: {n_bronze}  (mong đợi: 4 — tất cả)")
    print()
    demo_bronze.select("name", "age", "room_number", "billing_amount").show(truncate=False)
    print("  ✓ Bronze giữ nguyên toàn bộ — kể cả rows xấu")
    print("  ✓ Alice: room_number = null  (schema enforcement: 'INVALID_ROOM' → IntegerType)")
    print("  ✓ Bob, Dave: giá trị sai nghiệp vụ nhưng VẪN vào Bronze (chưa DQ)")

    # ── 3. Bronze → Silver: DQ chạy tại đây ──────────────────────
    section("STEP 3  Bronze → Silver  (DQ chạy tại đây)")
    print("\n  Chạy BronzeToSilver...\n")
    BronzeToSilver().run()

    # ── 4. Quarantine ─────────────────────────────────────────────
    section("STEP 4  Quarantine  —  Rows bị DQ reject tại Silver stage")
    try:
        q_df   = spark.read.format("delta").load(f"s3a://{SETTINGS.bucket_bronze}/quarantine/patients/")
        demo_q = q_df.filter(F.col("source_file").contains(f"demo_patients_bad_{DEMO_TAG}"))
        n_q    = demo_q.count()
        print(f"\n  Rows bị quarantine: {n_q}  (mong đợi: 3 — Bob, row-không-tên, Dave)")
        print()
        demo_q.select("name", "age", "billing_amount", "dq_failed_rule").show(truncate=False)
    except Exception as e:
        print(f"\n  Không đọc được quarantine: {e}")

    # ── 5. Silver ─────────────────────────────────────────────────
    section("STEP 5  Silver  —  Chỉ rows sạch được promote")
    try:
        silver_df   = spark.read.format("delta").load(f"s3a://{SETTINGS.bucket_silver}/data/")
        demo_silver = silver_df.filter(F.col("name").isin("Alice TypeEnforce"))
        n_silver    = demo_silver.count()
        print(f"\n  Demo rows trong Silver: {n_silver}  (mong đợi: 1 — chỉ Alice)")
        print()
        demo_silver.select("name", "age", "room_number", "billing_amount").show(truncate=False)
    except Exception as e:
        print(f"\n  Không đọc được Silver: {e}")

    # ── 6. Tổng kết ───────────────────────────────────────────────
    section("TỔNG KẾT  —  Medallion đúng chuẩn")
    print(f"""
  ┌────────────────────┬──────────┬─────────────────────────────────────┐
  │ Row                │ Bronze   │ Silver / Quarantine                 │
  ├────────────────────┼──────────┼─────────────────────────────────────┤
  │ Alice TypeEnforce  │ ✓ (4/4)  │ Silver ✓  (room_number=null)       │
  │ Bob NegativeAge    │ ✓ (4/4)  │ Quarantine  (invalid_age)          │
  │ (không tên)        │ ✓ (4/4)  │ Quarantine  (null_name)            │
  │ Dave NegBilling    │ ✓ (4/4)  │ Quarantine  (invalid_billing)      │
  └────────────────────┴──────────┴─────────────────────────────────────┘

  Bronze  : giữ 100% data thô — có thể replay bất kỳ lúc nào
  Silver  : chỉ data sạch — DQ gate giữa Bronze và Silver
  Quarantine: rows xấu không mất — audit được, re-ingest được
""")
    spark.stop()


if __name__ == "__main__":
    main()
