"""
Tạo và upload CSV xấu lên MinIO raw bucket để demo.
Không cần Spark — chỉ dùng boto3.

Chạy TRƯỚC khi chạy pipeline:
    python -m src.scripts.create_demo_data
"""
from datetime import datetime
from src.storage.s3_client import S3Client
from src.common.config import SETTINGS

DEMO_TAG = datetime.now().strftime("%Y%m%d_%H%M%S")
CSV_KEY  = f"demo_patients_bad_{DEMO_TAG}.csv"

BAD_CSV = (
    "Name,Age,Gender,Blood Type,Medical Condition,Date of Admission,"
    "Doctor,Hospital,Insurance Provider,Billing Amount,Room Number,"
    "Admission Type,Discharge Date,Medication,Test Results\n"
    # SCHEMA: room_number="INVALID_ROOM" → Spark ép thành null
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

def main():
    s3 = S3Client()
    s3.put_bytes(SETTINGS.bucket_raw, CSV_KEY, BAD_CSV.encode("utf-8"), "text/csv")

    print(f"\n✓ Uploaded: s3://{SETTINGS.bucket_raw}/{CSV_KEY}")
    print("\nNội dung CSV:")
    for line in BAD_CSV.strip().splitlines():
        print(f"  {line}")

    print(f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Bước tiếp theo — chạy từng lệnh và quan sát kết quả:

  BƯỚC 2  Bronze ingestion (nhận HẾT, không DQ):
    python -m src.jobs.batch_etl --mode bronze

  BƯỚC 3  Kiểm tra Bronze (phải thấy 4 rows — kể cả xấu):
    python src/tests/read_data_simple.py bronze

  BƯỚC 4  Silver processing (DQ chạy TẠI ĐÂY):
    python -m src.jobs.batch_etl --mode silver

  BƯỚC 5  Kiểm tra Quarantine (phải thấy 3 rows bị reject):
    python src/tests/read_data_simple.py quarantine
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")

if __name__ == "__main__":
    main()
