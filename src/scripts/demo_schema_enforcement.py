"""
Demo: Schema Enforcement vs Data Quality (DQ) Business Rules
============================================================
Standalone script — không cần MinIO hay Delta.
Dùng local Spark + temp CSV để minh họa 2 cơ chế:

  SCHEMA ENFORCEMENT  →  Spark ép kiểu khi đọc CSV (sai type → NULL)
  DQ BUSINESS RULES   →  Lọc giá trị không hợp lệ về mặt nghiệp vụ

Chạy:
    python -m src.scripts.demo_schema_enforcement
"""

from __future__ import annotations
import os, sys, tempfile, textwrap

# ── Spark (local mode, không cần S3/Delta jars) ──────────────────
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType,
)
from pyspark.sql.functions import col, lit, trim, when

def _make_spark() -> SparkSession:
    spark = (SparkSession.builder
             .appName("demo_schema_enforcement")
             .master("local[2]")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# ── Schema (trích từ HealthcareSchemas.patients_schema) ──────────
PATIENTS_SCHEMA = StructType([
    StructField("Name",              StringType(),  True),
    StructField("Age",               IntegerType(), True),   # ← int
    StructField("Gender",            StringType(),  True),
    StructField("Blood Type",        StringType(),  True),
    StructField("Medical Condition", StringType(),  True),
    StructField("Date of Admission", DateType(),    True),
    StructField("Doctor",            StringType(),  True),
    StructField("Hospital",          StringType(),  True),
    StructField("Insurance Provider",StringType(),  True),
    StructField("Billing Amount",    DoubleType(),  True),   # ← double
    StructField("Room Number",       IntegerType(), True),   # ← int
    StructField("Admission Type",    StringType(),  True),
    StructField("Discharge Date",    DateType(),    True),
    StructField("Medication",        StringType(),  True),
    StructField("Test Results",      StringType(),  True),
])

# ── Test data ────────────────────────────────────────────────────
#
#  Row 1  Alice TypeEnforce  → Room Number = "INVALID_ROOM"
#           Schema: IntegerType không cast được → room_number = NULL
#           Không có DQ rule cho room_number → Alice VÀO Bronze
#
#  Row 2  Bob NegativeAge   → Age = -999
#           Kiểu int đúng, nhưng vi phạm rule: 0 ≤ age ≤ 115
#           → QUARANTINE  (rule: invalid_age)
#
#  Row 3  Carol EmptyName   → Name = ""  (rỗng)
#           Vi phạm rule: name không được rỗng/null
#           → QUARANTINE  (rule: null_name)
#
#  Row 4  Dave NegBilling   → Billing Amount = -100.0
#           Vi phạm rule: billing_amount > 0
#           → QUARANTINE  (rule: invalid_billing)
#
TEST_CSV = textwrap.dedent("""\
    Name,Age,Gender,Blood Type,Medical Condition,Date of Admission,Doctor,Hospital,Insurance Provider,Billing Amount,Room Number,Admission Type,Discharge Date,Medication,Test Results
    Alice TypeEnforce,45,Female,A+,Hypertension,2024-01-15,Dr. Smith,City Hospital,Aetna,5000.50,INVALID_ROOM,Elective,2024-01-20,Aspirin,Normal
    Bob NegativeAge,-999,Male,B+,Diabetes,2024-01-10,Dr. Jones,Metro Hospital,Blue Cross,5000.00,303,Emergency,2024-01-15,Metformin,Normal
    Carol EmptyName,,Female,O-,Asthma,2024-01-05,Dr. Brown,General Hospital,Cigna,3000.00,404,Urgent,2024-01-10,Albuterol,Normal
    Dave NegBilling,30,Male,AB+,Cancer,2024-02-01,Dr. White,Specialist Clinic,United,-100.00,505,Elective,2024-02-10,Chemotherapy,Normal
""")

SEP  = "=" * 68
SEP2 = "-" * 68

def banner(title: str) -> None:
    print(f"\n{SEP}\n  {title}\n{SEP}")

def note(text: str) -> None:
    print(f"\n  ► {text}")


# ════════════════════════════════════════════════════════════════
def main() -> None:

    banner("DEMO: Schema Enforcement  vs  DQ Business Rules")

    # ── 0. Giải thích khái niệm ───────────────────────────────
    print("""
  Hai cơ chế BẢO VỆ dữ liệu khác nhau trong pipeline:

  ┌──────────────────────┬───────────────────────────────────────┐
  │ Schema Enforcement   │ Spark ép kiểu khi ĐỌC CSV             │
  │                      │ Sai type → NULL  (row không bị mất)   │
  │                      │ Xảy ra tại: _read_many_csv()          │
  ├──────────────────────┼───────────────────────────────────────┤
  │ DQ Business Rules    │ Lọc giá trị sai LOGIC NGHIỆP VỤ      │
  │                      │ Vi phạm rule → Quarantine             │
  │                      │ Xảy ra tại: dq_checker.check()        │
  └──────────────────────┴───────────────────────────────────────┘
""")

    # ── 1. Hiển thị input CSV ─────────────────────────────────
    banner("INPUT  —  Test CSV (4 rows)")
    print()
    for line in TEST_CSV.strip().splitlines():
        print(f"  {line}")
    print()
    print(f"  {SEP2}")
    print("  Row 1  Alice TypeEnforce  →  Room Number = 'INVALID_ROOM'  (khai báo: IntegerType)")
    print("  Row 2  Bob NegativeAge    →  Age = -999                    (khai báo: IntegerType)")
    print("  Row 3  Carol EmptyName    →  Name = ''                     (rỗng)")
    print("  Row 4  Dave NegBilling    →  Billing Amount = -100.0       (khai báo: DoubleType)")

    # ── 2. Khởi động Spark ────────────────────────────────────
    banner("STEP 1  —  Schema Enforcement  (Spark đọc CSV với StructType)")
    print("\n  Đang khởi động Spark local...\n")
    spark = _make_spark()

    # Ghi CSV ra temp file rồi đọc với schema
    tmp_dir = tempfile.mkdtemp(prefix="demo_schema_")
    tmp_csv = os.path.join(tmp_dir, "test_patients.csv")
    with open(tmp_csv, "w", encoding="utf-8") as f:
        f.write(TEST_CSV)

    # Đây là cách csv_ingestor.py đọc file: .schema(PATIENTS_SCHEMA)
    # mode mặc định = PERMISSIVE: sai type → null, không crash
    df_raw = (spark.read
              .option("header", True)
              .option("mode", "PERMISSIVE")   # default mode
              .schema(PATIENTS_SCHEMA)
              .csv(tmp_csv))

    # Normalize tên cột (giống _normalize_column_names)
    import re
    df = df_raw
    for old in df.columns:
        new = re.sub(r'[ ,;{}()\n\t=]+', '_', old).strip('_').lower()
        if old != new:
            df = df.withColumnRenamed(old, new)

    note("DataFrame sau khi Spark đọc với schema (TRƯỚC DQ check):")
    print()
    df.select("name", "age", "room_number", "billing_amount").show(truncate=False)

    print("  Quan sát:")
    print("  • Alice: room_number = null")
    print("    'INVALID_ROOM' không cast được sang IntegerType")
    print("    → Spark ép thành null  (PERMISSIVE mode — row không bị mất)")
    print()
    print("  • Bob, Carol, Dave: tất cả các cột đúng kiểu dữ liệu")
    print("    (-999 là int hợp lệ, '' là string hợp lệ, -100.0 là double hợp lệ)")
    print("    → Schema Enforcement KHÔNG bắt được các vi phạm này")

    # ── 3. DQ check ───────────────────────────────────────────
    banner("STEP 2  —  DQ Business Rules  (dq_checker.check)")

    # DQ rules định nghĩa SAU khi Spark đã khởi động
    dq_rules = [
        ("null_age",        col("age").isNull()),
        ("invalid_age",     col("age").isNotNull() & ((col("age") < 0) | (col("age") > 115))),
        ("null_name",       col("name").isNull() | (trim(col("name")) == "")),
        ("invalid_billing", col("billing_amount").isNotNull() & (col("billing_amount") <= 0)),
        ("invalid_dates",   col("discharge_date").isNotNull() & col("date_of_admission").isNotNull()
                            & (col("discharge_date") < col("date_of_admission"))),
    ]

    note("Rules áp dụng (từ csv_ingestor.py:129-136):")
    print()
    print("  null_age        →  age IS NULL")
    print("  invalid_age     →  age < 0 OR age > 115")
    print("  null_name       →  name IS NULL OR trim(name) = ''")
    print("  invalid_billing →  billing_amount <= 0")
    print("  invalid_dates   →  discharge_date < date_of_admission")

    # Áp DQ rules (single-pass, lấy rule đầu tiên vi phạm)
    reason_expr = lit(None).cast("string")
    for rule_name, condition in reversed(dq_rules):
        reason_expr = when(condition, lit(rule_name)).otherwise(reason_expr)

    tagged = df.withColumn("_dq_reason", reason_expr)
    good   = tagged.filter(col("_dq_reason").isNull()).drop("_dq_reason")
    bad    = tagged.filter(col("_dq_reason").isNotNull()).withColumnRenamed("_dq_reason", "dq_failed_rule")

    total      = df.count()
    n_good     = good.count()
    n_bad      = bad.count()

    print(f"\n  Tổng cộng: {total} rows  →  {n_good} passed DQ  |  {n_bad} quarantined")

    # ── 3a. Bronze (good rows) ────────────────────────────────
    note(f"[BRONZE]  Rows đi vào Bronze  ({n_good} row):")
    print()
    good.select("name", "age", "room_number", "billing_amount").show(truncate=False)
    print("  → Alice vào Bronze với room_number = null")
    print("    (Schema enforcement đã xử lý, DQ không có rule nào reject Alice)")

    # ── 3b. Quarantine (bad rows) ─────────────────────────────
    note(f"[QUARANTINE]  Rows bị reject  ({n_bad} rows):")
    print()
    bad.select("name", "age", "billing_amount", "dq_failed_rule").show(truncate=False)
    print("  → Bob: age = -999 → invalid_age")
    print("  → Carol: name rỗng → null_name")
    print("    (Spark đọc '' thành null sau khi trim → vi phạm null_name)")
    print("  → Dave: billing = -100.0 → invalid_billing")

    # ── 4. Tổng kết ───────────────────────────────────────────
    banner("TỔNG KẾT")
    print("""
  ┌──────────────────────┬─────────────┬─────────────────────────────────────┐
  │ Row                  │ Đi đâu      │ Vì sao                              │
  ├──────────────────────┼─────────────┼─────────────────────────────────────┤
  │ Alice TypeEnforce    │ Bronze ✓    │ Schema: 'INVALID_ROOM' → null       │
  │                      │             │ DQ: không có rule nào check         │
  │                      │             │ room_number → Alice PASS            │
  ├──────────────────────┼─────────────┼─────────────────────────────────────┤
  │ Bob NegativeAge      │ Quarantine  │ DQ rule: invalid_age                │
  │                      │             │ -999 đúng kiểu int, sai nghiệp vụ   │
  ├──────────────────────┼─────────────┼─────────────────────────────────────┤
  │ Carol EmptyName      │ Quarantine  │ DQ rule: null_name                  │
  │                      │             │ '' là string hợp lệ, sai nghiệp vụ  │
  ├──────────────────────┼─────────────┼─────────────────────────────────────┤
  │ Dave NegBilling      │ Quarantine  │ DQ rule: invalid_billing            │
  │                      │             │ -100.0 đúng kiểu double, sai logic   │
  └──────────────────────┴─────────────┴─────────────────────────────────────┘

  Kết luận:
  • Schema Enforcement bắt LỖI KIỂU DỮ LIỆU  → ép về null, row không mất
  • DQ Business Rules bắt LỖI NGHIỆP VỤ      → tách sang quarantine, row không mất
  • Hai tầng bổ sung cho nhau, đều KHÔNG xóa dữ liệu
""")

    spark.stop()


if __name__ == "__main__":
    main()
