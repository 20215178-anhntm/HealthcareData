"""
Demo: Delta Lake Time Travel
============================
Use case thực tế nhất:
  1. Xem lịch sử version của Silver layer
  2. Đọc Silver tại version cụ thể (versionAsOf / timestampAsOf)
  3. [Nếu cần] Restore Silver về version trước khi ghi sai

Chạy sau khi đã có data ở Silver:
    python -m src.scripts.demo_time_travel
"""
from src.common import SparkFactory
from src.common.config import SETTINGS
from src.common.logging import setup_logger
from delta.tables import DeltaTable

log = setup_logger("demo_time_travel")

SEP  = "=" * 65
SEP2 = "-" * 65

def section(title: str) -> None:
    print(f"\n{SEP}\n  {title}\n{SEP}")


def main():
    print(f"\n{SEP}")
    print("  DEMO: Delta Lake Time Travel")
    print(f"  Mục tiêu: mỗi lần ghi Delta = 1 version → có thể quay lại bất kỳ lúc nào")
    print(SEP)

    spark = SparkFactory.get("demo_time_travel")
    spark.sparkContext.setLogLevel("ERROR")

    silver_path = f"s3a://{SETTINGS.bucket_silver}/data/"
    bronze_patients = f"s3a://{SETTINGS.bucket_bronze}/patients/"

    # ── 1. Lịch sử Silver ─────────────────────────────────────────
    section("STEP 1  Lịch sử version của Silver layer")
    try:
        silver_table = DeltaTable.forPath(spark, silver_path)
        print()
        silver_table.history().select(
            "version", "timestamp", "operation", "operationParameters"
        ).show(20, truncate=False)
        print("  → Mỗi lần chạy BronzeToSilver tạo ra 1 version mới")
        print("  → Có thể query bất kỳ version nào trong lịch sử này")
    except Exception as e:
        print(f"  Silver chưa có data: {e}")
        print("  Chạy: python -m src.jobs.batch_etl --mode silver trước")
        spark.stop()
        return

    # ── 2. Đọc Silver tại version cụ thể ─────────────────────────
    section("STEP 2  Query Silver tại version cụ thể (versionAsOf)")
    try:
        current = spark.read.format("delta").load(silver_path)
        current_count = current.count()
        print(f"\n  Version hiện tại: {current_count} rows")

        # Đọc version 0 — snapshot đầu tiên của Silver
        v0 = spark.read.format("delta").option("versionAsOf", 0).load(silver_path)
        v0_count = v0.count()
        print(f"  Version 0 (đầu tiên): {v0_count} rows")
        print(f"\n  Rows thêm vào từ v0 đến nay: {current_count - v0_count}")
        print()
        print("  Cú pháp query theo version:")
        print(f'    spark.read.format("delta").option("versionAsOf", 0).load("{silver_path}")')
        print()
        print("  Cú pháp query theo timestamp:")
        print(f'    spark.read.format("delta").option("timestampAsOf", "2025-10-26").load("{silver_path}")')
    except Exception as e:
        print(f"  Lỗi: {e}")

    # ── 3. Lịch sử Bronze/patients ────────────────────────────────
    section("STEP 3  Lịch sử Bronze/patients  (audit raw data)")
    try:
        bronze_table = DeltaTable.forPath(spark, bronze_patients)
        print()
        bronze_table.history().select(
            "version", "timestamp", "operation"
        ).show(10, truncate=False)
        print("  → Bronze giữ toàn bộ lịch sử ingestion")
        print("  → Có thể xem raw data tại thời điểm bất kỳ để audit")
    except Exception as e:
        print(f"  Lỗi: {e}")

    # ── 4. Use case: Restore ──────────────────────────────────────
    section("STEP 4  Use case: Restore Silver sau pipeline chạy sai")
    print("""
  Tình huống: Silver version 2 bị ghi sai (DQ rules lỗi, join nhầm...)
  → Không cần rerun toàn bộ pipeline, chỉ cần RESTORE:

  Cú pháp:
    from delta.tables import DeltaTable
    DeltaTable.forPath(spark, silver_path).restoreToVersion(1)

  Hoặc bằng SQL:
    spark.sql(f"RESTORE TABLE delta.`{silver_path}` TO VERSION AS OF 1")
    spark.sql(f"RESTORE TABLE delta.`{silver_path}` TO TIMESTAMP AS OF '2025-10-26'")

  Tại sao quan trọng?
    • Không cần rerun Bronze ingestion (tốn giờ)
    • Không cần xử lý lại toàn bộ raw data
    • RESTORE chỉ mất vài giây — Delta cập nhật metadata, không copy data
""")

    # ── 5. Tóm tắt ───────────────────────────────────────────────
    section("TỔNG KẾT")
    print("""
  Time Travel hoạt động TỰ ĐỘNG trên mọi Delta table (Bronze/Silver/Gold)
  Không cần implement thêm gì — chỉ cần dùng Delta Lake format.

  ┌──────────┬──────────────────────────────────────────────────────┐
  │ Layer    │ Use case Time Travel                                 │
  ├──────────┼──────────────────────────────────────────────────────┤
  │ Bronze   │ Audit: raw data trông như thế nào tại thời điểm X   │
  │ Silver   │ Restore sau DQ rules thay đổi hoặc pipeline lỗi     │
  │ Gold     │ Audit KPI thay đổi theo thời gian                   │
  └──────────┴──────────────────────────────────────────────────────┘
""")

    spark.stop()


if __name__ == "__main__":
    main()
