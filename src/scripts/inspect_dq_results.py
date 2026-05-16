"""
Kiểm tra dữ liệu thật trong Bronze + Quarantine trên MinIO.
Tìm bằng chứng schema enforcement (null values) và DQ rejections.

Chạy:
    python -m src.scripts.inspect_dq_results
"""

from __future__ import annotations
from pyspark.sql import functions as F
from src.common import SparkFactory
from src.common.config import SETTINGS

SEP = "=" * 65

def section(title: str) -> None:
    print(f"\n{SEP}\n  {title}\n{SEP}")

def main() -> None:
    print("\nKhởi động Spark...\n")
    spark = SparkFactory.get("inspect_dq")
    spark.sparkContext.setLogLevel("ERROR")

    datasets = ["patients", "insurance", "diabetes", "noshows"]

    # ── 1. Quarantine ─────────────────────────────────────────────
    section("QUARANTINE  —  Rows bị DQ reject (dữ liệu thật)")

    any_quarantine = False
    for ds in datasets:
        path = f"s3a://{SETTINGS.bucket_bronze}/quarantine/{ds}/"
        try:
            df = spark.read.format("delta").load(path)
            count = df.count()
            if count == 0:
                print(f"\n  [{ds}]  quarantine trống")
                continue
            any_quarantine = True
            print(f"\n  [{ds}]  {count} rows bị quarantine")
            print()
            # Hiển thị breakdown theo rule
            df.groupBy("dq_failed_rule").count() \
              .orderBy(F.desc("count")) \
              .show(truncate=False)
            # Hiển thị vài rows mẫu
            print(f"  Mẫu 5 rows từ quarantine/{ds}/:")
            cols = [c for c in df.columns
                    if c not in ("dq_timestamp", "_source_file", "source_file", "channel")]
            df.select(cols[:6] + ["dq_failed_rule"]).show(5, truncate=False)
        except Exception as e:
            if "Path does not exist" in str(e) or "is not a Delta table" in str(e):
                print(f"\n  [{ds}]  quarantine chưa có (chưa chạy ingestion lần nào)")
            else:
                print(f"\n  [{ds}]  lỗi đọc quarantine: {e}")

    if not any_quarantine:
        print("\n  ⚠  Chưa có quarantine data nào.")
        print("     Cần chạy: python -m src.jobs.batch_etl --mode bronze")

    # ── 2. Bronze — null values (schema enforcement) ──────────────
    section("BRONZE  —  Null values từ Schema Enforcement")

    # Cột typed (không phải string) theo dataset
    typed_cols = {
        "patients": ["age", "billing_amount", "room_number",
                     "date_of_admission", "discharge_date"],
        "insurance": ["age", "bmi", "children", "charges"],
        "diabetes":  ["age", "glucose", "blood_pressure", "bmi", "insulin"],
        "noshows":   ["age", "patientid"],
    }

    any_nulls = False
    for ds in datasets:
        path = f"s3a://{SETTINGS.bucket_bronze}/{ds}/"
        try:
            df = spark.read.format("delta").load(path)
            total = df.count()
            cols_to_check = [c for c in typed_cols.get(ds, []) if c in df.columns]
            if not cols_to_check:
                print(f"\n  [{ds}]  không tìm thấy cột typed để kiểm tra")
                continue

            # Đếm null trên từng cột typed
            null_counts = df.select(
                [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in cols_to_check]
            ).collect()[0].asDict()

            has_null = any(v > 0 for v in null_counts.values())
            print(f"\n  [{ds}]  tổng {total} rows")
            for col_name, cnt in null_counts.items():
                flag = "  ← có null (schema enforcement!)" if cnt > 0 else ""
                print(f"    {col_name:<25}  null={cnt}{flag}")

            if has_null:
                any_nulls = True
                # Hiển thị vài rows có null
                null_filter = " OR ".join([f"`{c}` IS NULL" for c in cols_to_check
                                           if null_counts[c] > 0])
                print(f"\n  Mẫu rows có null trong [{ds}]:")
                df.filter(null_filter).select(cols_to_check[:5]).show(5, truncate=False)

        except Exception as e:
            if "Path does not exist" in str(e) or "is not a Delta table" in str(e):
                print(f"\n  [{ds}]  Bronze chưa có (chưa chạy ingestion)")
            else:
                print(f"\n  [{ds}]  lỗi: {e}")

    if not any_nulls:
        print("\n  Không tìm thấy null trong các cột typed.")
        print("  → Có thể dữ liệu nguồn đã sạch hoàn toàn về kiểu dữ liệu.")

    # ── 3. Tóm tắt ────────────────────────────────────────────────
    section("TÓM TẮT")
    print("""
  Để dùng kết quả này cho slide:

  • Quarantine rows  →  chứng minh DQ Business Rules hoạt động
  • Null trong cột typed  →  chứng minh Schema Enforcement hoạt động
  • Nếu cả 2 đều trống → dữ liệu nguồn đã sạch, cần inject data xấu để demo
""")

    spark.stop()


if __name__ == "__main__":
    main()
