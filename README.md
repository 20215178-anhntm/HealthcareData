# Healthcare CDP — Data Lakehouse

## Kiến trúc CDP (Customer Data Platform)

Dự án mô phỏng một **CDP trong lĩnh vực y tế** theo kiến trúc Medallion (Bronze → Silver → Gold):

| Khái niệm CDP | Trong dự án này |
|---|---|
| **Customer** | Bệnh nhân (patients) |
| **Channel 1 — Hospital** | `patients.csv` — hồ sơ điều trị, chi phí nhập viện |
| **Channel 2 — Insurance** | `insurance.csv` — thông tin bảo hiểm, BMI, phí bảo hiểm |
| **Channel 3 — Lab Results** | `diabetes.csv` — kết quả xét nghiệm glucose, insulin |
| **Channel 4 — Appointment** | `noshows.csv` — lịch sử đặt khám, tỷ lệ vắng mặt |
| **Unified Profile** | Silver layer — 1 row/bệnh nhân, kết hợp đủ 4 channels |
| **KPI / Insights** | Gold layer — chi phí trung bình theo giới tính |

### Luồng dữ liệu

```
Raw CSVs (MinIO raw-healthcare)
    ↓  [Bronze Ingestion + DQ Rules]
Bronze Delta Tables (patients/ insurance/ diabetes/ noshows/)
    ↓  [Aggregate by age/gender → Left Join]
Silver Delta Table  (~55K rows, 1 row per patient)
    ↓  [Group by gender → avg cost KPI]
Gold Delta Table    (2 rows: Female / Male)
```

### Tại sao join bằng age + gender?

Các dataset không có `patient_id` chung, nên không thể join trực tiếp 1-1. Thay vào đó:
- Insurance và Noshows được **aggregate theo age+gender** trước → mỗi nhóm tuổi/giới ra 1 row thống kê
- Diabetes được **aggregate theo age** → mỗi độ tuổi ra 1 row thống kê
- Bệnh nhân nhận thông tin **trung bình của nhóm** thay vì thông tin cá nhân

Cách này tránh cartesian explosion (55K × 1.3K = 71M rows) và giữ Silver đúng kích thước ~55K rows.

---

# Structure

bigdata-healthcare/
├─ docker-compose/
│ ├─ kafka_cluster.yaml
│ ├─ minio.yaml
│ ├─ mongodb.yaml
│ ├─ spark.yaml
│ ├─ superset.yaml
│ ├─ mongo-keyfile
│ ├─ requirements.txt # for spark image (PySpark driver & executors)
│ ├─ run.sh # script to start all services
│ ├─ spark-apps/ # mount source code into Spark container
│ └─ spark-data/ # checkpoint, warehouse (optional)
│
├─ dataset/
│ ├─ raw/
│ │ ├─ healthcare_patients.csv
│ │ ├─ healthcare_insurance.csv
│ │ └─ healthcare_diabetes.csv
│ ├─ bronze/
│ ├─ silver/
│ └─ gold/
│
├─ src/ # Python/PySpark source code as package
│ ├─ **init**.py
│ ├─ common/
│ │ ├─ **init**.py
│ │ ├─ config.py # read ENV, central settings
│ │ └─ spark_factory.py # create SparkSession for S3 (MinIO), Kafka
│ ├─ storage/
│ │ ├─ **init**.py
│ │ └─ s3_client.py # MinIO manipulation helper (boto3 or MC CLI)
│ ├─ validation/
│ │ ├─ **init**.py
│ │ └─ schema.py # define & test schema, casting
│ ├─ ingestion/
│ │ ├─ **init**.py
│ │ └─ csv_ingestor.py # read CSV → bronze (parquet)
│ ├─ processing/
│ │ ├─ **init**.py
│ │ ├─ bronze_to_silver.py # normalize, join 4 sources
│ │ └─ silver_to_gold.py # aggregate KPI for dashboard
│ ├─ streaming/
│ │ ├─ **init**.py
│ │ └─ kafka_to_mongo.py # Structured Streaming: Kafka → Spark → Mongo
│ └─ jobs/
│ ├─ **init**.py
│ ├─ batch_etl.py # entrypoint runs full pipeline raw→gold
│ └─ stream_orders.py # entrypoint job streaming
│
├─ .env # environment variables (MINIO, MONGO, KAFKA…)
├─ requirements.txt # for dev local / notebook
└─ README.md

# Run

## Xóa manifest để bắt đầu lại từ đầu (hoặc có thể giữ manifest nếu muốn skip files đã processed)

## Chạy lại bronze ingestion

```
python -m src.jobs.batch_etl --mode bronze --since 2025-10-26
```

## Sau đó chạy silver và gold

```
python -m src.jobs.batch_etl --mode silver
```

```
python -m src.jobs.batch_etl --mode gold
```

## Chạy toàn bộ (sau khi clean)

```
python -m src.jobs.batch_etl --mode all --since 2025-10-26
```

## Kiểm tra output Bronze, Silver, Gold

```bash
    python src/tests/read_data_simple.py bronze
    python src/tests/read_data_simple.py silver
    python src/tests/read_data_simple.py gold
```

## Kiểm tra Kafka Topic

```bash
# 1. Xem danh sách topic
kafka-topics --list --bootstrap-server localhost:9092

# 2. Mô tả topic
kafka-topics --describe --topic healthcare_events --bootstrap-server localhost:9092

# 3. Đọc dữ liệu (5 bản ghi đầu)
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic healthcare_events \
  --from-beginning \
  --max-messages 5
```

## Run Streaming

```bash
# 1. Producer mock data to Kafka topic
python src/streaming/kafka_producer.py

# 2. Run spark submit
spark-submit \
  --packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  src/streaming/stream_processor.py
```
