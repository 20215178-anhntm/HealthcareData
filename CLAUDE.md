# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**bigdata-integration** is a healthcare data platform implementing a medallion architecture (Bronze → Silver → Gold) using Apache Spark, MinIO (S3-compatible storage), Kafka, MongoDB, and DuckDB. The pipeline processes four healthcare datasets (patients, insurance, diabetes, no-shows) through multi-stage batch transformations plus a real-time streaming path.

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Data Processing | Apache Spark 3.5.1 (PySpark) |
| Object Storage | MinIO (S3-compatible, S3A protocol) |
| Stream Processing | Kafka 7.3.0 (3-broker cluster) |
| Database | MongoDB 6.0 (replica set) |
| Analytics DB | DuckDB 1.4.1 |
| Visualization | Apache Superset |

**Python dependencies:** `pandas`, `numpy`, `pyspark==3.5.1`, `delta-spark==3.1.0`, `kafka-python`, `minio`, `python-dotenv`, `pymongo`, `pyarrow`, `boto3`, `duckdb==1.4.1`

## Development Commands

### Infrastructure

```bash
# Start all services (MinIO, Kafka, MongoDB, Spark, Superset)
cd docker-compose && ./run.sh

# Stop all services
cd docker-compose && ./end.sh
```

### Batch Pipeline

```bash
# Full pipeline: raw CSVs → Bronze → Silver → Gold
python -m src.jobs.batch_etl --mode all

# Individual stages (supports --since YYYY-MM-DD for incremental bronze)
python -m src.jobs.batch_etl --mode bronze --since 2025-10-26
python -m src.jobs.batch_etl --mode silver
python -m src.jobs.batch_etl --mode gold
```

### Streaming Pipeline

```bash
# Terminal 1: produce mock healthcare events to Kafka
python src/streaming/kafka_producer.py

# Terminal 2: Spark Structured Streaming job (reads Kafka → MongoDB)
# Can run directly (uses embedded SparkSession) or via spark-submit:
python src/streaming/stream_processor.py

# spark-submit with explicit packages:
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  src/streaming/stream_processor.py
```

### Inspect Data

```bash
python src/tests/read_data_simple.py bronze
python src/tests/read_data_simple.py silver
python src/tests/read_data_simple.py gold
```

### Connectivity Tests

```bash
python -m src.tests.test_spark_minio
python -m src.tests.test_connect_mongo
```

### DuckDB / Analytics

```bash
# Build DuckDB from Silver layer (used by Superset)
python src/scripts/build_duckdb.py
# Superset: http://localhost:8088 — connection URI: duckdb:////app/data/silver.duckdb
```

### Delta Lake Time Travel (demo)

```bash
# Xem lịch sử thay đổi + so sánh phiên bản của Bronze/Silver/Gold
python -m src.scripts.demo_time_travel
```

## Architecture

### Data Flow

```
Raw CSVs (MinIO: raw-healthcare bucket)
  ↓  CsvIngestor — manifest-based dedup, metadata injection
Bronze (MinIO: bronze bucket, Parquet, partitioned by dataset/load_date)
  ├─ patients (55.5K rows)
  ├─ insurance (1.3K rows)
  ├─ diabetes  (2.8K rows, zeros = missing values)
  └─ noshows   (107K rows)
  ↓  BronzeToSilver — schema normalization, left-join on age/gender
Silver (MinIO: silver bucket, healthcare_unified table)
  ↓  SilverToGold — gender-level KPI aggregation
Gold (MinIO: gold bucket, kpi_cost_by_gender)

Kafka topic (healthcare_events)
  ↓  StreamProcessor (Spark Structured Streaming)
MongoDB (healthcare.patient_stream)
```

### Key Modules

**`src/jobs/batch_etl.py`** — CLI entry point; `--mode [all|bronze|silver|gold]`, `--since YYYY-MM-DD`

**`src/ingestion/csv_ingestor.py`** — Discovers CSVs in S3 by pattern, tracks processed files via manifest at `_manifests/ingested.csv` in the bucket (prevents re-processing). To force re-ingest, delete the manifest key.

**`src/processing/bronze_to_silver.py`** — Reads Bronze partitions, normalizes column names (snake_case, handles upstream typos like `Hipertension`, `Handcap`), creates `name_norm` column, left-joins all four datasets on age/gender keys.

**`src/processing/silver_to_gold.py`** — Aggregates Silver by gender: `avg_insurance_charges`, `avg_billing_amount` → `kpi_cost_by_gender`.

**`src/common/spark_factory.py`** — Centralized `SparkSession` builder with S3A/MinIO config pre-loaded (path-style access, no SSL, Hadoop AWS 3.3.4).

**`src/storage/s3_client.py`** — Boto3 wrapper with 3-attempt exponential backoff retry decorator.

**`src/streaming/stream_processor.py`** — Spark Structured Streaming: reads Kafka JSON → parses schema → writes to MongoDB via `WriterDB`. Uses local `./checkpoints/` (not MinIO) to avoid S3A issues on Windows.

**`src/common/config.py`** — `Settings` frozen dataclass; all values read from `.env` via `python-dotenv`.

## Configuration (.env)

```
MINIO_ENDPOINT=http://127.0.0.1:9000
MINIO_ACCESS_KEY=root_admin
MINIO_SECRET_KEY=root_admin

S3_BUCKET_RAW=raw-healthcare
S3_BUCKET_BRONZE=bronze
S3_BUCKET_SILVER=silver
S3_BUCKET_GOLD=gold
S3_BUCKET_CHECKPOINT=bronze
S3_PREFIX_CHECKPOINT=spark-checkpoints/healthcare

KAFKA_TOPIC=healthcare_events
KAFKA_BOOTSTRAP=127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092

MONGO_URI=mongodb://admin:admin@127.0.0.1:27018/?directConnection=true&authSource=admin
MONGO_DB=healthcare
MONGO_COLLECTION=patient_stream

SPARK_MASTER_URL=local[*]
SPARK_SHUFFLE_PARTITIONS=4
```

## Docker Compose Services

All services share a `bigdata` network. Compose files: `mongodb.yaml`, `minio.yaml`, `kafka_cluster.yaml`, `spark.yaml`, `superset.yaml`.

| Service | Ports | Purpose |
|---------|-------|---------|
| MinIO | 9000 (API), 9001 (UI) | S3-compatible object storage |
| Kafka (3 brokers) | 19092, 29092, 39092 | Event streaming |
| Zookeeper | 2181 | Kafka coordination |
| Schema Registry | 8084 | Kafka schema management |
| Kafka UI | 8090 | Kafka admin dashboard |
| MongoDB | 27018 | Real-time data sink, replica set |
| Spark Master | 8080 (UI), 7077 (RPC) | Distributed processing |
| Spark Workers | 8081, 8082 | Executors |
| Superset | 8088 | BI dashboard |
| Postgres | 5432 | Superset metadata |

## Important Patterns

**Manifest dedup:** `CsvIngestor` writes `_manifests/ingested.csv` to S3; files are identified by S3 key path (not content hash). Delete the manifest to force re-ingestion.

**Zeros as nulls:** The diabetes dataset uses `0` as a sentinel for missing `Insulin`, `BloodPressure`, `Glucose` values — these are not flagged as missing in current code.

**S3A for MinIO:** Requires `path.style.access=true`, `connection.ssl.enabled=false`, and Hadoop AWS SDK 3.3.4 (must match `spark.jars.packages`).

**Streaming checkpoints on Windows:** `stream_processor.py` uses local `./checkpoints/` instead of MinIO to avoid S3A `NoSuchBucket` errors on Windows.

**No unit tests:** Validation is done via CLI scripts in `src/tests/`. No pytest suite exists.

## Known TODOs

- No raw zone: files go directly to Bronze with no immutable replay copy.
- Column names with spaces (e.g., `Blood Type`, `Date of Admission`) cause SQL issues; no full snake_case normalization yet.
- Zero-as-null in diabetes not flagged — no `*_is_missing` columns.
- No quarantine zone for invalid rows (negative ages, bad dates).
- `dropDuplicates()` on all columns rather than per-dataset natural keys.
