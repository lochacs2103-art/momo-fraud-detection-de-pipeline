# MoMo Fraud Detection — Data Engineering Pipeline

Enterprise-grade batch data pipeline cho bài toán fraud detection tại fintech.

## Tech Stack

| Layer | Tool |
|---|---|
| Source DB | PostgreSQL 14 |
| Processing | Apache Spark 3.5 (PySpark) |
| Storage | HDFS (Hadoop 3.2) + Tiered SSD/HDD |
| Catalog | Apache Hive Metastore 3.1 |
| Query Engine | Trino 435 |
| Transformation | dbt-trino 1.7 |
| Orchestration | Apache Airflow 2.8 |
| Visualization | Apache Superset 3.1 |
| Containerization | Docker Compose |

## Architecture

```
CSV/JSON → PostgreSQL (source-db)
               ↓ Spark JDBC parallel read
           HDFS Raw Layer     [SSD - HOT]
               ↓ Spark cleaning + enrichment
           HDFS Staging Layer [SSD - HOT]
               ↓ Spark + dbt
           HDFS Warehouse     [SSD - HOT]
               ↓ Tiering jobs
           Warm (8-90d) / Cold (91d+) [HDD]
               ↓
           Trino → Superset Dashboards
```

## Key Features

- **JDBC Parallel Read** — 8 concurrent connections cho transactions table
- **Tiered Storage** — HDFS Storage Policy: SSD cho hot data, HDD cho warm/cold
- **Amount Parser** — xử lý multi-locale formats ($1,234.56 / 1.234,56 / ₫200,000), quarantine ambiguous records
- **SCD Type 2** — track lịch sử thay đổi user info
- **Data Cleaning** — online transaction detection, error flag explosion, credit score bands, age groups, account age
- **Airflow DAGs** — @daily catchup, max_active_runs=4, MSCK REPAIR sau mỗi ingest
- **Spark Optimization** — AQE, broadcast join, dynamic partition overwrite, Pandas UDF, memory tuning

## Quick Start

```bash
# 1. Download PostgreSQL JDBC driver
make download-jars

# 2. Start toàn bộ stack
make up

# 3. Setup HDFS + Storage Policies (SSD/HDD)
make hdfs-init

# 4. Tạo Hive external tables
make hive-init

# 5. Chạy ingestion
make ingest
```

## Services sau khi chạy `make up`

| Service | URL | Credentials |
|---|---|---|
| HDFS Web UI | http://localhost:9870 | — |
| Spark Web UI | http://localhost:8081 | — |
| Trino UI | http://localhost:8082 | — |
| Airflow UI | http://localhost:8083 | admin/admin |
| Superset UI | http://localhost:8088 | admin/admin |
| Source DB | localhost:5432/momo_source | momo/momo |

## Docs

- `PROJECT_SUMMARY.md` — tổng hợp toàn bộ architecture, design decisions, file structure
- `docs/pipeline_explained.html` — visual guide, mở bằng browser
- `docs/spark_memory_guide.md` — Spark memory layout chi tiết
- `myReadme.md` — engineering decisions log
